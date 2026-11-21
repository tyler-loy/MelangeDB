using System.Buffers.Binary;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Core;

/// <summary>
/// The commit log's segments: one file per ownership term (phase 30 step 2b, the storage fence).
/// An owner under term <c>t</c> appends only to its own segment. Opening under a higher term
/// <em>seals</em> the previous owner's segment at its last valid record, the <b>cut</b>, and
/// records the cut in <see cref="FileName"/>. Nothing past a cut is ever read again, so a previous
/// owner that was paused rather than dead, and resumes writing, writes where nobody looks; and the
/// acknowledgement re-check (step 2a) means none of those writes was acknowledged. This fences two
/// writers on any shared filesystem, whatever it does with <c>melange.lock</c>.
/// <para>
/// A directory with no manifest is one segment, <c>melange.log</c>, at term 0: every log that
/// predates segments, and every non-cluster engine for good. Migration renames nothing: the first
/// open under a higher term seals <c>melange.log</c> where it stands. Sealed segments are never
/// written by anyone who did not write them; truncation deletes them whole once they fall below
/// the base.
/// </para>
/// </summary>
internal static class LogSegments
{
    /// <summary>The manifest's file name, beside the log.</summary>
    public const string FileName = "melange.segments";

    /// <summary>The segment file a pre-segment log (and every term-0 log) appends to.</summary>
    public const string LegacyFileName = "melange.log";

    /// <summary>A sealed segment: the term that wrote it, its file, and the last LSN that belongs to the log.</summary>
    public sealed record Sealed(long Term, string File, ulong Cut);

    /// <summary>The live segment: the term appending to it, and its file.</summary>
    public sealed record Active(long Term, string File);

    /// <summary>The manifest: sealed segments in LSN order, then the live one.</summary>
    public sealed record Manifest(IReadOnlyList<Sealed> SealedSegments, Active ActiveSegment)
    {
        /// <summary>The highest cut, or 0 when nothing is sealed: where the live segment's LSNs begin after.</summary>
        public ulong LastCut => SealedSegments.Count == 0 ? 0 : SealedSegments[^1].Cut;

        /// <summary>Whether a manifest exists to say so; a directory without one is a single legacy segment.</summary>
        public bool Implicit { get; init; }
    }

    private sealed record Document(int Version, List<Sealed> Sealed, Active Active);

    /// <summary>The segment file for <paramref name="term"/>: the legacy name at term 0, so an unclustered log keeps its file.</summary>
    public static string SegmentFileName(long term) => term == 0 ? LegacyFileName : $"melange.log.{term}";

    /// <summary>Whether <paramref name="logDirectory"/> holds a commit log in either layout.</summary>
    public static bool IsLogDirectory(string logDirectory) =>
        File.Exists(Path.Combine(logDirectory, LegacyFileName)) || File.Exists(Path.Combine(logDirectory, FileName));

    /// <summary>The directory's manifest, or the implicit single legacy segment when it has none.</summary>
    public static Manifest Load(string logDirectory)
    {
        var path = Path.Combine(logDirectory, FileName);
        if (!File.Exists(path))
            return new Manifest([], new Active(0, LegacyFileName)) { Implicit = true };
        var document = JsonSerializer.Deserialize<Document>(File.ReadAllBytes(path))
            ?? throw new InvalidDataException($"'{path}' is empty.");
        if (document.Version != 1)
            throw new InvalidDataException($"'{path}' has segment manifest version {document.Version}; this build reads version 1.");
        return new Manifest(document.Sealed, document.Active);
    }

    /// <summary>Writes the manifest atomically and durably: a crash leaves the old one or the new one.</summary>
    public static void Save(string logDirectory, Manifest manifest) =>
        DurableFile.WriteAllBytes(
            Path.Combine(logDirectory, FileName),
            JsonSerializer.SerializeToUtf8Bytes(new Document(1, [.. manifest.SealedSegments], manifest.ActiveSegment)));

    /// <summary>
    /// Opens the directory's segments for an owner under <paramref name="term"/> (null adopts the
    /// directory's current term). Under a higher term the live segment is sealed and a new one
    /// begins; under a lower term the open is refused, because a lower term is a previous owner.
    /// Called holding <c>melange.lock</c>, before the live segment is opened.
    /// </summary>
    /// <param name="durableFloor">The newest snapshot's LSN: a cut below it would drop history the snapshot proves was durable.</param>
    public static Manifest OpenForTerm(string logDirectory, long? term, ulong durableFloor, ulong baseLsn, ILogger logger)
    {
        var manifest = Load(logDirectory);
        var current = manifest.ActiveSegment.Term;
        if (term is not { } requested || requested == current)
            return manifest;
        if (requested < current)
        {
            throw new StaleTermException(
                $"'{logDirectory}' is at ownership term {current}, and this owner holds term {requested}: another node " +
                "has owned the shard since. A previous owner never writes again; reopen under the current fencing token.");
        }

        var activePath = Path.Combine(logDirectory, manifest.ActiveSegment.File);
        var sealedSegments = manifest.SealedSegments.ToList();
        if (File.Exists(activePath))
        {
            var cut = CutAt(activePath, Math.Max(manifest.LastCut, baseLsn), durableFloor);
            sealedSegments.Add(new Sealed(current, manifest.ActiveSegment.File, cut));
            LogMessages.SegmentSealed(logger, logDirectory, current, cut, requested);
        }

        var next = new Manifest(sealedSegments, new Active(requested, SegmentFileName(requested)));
        Save(logDirectory, next);
        return next;
    }

    /// <summary>
    /// Seals a segment: makes what it holds durable, and returns the last LSN of its intact prefix.
    /// Reads like recovery and judges like recovery, but never writes the file: a torn tail is
    /// where the cut goes, not something to truncate, because the file may still have a writer.
    /// </summary>
    private static ulong CutAt(string path, ulong floorBelow, ulong durableFloor)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);

        // Whatever the previous owner's process wrote may still sit in an OS cache on this host;
        // the cut is about to make it history, so it has to reach the disk first. An fsync writes
        // nothing of ours.
        stream.Flush(flushToDisk: true);
        var cut = floorBelow;
        foreach (var record in LogFileFormat.WalkRecords(stream, path))
            cut = Math.Max(cut, record.Lsn);
        if (cut < durableFloor)
        {
            throw new InvalidDataException(
                $"'{path}' ends at LSN {cut}, but the snapshot at LSN {durableFloor} proves records up to it were durable. " +
                "The log is missing committed history; restore from backup.");
        }

        return cut;
    }

    /// <summary>
    /// Every intact record of a log directory, in LSN order, across its segments: each sealed one
    /// up to its cut, then the live one to its end. Read-only and lock-free, for backup capture
    /// (and, in step 2c, a follower). Handles are opened up front, so the walk is of one moment's
    /// files; a segment deleted by a truncation between the manifest read and the open surfaces as
    /// <see cref="InvalidOperationException"/>, which the capture paths retry.
    /// </summary>
    public sealed class Reader : IDisposable
    {
        private readonly List<(FileStream Stream, string Path, ulong? Cut)> _segments = [];

        public Reader(string logDirectory)
        {
            var manifest = Load(logDirectory);
            try
            {
                foreach (var segment in manifest.SealedSegments)
                    _segments.Add((OpenShared(Path.Combine(logDirectory, segment.File)), Path.Combine(logDirectory, segment.File), segment.Cut));
                var active = Path.Combine(logDirectory, manifest.ActiveSegment.File);
                if (File.Exists(active) || !manifest.Implicit)
                    _segments.Add((OpenShared(active), active, null));
            }
            catch (FileNotFoundException exception)
            {
                Dispose();
                throw new InvalidOperationException($"'{logDirectory}': a log segment disappeared while it was opened; a truncation raced this read.", exception);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>Walks the records; may be called more than once over the same handles.</summary>
        public IEnumerable<LogFileFormat.RawRecord> Walk()
        {
            foreach (var (stream, path, cut) in _segments)
            {
                foreach (var record in LogFileFormat.WalkRecords(stream, path))
                {
                    if (cut is { } last && record.Lsn > last)
                        break;
                    yield return record;
                }
            }
        }

        public void Dispose()
        {
            foreach (var (stream, _, _) in _segments)
                stream.Dispose();
            _segments.Clear();
        }

        private static FileStream OpenShared(string path) =>
            new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, long, ulong, long, Exception?> SegmentSealedMessage =
            LoggerMessage.Define<string, long, ulong, long>(
                LogLevel.Information,
                new EventId(1011, "LogSegmentSealed"),
                "Sealed '{LogDirectory}' term {Term}'s log segment at LSN {Cut}: term {NewTerm} owns the log now, and nothing written to that segment past the cut is history.");

        public static void SegmentSealed(ILogger logger, string directory, long term, ulong cut, long newTerm) =>
            SegmentSealedMessage(logger, directory, term, cut, newTerm, null);
    }
}

/// <summary>
/// A log opened under an ownership term lower than the one its directory is already at: a previous
/// owner, which must never write again. Not transient for that owner; the shard's current owner
/// holds the current term.
/// </summary>
public sealed class StaleTermException(string message) : InvalidOperationException(message);
