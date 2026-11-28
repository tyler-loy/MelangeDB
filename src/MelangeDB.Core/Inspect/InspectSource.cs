using System.Buffers.Binary;
using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>What an <see cref="InspectSource"/> reads: a data directory, or a <c>.mbak</c> archive.</summary>
public enum InspectSourceKind
{
    /// <summary>An engine's log directory, the one <c>CommitLog:Path</c> names (or a shard's).</summary>
    DataDirectory = 1,

    /// <summary>One engine of a <c>.mbak</c> backup archive.</summary>
    Archive = 2,
}

/// <summary>
/// The facts about a source that an investigation fails at the edges of: whose history it is
/// (epoch, lineage, provenance), which LSNs it can show (<see cref="LogFloor"/> to
/// <see cref="HeadLsn"/>), and where a state can be rebuilt from (<see cref="SnapshotLsn"/>).
/// </summary>
public sealed record InspectFacts
{
    public required InspectSourceKind Kind { get; init; }

    /// <summary>The full path that was opened: the log directory, or the archive file.</summary>
    public required string Path { get; init; }

    /// <summary>The archive engine being read (<c>node</c>, <c>hub</c>, <c>shard-k</c>); null for a directory.</summary>
    public string? EngineKey { get; init; }

    /// <summary>Every engine an archive holds; empty for a directory.</summary>
    public IReadOnlyList<string> Engines { get; init; } = [];

    /// <summary>
    /// The epoch the LSNs belong to: an LSN means something only with it. For an archive, the
    /// epoch of the world it was captured from. <see cref="Guid.Empty"/> when none is recorded.
    /// </summary>
    public required Guid Epoch { get; init; }

    /// <summary>The epochs this log descends from, oldest first, each with its fork point; empty when it was never restored.</summary>
    public required IReadOnlyList<EpochAncestor> Lineage { get; init; }

    /// <summary>What a cloned world is a clone of; null when it is not a clone (and always for an archive).</summary>
    public CloneProvenance? Provenance { get; init; }

    /// <summary>The truncation base: records at or below it are gone from the log.</summary>
    public required ulong BaseLsn { get; init; }

    /// <summary>The LSN the snapshot covers, or 0 when there is no snapshot this source can use.</summary>
    public required ulong SnapshotLsn { get; init; }

    /// <summary>The snapshot's tail-record timestamp; null without a snapshot.</summary>
    public Timestamp? SnapshotTimestamp { get; init; }

    /// <summary>A snapshot file exists but belongs to another epoch, so recovery (and this reader) ignore it.</summary>
    public bool SnapshotIgnored { get; init; }

    /// <summary>
    /// The highest LSN this source holds no record for: records at or below it can be named by no
    /// one here. The truncation base for a directory; for an archive, its snapshot's LSN, since an
    /// archive carries only the log tail above its snapshot.
    /// </summary>
    public required ulong LogFloor { get; init; }

    /// <summary>The first record's LSN, or 0 when the source holds none.</summary>
    public required ulong FirstLsn { get; init; }

    /// <summary>
    /// The last LSN this read reached: the newest intact record, or the floor or snapshot when there
    /// are no records. A live engine may be ahead of it; see <see cref="InspectSource.CheckForChanges"/>.
    /// </summary>
    public required ulong HeadLsn { get; init; }

    /// <summary>How many records lie between <see cref="FirstLsn"/> and <see cref="HeadLsn"/>.</summary>
    public required long RecordCount { get; init; }

    /// <summary>The ownership term of a directory's live segment (0 for a log that never changed owner).</summary>
    public long Term { get; init; }

    /// <summary>How many sealed segments a directory's log reads through before its live one.</summary>
    public int SealedSegments { get; init; }

    /// <summary>
    /// Bytes after the head in the live segment that are not an intact record: a crash's torn tail,
    /// or, in a live directory, an append still being written. Never history.
    /// </summary>
    public long TornTailBytes { get; init; }

    /// <summary>Places where the LSNs jump: each pair is the LSN before the jump and the one after it.</summary>
    public IReadOnlyList<(ulong After, ulong Next)> Gaps { get; init; } = [];

    /// <summary>Whether the source records its row shapes (<c>melange.shape</c>); without it rows show only as bytes.</summary>
    public bool ShapeRecorded { get; init; }

    /// <summary>When an archive was captured; null for a directory.</summary>
    public DateTimeOffset? CapturedAt { get; init; }
}

/// <summary>What moved under a directory source after it was opened; see <see cref="InspectSource.CheckForChanges"/>.</summary>
/// <param name="OwnerChanged">The segment manifest no longer matches the one read at open: another node took the shard and sealed the live segment.</param>
/// <param name="Appended">The live segment grew past what was read: the owner is still appending, and is ahead of <see cref="InspectFacts.HeadLsn"/>.</param>
/// <param name="Truncated">The truncation base moved: records this read could show may be gone from the directory now.</param>
public sealed record InspectChanges(bool OwnerChanged, bool Appended, bool Truncated)
{
    public static InspectChanges None { get; } = new(false, false, false);

    public bool Any => OwnerChanged || Appended || Truncated;
}

/// <summary>
/// A read-only view of one engine's history: a data directory or one engine of a <c>.mbak</c>
/// archive (road-to-0.3 phase 21). It reads the commit log's records by LSN, the snapshot's rows,
/// and the shape sidecar that says what their bytes mean, and decodes rows and arguments without
/// the application's assemblies.
/// <para>
/// <b>It never writes and takes no lock.</b> Every file is opened for reading with
/// <see cref="FileShare.ReadWrite"/> and <see cref="FileShare.Delete"/>, so it can read a directory
/// a live engine or another cluster node owns: <see cref="FileCommitLog"/> cannot be the reader,
/// because opening it takes <c>melange.lock</c> and its term fence can seal a segment. The view is
/// of the moment it opened: the segment handles are held until disposal, and records are never
/// shown past the head that first read reached. What changed since is reported by
/// <see cref="CheckForChanges"/>, not silently read.
/// </para>
/// <para>
/// A torn tail is expected, not an error: a live engine's append in progress looks exactly like a
/// crash's. A CRC failure with intact records after it is corruption and refuses the open.
/// </para>
/// <para>Enumerate one of a directory source's record walks at a time: they share its handles.</para>
/// </summary>
public sealed class InspectSource : IDisposable
{
    private const int IndexStride = 1024;
    private const int OpenAttempts = 3;

    private readonly Body _body;
    private readonly Dictionary<ShapeEntry, Dictionary<TableId, InspectTable>> _tables = [];

    private InspectSource(Body body, InspectFacts facts, ShapeHistory? shapes)
    {
        _body = body;
        Facts = facts;
        Shapes = shapes;
    }

    /// <summary>The source's facts, as of the moment it was opened.</summary>
    public InspectFacts Facts { get; }

    /// <summary>The shape history from the source's <c>melange.shape</c>, or null when it has none.</summary>
    public ShapeHistory? Shapes { get; }

    /// <summary>
    /// Opens <paramref name="path"/>: a log directory, a segment file inside one, or a <c>.mbak</c>
    /// archive. <paramref name="engineKey"/> picks an archive's engine; it may be omitted when the
    /// archive holds only one.
    /// </summary>
    /// <exception cref="InvalidOperationException">The path is not something this reader can open, or names no such engine.</exception>
    /// <exception cref="InvalidDataException">The source is corrupt beyond a torn tail.</exception>
    public static InspectSource Open(string path, string? engineKey = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        var full = System.IO.Path.GetFullPath(path);
        if (File.Exists(full))
        {
            var name = System.IO.Path.GetFileName(full);
            if (name == LogSegments.LegacyFileName || name.StartsWith(LogSegments.LegacyFileName + ".", StringComparison.Ordinal))
                return OpenDirectory(System.IO.Path.GetDirectoryName(full)!, engineKey);
            return OpenArchive(full, engineKey);
        }

        if (Directory.Exists(full))
            return OpenDirectory(full, engineKey);
        throw new FileNotFoundException($"'{full}' does not exist.", full);
    }

    /// <summary>
    /// Every record from <paramref name="fromLsn"/> to <paramref name="toLsn"/> inclusive, in LSN
    /// order, never past <see cref="InspectFacts.HeadLsn"/>. Records the source does not hold (at or
    /// below <see cref="InspectFacts.LogFloor"/>) are simply absent; <see cref="ExplainUnreachable"/>
    /// says why.
    /// </summary>
    public IEnumerable<CommitRecord> Records(ulong fromLsn = 0, ulong toLsn = ulong.MaxValue)
    {
        foreach (var raw in _body.Walk(fromLsn, Math.Min(toLsn, Facts.HeadLsn)))
            yield return LogRecordCodec.ReadPayload(raw.Payload, FileCommitLog.FrameSize + raw.Payload.Length);
    }

    /// <summary>The record at <paramref name="lsn"/>, or null when this source does not hold it.</summary>
    public CommitRecord? Record(ulong lsn) => Records(lsn, lsn).FirstOrDefault();

    /// <summary>
    /// The snapshot's rows, as stored: each decodes under the shape governing
    /// <see cref="InspectFacts.SnapshotLsn"/>. Empty when there is no usable snapshot.
    /// </summary>
    /// <exception cref="InvalidOperationException">A live engine replaced the snapshot after the source opened.</exception>
    public IEnumerable<SnapshotRow> SnapshotRows() =>
        Facts.SnapshotLsn == 0 ? [] : _body.SnapshotRows(Facts);

    /// <summary>
    /// Why the record at <paramref name="lsn"/> is not in this source, or null when it is (or would
    /// be, were it written). The message names the floor and the remedy, because "the answer was
    /// truncated away" is the common outcome of exactly the investigations that matter.
    /// </summary>
    public string? ExplainUnreachable(ulong lsn)
    {
        if (lsn > Facts.HeadLsn)
        {
            return Facts.Kind == InspectSourceKind.Archive
                ? $"LSN {lsn} is past this archive's head ({Facts.HeadLsn}): the archive was captured before it was written."
                : $"LSN {lsn} is past the head this read reached ({Facts.HeadLsn}). If an engine is running against this directory it may have written it since; open the directory again.";
        }

        if (lsn == 0 || lsn > Facts.LogFloor)
            return null;
        if (Facts.Kind == InspectSourceKind.Archive)
        {
            return $"LSN {lsn} is at or below this archive's snapshot (LSN {Facts.SnapshotLsn}): an archive carries the snapshot's rows " +
                "and only the log records above it. An older archive, or the source directory while its log still reaches back, holds it.";
        }

        return $"LSN {lsn} has been truncated away: this directory's log begins after LSN {Facts.BaseLsn} (its truncation base). " +
            "Every truncation floor has a name; the runbook \"the log is growing\" in docs/OBSERVABILITY.md says which holder lets the log go and when. " +
            "A backup taken before that truncation still holds the record: inspect the archive instead.";
    }

    /// <summary>
    /// What moved under a directory source since it opened: a new owner sealing the live segment,
    /// an owner appending past the head, a truncation raising the base. Always
    /// <see cref="InspectChanges.None"/> for an archive, which never changes.
    /// </summary>
    public InspectChanges CheckForChanges() => _body.CheckForChanges();

    /// <summary>The table <paramref name="table"/> was under the shape governing <paramref name="lsn"/>, or null when no shape names it.</summary>
    public InspectTable? TableAt(ulong lsn, TableId table) =>
        TablesOf(lsn) is { } tables && tables.TryGetValue(table, out var found) ? found : null;

    /// <summary>Every table the shape governing <paramref name="lsn"/> records, by name; empty without a shape sidecar.</summary>
    public IReadOnlyList<InspectTable> TablesAt(ulong lsn) =>
        TablesOf(lsn) is { } tables ? [.. tables.Values.OrderBy(static t => t.Name, StringComparer.Ordinal)] : [];

    /// <summary>The table named <paramref name="name"/> under the shape governing <paramref name="lsn"/>, or null.</summary>
    public InspectTable? FindTable(string name, ulong lsn) => TableAt(lsn, TableId.FromName(name)) is { } table && table.Name == name ? table : null;

    /// <summary>Decodes a row written at <paramref name="lsn"/> under the shape that governed it then.</summary>
    public DecodedRow DecodeRow(ulong lsn, TableId table, ReadOnlySpan<byte> row) =>
        TableAt(lsn, table) is { } known
            ? RowDecoder.DecodeRow(known.Shape, row)
            : new DecodedRow([], Facts.ShapeRecorded
                ? $"No shape governing LSN {lsn} names table {table}; its bytes cannot be decoded."
                : "This source records no row shapes (no melange.shape), so its rows show only as bytes.");

    /// <summary>Decodes a primary key written at <paramref name="lsn"/>.</summary>
    public DecodedKey DecodeKey(ulong lsn, TableId table, in RowKey key) =>
        TableAt(lsn, table) is { } known
            ? RowDecoder.DecodeKey(known.Shape, key)
            : new DecodedKey(null, null, null, $"No shape names table {table}; the key shows only as bytes.");

    public void Dispose() => _body.Dispose();

    private Dictionary<TableId, InspectTable>? TablesOf(ulong lsn)
    {
        if (Shapes is null)
            return null;
        var entry = Shapes.At(lsn);
        lock (_tables)
        {
            if (!_tables.TryGetValue(entry, out var tables))
            {
                tables = entry.Tables.ToDictionary(
                    static pair => TableId.FromName(pair.Key),
                    static pair => new InspectTable(TableId.FromName(pair.Key), pair.Key, pair.Value));
                _tables[entry] = tables;
            }

            return tables;
        }
    }

    // --- Data directories -------------------------------------------------------------------

    private static InspectSource OpenDirectory(string directory, string? engineKey)
    {
        if (engineKey is not null)
            throw new InvalidOperationException($"'{directory}' is a data directory; an engine is chosen only inside an archive.");
        if (!LogSegments.IsLogDirectory(directory))
            throw new InvalidOperationException(NotALogDirectory(directory));

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return OpenDirectoryOnce(directory);
            }
            catch (SourceMovedException) when (attempt < OpenAttempts)
            {
                // A truncation or a seal raced the open; the next attempt sees the settled files.
            }
            catch (SourceMovedException exception)
            {
                throw new InvalidOperationException(
                    $"'{directory}' kept changing while it was opened ({exception.Message}); a live engine is truncating or a new owner is " +
                    "sealing it faster than it can be read. Back it up (melange backup) and inspect the archive.", exception);
            }
        }
    }

    private static InspectSource OpenDirectoryOnce(string directory)
    {
        var baseLsn = ReadBaseLsn(directory);
        var manifest = LogSegments.Load(directory);
        var body = new DirectoryBody(directory, manifest, baseLsn);
        try
        {
            var walk = body.Index();
            if (ReadBaseLsn(directory) != baseLsn)
                throw new SourceMovedException("the truncation base moved");

            var epoch = ReadEpoch(directory);
            var (snapshotLsn, snapshotTimestamp, ignored) = ReadSnapshotHeader(directory, epoch);
            var shapePath = System.IO.Path.Combine(directory, ShapeHistory.FileName);
            var shapeBytes = ReadShared(shapePath);
            var shapes = shapeBytes is null ? null : ShapeHistory.Parse(shapeBytes, shapePath);
            var facts = new InspectFacts
            {
                Kind = InspectSourceKind.DataDirectory,
                Path = directory,
                Epoch = epoch,
                Lineage = EpochLineage.Load(directory).Ancestors,
                Provenance = CloneProvenance.TryRead(directory),
                BaseLsn = baseLsn,
                SnapshotLsn = snapshotLsn,
                SnapshotTimestamp = snapshotTimestamp,
                SnapshotIgnored = ignored,
                LogFloor = baseLsn,
                FirstLsn = walk.First,
                HeadLsn = walk.Count > 0 ? walk.Head : Math.Max(baseLsn, snapshotLsn),
                RecordCount = walk.Count,
                Term = manifest.ActiveSegment.Term,
                SealedSegments = manifest.SealedSegments.Count,
                TornTailBytes = walk.TornTailBytes,
                Gaps = walk.Gaps,
                ShapeRecorded = shapes is not null,
            };
            return new InspectSource(body, facts, shapes);
        }
        catch
        {
            body.Dispose();
            throw;
        }
    }

    private static string NotALogDirectory(string directory)
    {
        var engines = Directory.Exists(directory)
            ? Directory.EnumerateDirectories(directory)
                .Where(static d => LogSegments.IsLogDirectory(d) || LogSegments.IsLogDirectory(System.IO.Path.Combine(d, "log")))
                .Select(static d => System.IO.Path.GetFileName(d))
                .Order(StringComparer.Ordinal)
                .ToList()
            : [];
        return engines.Count == 0
            ? $"'{directory}' holds no melange.log or melange.segments: it is not a MelangeDB data directory. " +
              "Point at the directory CommitLog:Path names, a shard's directory, or a .mbak archive."
            : $"'{directory}' is not a data directory itself, but it holds engines: {string.Join(", ", engines)}. " +
              "Each engine has its own log and its own LSNs; point at one of them.";
    }

    private static ulong ReadBaseLsn(string directory) =>
        ReadShared(System.IO.Path.Combine(directory, "melange.base")) is { Length: 8 } bytes
            ? BinaryPrimitives.ReadUInt64LittleEndian(bytes)
            : 0;

    private static Guid ReadEpoch(string directory) =>
        ReadShared(System.IO.Path.Combine(directory, FileCommitLog.EpochFileName)) is { Length: 16 } bytes
            ? new Guid(bytes)
            : Guid.Empty;

    private static (ulong Lsn, Timestamp? Timestamp, bool Ignored) ReadSnapshotHeader(string directory, Guid epoch)
    {
        var path = System.IO.Path.Combine(directory, SnapshotFile.FileName);
        if (!File.Exists(path))
            return (0, null, false);
        using var reader = OpenSnapshot(path);
        if (reader is null)
            return (0, null, false);
        return reader.Header.Epoch == epoch
            ? (reader.Header.Lsn, reader.Header.Timestamp, false)
            : (0, null, true);
    }

    private static SnapshotReader? OpenSnapshot(string path)
    {
        FileStream stream;
        try
        {
            stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        }
        catch (FileNotFoundException)
        {
            return null;
        }

        return new SnapshotReader(path, stream);
    }

    private static byte[]? ReadShared(string path)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var bytes = new byte[stream.Length];
            stream.ReadExactly(bytes);
            return bytes;
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    // --- Archives ---------------------------------------------------------------------------

    private static InspectSource OpenArchive(string path, string? engineKey)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        var reader = new ArchiveFrameReader(stream, path);
        reader.ReadHeader();
        var manifest = ArchiveRestore.ReadJsonFrame<ArchiveManifest>(reader, ArchiveFrameType.Manifest, path);
        var key = engineKey ?? (manifest.Engines.Count == 1
            ? manifest.Engines[0]
            : throw new InvalidOperationException(
                $"'{path}' holds {manifest.Engines.Count} engines ({string.Join(", ", manifest.Engines)}), each with its own log and LSNs; name one."));
        if (!manifest.Engines.Contains(key))
            throw new InvalidOperationException($"'{path}' holds no engine '{key}'; it holds {string.Join(", ", manifest.Engines)}.");

        while (true)
        {
            var identity = ArchiveRestore.ReadJsonFrame<ArchiveEngineIdentity>(reader, ArchiveFrameType.EngineBegin, path);
            var snapshotStart = stream.Position;
            var body = new ArchiveBody(path, snapshotStart);
            ShapeHistory? shapes = null;
            long snapshotRows = 0;
            long records = 0;
            var first = 0UL;
            var head = 0UL;
            List<(ulong, ulong)> gaps = [];
            ArchiveEngineFooter? footer = null;
            while (footer is null)
            {
                var offset = stream.Position;
                var frame = reader.ReadFrame() ?? throw new InvalidDataException($"'{path}': engine '{identity.Key}' ends before its end frame; the archive is truncated.");
                switch (frame.Type)
                {
                    case ArchiveFrameType.SnapshotRow:
                        snapshotRows++;
                        break;
                    case ArchiveFrameType.LogRecord when frame.Payload.Length >= 10:
                        var lsn = BinaryPrimitives.ReadUInt64LittleEndian(frame.Payload.AsSpan(2));
                        if (records > 0 && lsn != head + 1)
                            gaps.Add((head, lsn));
                        if (records % IndexStride == 0)
                            body.AddIndex(lsn, offset);
                        if (records == 0)
                            first = lsn;
                        head = lsn;
                        records++;
                        break;
                    case ArchiveFrameType.Sidecar:
                        var (name, content) = ArchiveRestore.ParseSidecarFrame(frame.Payload, path);
                        if (name == ShapeHistory.FileName && identity.Key == key)
                            shapes = ShapeHistory.Parse(content, $"{path} ({identity.Key}: {name})");
                        break;
                    case ArchiveFrameType.EngineEnd:
                        footer = JsonSerializer.Deserialize<ArchiveEngineFooter>(frame.Payload)
                            ?? throw new InvalidDataException($"'{path}': engine '{identity.Key}' has an empty end frame.");
                        break;
                    default:
                        throw new InvalidDataException($"'{path}': frame {reader.FrameIndex} ({frame.Type}) is out of place in engine '{identity.Key}'; the archive is corrupt.");
                }
            }

            if (identity.Key != key)
                continue;
            if (footer.SnapshotRows != snapshotRows || footer.TailRecords != records)
            {
                throw new InvalidDataException(
                    $"'{path}': engine '{key}' declares {footer.SnapshotRows} snapshot rows and {footer.TailRecords} records but holds " +
                    $"{snapshotRows} and {records}; the archive is corrupt.");
            }

            if (records > 0 && first != identity.SnapshotLsn + 1)
                gaps.Insert(0, (identity.SnapshotLsn, first));
            var facts = new InspectFacts
            {
                Kind = InspectSourceKind.Archive,
                Path = path,
                EngineKey = key,
                Engines = manifest.Engines,
                Epoch = identity.SourceEpoch,
                Lineage = identity.Lineage ?? [],
                BaseLsn = identity.BaseLsn,
                SnapshotLsn = identity.SnapshotLsn,
                SnapshotTimestamp = identity.SnapshotLsn == 0 ? null : new Timestamp(identity.SnapshotTimestampMicros),
                LogFloor = identity.SnapshotLsn,
                FirstLsn = first,
                HeadLsn = records > 0 ? head : identity.HeadLsn,
                RecordCount = records,
                Gaps = gaps,
                ShapeRecorded = shapes is not null,
                CapturedAt = DateTimeOffset.FromUnixTimeMilliseconds(manifest.CapturedAtUnixMs),
            };
            return new InspectSource(body, facts, shapes);
        }
    }

    // --- Bodies -----------------------------------------------------------------------------

    private abstract class Body : IDisposable
    {
        public abstract IEnumerable<LogFileFormat.RawRecord> Walk(ulong fromLsn, ulong toLsn);

        public abstract IEnumerable<SnapshotRow> SnapshotRows(InspectFacts facts);

        public virtual InspectChanges CheckForChanges() => InspectChanges.None;

        public virtual void Dispose()
        {
        }

        /// <summary>The index entry to start a walk for <paramref name="fromLsn"/> at: the last one at or below it.</summary>
        protected static int StartEntry<T>(List<T> index, ulong fromLsn, Func<T, ulong> lsnOf)
        {
            int low = 0, high = index.Count - 1, found = -1;
            while (low <= high)
            {
                var middle = (low + high) / 2;
                if (lsnOf(index[middle]) <= fromLsn)
                {
                    found = middle;
                    low = middle + 1;
                }
                else
                {
                    high = middle - 1;
                }
            }

            return found;
        }
    }

    private sealed class DirectoryBody : Body
    {
        private readonly string _directory;
        private readonly LogSegments.Manifest _manifest;
        private readonly ulong _baseLsn;
        private readonly List<(FileStream Stream, string Path, ulong? Cut)> _segments = [];
        private readonly List<(ulong Lsn, int Segment, long Offset)> _index = [];
        private long _liveLengthAtOpen;
        private int _walking;

        public DirectoryBody(string directory, LogSegments.Manifest manifest, ulong baseLsn)
        {
            _directory = directory;
            _manifest = manifest;
            _baseLsn = baseLsn;
            try
            {
                foreach (var segment in manifest.SealedSegments)
                    _segments.Add((OpenShared(System.IO.Path.Combine(directory, segment.File)), segment.File, segment.Cut));
                var active = System.IO.Path.Combine(directory, manifest.ActiveSegment.File);
                if (File.Exists(active))
                    _segments.Add((OpenShared(active), manifest.ActiveSegment.File, null));
            }
            catch (FileNotFoundException exception)
            {
                Dispose();
                throw new SourceMovedException($"segment '{System.IO.Path.GetFileName(exception.FileName)}' disappeared");
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>The open's one full pass: CRC-checks every record, finds the head, and builds the seek index.</summary>
        public (ulong First, ulong Head, long Count, long TornTailBytes, List<(ulong, ulong)> Gaps) Index()
        {
            ulong first = 0, head = 0;
            long count = 0, torn = 0;
            List<(ulong, ulong)> gaps = [];
            for (var s = 0; s < _segments.Count; s++)
            {
                var (stream, file, cut) = _segments[s];
                var end = new LogFileFormat.WalkEnd();
                foreach (var record in LogFileFormat.WalkRecords(stream, System.IO.Path.Combine(_directory, file), end: end))
                {
                    if (cut is { } last && record.Lsn > last)
                        break; // A previous owner's tail past the cut: never history.
                    if (record.Lsn <= _baseLsn || (count > 0 && record.Lsn <= head))
                        continue;
                    var expected = count > 0 ? head + 1 : _baseLsn + 1;
                    if (record.Lsn != expected)
                        gaps.Add((expected - 1, record.Lsn));
                    if (count % IndexStride == 0)
                        _index.Add((record.Lsn, s, record.Offset));
                    if (count == 0)
                        first = record.Lsn;
                    head = record.Lsn;
                    count++;
                }

                if (cut is null)
                {
                    torn = end.TrailingBytes;
                    _liveLengthAtOpen = stream.Length;
                }
            }

            return (first, head, count, torn, gaps);
        }

        public override IEnumerable<LogFileFormat.RawRecord> Walk(ulong fromLsn, ulong toLsn)
        {
            if (fromLsn > toLsn || _index.Count == 0)
                yield break;
            if (Interlocked.Exchange(ref _walking, 1) != 0)
                throw new InvalidOperationException("A data directory source walks its records one enumeration at a time; finish the first walk before starting another.");
            try
            {
                var start = StartEntry(_index, fromLsn, static e => e.Lsn);
                var (segment, offset) = start < 0 ? (_index[0].Segment, _index[0].Offset) : (_index[start].Segment, _index[start].Offset);
                ulong last = 0;
                for (var s = segment; s < _segments.Count; s++)
                {
                    var (stream, file, cut) = _segments[s];
                    foreach (var record in LogFileFormat.WalkRecords(stream, System.IO.Path.Combine(_directory, file), s == segment ? offset : 0))
                    {
                        if (cut is { } sealedCut && record.Lsn > sealedCut)
                            break;
                        if (record.Lsn <= _baseLsn || record.Lsn <= last || record.Lsn < fromLsn)
                            continue;
                        if (record.Lsn > toLsn)
                            yield break;
                        last = record.Lsn;
                        yield return record;
                    }
                }
            }
            finally
            {
                Volatile.Write(ref _walking, 0);
            }
        }

        public override IEnumerable<SnapshotRow> SnapshotRows(InspectFacts facts)
        {
            var path = System.IO.Path.Combine(_directory, SnapshotFile.FileName);
            using var reader = OpenSnapshot(path);
            if (reader is null || reader.Header.Lsn != facts.SnapshotLsn || reader.Header.Epoch != facts.Epoch)
            {
                throw new InvalidOperationException(
                    $"'{path}' was replaced after this source opened (it covered LSN {facts.SnapshotLsn}" +
                    (reader is null ? "; it is gone now)." : $"; it now covers LSN {reader.Header.Lsn}).") +
                    " A live engine took a new snapshot; open the directory again.");
            }

            foreach (var row in reader.Rows())
                yield return row;
        }

        public override InspectChanges CheckForChanges()
        {
            bool ownerChanged;
            try
            {
                var now = LogSegments.Load(_directory);
                ownerChanged = now.ActiveSegment != _manifest.ActiveSegment;
            }
            catch (Exception exception) when (exception is IOException or InvalidDataException or JsonException)
            {
                ownerChanged = true;
            }

            var appended = false;
            if (_segments.Count > 0 && _segments[^1].Cut is null)
            {
                try
                {
                    appended = _segments[^1].Stream.Length > _liveLengthAtOpen;
                }
                catch (ObjectDisposedException)
                {
                }
            }

            return new InspectChanges(ownerChanged, appended, ReadBaseLsn(_directory) != _baseLsn);
        }

        public override void Dispose()
        {
            foreach (var (stream, _, _) in _segments)
                stream.Dispose();
            _segments.Clear();
        }

        private static FileStream OpenShared(string path) =>
            new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
    }

    private sealed class ArchiveBody(string path, long snapshotStart) : Body
    {
        private readonly List<(ulong Lsn, long Offset)> _index = [];

        public void AddIndex(ulong lsn, long offset) => _index.Add((lsn, offset));

        public override IEnumerable<LogFileFormat.RawRecord> Walk(ulong fromLsn, ulong toLsn)
        {
            if (fromLsn > toLsn || _index.Count == 0)
                yield break;
            var start = StartEntry(_index, fromLsn, static e => e.Lsn);
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            stream.Seek(_index[Math.Max(start, 0)].Offset, SeekOrigin.Begin);
            var reader = new ArchiveFrameReader(stream, path);
            while (reader.ReadFrame() is { Type: ArchiveFrameType.LogRecord } frame)
            {
                var lsn = BinaryPrimitives.ReadUInt64LittleEndian(frame.Payload.AsSpan(2));
                if (lsn < fromLsn)
                    continue;
                if (lsn > toLsn)
                    yield break;
                yield return new LogFileFormat.RawRecord(lsn, frame.Payload);
            }
        }

        public override IEnumerable<SnapshotRow> SnapshotRows(InspectFacts facts)
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            stream.Seek(snapshotStart, SeekOrigin.Begin);
            var reader = new ArchiveFrameReader(stream, path);
            while (reader.ReadFrame() is { Type: ArchiveFrameType.SnapshotRow } frame)
                yield return ArchiveRestore.ParseSnapshotRowFrame(frame.Payload, path);
        }
    }

    private sealed class SourceMovedException(string message) : Exception(message);
}

/// <summary>One table as a source's shape history records it at some LSN.</summary>
public sealed record InspectTable(TableId Id, string Name, TableShape Shape)
{
    /// <summary>Whether the table's rows live only in Postgres: they ride the log but are never in a snapshot or an archive.</summary>
    public bool IsArchiveTier => Shape.Tier == ShapeTiers.Archive;

    /// <summary>The primary key column, or null in a damaged shape that names a key it does not list.</summary>
    public ShapeColumn? KeyColumn => Shape.Columns.FirstOrDefault(c => c.Name == Shape.Key);
}
