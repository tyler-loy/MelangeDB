using System.Text;
using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// The offline capture: reads one engine's data directory — log, snapshot, sidecars — and streams
/// it into an archive, changing nothing. This is the supported form of the "stop the process and
/// copy the directory" folklore, minus its failure modes: it refuses a directory whose log is
/// held open by a live process, refuses one that would not boot (recovery's own refusals,
/// mirrored), and misses no sidecar, because the sidecar list lives here rather than in a runbook.
/// </summary>
internal static class DataDirectoryCapture
{
    /// <summary>
    /// Captures the engine in <paramref name="dataDirectory"/> as one archive engine section.
    /// The directory is the engine's log directory — the one <c>CommitLog:Path</c> points at.
    /// </summary>
    public static BackupEngineSummary Capture(string dataDirectory, string engineKey, ArchiveFrameWriter writer)
    {
        var directory = Path.GetFullPath(dataDirectory);
        var logPath = Path.Combine(directory, "melange.log");
        if (!File.Exists(logPath))
        {
            throw new InvalidOperationException(
                $"'{directory}' does not contain melange.log — it is not a MelangeDB data directory. " +
                "Point the backup at the directory CommitLog:Path names (or the shard's log directory).");
        }

        using var log = OpenLogExcludingWriters(logPath);

        var epoch = ReadEpoch(Path.Combine(directory, "melange.epoch"));
        var baseLsn = ReadBaseLsn(Path.Combine(directory, "melange.base"));
        var snapshotPath = Path.Combine(directory, SnapshotFile.FileName);

        SnapshotReader? snapshot = null;
        try
        {
            if (File.Exists(snapshotPath))
            {
                snapshot = SnapshotFile.Open(snapshotPath);
                if (snapshot.Header.Epoch != epoch)
                {
                    if (baseLsn > 0)
                    {
                        throw new InvalidOperationException(
                            $"'{directory}': the snapshot belongs to a different log epoch and the log has been truncated. " +
                            "This directory would not boot, so it cannot be backed up as-is; restore from an earlier backup.");
                    }

                    // A stale snapshot beside an untruncated log: recovery ignores it and replays
                    // from the start, so the backup does the same — the log alone is the truth.
                    snapshot.Dispose();
                    snapshot = null;
                }
                else if (snapshot.Header.Lsn < baseLsn)
                {
                    throw new InvalidOperationException(
                        $"'{directory}': the snapshot (LSN {snapshot.Header.Lsn}) predates the log's truncation base ({baseLsn}). " +
                        "This directory would not boot, so it cannot be backed up as-is; restore from an earlier backup.");
                }
            }
            else if (baseLsn > 0)
            {
                throw new InvalidOperationException(
                    $"'{directory}': the log has been truncated (base LSN {baseLsn}) but no snapshot covers the removed range. " +
                    "This directory would not boot, so it cannot be backed up as-is; restore from an earlier backup.");
            }

            var snapshotLsn = snapshot?.Header.Lsn ?? 0;

            // First pass: the head is part of the identity frame, which streams before the tail.
            var headLsn = Math.Max(baseLsn, snapshotLsn);
            long tailRecords = 0;
            foreach (var record in LogFileFormat.WalkRecords(log, logPath))
            {
                headLsn = Math.Max(headLsn, record.Lsn);
                if (record.Lsn > snapshotLsn)
                    tailRecords++;
            }

            var identity = new ArchiveEngineIdentity
            {
                Key = engineKey,
                SourceEpoch = epoch,
                BaseLsn = baseLsn,
                SnapshotLsn = snapshotLsn,
                HeadLsn = headLsn,
                SnapshotTimestampMicros = snapshot?.Header.Timestamp.UnixTimeMicroseconds ?? 0,
                Sequences = snapshot is null
                    ? []
                    : [.. snapshot.Header.Sequences.Select(pair => new ArchiveSequence { Table = pair.Key.Value, Next = pair.Value })],
            };
            writer.WriteFrame(ArchiveFrameType.EngineBegin, JsonSerializer.SerializeToUtf8Bytes(identity));

            long snapshotRows = 0;
            if (snapshot is not null)
            {
                foreach (var row in snapshot.Rows())
                {
                    WriteSnapshotRowFrame(writer, row);
                    snapshotRows++;
                }
            }

            // Second pass streams the tail. The write-excluding handle is held across both passes,
            // so the file cannot have changed between the count and the stream.
            long streamed = 0;
            foreach (var record in LogFileFormat.WalkRecords(log, logPath))
            {
                if (record.Lsn <= snapshotLsn)
                    continue;
                writer.WriteFrame(ArchiveFrameType.LogRecord, record.Payload);
                streamed++;
            }

            if (streamed != tailRecords)
                throw new InvalidOperationException($"'{directory}': the log changed during the backup ({tailRecords} tail records counted, {streamed} streamed).");

            var eventsPath = Path.Combine(directory, "melange.events.json");
            if (File.Exists(eventsPath))
                WriteSidecarFrame(writer, "melange.events.json", File.ReadAllBytes(eventsPath));

            writer.WriteFrame(
                ArchiveFrameType.EngineEnd,
                JsonSerializer.SerializeToUtf8Bytes(new ArchiveEngineFooter { SnapshotRows = snapshotRows, TailRecords = tailRecords }));

            return new BackupEngineSummary(engineKey, epoch, baseLsn, snapshotLsn, headLsn, snapshotRows, tailRecords);
        }
        finally
        {
            snapshot?.Dispose();
        }
    }

    internal static void WriteSnapshotRowFrame(ArchiveFrameWriter writer, SnapshotRow row)
    {
        var payload = new byte[13 + row.Key.Length + row.Row.Length];
        var span = payload.AsSpan();
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(span, row.Table.Value);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[4..], row.Key.Length);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[8..], row.Row.Length);
        span[12] = 0; // Reserved; keeps the layout aligned with a future flags byte.
        row.Key.Span.CopyTo(span[13..]);
        row.Row.Span.CopyTo(span[(13 + row.Key.Length)..]);
        writer.WriteFrame(ArchiveFrameType.SnapshotRow, payload);
    }

    internal static void WriteSidecarFrame(ArchiveFrameWriter writer, string name, byte[] content)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var payload = new byte[2 + nameBytes.Length + content.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(payload, (ushort)nameBytes.Length);
        nameBytes.CopyTo(payload, 2);
        content.CopyTo(payload, 2 + nameBytes.Length);
        writer.WriteFrame(ArchiveFrameType.Sidecar, payload);
    }

    /// <summary>
    /// Opens the log requesting write access — not to write, but because a live
    /// <see cref="FileCommitLog"/> shares only read and delete, so requesting write is the probe
    /// that distinguishes a stopped server from a running one. The handle is held for the whole
    /// capture, which also stops a server from <em>starting</em> against the directory mid-backup.
    /// A directory on read-only media cannot have a live writer, so that case falls back to a
    /// plain read handle instead of failing.
    /// </summary>
    private static FileStream OpenLogExcludingWriters(string logPath)
    {
        try
        {
            return new FileStream(logPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
        }
        catch (UnauthorizedAccessException)
        {
            return new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException(
                $"'{logPath}' is open by a live process. The offline backup reads a stopped server's files — " +
                "stop the server and retry. (This refusal is the point: copying a live directory is how backups go subtly wrong.)",
                exception);
        }
    }

    private static Guid ReadEpoch(string path)
    {
        if (!File.Exists(path))
            return Guid.Empty;
        var bytes = File.ReadAllBytes(path);
        return bytes.Length == 16 ? new Guid(bytes) : Guid.Empty;
    }

    private static ulong ReadBaseLsn(string path)
    {
        if (!File.Exists(path))
            return 0;
        var bytes = File.ReadAllBytes(path);
        return bytes.Length == 8 ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes) : 0;
    }
}
