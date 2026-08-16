using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// Materializes data directories from an archive — directories a server then boots from through
/// ordinary recovery, because a restore that had its own load path would be a second recovery
/// mechanism to keep correct. Three semantics are the design:
/// <list type="bullet">
/// <item><b>A new epoch is minted, always.</b> A restore is a rewind; a client whose resume
/// cursor sits past the restored head must full-resync, not resume into history that no longer
/// happened. The epoch mint is the existing mechanism that forces exactly that.</item>
/// <item><b>The target must be empty.</b> Restore is replacement, not merge — and refusing a
/// non-empty target is also the guard against restoring beside the deployment that produced the
/// archive, which would be two live worlds sharing an originator id.</item>
/// <item><b>All or nothing.</b> Any failure removes everything this restore wrote; a partial
/// world is never left behind to be booted by mistake.</item>
/// </list>
/// </summary>
internal static class ArchiveRestore
{
    public static RestoreSummary Restore(string archivePath, string targetDirectory)
    {
        if (!File.Exists(archivePath))
            throw new FileNotFoundException($"Archive '{archivePath}' does not exist.", archivePath);
        var target = Path.GetFullPath(targetDirectory);
        if (File.Exists(target))
            throw new InvalidOperationException($"'{target}' is a file; restore needs a directory path.");
        var createdTarget = !Directory.Exists(target);
        if (!createdTarget && Directory.EnumerateFileSystemEntries(target).Any())
        {
            throw new InvalidOperationException(
                $"'{target}' is not empty. Restore refuses a non-empty target: it materializes a whole data " +
                "directory, and merging into existing state is how two worlds end up sharing one history. " +
                "Point it at a new or empty directory.");
        }

        Directory.CreateDirectory(target);
        try
        {
            using var stream = new FileStream(archivePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var reader = new ArchiveFrameReader(stream, archivePath);
            reader.ReadHeader();

            var manifest = ReadJsonFrame<ArchiveManifest>(reader, ArchiveFrameType.Manifest, archivePath);
            if (manifest.Engines.Count != 1)
            {
                throw new NotSupportedException(
                    $"'{archivePath}' is a cluster archive ({manifest.Engines.Count} engines); this build restores " +
                    "single-engine archives. Cluster restore ships with the cluster backup form.");
            }

            var engines = new List<RestoredEngineSummary>();
            foreach (var _ in manifest.Engines)
                engines.Add(RestoreEngine(reader, target, archivePath));

            var footer = ReadJsonFrame<ArchiveFooter>(reader, ArchiveFrameType.ArchiveEnd, archivePath);
            if (footer.Engines != manifest.Engines.Count)
                throw new InvalidDataException($"'{archivePath}': the archive-end frame counts {footer.Engines} engines but the manifest promised {manifest.Engines.Count}.");
            if (reader.ReadFrame() is not null)
                throw new InvalidDataException($"'{archivePath}': data follows the archive-end frame; the archive is corrupt.");

            return new RestoreSummary(engines);
        }
        catch
        {
            // A partial world must not survive to be booted by mistake.
            try
            {
                if (createdTarget)
                    Directory.Delete(target, recursive: true);
                else
                {
                    foreach (var entry in Directory.EnumerateFileSystemEntries(target))
                    {
                        if (Directory.Exists(entry))
                            Directory.Delete(entry, recursive: true);
                        else
                            File.Delete(entry);
                    }
                }
            }
            catch (IOException)
            {
                // Cleanup is best-effort; the original failure is the one worth surfacing.
            }

            throw;
        }
    }

    private static RestoredEngineSummary RestoreEngine(ArchiveFrameReader reader, string engineDirectory, string source)
    {
        var identity = ReadJsonFrame<ArchiveEngineIdentity>(reader, ArchiveFrameType.EngineBegin, source);
        var newEpoch = Guid.NewGuid();
        Directory.CreateDirectory(engineDirectory);

        long snapshotRows = 0;
        if (identity.SnapshotLsn > 0)
        {
            SnapshotFile.Write(
                Path.Combine(engineDirectory, SnapshotFile.FileName),
                new SnapshotFile.Header
                {
                    Epoch = newEpoch,
                    Lsn = identity.SnapshotLsn,
                    Timestamp = new Timestamp(identity.SnapshotTimestampMicros),
                    Sequences = [.. identity.Sequences.Select(s => new KeyValuePair<TableId, ulong>(new TableId(s.Table), s.Next))],
                },
                SnapshotRowFrames(reader, source).Select(row =>
                {
                    snapshotRows++;
                    return (row.Table, (IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>>)[new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(row.Key, row.Row)]);
                }));
        }

        long tailRecords = 0;
        var expectedLsn = identity.SnapshotLsn;
        LogFileFormat.WriteLogFile(
            Path.Combine(engineDirectory, "melange.log"),
            LogRecordFrames(reader, source).Select(payload =>
            {
                var lsn = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(2));
                expectedLsn++;
                if (lsn != expectedLsn)
                    throw new InvalidDataException($"'{source}': log records are not contiguous (expected LSN {expectedLsn}, found {lsn}); the archive is corrupt.");
                tailRecords++;
                return payload;
            }));
        if (expectedLsn != identity.HeadLsn)
            throw new InvalidDataException($"'{source}': the log tail ends at LSN {expectedLsn} but the engine's identity promises head {identity.HeadLsn}; the archive is corrupt.");

        while (reader.ReadFrame() is { } frame)
        {
            if (frame.Type != ArchiveFrameType.Sidecar)
            {
                reader.PushBack(frame);
                break;
            }

            var (name, content) = ParseSidecarFrame(frame.Payload, source);
            switch (name)
            {
                case "melange.events.json":
                    File.WriteAllBytes(Path.Combine(engineDirectory, name), ClampEventCheckpoints(content, identity.HeadLsn));
                    break;
                default:
                    throw new InvalidDataException($"'{source}': unknown sidecar '{name}' in a version-{ArchiveFormat.FormatVersion} archive; the archive is corrupt.");
            }
        }

        var footer = ReadJsonFrame<ArchiveEngineFooter>(reader, ArchiveFrameType.EngineEnd, source);
        if (footer.SnapshotRows != snapshotRows || footer.TailRecords != tailRecords)
        {
            throw new InvalidDataException(
                $"'{source}': engine '{identity.Key}' streamed {snapshotRows} snapshot rows and {tailRecords} records " +
                $"but its end frame promises {footer.SnapshotRows} and {footer.TailRecords}; the archive is corrupt.");
        }

        // Identity last: an interrupted restore leaves no epoch sidecar, so nothing half-written
        // can pass for a bootable directory even if cleanup itself was interrupted.
        if (identity.SnapshotLsn > 0)
        {
            var baseBytes = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(baseBytes, identity.SnapshotLsn);
            File.WriteAllBytes(Path.Combine(engineDirectory, "melange.base"), baseBytes);
        }

        File.WriteAllBytes(Path.Combine(engineDirectory, "melange.epoch"), newEpoch.ToByteArray());

        return new RestoredEngineSummary(identity.Key, newEpoch, identity.SnapshotLsn, identity.HeadLsn, engineDirectory);
    }

    private static IEnumerable<SnapshotRow> SnapshotRowFrames(ArchiveFrameReader reader, string source)
    {
        while (reader.ReadFrame() is { } frame)
        {
            if (frame.Type != ArchiveFrameType.SnapshotRow)
            {
                reader.PushBack(frame);
                yield break;
            }

            yield return ParseSnapshotRowFrame(frame.Payload, source);
        }
    }

    private static IEnumerable<byte[]> LogRecordFrames(ArchiveFrameReader reader, string source)
    {
        while (reader.ReadFrame() is { } frame)
        {
            if (frame.Type != ArchiveFrameType.LogRecord)
            {
                reader.PushBack(frame);
                yield break;
            }

            if (frame.Payload.Length < 10)
                throw new InvalidDataException($"'{source}': a log-record frame is too short to carry a record; the archive is corrupt.");
            yield return frame.Payload;
        }
    }

    internal static SnapshotRow ParseSnapshotRowFrame(byte[] payload, string source)
    {
        if (payload.Length < 13)
            throw new InvalidDataException($"'{source}': a snapshot-row frame is too short; the archive is corrupt.");
        var table = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        var keyLength = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(4));
        var rowLength = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(8));
        if (keyLength < 0 || rowLength < 0 || 13 + (long)keyLength + rowLength != payload.Length)
            throw new InvalidDataException($"'{source}': a snapshot-row frame's lengths disagree with its size; the archive is corrupt.");
        var key = payload.AsSpan(13, keyLength).ToArray();
        var row = payload.AsSpan(13 + keyLength, rowLength).ToArray();
        return new SnapshotRow(new TableId(table), new RowKey(key), row);
    }

    internal static (string Name, byte[] Content) ParseSidecarFrame(byte[] payload, string source)
    {
        if (payload.Length < 2)
            throw new InvalidDataException($"'{source}': a sidecar frame is too short; the archive is corrupt.");
        int nameLength = BinaryPrimitives.ReadUInt16LittleEndian(payload);
        if (2 + nameLength > payload.Length)
            throw new InvalidDataException($"'{source}': a sidecar frame's name overruns the frame; the archive is corrupt.");
        var name = Encoding.UTF8.GetString(payload, 2, nameLength);
        return (name, payload.AsSpan(2 + nameLength).ToArray());
    }

    /// <summary>
    /// Subscriber checkpoints are part of the truth — what has been delivered — so the archive
    /// carries them. But a checkpoint above the restored head points into history that no longer
    /// happened, and a subscriber resuming there would silently skip everything committed after
    /// the restore. Clamping to the head turns that into at-least-once redelivery, which the bus
    /// already permits.
    /// </summary>
    internal static byte[] ClampEventCheckpoints(byte[] content, ulong headLsn)
    {
        Dictionary<string, EventCheckpointStore.Entry>? entries;
        try
        {
            entries = JsonSerializer.Deserialize<Dictionary<string, EventCheckpointStore.Entry>>(content);
        }
        catch (JsonException)
        {
            return content; // A torn source file was archived verbatim; the bus tolerates it.
        }

        if (entries is null)
            return content;
        foreach (var entry in entries.Values)
            entry.Lsn = Math.Min(entry.Lsn, headLsn);
        return JsonSerializer.SerializeToUtf8Bytes(entries, new JsonSerializerOptions { WriteIndented = true });
    }

    internal static T ReadJsonFrame<T>(ArchiveFrameReader reader, ArchiveFrameType expected, string source)
    {
        var frame = reader.ReadFrame()
            ?? throw new InvalidDataException($"'{source}': the archive ends where a {expected} frame was expected; the archive is truncated.");
        if (frame.Type != expected)
            throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} is a {frame.Type} where a {expected} was expected; the archive is corrupt.");
        try
        {
            return JsonSerializer.Deserialize<T>(frame.Payload)
                ?? throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} ({expected}) is empty; the archive is corrupt.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} ({expected}) does not parse; the archive is corrupt.", exception);
        }
    }
}
