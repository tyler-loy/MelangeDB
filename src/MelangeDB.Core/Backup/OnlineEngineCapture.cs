using System.Text.Json;

namespace MelangeDB.Core;

/// <summary>
/// The online capture: streams a live engine into an archive at a fenced LSN while commits
/// continue. Consistency costs three ordered moves and no lock:
/// <list type="number">
/// <item><see cref="MelangeEngine.PinTruncation"/> first — while the pin is held no new
/// compaction is decided, so every record above the snapshot stays on disk. A compaction decided
/// before the pin may still land during the walk: its floor never passes the snapshot already on
/// disk, so it removes nothing the walk needs, and the walk's own handle keeps reading the file it
/// opened — the log's lazy-reader convention.</item>
/// <item>The snapshot handle opens before the fence is read, so the fence — the head LSN at
/// capture — is always at or above the snapshot's LSN.</item>
/// <item>The fence is read before the log's buffers are flushed, so every record at or below the
/// fence is visible to the file walk; records committed after it are simply not part of this
/// capture.</item>
/// </list>
/// The pin is scoped to the capture and released in a finally — like every truncation pin (saga
/// markers, subscriber checkpoints) it must be bounded, because a wedged backup client must not
/// become a full disk. The caller enforces the wall-clock bound; this class guarantees release.
/// </summary>
internal static class OnlineEngineCapture
{
    public static BackupEngineSummary Capture(MelangeEngine engine, string engineKey, ArchiveFrameWriter writer)
    {
        using var pin = engine.PinTruncation();
        SnapshotReader? snapshot = OpenLiveSnapshot(engine.SnapshotPath);
        try
        {
            var epoch = engine.Log.EpochId;
            if (snapshot is not null && snapshot.Header.Epoch != epoch)
            {
                // A stale snapshot from a previous incarnation, still on disk because the engine
                // booted past it (base zero) and has not overwritten it yet: the log alone is the
                // truth, exactly as recovery judged it.
                snapshot.Dispose();
                snapshot = null;
            }

            var baseLsn = engine.Log.BaseLsn;
            var snapshotLsn = snapshot?.Header.Lsn ?? 0;

            // Fenced at the durable watermark, not the head: under group commit a record can sit
            // buffered while its commit still waits for the fsync, and a backup must never carry
            // an LSN whose caller was not acknowledged — a crash on the source would untell it,
            // leaving the archive claiming history the origin never had.
            var fence = engine.Log.DurableLsn;
            engine.LogFile.FlushBuffers();

            var identity = new ArchiveEngineIdentity
            {
                Key = engineKey,
                SourceEpoch = epoch,
                BaseLsn = baseLsn,
                SnapshotLsn = snapshotLsn,
                HeadLsn = fence,
                SnapshotTimestampMicros = snapshot?.Header.Timestamp.UnixTimeMicroseconds ?? 0,
                Sequences = snapshot is null
                    ? []
                    : [.. snapshot.Header.Sequences.Select(pair => new ArchiveSequence { Table = pair.Key.Value, Next = pair.Value })],
                Lineage = engine.Lineage.Ancestors is { Count: > 0 } ancestors ? ancestors : null,
            };
            writer.WriteFrame(ArchiveFrameType.EngineBegin, JsonSerializer.SerializeToUtf8Bytes(identity));

            long snapshotRows = 0;
            if (snapshot is not null)
            {
                foreach (var row in snapshot.Rows())
                {
                    DataDirectoryCapture.WriteSnapshotRowFrame(writer, row);
                    snapshotRows++;
                }
            }

            long tailRecords = 0;
            using (var log = new LogSegments.Reader(Path.GetDirectoryName(engine.LogFile.FilePath)!))
            {
                foreach (var record in log.Walk())
                {
                    if (record.Lsn <= snapshotLsn || record.Lsn > fence)
                        continue;
                    writer.WriteFrame(ArchiveFrameType.LogRecord, record.Payload);
                    tailRecords++;
                }
            }

            if (tailRecords != (long)(fence - snapshotLsn))
            {
                throw new InvalidOperationException(
                    $"The online capture expected {fence - snapshotLsn} records between LSN {snapshotLsn} and the fence {fence} " +
                    $"but found {tailRecords}; the log changed in a way the pin should have prevented.");
            }

            // Atomic-swap sidecar: a concurrent rewrite hands us the old or the new file, both
            // complete. Entries past the fence clamp at restore, like every rewound checkpoint.
            var eventsPath = Path.Combine(Path.GetDirectoryName(engine.LogFile.FilePath)!, "melange.events.json");
            if (ReadLiveSidecar(eventsPath) is { } events)
                DataDirectoryCapture.WriteSidecarFrame(writer, "melange.events.json", events);

            // The shape history is LSN-keyed and epoch-independent, so the live engine's copy is
            // exactly what the archive needs — and entries only ever append at boot, so a live
            // read races nothing.
            DataDirectoryCapture.WriteSidecarFrame(writer, ShapeHistory.FileName, engine.Shapes.History.ToBytes());

            writer.WriteFrame(
                ArchiveFrameType.EngineEnd,
                JsonSerializer.SerializeToUtf8Bytes(new ArchiveEngineFooter { SnapshotRows = snapshotRows, TailRecords = tailRecords }));

            return new BackupEngineSummary(engineKey, epoch, baseLsn, snapshotLsn, fence, snapshotRows, tailRecords);
        }
        finally
        {
            snapshot?.Dispose();
        }
    }

    /// <summary>Tries at reading a sidecar the live engine is replacing: the first, then retries.</summary>
    private const int SidecarReadAttempts = 10;

    /// <summary>
    /// Reads a sidecar the live engine replaces atomically (the event checkpoints, after every
    /// delivered event), or null if there is none. Two Windows hazards, both #321: a handle that
    /// does not share delete makes the engine's replace fail, so this one shares it, as
    /// <see cref="OpenLiveSnapshot"/> does; and an open that lands in the middle of a replace fails
    /// with a sharing violation, so it retries briefly. Either way the bytes are the old file or
    /// the new one, both complete.
    /// </summary>
    internal static byte[]? ReadLiveSidecar(string path)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var stream = OpenLiveSidecar(path);
                var bytes = new byte[stream.Length];
                stream.ReadExactly(bytes);
                return bytes;
            }
            catch (FileNotFoundException)
            {
                return null;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException && attempt < SidecarReadAttempts)
            {
                Thread.Sleep(5 * attempt);
            }
        }
    }

    /// <summary>A read handle on a live sidecar that never blocks the engine replacing it.</summary>
    internal static FileStream OpenLiveSidecar(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

    /// <summary>
    /// Opens the snapshot sharing write and delete: a snapshot completing mid-stream atomically
    /// replaces the file, and this handle must neither block that swap nor be broken by it — it
    /// keeps reading the old, complete content, the log's lazy-reader convention.
    /// </summary>
    private static SnapshotReader? OpenLiveSnapshot(string path)
    {
        if (!File.Exists(path))
            return null;
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        try
        {
            return new SnapshotReader(path, stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }
}
