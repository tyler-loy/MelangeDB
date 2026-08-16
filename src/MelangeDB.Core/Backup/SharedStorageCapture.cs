using System.Buffers.Binary;

namespace MelangeDB.Core;

/// <summary>
/// The cluster fan-out's per-shard capture: reads a shard engine's directory over shared storage
/// while its owning node keeps serving it — no remote pin, no quiesce, no channel to the owner at
/// all. Consistency is handle-based instead: the base sidecar is read first (it only advances to
/// a snapshot that exists), the snapshot handle opens second (so its LSN is at or above that
/// base), the log handle opens third, and both walk passes share it — an appended-to file only
/// grows under a handle, and a compaction swap leaves the old content under the old one. The one
/// race that survives — a snapshot completing and truncating between the snapshot open and the
/// log open — surfaces as a gap in the record chain, and the capture retries with fresh handles
/// rather than archiving a hole. Each shard's fence is its own last record: the cluster archive
/// is <b>per-shard consistent, not globally consistent</b>, which is the honest property — there
/// is no global total order to capture.
/// </summary>
internal static class SharedStorageCapture
{
    private const int Attempts = 3;

    /// <summary>Captures one shard engine directory (the shard root holding <c>log/</c> and its sidecars).</summary>
    public static BackupEngineSummary Capture(string shardDirectory, string engineKey, ArchiveFrameWriter writer)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return CaptureOnce(shardDirectory, engineKey, writer);
            }
            catch (InvalidOperationException) when (attempt < Attempts)
            {
                // A truncation raced the handle opens; fresh handles see the post-truncation
                // snapshot and the retry streams a consistent capture.
            }
        }
    }

    private static BackupEngineSummary CaptureOnce(string shardDirectory, string engineKey, ArchiveFrameWriter writer)
    {
        var logDirectory = Path.Combine(shardDirectory, "log");
        var logPath = Path.Combine(logDirectory, "melange.log");
        if (!File.Exists(logPath))
            throw new InvalidOperationException($"'{shardDirectory}' does not contain log/melange.log — it is not a shard data directory.");

        var epoch = ReadEpoch(Path.Combine(logDirectory, "melange.epoch"));
        var baseLsn = ReadBaseLsn(Path.Combine(logDirectory, "melange.base"));

        SnapshotReader? snapshot = null;
        try
        {
            snapshot = OpenLiveSnapshot(Path.Combine(logDirectory, SnapshotFile.FileName));
            if (snapshot is not null && snapshot.Header.Epoch != epoch)
            {
                // A stale snapshot beside an untruncated log, exactly as recovery treats it. With
                // a truncated log this state cannot outlive the race window (the epoch sidecar
                // and the snapshot advance together on a live engine), so a mismatch here retries
                // rather than refusing a directory that is merely mid-write.
                if (baseLsn > 0)
                    throw new InvalidOperationException($"'{shardDirectory}': snapshot and epoch disagree; a rewrite raced this capture.");
                snapshot.Dispose();
                snapshot = null;
            }

            if (snapshot is null && baseLsn > 0)
                throw new InvalidOperationException($"'{shardDirectory}': the log is truncated but no snapshot is readable; a rewrite raced this capture.");
            if (snapshot is not null && snapshot.Header.Lsn < baseLsn)
                throw new InvalidOperationException($"'{shardDirectory}': the snapshot predates the truncation base; a rewrite raced this capture.");

            // Read + write + delete sharing throughout: the owner appends, snapshots, and
            // compacts underneath these handles, and every one of those must proceed.
            using var log = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            return DataDirectoryCapture.CaptureCore(
                engineKey, writer, log, logPath, shardDirectory, snapshot, epoch, baseLsn,
                fence => BorrowedSidecar(shardDirectory, fence));
        }
        finally
        {
            snapshot?.Dispose();
        }
    }

    /// <summary>
    /// The border registry rides along when it exists and is not from the fence's future — the
    /// owner rewrites it at the head whenever truncation is decided, which can be past this
    /// capture's fence; a skipped sidecar only costs the restored shard the loud
    /// rebuild-from-content path recovery already has.
    /// </summary>
    private static IEnumerable<(string Name, byte[] Content)> BorrowedSidecar(string shardDirectory, ulong fence)
    {
        var path = Path.Combine(shardDirectory, "borrowed.sidecar");
        if (!File.Exists(path))
            yield break;
        var content = File.ReadAllBytes(path);
        ulong? lsn = null;
        try
        {
            if (System.Text.Json.Nodes.JsonNode.Parse(content)?["Lsn"] is { } node)
                lsn = (ulong)node;
        }
        catch (System.Text.Json.JsonException)
        {
            yield break; // Unusable sidecar; the restored shard rebuilds its registry loudly.
        }

        if (lsn is { } value && value <= fence)
            yield return ("borrowed.sidecar", content);
    }

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
        return bytes.Length == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(bytes) : 0;
    }
}
