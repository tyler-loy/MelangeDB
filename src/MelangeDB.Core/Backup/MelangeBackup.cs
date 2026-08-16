namespace MelangeDB.Core;

/// <summary>
/// Backup, restore, and verify over the <c>.mbak</c> archive format — the API the `melange` CLI
/// verbs call, public so an operator's own tooling can call the same thing the CLI does. The
/// design writes itself off one shipped fact: the commit log is the source of truth and every
/// store is a projection of it. So an archive is the truth, not the projections — snapshot plus
/// log tail plus sidecars, per engine — and a restore materializes directories that ordinary
/// recovery boots, the way every restart already does.
/// </summary>
public static class MelangeBackup
{
    /// <summary>
    /// The offline backup: captures a stopped server's data directory into
    /// <paramref name="outputPath"/>. Refuses a directory whose log is open by a live process,
    /// and writes the archive to a temp file swapped in atomically, so an interrupted backup
    /// never leaves a plausible-looking partial archive behind.
    /// </summary>
    public static BackupSummary Create(string dataDirectory, string outputPath)
    {
        var tempPath = outputPath + ".tmp";
        try
        {
            return CreateAt(dataDirectory, tempPath, outputPath);
        }
        catch
        {
            try
            {
                File.Delete(tempPath);
            }
            catch (IOException)
            {
                // Best-effort; the capture failure is the one worth surfacing.
            }

            throw;
        }
    }

    private static BackupSummary CreateAt(string dataDirectory, string tempPath, string outputPath)
    {
        BackupSummary summary;
        using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var writer = new ArchiveFrameWriter(stream);
            writer.WriteHeader();
            writer.WriteFrame(
                ArchiveFrameType.Manifest,
                System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new ArchiveManifest
                {
                    CapturedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Engines = [ArchiveFormat.SingleNodeEngineKey],
                }));
            var engine = DataDirectoryCapture.Capture(dataDirectory, ArchiveFormat.SingleNodeEngineKey, writer);
            writer.WriteFrame(
                ArchiveFrameType.ArchiveEnd,
                System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new ArchiveFooter { Engines = 1 }));
            stream.Flush(flushToDisk: true);
            summary = new BackupSummary([engine], writer.BytesWritten);
        }

        File.Move(tempPath, outputPath, overwrite: true);
        return summary;
    }

    /// <summary>
    /// The online backup: streams a live engine into <paramref name="destination"/> at a fenced
    /// LSN while commits continue, holding a truncation pin for exactly the stream's duration.
    /// This is what <c>/melange/backup</c> serves; it is public so in-process tooling (a
    /// scheduled job inside the host, say) can take the same capture without HTTP in between.
    /// </summary>
    public static BackupSummary CreateOnline(MelangeEngine engine, Stream destination)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(destination);
        var writer = new ArchiveFrameWriter(destination);
        writer.WriteHeader();
        writer.WriteFrame(
            ArchiveFrameType.Manifest,
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new ArchiveManifest
            {
                CapturedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Engines = [ArchiveFormat.SingleNodeEngineKey],
            }));
        var summary = OnlineEngineCapture.Capture(engine, ArchiveFormat.SingleNodeEngineKey, writer);
        writer.WriteFrame(
            ArchiveFrameType.ArchiveEnd,
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new ArchiveFooter { Engines = 1 }));
        return new BackupSummary([summary], writer.BytesWritten);
    }

    /// <summary>
    /// The cluster backup: the hub's own engine plus every shard engine found under
    /// <paramref name="shardDataPath"/> on shared storage, under one manifest keyed by shard.
    /// One fenced LSN per engine — the archive is <b>per-shard consistent, not globally
    /// consistent</b>, because there is no global total order to capture; cross-shard skew is
    /// bounded by the capture window. The hub's engine streams under a truncation pin; shard
    /// engines stream handle-consistently over shared storage while their owners keep serving
    /// them, with no remote pin and no quiesce. Restore materializes <c>hub/</c> (point the
    /// hub's <c>CommitLog:Path</c> there) and <c>shards/shard-k/</c> (point every node's
    /// <c>Cluster:ShardDataPath</c> at <c>shards/</c>).
    /// </summary>
    public static BackupSummary CreateClusterOnline(MelangeEngine hubEngine, string shardDataPath, Stream destination)
    {
        ArgumentNullException.ThrowIfNull(hubEngine);
        ArgumentNullException.ThrowIfNull(destination);
        var shardDirectories = Directory.Exists(shardDataPath)
            ? Directory.EnumerateDirectories(shardDataPath, ArchiveFormat.ShardEngineKeyPrefix + "*")
                .Where(static dir => File.Exists(Path.Combine(dir, "log", "melange.log")))
                .OrderBy(static dir => Path.GetFileName(dir), StringComparer.Ordinal)
                .ToList()
            : [];

        var writer = new ArchiveFrameWriter(destination);
        writer.WriteHeader();
        writer.WriteFrame(
            ArchiveFrameType.Manifest,
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new ArchiveManifest
            {
                CapturedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Engines = [ArchiveFormat.HubEngineKey, .. shardDirectories.Select(static dir => Path.GetFileName(dir)!)],
            }));

        var engines = new List<BackupEngineSummary>
        {
            OnlineEngineCapture.Capture(hubEngine, ArchiveFormat.HubEngineKey, writer),
        };
        foreach (var directory in shardDirectories)
            engines.Add(SharedStorageCapture.Capture(directory, Path.GetFileName(directory)!, writer));

        writer.WriteFrame(
            ArchiveFrameType.ArchiveEnd,
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new ArchiveFooter { Engines = engines.Count }));
        return new BackupSummary(engines, writer.BytesWritten);
    }

    /// <summary>
    /// Materializes data directories from <paramref name="archivePath"/> into an empty
    /// <paramref name="targetDirectory"/> — a rewind, for replacement, not cloning: a fresh epoch
    /// is always minted, so clients holding pre-restore resume cursors are refused resume and
    /// full-resync. Any failure removes everything the restore wrote.
    /// </summary>
    public static RestoreSummary Restore(string archivePath, string targetDirectory)
        => ArchiveRestore.Restore(archivePath, targetDirectory);

    /// <summary>
    /// CRC-walks every frame and dry-replays the archive into an in-memory projection, reporting
    /// per-table row counts and the LSN range. Any corruption throws
    /// <see cref="InvalidDataException"/> naming the frame. An unverified backup is a hope, not a
    /// backup — run this in CI against every nightly archive.
    /// </summary>
    public static BackupVerifyReport Verify(string archivePath)
        => ArchiveVerifier.Verify(archivePath);
}

/// <summary>One engine's capture, as recorded in the archive.</summary>
public sealed record BackupEngineSummary(
    string Key,
    Guid SourceEpoch,
    ulong BaseLsn,
    ulong SnapshotLsn,
    ulong HeadLsn,
    long SnapshotRows,
    long TailRecords);

/// <summary>What a backup wrote: the engines captured and the archive's size in bytes.</summary>
public sealed record BackupSummary(IReadOnlyList<BackupEngineSummary> Engines, long TotalBytes);

/// <summary>
/// One engine as restored: its fresh epoch (minted always — a restore is a rewind, and the mint
/// is what forces stale resume cursors into a full resync) and where its files landed.
/// </summary>
public sealed record RestoredEngineSummary(
    string Key,
    Guid NewEpoch,
    ulong SnapshotLsn,
    ulong HeadLsn,
    string Directory);

/// <summary>What a restore materialized.</summary>
public sealed record RestoreSummary(IReadOnlyList<RestoredEngineSummary> Engines);

/// <summary>One verified engine: its identity and the dry-replay's per-table live row counts.</summary>
public sealed record VerifiedEngineReport(
    BackupEngineSummary Identity,
    IReadOnlyDictionary<uint, long> RowsByTable);

/// <summary>A verify that returned (rather than throwing) proves every frame intact.</summary>
public sealed record BackupVerifyReport(long CapturedAtUnixMs, IReadOnlyList<VerifiedEngineReport> Engines);
