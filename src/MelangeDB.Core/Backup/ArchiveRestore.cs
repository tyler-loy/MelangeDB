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
    /// <summary>
    /// What this materialization is, beyond a plain restore. Internal because the difference
    /// between a restore and a clone is a <em>verb</em>, not a flag: the semantics differ in kind,
    /// and a flag would invite using one where the other was meant.
    /// </summary>
    internal sealed record RestorePlan(ulong? AtLsn = null, bool IsClone = false);

    public static RestoreSummary Restore(string archivePath, string targetDirectory, RestoreOptions? options = null)
        => Restore(archivePath, targetDirectory, new RestorePlan(options?.AtLsn));

    public static RestoreSummary Restore(string archivePath, string targetDirectory, RestorePlan plan)
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
            if (manifest.Engines.Count == 0)
                throw new InvalidDataException($"'{archivePath}': the manifest names no engines; the archive is corrupt.");
            RefusePointInTimeOnClusterArchives(manifest, plan, archivePath);

            var engines = new List<RestoredEngineSummary>();
            foreach (var key in manifest.Engines)
                engines.Add(RestoreEngine(reader, DirectoryFor(manifest, key, target, archivePath), key, archivePath, manifest, plan, archivePath));

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

    /// <summary>
    /// A cluster archive is per-shard consistent at <em>different</em> fences — the capture holds
    /// no global total order, because none exists. One LSN therefore names no cross-shard moment,
    /// and per-shard LSNs would manufacture a consistency the capture never had. Refused rather
    /// than approximated.
    /// </summary>
    private static void RefusePointInTimeOnClusterArchives(ArchiveManifest manifest, RestorePlan options, string source)
    {
        if (options.AtLsn is null)
            return;
        if (manifest.Engines.Count == 1 && manifest.Engines[0] == ArchiveFormat.SingleNodeEngineKey)
            return;
        throw new InvalidOperationException(
            $"'{source}' is a cluster archive ({manifest.Engines.Count} engine(s)); point-in-time restore takes " +
            "single-engine archives only. Each engine was captured at its own fence, so one LSN names no moment " +
            "the cluster ever occupied, and per-shard LSNs would manufacture a consistency the capture never had. " +
            "Restore the archive whole, or take the point-in-time restore from a single-node archive.");
    }

    /// <summary>
    /// The restored layout: a single-node archive materializes straight into the target (point
    /// <c>CommitLog:Path</c> there); a cluster archive materializes <c>hub/</c> and
    /// <c>shards/shard-k/</c> (point the hub's <c>CommitLog:Path</c> at <c>hub/</c> and every
    /// node's <c>Cluster:ShardDataPath</c> at <c>shards/</c>).
    /// </summary>
    private static string DirectoryFor(ArchiveManifest manifest, string key, string target, string source)
    {
        if (key == ArchiveFormat.SingleNodeEngineKey)
        {
            return manifest.Engines.Count == 1
                ? target
                : throw new InvalidDataException($"'{source}': a single-node engine inside a multi-engine manifest; the archive is corrupt.");
        }

        if (key == ArchiveFormat.HubEngineKey)
            return Path.Combine(target, "hub");
        if (key.StartsWith(ArchiveFormat.ShardEngineKeyPrefix, StringComparison.Ordinal) && key.Length > ArchiveFormat.ShardEngineKeyPrefix.Length)
            return Path.Combine(target, "shards", key, "log");
        throw new InvalidDataException($"'{source}': unknown engine key '{key}' in a version-{ArchiveFormat.FormatVersion} archive; the archive is corrupt.");
    }

    private static RestoredEngineSummary RestoreEngine(
        ArchiveFrameReader reader,
        string engineDirectory,
        string engineKey,
        string source,
        ArchiveManifest manifest,
        RestorePlan options,
        string archivePath)
    {
        var identity = ReadJsonFrame<ArchiveEngineIdentity>(reader, ArchiveFrameType.EngineBegin, source);
        if (identity.Key != engineKey)
            throw new InvalidDataException($"'{source}': engine '{identity.Key}' arrived where the manifest promised '{engineKey}'; the archive is corrupt.");
        var isShard = engineKey.StartsWith(ArchiveFormat.ShardEngineKeyPrefix, StringComparison.Ordinal);
        var restoredHead = ResolveCut(identity, options, source);
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

        // Every record frame is read and checked whichever LSN the restore stops at — the archive's
        // own integrity claims (contiguity, the promised head, the end frame's count) are about
        // what was captured, not about what this restore chose to keep. Only the writing stops
        // early; the walk never does.
        long tailRecords = 0;
        var expectedLsn = identity.SnapshotLsn;
        LogFileFormat.WriteLogFile(
            Path.Combine(engineDirectory, "melange.log"),
            LogRecordFrames(reader, source)
                .Select(payload =>
                {
                    var lsn = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(2));
                    expectedLsn++;
                    if (lsn != expectedLsn)
                        throw new InvalidDataException($"'{source}': log records are not contiguous (expected LSN {expectedLsn}, found {lsn}); the archive is corrupt.");
                    tailRecords++;
                    return (Lsn: lsn, Payload: payload);
                })
                .Where(record => record.Lsn <= restoredHead)
                .Select(static record => record.Payload));
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
                case "melange.events.json" when options.IsClone:
                    // Dropped, not clamped. A clone has no subscribers yet, and production's
                    // event-delivery state resuming in staging — handlers deciding they have
                    // already delivered what this world has never emitted — is exactly the
                    // confusion the verb exists to prevent. Absent means "start from the
                    // beginning", which is what a new world means.
                    break;
                case "melange.events.json":
                    File.WriteAllBytes(Path.Combine(engineDirectory, name), ClampEventCheckpoints(content, restoredHead));
                    break;
                case ShapeHistory.FileName:
                    // The shape history is LSN-keyed and epoch-independent; restore changes no
                    // LSNs, so it restores verbatim — it is what lets newer code boot the
                    // restored directory through an ordinary migration boot.
                    File.WriteAllBytes(Path.Combine(engineDirectory, name), content);
                    break;
                case "borrowed.sidecar" when isShard:
                    // The border registry lives in the shard root, one level above the log
                    // directory, and names an epoch — rewritten to the fresh one so recovery
                    // trusts it instead of falling back to the loud full-scan rebuild.
                    File.WriteAllBytes(
                        Path.Combine(Directory.GetParent(engineDirectory)!.FullName, name),
                        RewriteSidecarEpoch(content, newEpoch));
                    break;
                default:
                    throw new InvalidDataException($"'{source}': unknown sidecar '{name}' for engine '{engineKey}' in a version-{ArchiveFormat.FormatVersion} archive; the archive is corrupt.");
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

        if (options.IsClone)
        {
            new CloneProvenance(
                CloneProvenance.CloneKind,
                identity.SourceEpoch,
                restoredHead,
                Path.GetFileName(archivePath),
                manifest.CapturedAtUnixMs,
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                newEpoch).Write(engineDirectory);
        }

        File.WriteAllBytes(Path.Combine(engineDirectory, "melange.epoch"), newEpoch.ToByteArray());

        return new RestoredEngineSummary(
            identity.Key, newEpoch, identity.SnapshotLsn, restoredHead, identity.HeadLsn, engineDirectory);
    }

    /// <summary>
    /// Where the tail is cut. Without <c>--at-lsn</c> that is the captured head; with it, the LSN
    /// the operator named — bounded below by the archive's snapshot (an archive cannot rewind
    /// below its own materialized floor, because everything under it exists only as snapshot
    /// state) and above by the captured head (there is nothing up there to restore).
    /// <para>
    /// AutoInc sequences come from the snapshot header and are then re-observed from the records
    /// this restore kept — so a cut rewinds the allocator along with everything else, and ids
    /// allocated in the discarded range are free again. That is the honest reading of a rewind:
    /// those allocations are not history any more, nothing inside the restored world refers to
    /// them, and the fresh epoch is what forces every consumer outside it (clients, the relational
    /// tier) to rebuild rather than carry a stale reference across the boundary. The archive
    /// cannot offer better — it carries sequences as of its snapshot, and observing the discarded
    /// range would need the schema a restore deliberately does not have.
    /// </para>
    /// </summary>
    private static ulong ResolveCut(ArchiveEngineIdentity identity, RestorePlan options, string source)
    {
        if (options.AtLsn is not { } atLsn)
            return identity.HeadLsn;
        if (atLsn > identity.HeadLsn)
        {
            throw new InvalidOperationException(
                $"'{source}' was captured at head LSN {identity.HeadLsn}; there is no LSN {atLsn} in it to restore to. " +
                "Point-in-time restore rewinds within one archive — it cannot roll forward past its capture.");
        }

        if (atLsn < identity.SnapshotLsn)
        {
            throw new InvalidOperationException(
                $"'{source}' materializes its state at snapshot LSN {identity.SnapshotLsn}; LSN {atLsn} is below that " +
                "floor and the records that would rewind to it are not in this archive. That moment still exists in " +
                "an earlier archive in the series — the one whose snapshot LSN is at or below " +
                $"{atLsn} (melange backup verify prints it). Restore that one --at-lsn {atLsn} instead.");
        }

        return atLsn;
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
    /// Rewrites the border-registry sidecar's epoch to the restored engine's fresh one. The
    /// sidecar's LSN stays: recovery seeds the registry from it and replays only the records
    /// above it, exactly as after any restart. An unparseable sidecar passes through verbatim —
    /// recovery's rebuild-from-content path is the loud safety net for that.
    /// </summary>
    internal static byte[] RewriteSidecarEpoch(byte[] content, Guid newEpoch)
    {
        try
        {
            if (System.Text.Json.Nodes.JsonNode.Parse(content) is not { } node)
                return content;
            node["Epoch"] = newEpoch.ToString();
            return System.Text.Encoding.UTF8.GetBytes(node.ToJsonString());
        }
        catch (JsonException)
        {
            return content;
        }
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
