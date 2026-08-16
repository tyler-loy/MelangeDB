namespace MelangeDB.Core;

/// <summary>
/// The proof half of the backup story: CRC-walks every frame, then dry-replays the archive into
/// an in-memory projection — snapshot rows loaded, then every log record's write set applied in
/// order — and reports per-table row counts and the LSN range. It proves the archive is complete,
/// uncorrupted, and structurally replayable <em>without the module DLL</em>; what it cannot prove
/// without the schema is index consistency and residency shape, which is why "boot a real server
/// against a restored directory in staging" remains the documented full-fidelity check. Cheap
/// enough to run in CI against every nightly archive — an unverified backup is a hope, not a
/// backup.
/// </summary>
internal static class ArchiveVerifier
{
    public static BackupVerifyReport Verify(string archivePath)
    {
        if (!File.Exists(archivePath))
            throw new FileNotFoundException($"Archive '{archivePath}' does not exist.", archivePath);
        using var stream = new FileStream(archivePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var reader = new ArchiveFrameReader(stream, archivePath);
        reader.ReadHeader();

        var manifest = ArchiveRestore.ReadJsonFrame<ArchiveManifest>(reader, ArchiveFrameType.Manifest, archivePath);
        var engines = new List<VerifiedEngineReport>();
        foreach (var expectedKey in manifest.Engines)
            engines.Add(VerifyEngine(reader, expectedKey, archivePath));

        var footer = ArchiveRestore.ReadJsonFrame<ArchiveFooter>(reader, ArchiveFrameType.ArchiveEnd, archivePath);
        if (footer.Engines != manifest.Engines.Count)
            throw new InvalidDataException($"'{archivePath}': the archive-end frame counts {footer.Engines} engines but the manifest promised {manifest.Engines.Count}.");
        if (reader.ReadFrame() is not null)
            throw new InvalidDataException($"'{archivePath}': data follows the archive-end frame; the archive is corrupt.");

        return new BackupVerifyReport(manifest.CapturedAtUnixMs, engines);
    }

    private static VerifiedEngineReport VerifyEngine(ArchiveFrameReader reader, string expectedKey, string source)
    {
        var identity = ArchiveRestore.ReadJsonFrame<ArchiveEngineIdentity>(reader, ArchiveFrameType.EngineBegin, source);
        if (identity.Key != expectedKey)
            throw new InvalidDataException($"'{source}': engine '{identity.Key}' arrived where the manifest promised '{expectedKey}'; the archive is corrupt.");
        if (identity.SnapshotLsn > identity.HeadLsn)
            throw new InvalidDataException($"'{source}': engine '{identity.Key}' declares a snapshot (LSN {identity.SnapshotLsn}) past its head ({identity.HeadLsn}); the archive is corrupt.");
        if (identity.BaseLsn > identity.SnapshotLsn)
            throw new InvalidDataException($"'{source}': engine '{identity.Key}' declares a truncation base ({identity.BaseLsn}) past its snapshot ({identity.SnapshotLsn}); the archive is corrupt.");

        // The projection: per table, the set of live keys. Row bytes are parsed but not retained,
        // so verifying an archive costs memory proportional to key count, not state size.
        var tables = new Dictionary<uint, HashSet<string>>();

        long snapshotRows = 0;
        long tailRecords = 0;
        var expectedLsn = identity.SnapshotLsn;
        var sawRecords = false;
        while (reader.ReadFrame() is { } frame)
        {
            if (frame.Type == ArchiveFrameType.SnapshotRow)
            {
                if (identity.SnapshotLsn == 0)
                    throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} is a snapshot row in an engine that declares no snapshot; the archive is corrupt.");
                if (sawRecords)
                    throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} is a snapshot row after log records began; the archive is corrupt.");
                var row = ArchiveRestore.ParseSnapshotRowFrame(frame.Payload, source);
                LiveKeys(tables, row.Table.Value).Add(Convert.ToHexString(row.Key.Span));
                snapshotRows++;
            }
            else if (frame.Type == ArchiveFrameType.LogRecord)
            {
                sawRecords = true;
                CommitRecord record;
                try
                {
                    record = LogRecordCodec.ReadPayload(frame.Payload, frame.Payload.Length);
                }
                catch (Exception exception) when (exception is InvalidDataException or EndOfStreamException or ArgumentOutOfRangeException)
                {
                    throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} does not parse as a log record; the archive is corrupt.", exception);
                }

                expectedLsn++;
                if (record.Lsn != expectedLsn)
                    throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} carries LSN {record.Lsn} where {expectedLsn} was expected; the archive is corrupt.");
                if (record.Lsn > identity.HeadLsn)
                    throw new InvalidDataException($"'{source}': frame {reader.FrameIndex} carries LSN {record.Lsn}, past the declared head {identity.HeadLsn}; the archive is corrupt.");
                foreach (var op in record.WriteSet)
                {
                    var keys = LiveKeys(tables, op.Table.Value);
                    if (op.Kind == RowOpKind.Delete)
                        keys.Remove(Convert.ToHexString(op.Key.Span));
                    else
                        keys.Add(Convert.ToHexString(op.Key.Span));
                }

                tailRecords++;
            }
            else if (frame.Type == ArchiveFrameType.Sidecar)
            {
                ArchiveRestore.ParseSidecarFrame(frame.Payload, source);
            }
            else
            {
                reader.PushBack(frame);
                break;
            }
        }

        if (expectedLsn != identity.HeadLsn)
            throw new InvalidDataException($"'{source}': engine '{identity.Key}' ends at LSN {expectedLsn} but declares head {identity.HeadLsn}; the archive is truncated or corrupt.");

        var footer = ArchiveRestore.ReadJsonFrame<ArchiveEngineFooter>(reader, ArchiveFrameType.EngineEnd, source);
        if (footer.SnapshotRows != snapshotRows || footer.TailRecords != tailRecords)
        {
            throw new InvalidDataException(
                $"'{source}': engine '{identity.Key}' holds {snapshotRows} snapshot rows and {tailRecords} records " +
                $"but its end frame promises {footer.SnapshotRows} and {footer.TailRecords}; the archive is corrupt.");
        }

        return new VerifiedEngineReport(
            new BackupEngineSummary(identity.Key, identity.SourceEpoch, identity.BaseLsn, identity.SnapshotLsn, identity.HeadLsn, snapshotRows, tailRecords),
            tables.Where(pair => pair.Value.Count > 0)
                .OrderBy(pair => pair.Key)
                .ToDictionary(pair => pair.Key, pair => (long)pair.Value.Count));
    }

    private static HashSet<string> LiveKeys(Dictionary<uint, HashSet<string>> tables, uint table)
    {
        if (!tables.TryGetValue(table, out var keys))
            tables[table] = keys = new HashSet<string>(StringComparer.Ordinal);
        return keys;
    }
}
