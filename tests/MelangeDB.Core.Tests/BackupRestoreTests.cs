using System.Text.Json;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The <c>.mbak</c> round trip (road-to-0.2 phase 15): a backup is the truth — snapshot plus log
/// tail plus sidecars — a restore materializes a directory ordinary recovery boots with a fresh
/// epoch, and verify proves an archive good before the day it is needed. The round trip is a
/// test, not a promise.
/// </summary>
public class BackupRestoreTests : IDisposable
{
    private readonly EngineHarness _harness = new(tables: [typeof(Player), typeof(InventoryItem), typeof(TerrainChunk), typeof(DecayTimer)]);
    private readonly List<string> _extraRoots = [];

    public void Dispose()
    {
        _harness.Dispose();
        foreach (var root in _extraRoots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private string TempDir()
    {
        var dir = Directory.CreateTempSubdirectory("melange-backup-").FullName;
        _extraRoots.Add(dir);
        return dir;
    }

    private string LogDir => _harness.Options.CommitLog.Path;

    private MelangeEngine BootRestored(string restoredDir)
    {
        var options = new MelangeDbOptions
        {
            CommitLog = { Path = restoredDir },
            HotStore = { Path = Path.Combine(TempDir(), "hot") },
        };
        return new MelangeEngine(options, EngineHarness.GeneratedRegistry(typeof(Player), typeof(InventoryItem), typeof(TerrainChunk), typeof(DecayTimer)));
    }

    private static List<string> Dump(MelangeEngine engine)
    {
        var dump = new List<string>();
        foreach (var table in engine.Schema.Tables)
        {
            foreach (var pair in engine.HotStore.Scan(table.Id))
                dump.Add($"{table.Name}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}");
        }

        return dump;
    }

    /// <summary>Resident rows, AutoInc allocations, a blob table, a snapshot, then a live tail with a delete.</summary>
    private void PopulateWithSnapshotAndTail()
    {
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("alice"), RoomId = 1, X = 1, Y = 2, Name = "alice" });
            ctx.Db.Insert(new Player { Id = Identity.Hash("bob"), RoomId = 1, X = 3, Y = 4, Name = "bob" });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("alice"), ItemName = "sword", Quantity = 1 });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("alice"), ItemName = "shield", Quantity = 1 });
            ctx.Db.Insert(new TerrainChunk { ChunkId = 7, Data = [1, 2, 3, 4, 5], Kind = ChunkKind.Ore });

            // A scheduled row: a timer whose row is state like any other, and whose survival is
            // what makes a restored world's simulation tick instead of standing still.
            ctx.Db.Insert(new DecayTimer { ScheduledAt = ScheduleAt.Interval(TimeSpan.FromMinutes(5)), Target = "ore-7" });
        });

        // Retention floor at zero so the snapshot genuinely truncates: the archive must round-trip
        // a log whose early records exist only in the snapshot.
        _harness.Options.Resume.RetentionWindowSeconds = 0;
        var snapshotLsn = _harness.Engine.TakeSnapshot();
        Assert.NotNull(snapshotLsn);
        Assert.True(_harness.Engine.Log.BaseLsn > 0, "the snapshot should have truncated the log");

        _harness.Invoke("Tail", ctx =>
        {
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "torch", Quantity = 3 });
            ctx.Db.Delete<InventoryItem>(2UL); // The shield dies in the tail — deletes must replay.
            ctx.Db.Insert(new TerrainChunk { ChunkId = 8, Data = [9, 9, 9], Kind = ChunkKind.Rock });
        });
    }

    [Fact]
    public void The_round_trip_restores_byte_identical_state_under_a_fresh_epoch_and_sequences_continue()
    {
        PopulateWithSnapshotAndTail();
        var dumpBefore = Dump(_harness.Engine);
        var epochBefore = _harness.Engine.Log.EpochId;
        var headBefore = _harness.Engine.Log.HeadLsn;
        _harness.Engine.Dispose();

        var archive = Path.Combine(TempDir(), "world.mbak");
        var backup = MelangeBackup.Create(LogDir, archive);
        var engine = Assert.Single(backup.Engines);
        Assert.Equal(epochBefore, engine.SourceEpoch);
        Assert.Equal(headBefore, engine.HeadLsn);
        Assert.True(engine.SnapshotLsn > 0);
        Assert.True(engine.TailRecords > 0);

        // An unverified backup is a hope, not a backup.
        var report = MelangeBackup.Verify(archive);
        var verified = Assert.Single(report.Engines);
        Assert.Equal(engine, verified.Identity);
        Assert.Equal(dumpBefore.Count, verified.RowsByTable.Values.Sum());

        var restoredDir = Path.Combine(TempDir(), "restored");
        var restore = MelangeBackup.Restore(archive, restoredDir);
        var restored = Assert.Single(restore.Engines);
        Assert.NotEqual(epochBefore, restored.NewEpoch);
        Assert.Equal(headBefore, restored.HeadLsn);

        using var rebooted = BootRestored(restoredDir);
        Assert.Equal(dumpBefore, Dump(rebooted));

        // The fresh epoch is what refuses stale resume cursors — minted always, no keep-epoch flag.
        Assert.Equal(restored.NewEpoch, rebooted.Log.EpochId);
        Assert.NotEqual(epochBefore, rebooted.Log.EpochId);

        // Sequences restore from the snapshot header and re-observe the tail: the next id is above
        // everything the world ever handed out, including the deleted shield's.
        rebooted.Invoke("After", EngineHarness.Caller, ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "lantern", Quantity = 1 });
            Assert.Equal(4UL, item.Id);
        });
    }

    [Fact]
    public void A_world_that_never_snapshotted_round_trips_from_its_log_alone()
    {
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("carol"), RoomId = 2, X = 0, Y = 0, Name = "carol" });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("carol"), ItemName = "rope", Quantity = 2 });
        });
        var dumpBefore = Dump(_harness.Engine);
        _harness.Engine.Dispose();

        var archive = Path.Combine(TempDir(), "world.mbak");
        var summary = MelangeBackup.Create(LogDir, archive);
        Assert.Equal(0UL, summary.Engines[0].SnapshotLsn);
        MelangeBackup.Verify(archive);

        var restoredDir = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restoredDir);
        using var rebooted = BootRestored(restoredDir);
        Assert.Equal(dumpBefore, Dump(rebooted));
    }

    [Fact]
    public void A_live_directory_is_refused_because_copying_a_live_directory_is_the_bug_this_verb_replaces()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("dan"), RoomId = 1, X = 0, Y = 0, Name = "dan" }));

        // The engine is live: it holds the directory's melange.lock exclusively, and the backup
        // probes that same lock precisely to be refused here. (The lock file exists because a
        // share-mode probe on the log itself only works on Windows; Unix maps only
        // FileShare.None onto a real lock, so this test would pass there and fail on CI.)
        var exception = Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Create(LogDir, Path.Combine(TempDir(), "world.mbak")));
        Assert.Contains("live process", exception.Message);
    }

    [Fact]
    public void Restore_refuses_a_non_empty_target_and_a_directory_that_is_not_a_data_directory_is_refused_by_backup()
    {
        PopulateWithSnapshotAndTail();
        _harness.Engine.Dispose();
        var archive = Path.Combine(TempDir(), "world.mbak");
        MelangeBackup.Create(LogDir, archive);

        var occupied = TempDir();
        File.WriteAllText(Path.Combine(occupied, "keepsake.txt"), "do not overwrite me");
        var refusal = Assert.Throws<InvalidOperationException>(() => MelangeBackup.Restore(archive, occupied));
        Assert.Contains("not empty", refusal.Message);
        Assert.Equal("do not overwrite me", File.ReadAllText(Path.Combine(occupied, "keepsake.txt")));

        var notADataDir = TempDir();
        var backupRefusal = Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Create(notADataDir, Path.Combine(TempDir(), "x.mbak")));
        Assert.Contains("melange.log", backupRefusal.Message);
    }

    [Fact]
    public void Every_single_bit_flip_fails_verify_and_restore_refuses_the_same_archive()
    {
        PopulateWithSnapshotAndTail();
        _harness.Engine.Dispose();
        var root = TempDir();
        var archive = Path.Combine(root, "world.mbak");
        MelangeBackup.Create(LogDir, archive);
        var pristine = File.ReadAllBytes(archive);

        var corrupt = Path.Combine(root, "corrupt.mbak");
        var restoreTarget = Path.Combine(root, "never-materialized");
        for (var offset = 0; offset < pristine.Length; offset++)
        {
            var bytes = (byte[])pristine.Clone();
            bytes[offset] ^= (byte)(1 << (offset % 8));
            File.WriteAllBytes(corrupt, bytes);

            Assert.Throws<InvalidDataException>(() => MelangeBackup.Verify(corrupt));

            // Restore refuses the same archive rather than materializing a partial world — sampled,
            // because a restore attempt costs directory churn and verify already ran at every byte.
            if (offset % 16 == 0)
            {
                Assert.Throws<InvalidDataException>(() => MelangeBackup.Restore(corrupt, restoreTarget));
                Assert.False(Directory.Exists(restoreTarget), "a refused restore must leave nothing behind");
            }
        }
    }

    [Fact]
    public void A_truncated_archive_fails_verify_at_every_cut()
    {
        PopulateWithSnapshotAndTail();
        _harness.Engine.Dispose();
        var root = TempDir();
        var archive = Path.Combine(root, "world.mbak");
        MelangeBackup.Create(LogDir, archive);
        var pristine = File.ReadAllBytes(archive);

        var cut = Path.Combine(root, "cut.mbak");
        for (var length = 0; length < pristine.Length; length += 7)
        {
            File.WriteAllBytes(cut, pristine.AsSpan(0, length).ToArray());
            Assert.Throws<InvalidDataException>(() => MelangeBackup.Verify(cut));
        }
    }

    [Fact]
    public void Subscriber_checkpoints_ride_along_and_clamp_to_the_restored_head()
    {
        PopulateWithSnapshotAndTail();
        var head = _harness.Engine.Log.HeadLsn;
        _harness.Engine.Dispose();

        // A checkpoint sidecar as the bus would leave it — one subscriber behind the head, one
        // (as a rewound archive can produce) pointing past it.
        File.WriteAllBytes(
            Path.Combine(LogDir, "melange.events.json"),
            JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, EventCheckpointStore.Entry>
            {
                ["steady"] = new() { Lsn = 1, LastActiveUnixMs = 5 },
                ["ahead"] = new() { Lsn = head + 100, LastActiveUnixMs = 5 },
            }));

        var archive = Path.Combine(TempDir(), "world.mbak");
        MelangeBackup.Create(LogDir, archive);
        var restoredDir = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restoredDir);

        var entries = JsonSerializer.Deserialize<Dictionary<string, EventCheckpointStore.Entry>>(
            File.ReadAllBytes(Path.Combine(restoredDir, "melange.events.json")))!;
        Assert.Equal(1UL, entries["steady"].Lsn);
        Assert.Equal(head, entries["ahead"].Lsn); // Clamped: resuming past the head would silently skip everything after the restore.
    }

    [Fact]
    public void A_directory_recovery_would_refuse_is_refused_by_backup_too()
    {
        PopulateWithSnapshotAndTail();
        _harness.Engine.Dispose();

        // A truncated log whose snapshot has gone missing: recovery would throw, so archiving it
        // as if it were a bootable world would be a lie.
        File.Delete(Path.Combine(LogDir, "melange.snapshot"));
        var exception = Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Create(LogDir, Path.Combine(TempDir(), "world.mbak")));
        Assert.Contains("would not boot", exception.Message);
    }

    [Fact]
    public void An_interrupted_backup_leaves_no_plausible_archive_behind()
    {
        PopulateWithSnapshotAndTail();
        _harness.Engine.Dispose();
        var output = Path.Combine(TempDir(), "world.mbak");

        // Sabotage: the temp file's destination directory disappears mid-write is hard to fake
        // portably, but a source that fails half-way is not — delete the snapshot's trailing CRC.
        var snapshotPath = Path.Combine(LogDir, "melange.snapshot");
        var snapshotBytes = File.ReadAllBytes(snapshotPath);
        File.WriteAllBytes(snapshotPath, snapshotBytes.AsSpan(0, snapshotBytes.Length - 1).ToArray());

        Assert.ThrowsAny<Exception>(() => MelangeBackup.Create(LogDir, output));
        Assert.False(File.Exists(output), "a failed backup must not leave a file that looks like an archive");
    }
}
