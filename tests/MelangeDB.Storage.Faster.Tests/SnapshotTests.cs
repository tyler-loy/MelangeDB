using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Snapshots and log compaction: snapshot at an LSN, truncate the log behind it, and restart from
/// snapshot plus tail replay to identical state. Truncation never passes the slowest applier
/// checkpoint, any registered truncation floor (the live event subscribers' seam), or the Resume
/// retention window — each floor has its own test. Runs against both store engines: the snapshot
/// machinery is the engine's and must not care which projection sits under it.
/// </summary>
public class SnapshotTests
{
    public static TheoryData<StoreKind> Stores => new(StoreKind.InMemory, StoreKind.Faster);

    private static StoreHarness CreateHarness(StoreKind kind, FakeClock clock, Action<MelangeDbOptions>? configure = null) =>
        new(
            kind,
            options =>
            {
                options.Resume.RetentionWindowSeconds = 0;
                configure?.Invoke(options);
            },
            timeProvider: clock);

    private static void Seed(StoreHarness harness, FakeClock clock, int rows, int offset = 0)
    {
        for (var i = offset; i < offset + rows; i++)
        {
            var id = i;
            harness.Invoke("seed", ctx =>
            {
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = id % 4, Data = StoreContractTests.MakeBlob(id, 600) });
                ctx.Db.Insert(new ItemDefinition { Id = id + 1, Name = $"item-{id}", Value = id });
            });
            clock.Advance(TimeSpan.FromSeconds(1));
        }
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Snapshot_truncate_restart_recovers_identical_state(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 20);
        clock.Advance(TimeSpan.FromMinutes(10));

        var head = harness.Engine.Log.HeadLsn;
        var snapshotLsn = harness.Engine.TakeSnapshot();
        Assert.Equal(head, snapshotLsn);
        Assert.True(File.Exists(harness.Engine.SnapshotPath));

        var log = (FileCommitLog)harness.Engine.Log;
        Assert.Equal(head, log.BaseLsn);
        Assert.Empty(harness.Engine.Log.ReadFrom(1)); // The history is physically gone.

        // Post-snapshot commits form the tail that replays on top of the snapshot.
        harness.Invoke("tail", ctx =>
        {
            ctx.Db.Insert(new TerrainBlob { ChunkId = 999, Region = 0, Data = StoreContractTests.MakeBlob(999, 5000) });
            ctx.Db.Delete<TerrainBlob>(3L);
        });

        var before = harness.Dump();
        harness.Restart();
        Assert.Equal(before, harness.Dump());
        Assert.Equal(head + 1, harness.Engine.Log.HeadLsn);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void AutoInc_survives_snapshot_truncate_restart(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        var lastId = 0UL;
        for (var i = 0; i < 5; i++)
            harness.Invoke("seed", ctx => lastId = ctx.Db.Insert(new Creature { ChunkId = 0, Name = "c", X = 0 }).Id);
        clock.Advance(TimeSpan.FromMinutes(10));
        harness.Engine.TakeSnapshot();
        harness.Restart();

        // The truncated log no longer carries the allocations; the snapshot's sequence table must.
        var next = 0UL;
        harness.Invoke("more", ctx => next = ctx.Db.Insert(new Creature { ChunkId = 0, Name = "d", X = 0 }).Id);
        Assert.True(next > lastId, $"id {next} must be allocated past snapshot-recovered id {lastId}");
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Truncation_never_passes_the_slowest_applier(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 5);
        harness.Engine.Appliers.Register(new StuckApplier(appliedLsn: 4));
        clock.Advance(TimeSpan.FromMinutes(10));

        harness.Engine.TakeSnapshot();
        var log = (FileCommitLog)harness.Engine.Log;
        Assert.Equal(4UL, log.BaseLsn);
        Assert.Equal(5UL, harness.Engine.Log.ReadFrom(1).First().Lsn); // Record 5 survives for the applier.
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Truncation_never_passes_a_live_event_subscriber_checkpoint(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 8);
        harness.Engine.AddTruncationFloor(() => 3UL); // The bus's MinimumLiveCheckpointLsn seam.
        clock.Advance(TimeSpan.FromMinutes(10));

        harness.Engine.TakeSnapshot();
        Assert.Equal(3UL, ((FileCommitLog)harness.Engine.Log).BaseLsn);
        Assert.Equal(4UL, harness.Engine.Log.ReadFrom(1).First().Lsn);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Truncation_never_passes_the_resume_retention_window(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = new StoreHarness(
            kind,
            options => options.Resume.RetentionWindowSeconds = 5,
            timeProvider: clock);

        // Ten commits, one second apart; at snapshot time the newest five are inside the window.
        Seed(harness, clock, 10);

        harness.Engine.TakeSnapshot();
        var log = (FileCommitLog)harness.Engine.Log;

        // now = t10; the window [t5, t10] pins records committed at t5..t9 — LSNs 6..10.
        Assert.Equal(5UL, log.BaseLsn);
        Assert.Equal(6UL, harness.Engine.Log.ReadFrom(1).First().Lsn);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void TruncateLog_off_keeps_the_log(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock, options => options.Snapshots.TruncateLog = false);
        Seed(harness, clock, 5);
        clock.Advance(TimeSpan.FromMinutes(10));

        harness.Engine.TakeSnapshot();
        Assert.Equal(0UL, ((FileCommitLog)harness.Engine.Log).BaseLsn);
        Assert.Equal(1UL, harness.Engine.Log.ReadFrom(1).First().Lsn);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Snapshots_disabled_takes_nothing(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock, options => options.Snapshots.Enabled = false);
        Seed(harness, clock, 3);
        Assert.Null(harness.Engine.TakeSnapshot());
        Assert.False(File.Exists(harness.Engine.SnapshotPath));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Automatic_snapshot_fires_on_the_transaction_interval(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock, options => options.Snapshots.IntervalTransactions = 5);
        Seed(harness, clock, 4); // Four commits: below the interval.
        Assert.False(File.Exists(harness.Engine.SnapshotPath));
        Seed(harness, clock, 1, offset: 4); // The fifth reaches it.
        Assert.True(File.Exists(harness.Engine.SnapshotPath));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Torn_snapshot_temp_file_is_ignored(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 4);
        var before = harness.Dump();

        // A crash mid-snapshot-write leaves only the temp file; the swap never happened.
        File.WriteAllBytes(harness.Engine.SnapshotPath + ".tmp", [1, 2, 3, 4]);
        harness.Restart();
        Assert.Equal(before, harness.Dump());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Truncated_log_with_missing_snapshot_fails_loudly(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 5);
        clock.Advance(TimeSpan.FromMinutes(10));
        harness.Engine.TakeSnapshot();
        File.Delete(harness.Engine.SnapshotPath);

        // Truncated history with no snapshot is unrecoverable; silence would rebuild a partial world.
        Assert.Throws<InvalidDataException>(harness.Restart);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Corrupt_snapshot_behind_truncated_log_fails_loudly(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 5);
        clock.Advance(TimeSpan.FromMinutes(10));
        harness.Engine.TakeSnapshot();

        var bytes = File.ReadAllBytes(harness.Engine.SnapshotPath);
        bytes[^10] ^= 0xFF;
        File.WriteAllBytes(harness.Engine.SnapshotPath, bytes);
        Assert.Throws<InvalidDataException>(harness.Restart);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Stale_snapshot_from_another_epoch_is_ignored(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = CreateHarness(kind, clock);
        Seed(harness, clock, 3);
        harness.Engine.TakeSnapshot();

        // A deleted log mints a fresh epoch; a snapshot surviving from the old one must not load.
        var snapshot = File.ReadAllBytes(harness.Engine.SnapshotPath);
        harness.Engine.Dispose();
        Directory.Delete(harness.Options.CommitLog.Path, recursive: true);
        Directory.CreateDirectory(harness.Options.CommitLog.Path);
        File.WriteAllBytes(harness.Engine.SnapshotPath, snapshot);

        harness.Restart();
        Assert.Empty(harness.Dump());
    }

    private sealed class StuckApplier(ulong appliedLsn) : ILogApplier
    {
        public string Name => "stuck-postgres";

        public ulong AppliedLsn { get; } = appliedLsn;

        public void Apply(CommitRecord record)
        {
        }
    }
}
