using System.Diagnostics;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The hotspot ceiling, measured rather than assumed. Spatial partitioning cannot split one
/// crowded location: everyone in the town square shares a writer, so the shard's serialized
/// transaction loop is the ceiling no cluster size lifts. Methodology: N players standing in one
/// chunk of one shard on a real shard node (engine, guards, monitor, durable log all in the
/// path); their movement reducers are issued in-process round-robin for a fixed wall window, and
/// sustained commits/second is the measure — in-process on purpose, so the number is the shard's
/// own ceiling, not the websocket stack's. Per-player update rate is then commits/sec divided by
/// N, and the degradation point for a given tick budget is commits/sec divided by the budget.
/// The measured figures are published in docs/CLUSTERING.md; this test asserts only sanity
/// floors, because absolute throughput is the machine's, not the suite's.
/// </summary>
public class HotspotMeasurementTests(ITestOutputHelper output)
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value;

    [Fact]
    public Task The_hotspot_ceiling_is_measured_as_sustained_commits_per_second_on_one_crowded_shard() =>
        MeasureAsync(fsyncPerCommit: true);

    /// <summary>
    /// The same measurement under <c>CommitLog:FsyncPolicy = Interval</c>: the default per-commit
    /// fsync makes the disk the ceiling; interval fsync shows the loop's own ceiling. Both belong
    /// in the docs, because choosing between them is choosing the ceiling.
    /// </summary>
    [Fact]
    public Task The_hotspot_ceiling_under_interval_fsync_is_the_transaction_loops_own() =>
        MeasureAsync(fsyncPerCommit: false);

    /// <summary>
    /// The group-commit measurement (road-to-0.2 phase 17): the same crowded shard, same per-commit
    /// durability, but the reducers issued from concurrent callers — the shape a real crowd
    /// actually has, since every player's Move arrives on its own transport thread. Concurrency is
    /// what group commit converts into batches: while one caller's fsync is in flight the others
    /// run their bodies and park, and the next flush covers them all. The mean records-per-fsync
    /// printed alongside is the batch formation made visible; sequential single-caller throughput
    /// (the original measurement above) is unchanged by design, because a lone caller pays exactly
    /// the old inline latency.
    /// </summary>
    [Fact]
    public Task The_hotspot_ceiling_under_concurrent_callers_shares_fsyncs() =>
        MeasureAsync(fsyncPerCommit: true, concurrency: 16);

    private async Task MeasureAsync(bool fsyncPerCommit, int concurrency = 1)
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 500, failureTimeoutMs: 60_000, spatial: true,
            extraSettings: fsyncPerCommit
                ? null
                : new Dictionary<string, string?>
                {
                    ["MelangeDb:CommitLog:FsyncPolicy"] = "Interval",
                    ["MelangeDb:CommitLog:FsyncIntervalMs"] = "50",
                });
        var shard = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;

        const int Players = 100;
        var players = Enumerable.Range(0, Players)
            .Select(static i => TestTokens.IdentityOf($"crowd-{i}"))
            .ToArray();
        var square = Chunks.Id(1, 1); // Interior: the crowd is the load, not boundary machinery.
        foreach (var player in players)
            shard.ReducerHost.Call("Move", player, square);

        // Warm-up, then the measured window. Every call is one full durable transaction on the
        // shard's single-writer loop — the same loop a real crowd's reducers serialize through.
        RunWindow(shard, players, square, TimeSpan.FromMilliseconds(500), concurrency);
        var window = TimeSpan.FromSeconds(2);
        var fsyncsBefore = shard.Engine.LogFile.FsyncCount;
        var commits = RunWindow(shard, players, square, window, concurrency);
        var fsyncs = shard.Engine.LogFile.FsyncCount - fsyncsBefore;

        var perSecond = commits / window.TotalSeconds;
        output.WriteLine($"Fsync {(fsyncPerCommit ? "per commit (default durability)" : "on a 50ms interval")}, {concurrency} caller(s):");
        output.WriteLine($"Sustained: {perSecond:F0} commits/s across {Players} players in one chunk.");
        if (fsyncPerCommit && fsyncs > 0)
            output.WriteLine($"Group commit: {commits} commits over {fsyncs} fsyncs — mean batch {(double)commits / fsyncs:F1}.");
        output.WriteLine($"Per-player update rate at this crowd size: {perSecond / Players:F1} Hz.");
        output.WriteLine($"Degradation point at a 20 Hz per-player budget: ~{perSecond / 20:F0} players.");
        output.WriteLine($"Degradation point at a 10 Hz per-player budget: ~{perSecond / 10:F0} players.");

        // Sanity floors only: the point of this test is that the number exists and is honest,
        // not that this machine is fast. The floor shrinks with MELANGE_TEST_TIME_SCALE the same
        // way deadlines stretch with it — a saturated CI runner fsyncing a shared disk sustains
        // a fraction of dev-hardware throughput without anything being structurally wrong.
        Assert.True(commits > 0);
        Assert.True(
            perSecond > 100.0 / TestTime.Scale,
            $"a shard sustaining {perSecond:F0} commits/s indicates something structurally wrong");
    }

    private static long RunWindow(ShardRuntime shard, Identity[] players, uint square, TimeSpan window, int concurrency)
    {
        if (concurrency == 1)
        {
            long commits = 0;
            var clock = Stopwatch.StartNew();
            while (clock.Elapsed < window)
            {
                var player = players[commits % players.Length];
                shard.ReducerHost.Call("Move", player, square);
                commits++;
            }

            return commits;
        }

        // Concurrent callers on dedicated threads, each round-robining its own slice of the
        // crowd — the transport's shape, where every player's call arrives on its own thread and
        // the engine's write lock is the serialization point.
        long total = 0;
        var callers = Enumerable.Range(0, concurrency).Select(caller => new Thread(() =>
        {
            long commits = 0;
            var clock = Stopwatch.StartNew();
            while (clock.Elapsed < window)
            {
                var player = players[(caller + (concurrency * commits)) % players.Length];
                shard.ReducerHost.Call("Move", player, square);
                commits++;
            }

            Interlocked.Add(ref total, commits);
        })).ToArray();
        foreach (var thread in callers)
            thread.Start();
        foreach (var thread in callers)
            thread.Join();
        return Interlocked.Read(ref total);
    }
}
