using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The rebalance loop (road-to-0.2 phase 13): sustained-hot nodes shed a shard through the drain,
/// with hysteresis at every layer. The tests use a tiny window and a near-zero hot threshold so
/// any real sustained load trips the trigger deterministically, and the decision rule — the
/// pair's maximum must strictly improve — is what the assertions actually exercise: it moves
/// exactly one of two co-located busy shards and then provably has nothing left worth moving.
/// </summary>
public sealed class RebalanceLoopTests
{
    private static readonly IReadOnlyDictionary<string, string?> Enabled = new Dictionary<string, string?>
    {
        ["MelangeDb:Cluster:RebalanceEnabled"] = "true",
        ["MelangeDb:Cluster:RebalanceWindowSeconds"] = "1",
        ["MelangeDb:Cluster:RebalanceHotUtilization"] = "0.01",

        // Long on purpose: after the one corrective move, nothing else may move for the rest of
        // the test, so a flapping loop fails the assertion instead of sneaking under it.
        ["MelangeDb:Cluster:ShardMoveMinIntervalMs"] = "600000",
    };

    [Fact]
    public async Task The_loop_moves_one_of_two_busy_colocated_shards_and_does_not_flap()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2, extraSettings: Enabled);
        foreach (var shard in new ulong[] { 80, 81, 82 })
            await fixture.EnsureShardOwnedAsync(shard);

        // Least-loaded assignment puts two of the three on one node; those two get the load.
        var owners = new Dictionary<ulong, string>();
        foreach (var shard in new ulong[] { 80, 81, 82 })
            owners[shard] = fixture.Hub.Membership.GetAssignment(new ShardKey(shard))!.NodeName!;
        var pumped = owners.GroupBy(static pair => pair.Value)
            .First(static group => group.Count() == 2)
            .Select(static pair => pair.Key)
            .ToArray();

        using var pump = new CancellationTokenSource();
        var pumps = pumped.Select(shard => Task.Run(async () =>
        {
            while (!pump.IsCancellationRequested)
            {
                try
                {
                    await fixture.Coordinator.ExecuteOnShardAsync(
                        new ShardKey(shard), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [(uint)shard, 1],
                        pump.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                    // Mid-drain the shard briefly serves nowhere; a live game would retry too.
                }
            }
        }, TestContext.Current.CancellationToken)).ToArray();

        try
        {
            await ClusterFixture.WaitUntilAsync(
                () => fixture.Hub.Metrics.DrainsCompleted == 1
                    && pumped
                        .Select(shard => fixture.Hub.Membership.GetAssignment(new ShardKey(shard))!.NodeName)
                        .Distinct()
                        .Count() == 2,
                "the loop split the two busy shards across the nodes",
                timeoutSeconds: 40);

            // Load stays on; the world is now balanced (u vs u), so the strict-improvement rule
            // has nothing left to move — a second drain within the window would be a flap.
            await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(3)), TestContext.Current.CancellationToken);
            Assert.Equal(1, fixture.Hub.Metrics.DrainsCompleted);
            Assert.Equal(0, fixture.Hub.Metrics.DrainsFailed);
        }
        finally
        {
            pump.Cancel();
            try
            {
                await Task.WhenAll(pumps);
            }
            catch (Exception)
            {
            }
        }
    }

    [Fact]
    public async Task A_hot_node_owning_a_single_shard_is_never_churned()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2, extraSettings: Enabled);
        var origin = await fixture.EnsureShardOwnedAsync(85);
        await fixture.EnsureShardOwnedAsync(86);

        using var pump = new CancellationTokenSource();
        var pumping = Task.Run(async () =>
        {
            while (!pump.IsCancellationRequested)
            {
                try
                {
                    await fixture.Coordinator.ExecuteOnShardAsync(
                        new ShardKey(85), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [85u, 1], pump.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                }
            }
        }, TestContext.Current.CancellationToken);

        try
        {
            // The node is genuinely hot and the loop genuinely refuses: moving the single shard
            // would relocate the hotspot, not rebalance it — the granularity ceiling, logged
            // (EventId 1732) rather than churned.
            await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(4)), TestContext.Current.CancellationToken);
            Assert.Equal(0, fixture.Hub.Metrics.DrainsStarted);
            Assert.Equal(origin.Name, fixture.Hub.Membership.GetAssignment(new ShardKey(85))!.NodeName);
        }
        finally
        {
            pump.Cancel();
            try
            {
                await pumping;
            }
            catch (Exception)
            {
            }
        }
    }

    [Fact]
    public async Task The_loop_is_off_by_default()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2);
        await fixture.EnsureShardOwnedAsync(87);
        await fixture.EnsureShardOwnedAsync(88);
        for (var i = 0; i < 50; i++)
        {
            await fixture.Coordinator.ExecuteOnShardAsync(
                new ShardKey(87), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [87u, 1],
                TestContext.Current.CancellationToken);
        }

        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(3)), TestContext.Current.CancellationToken);
        Assert.Equal(0, fixture.Hub.Metrics.DrainsStarted);
    }
}
