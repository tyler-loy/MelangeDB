using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Scale-in (road-to-0.2 phase 14): when the fleet's aggregate sustained load fits on one node
/// fewer under the cold threshold, the emptiest node is drained and handed back — behind its own
/// switch, floored by <c>Cluster:MinNodes</c>, and never flapping against scale-out across the
/// dead zone between the two thresholds.
/// </summary>
public sealed class ScaleInTests
{
    /// <summary>Pumps reducer calls at the given shards until cancelled, riding out drain windows.</summary>
    private static Task[] Pump(ClusterFixture fixture, ulong[] shards, CancellationToken ct) =>
        shards.Select(shard => Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await fixture.Coordinator.ExecuteOnShardAsync(
                        new ShardKey(shard), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [(uint)shard, 1], ct);
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

    private static async Task DrainPumpsAsync(Task[] pumps)
    {
        try
        {
            await Task.WhenAll(pumps);
        }
        catch (Exception)
        {
        }
    }

    [Fact]
    public async Task The_whole_curve_grows_to_the_ceiling_then_drains_back_to_the_floor_and_the_surplus_exits()
    {
        var provisioner = new ScriptedProvisioner();
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 2,
            failureTimeoutMs: 5_000,
            extraSettings: new Dictionary<string, string?>
            {
                ["MelangeDb:Cluster:RebalanceEnabled"] = "true",
                ["MelangeDb:Cluster:RebalanceWindowSeconds"] = "1",
                ["MelangeDb:Cluster:RebalanceHotUtilization"] = "0.01",
                ["MelangeDb:Cluster:RebalanceColdUtilization"] = "0.005",
                ["MelangeDb:Cluster:ShardMoveMinIntervalMs"] = "600000",
                ["MelangeDb:Cluster:MaxNodes"] = "3",
                ["MelangeDb:Cluster:MinNodes"] = "2",
                ["MelangeDb:Cluster:ScaleInEnabled"] = "true",
                ["MelangeDb:Cluster:ScaleInCooldownMs"] = "2000",
                ["MelangeDb:Cluster:ProvisionTicketTimeoutMs"] = "60000",
            },
            configureHub: services => services.AddSingleton<INodeProvisioner>(provisioner));

        var testCt = TestContext.Current.CancellationToken;
        provisioner.OnRequest = (_, _) =>
        {
            _ = Task.Run(() => fixture.AddNodeAsync("node-c"), testCt);
            return Task.FromResult(new ProvisionTicket("ticket-1", "node-c"));
        };
        provisioner.OnDecommission = (name, _) => fixture.StopNodeAsync(name);

        ulong[] shards = [70, 71, 72];
        foreach (var shard in shards)
            await fixture.EnsureShardOwnedAsync(shard);

        // 2 p.m.: three busy shards on two nodes. Move-one cannot help (hot to hot), so the loop
        // provisions node-c and spreads onto it — one shard per node.
        using var pump = new CancellationTokenSource();
        var pumps = Pump(fixture, shards, pump.Token);
        try
        {
            await ClusterFixture.WaitUntilAsync(
                () => fixture.Hub.Metrics.ProvisionsFulfilled == 1
                    && fixture.Hub.Metrics.DrainsCompleted == 1
                    && shards.Select(s => fixture.Hub.Membership.GetAssignment(new ShardKey(s))!.NodeName).Distinct().Count() == 3,
                "the fleet grew to three nodes and the load spread one shard per node",
                timeoutSeconds: 60);

            // Still all-hot, but the fleet is at Cluster:MaxNodes: the ceiling holds, provably —
            // exactly one request ever, however long the pressure lasts.
            await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(3)), testCt);
            Assert.Single(provisioner.Requests);
            Assert.Empty(provisioner.Decommissions);
        }
        finally
        {
            // 2 a.m.: the crowd leaves.
            pump.Cancel();
            await DrainPumpsAsync(pumps);
        }

        // The fleet cools, the emptiest node is consolidated away, and the surplus process exits.
        await ClusterFixture.WaitUntilAsync(
            () => provisioner.Decommissions.Count == 1,
            "the fleet drained its emptiest node and handed it back",
            timeoutSeconds: 60);
        await ClusterFixture.WaitUntilAsync(
            () => fixture.Hub.Membership.Nodes().Count(static n => n.Alive) == 2,
            "the decommissioned node left membership",
            timeoutSeconds: 30);

        // The floor holds: two nodes is Cluster:MinNodes, so consolidation stops for good.
        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(4)), testCt);
        Assert.Single(provisioner.Decommissions);
        Assert.Single(provisioner.Requests);
        Assert.Equal(0, fixture.Hub.Metrics.DrainsFailed);

        // Every shard survived the whole curve and serves from a live node.
        foreach (var shard in shards)
        {
            await fixture.Coordinator.ExecuteOnShardAsync(
                new ShardKey(shard), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [(uint)shard, 1], testCt);
        }
    }

    [Fact]
    public async Task Load_oscillating_inside_the_dead_zone_provisions_and_decommissions_nothing()
    {
        var provisioner = new ScriptedProvisioner();
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 2,
            extraSettings: new Dictionary<string, string?>
            {
                ["MelangeDb:Cluster:RebalanceEnabled"] = "true",

                // A window longer than the oscillation period: the sustained mean sits between
                // the far-apart thresholds however the instantaneous load wobbles — the dead zone
                // and the window doing exactly the anti-flap work the design assigns them.
                ["MelangeDb:Cluster:RebalanceWindowSeconds"] = "3",
                ["MelangeDb:Cluster:RebalanceHotUtilization"] = "0.75",
                ["MelangeDb:Cluster:RebalanceColdUtilization"] = "0.0001",
                ["MelangeDb:Cluster:ShardMoveMinIntervalMs"] = "600000",
                ["MelangeDb:Cluster:MaxNodes"] = "4",
                ["MelangeDb:Cluster:MinNodes"] = "1",
                ["MelangeDb:Cluster:ScaleInEnabled"] = "true",
                ["MelangeDb:Cluster:ScaleInCooldownMs"] = "0",
            },
            configureHub: services => services.AddSingleton<INodeProvisioner>(provisioner));

        await fixture.EnsureShardOwnedAsync(74);
        await fixture.EnsureShardOwnedAsync(75);

        // Bursts with gaps: on for a beat, off for a beat, for several whole windows.
        var testCt = TestContext.Current.CancellationToken;
        var end = DateTime.UtcNow + TestTime.Dilated(TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < end)
        {
            using var burst = CancellationTokenSource.CreateLinkedTokenSource(testCt);
            burst.CancelAfter(400);
            var pumps = Pump(fixture, [74, 75], burst.Token);
            await DrainPumpsAsync(pumps);
            await Task.Delay(600, testCt);
        }

        Assert.Empty(provisioner.Requests);
        Assert.Empty(provisioner.Decommissions);
        Assert.Equal(0, fixture.Hub.Metrics.DrainsStarted);
    }

    [Fact]
    public async Task Scale_in_is_off_by_default_even_with_a_provisioner_and_a_cold_fleet()
    {
        var provisioner = new ScriptedProvisioner();
        await using var fixture = await ClusterFixture.StartAsync(
            shardNodes: 2,
            extraSettings: new Dictionary<string, string?>
            {
                ["MelangeDb:Cluster:RebalanceEnabled"] = "true",
                ["MelangeDb:Cluster:RebalanceWindowSeconds"] = "1",
                ["MelangeDb:Cluster:MaxNodes"] = "4",
            },
            configureHub: services => services.AddSingleton<INodeProvisioner>(provisioner));

        await fixture.EnsureShardOwnedAsync(76);
        await fixture.EnsureShardOwnedAsync(77);

        // An idle two-node fleet is as cold as fleets get, and MinNodes defaults to 1 — only the
        // switch is holding consolidation back.
        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(4)), TestContext.Current.CancellationToken);
        Assert.Empty(provisioner.Decommissions);
        Assert.Equal(0, fixture.Hub.Metrics.DrainsStarted);
        Assert.Equal(2, fixture.Hub.Membership.Nodes().Count(static n => n.Alive));
    }
}
