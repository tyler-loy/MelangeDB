using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Node death and fencing, on clusters these tests own (they kill nodes, so nothing is shared).
/// </summary>
public class FailoverTests
{
    [Fact]
    public async Task Killing_a_shard_node_reassigns_its_shards_and_the_new_owner_recovers_them()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 1_500);
        var owner = await cluster.EnsureShardOwnedAsync(1);
        var shard = owner.Runtime.TryGetShard(new ShardKey(1))!;
        var tokenBefore = shard.FencingToken;
        shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 77);
        shard.ReducerHost.Call("ScheduleTick", ClusterFixture.Caller, 60L);
        await ClusterFixture.WaitUntilAsync(
            () => shard.Engine.CommittedView.Find<TickCount>(1L) is not null,
            "the timer fired at least once on the original owner");

        await cluster.StopNodeAsync(owner.Name);
        var survivor = cluster.Nodes.Single(n => n.Name != owner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => survivor.Runtime.TryGetShard(new ShardKey(1)) is not null,
            "the hub reassigned the dead node's shard to the survivor");

        var recovered = survivor.Runtime.TryGetShard(new ShardKey(1))!;
        Assert.True(recovered.FencingToken > tokenBefore); // A new ownership term.
        Assert.Equal(77, recovered.Engine.CommittedView.Scan<Mob>().Single().Hp); // The shard's log is the shard.

        // Recovery is not creation: the new owner opens a log that already has a head, so init
        // reducers do not run again and the shard keeps the one set of timer rows it was seeded
        // with. Re-seeding here would double every timer on every reassignment.
        Assert.Equal(1, recovered.Engine.CommittedView.Count<SeededTick>());

        // Scheduled reducers keep firing — on the new owner, and only there.
        var ticksAtTakeover = recovered.Engine.CommittedView.Find<TickCount>(1L)?.Count ?? 0;
        await ClusterFixture.WaitUntilAsync(
            () => (recovered.Engine.CommittedView.Find<TickCount>(1L)?.Count ?? 0) > ticksAtTakeover,
            "the recovered timer resumed on the new owner");

        // The revived node re-registers and learns it owns nothing; the shard stays where it is.
        await cluster.StartNodeAsync(owner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.Nodes().Count(static n => n.Alive) == 2,
            "the revived node re-registered");
        await Task.Delay(400, TestContext.Current.CancellationToken);
        Assert.Empty(cluster.Node(owner.Name).Runtime.OwnedShards);
        Assert.Equal(survivor.Name, cluster.Hub.Membership.GetAssignment(new ShardKey(1))!.NodeName);
    }

    [Fact]
    public async Task A_partitioned_node_fences_its_own_writes_and_resumes_under_a_bumped_token_when_healed()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 1_200);
        var owner = await cluster.EnsureShardOwnedAsync(1);
        var shard = owner.Runtime.TryGetShard(new ShardKey(1))!;
        var tokenBefore = shard.FencingToken;
        shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 5);

        // Partition: the node stops heartbeating. Its lease expires on the same clock the hub
        // uses to suspect it dead, so by the time the hub could reassign, the node has already
        // stopped accepting writes — the wrongly-suspected-dead node cannot keep writing players
        // it no longer owns.
        owner.Runtime.SuspendHeartbeats = true;
        await ClusterFixture.WaitUntilAsync(() => !owner.Runtime.LeaseValid(), "the lease expired");

        var fenced = Assert.Throws<ShardFencedException>(
            () => shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 6));
        Assert.Contains("lease", fenced.Message);
        Assert.Contains("fenc", fenced.Message);

        // The melange-shard health check reports the contested ownership.
        var health = owner.App!.Services.GetRequiredService<MelangeShardHealthCheck>();
        var whileFenced = await health.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Unhealthy, whileFenced.Status);

        // The hub, on the same clock, suspects the node dead and orphans the shard (there is no
        // other node to take it) under a bumped fencing token.
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.GetAssignment(new ShardKey(1))!.NodeName is null,
            "the hub marked the partitioned node dead and orphaned its shard");

        // Heal the partition: the node re-registers, gets the (unowned) shard back under a new
        // fencing token, reopens it from its log, and serves writes again.
        owner.Runtime.SuspendHeartbeats = false;
        ShardRuntime? reopened = null;
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                reopened = owner.Runtime.TryGetShard(new ShardKey(1));
                return reopened is not null && reopened.FencingToken > tokenBefore && owner.Runtime.LeaseValid();
            },
            "the healed node reacquired the shard under a bumped fencing token");

        reopened!.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 7);
        Assert.Equal(2, reopened.Engine.CommittedView.Count<Mob>());

        var afterHeal = await health.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Healthy, afterHeal.Status);
    }
}
