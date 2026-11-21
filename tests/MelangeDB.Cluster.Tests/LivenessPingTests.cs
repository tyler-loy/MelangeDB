using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The liveness ping (phase 30): a dedicated thread on the node and an inline answer on the hub,
/// so a node whose thread pool is starved is not mistaken for a dead one.
/// </summary>
public class LivenessPingTests
{
    /// <summary>
    /// The heartbeat loop runs on the thread pool. Under load its delay fired seconds late on
    /// healthy nodes (phase 30's soak), so a short failure timeout declared them dead. The pinger
    /// does not depend on the pool: with the heartbeat loop stalled outright, the node stays alive
    /// and keeps its lease for several timeouts.
    /// </summary>
    [Fact]
    public async Task A_node_whose_heartbeat_loop_is_stalled_stays_alive_on_pings_alone()
    {
        const int timeoutMs = 1_000;
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 100, failureTimeoutMs: timeoutMs);
        var node = cluster.Nodes.Single();
        var owner = await cluster.EnsureShardOwnedAsync(1);
        Assert.Same(node, owner);

        node.Runtime.StallHeartbeatLoop = true;
        var heartbeatsBefore = node.Metrics.SentByType.GetValueOrDefault("heartbeat");
        await Task.Delay(TestTime.Dilated(TimeSpan.FromMilliseconds(4 * timeoutMs)), TestContext.Current.CancellationToken);

        Assert.Equal(heartbeatsBefore, node.Metrics.SentByType.GetValueOrDefault("heartbeat")); // Stalled indeed.
        Assert.True(cluster.Hub.Membership.Nodes().Single().Alive, "the hub declared a pinging node dead");
        Assert.Equal(0, cluster.Hub.Metrics.NodesDeclaredDead);
        Assert.True(node.Runtime.LeaseValid(), "the lease lapsed on a pinging node");
        node.Runtime.TryGetShard(new ShardKey(1))!.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 3);
    }

    /// <summary>
    /// A ping's reply renews the lease but, unlike a heartbeat's, carries no assignments, so it
    /// would leave a node the hub had declared dead holding a live lease over shards already given
    /// to someone else. Pings from a dead node are refused; only its heartbeat brings it back.
    /// </summary>
    [Fact]
    public async Task Pings_cannot_bring_back_a_node_the_hub_declared_dead()
    {
        const int timeoutMs = 1_000;
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 100, failureTimeoutMs: timeoutMs);
        var node = cluster.Nodes.Single();
        await cluster.EnsureShardOwnedAsync(1);

        // Partitioned (no heartbeats, no pings) until the hub declares the node dead.
        node.Runtime.SuspendHeartbeats = true;
        await ClusterFixture.WaitUntilAsync(
            () => !cluster.Hub.Membership.Nodes().Single().Alive,
            "the hub declared the silent node dead");

        // Pings resume, heartbeats do not: the hub refuses them and the lease stays lapsed.
        node.Runtime.StallHeartbeatLoop = true;
        node.Runtime.SuspendHeartbeats = false;
        await ClusterFixture.WaitUntilAsync(
            () => node.Metrics.ReceivedByType.GetValueOrDefault("error") > 2,
            "the node's pings were answered with refusals");
        Assert.False(cluster.Hub.Membership.Nodes().Single().Alive);
        Assert.False(node.Runtime.LeaseValid(), "a refused ping renewed the lease");
        Assert.Null(node.Runtime.TryGetShard(new ShardKey(1)));

        // The heartbeat, whose reply carries the assignments, re-registers it.
        node.Runtime.StallHeartbeatLoop = false;
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.Nodes().Single().Alive && node.Runtime.LeaseValid(),
            "the heartbeat brought the node back");
    }
}
