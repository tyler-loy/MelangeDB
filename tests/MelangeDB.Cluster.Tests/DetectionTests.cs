using System.Diagnostics;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Phase 30 step 1, detection (design/shard-ha.md): phase 20 found detection is most of the
/// failover window, so these pin how soon a death is declared.
/// </summary>
public class DetectionTests
{
    [Fact]
    public async Task A_silent_node_is_declared_dead_at_its_deadline_not_up_to_half_a_timeout_later()
    {
        // #201: the sweep ran every FailureTimeoutMs/2, so a death was declared anywhere from 1.0
        // to 1.5 timeouts after the last heartbeat. The deadline check runs every twentieth.
        const int timeoutMs = 3_000;
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: timeoutMs);
        var victim = cluster.Nodes[0].Name;

        var stopwatch = Stopwatch.StartNew();
        await cluster.StopNodeAsync(victim);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.Nodes().Single(node => node.NodeName == victim) is { Alive: false },
            "the stopped node was declared dead");

        // Deadline (one timeout, plus at most one heartbeat interval since the last beat landed)
        // and one check period, with room for a loaded machine: well under the old 1.5x.
        var bound = TestTime.Dilated(TimeSpan.FromMilliseconds(timeoutMs * 1.3));
        Assert.True(stopwatch.Elapsed < bound, $"declared dead after {stopwatch.ElapsedMilliseconds} ms; the bound is {bound.TotalMilliseconds} ms");
    }

    [Fact]
    public async Task With_fast_failover_a_dead_process_is_declared_dead_in_about_a_heartbeat()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2,
            heartbeatMs: 150,
            failureTimeoutMs: 30_000,
            extraSettings: new Dictionary<string, string?> { ["MelangeDb:Cluster:FastFailoverOnLinkLoss"] = "true" });
        var owner = await cluster.EnsureShardOwnedAsync(1);
        var survivor = cluster.Nodes.Single(node => node.Name != owner.Name);

        // A dead process drops its node link at once (and the OS drops its log lock), where a
        // timeout would wait the full 30 s.
        await cluster.StopNodeAsync(owner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.Nodes().Single(node => node.NodeName == owner.Name) is { Alive: false },
            "the node whose link dropped was declared dead long before its 30 s timeout",
            timeoutSeconds: 5);

        await ClusterFixture.WaitUntilAsync(
            () => survivor.Runtime.TryGetShard(new ShardKey(1)) is not null,
            "the survivor opened the shard",
            timeoutSeconds: 10);
    }

    [Fact]
    public async Task Fast_failover_is_off_by_default()
    {
        // It is only safe on storage that enforces the log lock across hosts, so a deployment
        // has to say so.
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 30_000);
        var victim = cluster.Nodes[0].Name;

        await cluster.StopNodeAsync(victim);
        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(3)), TestContext.Current.CancellationToken);
        Assert.True(cluster.Hub.Membership.Nodes().Single(node => node.NodeName == victim).Alive);
    }
}
