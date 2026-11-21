using System.Diagnostics;
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

        // Liveness is measured on both ends while the cluster is healthy: the hub times the gaps
        // between a node's heartbeats, the node times how long each lease had to last.
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Metrics.HeartbeatGaps.Count > 0 && owner.Runtime.Metrics.LeaseRenewalGaps.Count > 0,
            "both ends recorded liveness gaps");
        var deathsBefore = cluster.Hub.Metrics.NodesDeclaredDead;

        await cluster.StopNodeAsync(owner.Name);
        var survivor = cluster.Nodes.Single(n => n.Name != owner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => survivor.Runtime.TryGetShard(new ShardKey(1)) is not null,
            "the hub reassigned the dead node's shard to the survivor");
        Assert.True(cluster.Hub.Metrics.NodesDeclaredDead > deathsBefore, "the death was counted");

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

        // Refused either way: fenced by the lapsed lease, or, once the watchdog has parked the
        // shard, by the closed engine. Neither is a write.
        var refused = Assert.ThrowsAny<Exception>(
            () => shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 6));
        Assert.True(
            refused is ShardFencedException or TransientRejectionException,
            $"a fenced node must refuse, not fail: {refused.GetType().Name}: {refused.Message}");

        // #216: the lapsed lease parks the shard, which releases its log for whichever node the
        // hub names next, instead of a partitioned owner holding it open indefinitely.
        await ClusterFixture.WaitUntilAsync(() => owner.Runtime.TryGetShard(new ShardKey(1)) is null, "the shard was parked");
        Assert.True(owner.Runtime.Metrics.LeaseLapses >= 1, "the lapse was counted");

        // The engine closes just after the shard leaves the shard set, so the lock frees a moment
        // later. A new owner's open retries the same way.
        var lockPath = Path.Combine(shard.Directory, "log", "melange.lock");
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                try
                {
                    using (File.Open(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                        return true;
                }
                catch (IOException)
                {
                    return false;
                }
            },
            "the parked engine released the shard's log");

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

    /// <summary>
    /// #198: the node that adopts a dead node's shard recovers it off its heartbeat path. A
    /// recovery that outlasts Cluster:FailureTimeoutMs used to run inside the heartbeat loop, so the
    /// healthy adopter went silent, the hub declared it dead too, and its lease lapsed on every shard
    /// it owned. Here the recovery is made to take three failure timeouts, and the adopter stays
    /// alive, keeps its lease, and keeps serving the shard it already had the whole time.
    /// </summary>
    [Fact]
    public async Task A_recovery_longer_than_the_failure_timeout_does_not_fence_the_node_doing_it()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 1_500);
        var owner = await cluster.EnsureShardOwnedAsync(1);
        var adopter = await cluster.EnsureShardOwnedAsync(2);
        Assert.NotEqual(owner.Name, adopter.Name); // Least-loaded placement puts them apart.
        owner.Runtime.TryGetShard(new ShardKey(1))!.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 77);

        var slowOpen = TestTime.Dilated(TimeSpan.FromMilliseconds(4_500));
        var recovering = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        adopter.Runtime.BeforeOpen = shard =>
        {
            if (shard.Value != 1)
                return;
            recovering.TrySetResult();
            Thread.Sleep(slowOpen);
        };

        await cluster.StopNodeAsync(owner.Name);
        await recovering.Task.WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken);

        // Throughout the recovery: the hub still counts the adopter alive, its lease holds, and
        // the shard it already owned takes writes.
        var clock = Stopwatch.StartNew();
        var writes = 0;
        while (adopter.Runtime.TryGetShard(new ShardKey(1)) is null)
        {
            Assert.True(clock.Elapsed < slowOpen + TestTime.Dilated(TimeSpan.FromSeconds(10)), "the slow open never finished");
            Assert.True(
                cluster.Hub.Membership.Nodes().Single(n => n.NodeName == adopter.Name).Alive,
                $"the hub declared the adopter dead {clock.Elapsed.TotalMilliseconds:F0} ms into its recovery");
            Assert.True(adopter.Runtime.LeaseValid(), "the adopter's lease lapsed while it recovered");
            adopter.Runtime.TryGetShard(new ShardKey(2))!.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 2u, writes++);
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        // The test only means something if the recovery outlasted the failure timeout it is about.
        Assert.True(clock.Elapsed > TimeSpan.FromMilliseconds(TestTime.Scale * 1_500), $"recovery took only {clock.Elapsed}");
        Assert.True(writes > 0);
        var recovered = adopter.Runtime.TryGetShard(new ShardKey(1))!;
        Assert.Equal(77, recovered.Engine.CommittedView.Scan<Mob>().Single().Hp);
        Assert.Equal(adopter.Name, cluster.Hub.Membership.GetAssignment(new ShardKey(1))!.NodeName);
    }

    /// <summary>
    /// A failed open (the previous owner still holding the log, a damaged directory) is retried on
    /// the next heartbeat and costs the node nothing else. It used to throw out of the heartbeat
    /// loop and drop the hub link, which is every other shard's lease too.
    /// </summary>
    [Fact]
    public async Task A_failed_open_is_retried_without_dropping_the_hub_link()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 3_000);
        var node = cluster.Nodes.Single();
        var failures = 0;
        node.Runtime.BeforeOpen = shard =>
        {
            if (shard.Value == 3 && Interlocked.Increment(ref failures) <= 3)
                throw new IOException("simulated: the log is still held by the previous owner");
        };
        var authsBefore = node.Runtime.Metrics.SentByType.GetValueOrDefault("auth");

        await cluster.EnsureShardOwnedAsync(3);

        Assert.True(Volatile.Read(ref failures) > 3, "the open was retried past its failures");

        // Every reconnect authenticates afresh; one link throughout means no new auth.
        Assert.Equal(authsBefore, node.Runtime.Metrics.SentByType.GetValueOrDefault("auth"));
        Assert.True(node.Runtime.LeaseValid());
        Assert.True(cluster.Hub.Membership.Nodes().Single().Alive);
    }
}
