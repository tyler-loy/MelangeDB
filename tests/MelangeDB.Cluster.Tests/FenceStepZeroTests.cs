using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Phase 30 step 0: the fence a warm replica will rely on, made to hold before anything is
/// promoted faster (design/shard-ha.md). Each test pins one of the gaps the design survey found.
/// </summary>
public class FenceStepZeroTests
{
    [Fact]
    public async Task The_lease_follows_the_hubs_failure_timeout_not_the_nodes_own()
    {
        // #214: the node used to measure its lease against its own setting, so one configured
        // longer than its hub held the lease long after the hub declared it dead.
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1,
            heartbeatMs: 150,
            failureTimeoutMs: 1_200,
            shardNodeSettings: new Dictionary<string, string?> { ["MelangeDb:Cluster:FailureTimeoutMs"] = "600000" });
        var owner = await cluster.EnsureShardOwnedAsync(1);

        owner.Runtime.SuspendHeartbeats = true;
        await ClusterFixture.WaitUntilAsync(
            () => !owner.Runtime.LeaseValid(),
            "the lease lapsed on the hub's 1.2 s timeout, not the node's 10 minutes",
            timeoutSeconds: 15);
    }

    [Fact]
    public async Task The_lease_lapses_before_the_hub_can_declare_the_node_dead()
    {
        // #214: the lease runs from the heartbeat's send, shortened by a margin, so a node can
        // never still believe it owns a shard the hub has already reassigned.
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 1_200);
        var owner = await cluster.EnsureShardOwnedAsync(1);

        owner.Runtime.SuspendHeartbeats = true;
        var leaseLapsedFirst = false;
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                var dead = !cluster.Hub.Membership.Nodes().Single().Alive;
                if (!dead && !owner.Runtime.LeaseValid())
                    leaseLapsedFirst = true;
                Assert.False(dead && !leaseLapsedFirst, "the hub declared the node dead while the node still held its lease");
                return dead;
            },
            "the hub declared the silent node dead");
        Assert.True(leaseLapsedFirst);
    }

    [Fact]
    public async Task Internal_commits_are_fenced_once_the_lease_lapses()
    {
        // #215: replica batches, border copies, and handoff markers went through the guard's
        // internal-origin early return and appended with an expired lease.
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var owner = await cluster.EnsureShardOwnedAsync(1);
        var shard = owner.Runtime.TryGetShard(new ShardKey(1))!;
        var leaseValid = false;
        var guard = new ShardCommitGuard(
            shard.Engine.Schema,
            () => shard.Engine.HotStore,
            shard.Shard,
            strategy: null,
            spanCheckEnabled: static () => false,
            leaseValid: () => leaseValid,
            frozenRows: static () => new HashSet<(TableId, RowKey)>(),
            borrowedOwner: static (_, _) => null);

        Assert.Throws<ShardFencedException>(() => guard.Validate("melange/replica", [], CommitOrigin.Internal));

        leaseValid = true;
        guard.Validate("melange/replica", [], CommitOrigin.Internal);
    }

    [Fact]
    public async Task Only_a_shards_current_owner_may_publish_its_border_and_events()
    {
        // #218: border batches, border resets, and forwarded events were routed without asking
        // whether the sender still owned the shard they came from.
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var owner = await cluster.EnsureShardOwnedAsync(1);
        var other = cluster.Nodes.Single(node => node.Name != owner.Name);

        Assert.True(cluster.Hub.IsCurrentOwner(owner.Name, 1));
        Assert.False(cluster.Hub.IsCurrentOwner(other.Name, 1));
        Assert.False(cluster.Hub.IsCurrentOwner(null, 1));
        Assert.False(cluster.Hub.IsCurrentOwner(owner.Name, 999));
    }

    [Fact]
    public async Task A_failing_membership_store_does_not_stop_the_failure_detector()
    {
        // #217: the dead-node sweep runs on a bare timer callback, where an exception is
        // unhandled. A membership-store outage would have taken the hub process, and this test
        // host with it, down.
        var store = new FlakyMembershipStore();
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2,
            heartbeatMs: 150,
            failureTimeoutMs: 1_200,
            configureHub: services => services.AddSingleton<IMembershipStore>(store));

        store.FailNodeListing = true;
        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(2)), TestContext.Current.CancellationToken);
        Assert.True(store.FailedListings > 0, "the sweep never ran into the failing store");
        store.FailNodeListing = false;

        // The detector still works once the store recovers: a stopped node is declared dead.
        var victim = cluster.Nodes[0].Name;
        await cluster.StopNodeAsync(victim);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.Nodes().Single(node => node.NodeName == victim) is { Alive: false },
            "the sweep declared the stopped node dead after the store recovered");
    }

    private sealed class FlakyMembershipStore : IMembershipStore
    {
        private readonly InMemoryMembershipStore _inner = new();
        private int _failedListings;

        public volatile bool FailNodeListing;

        public int FailedListings => Volatile.Read(ref _failedListings);

        public IReadOnlyList<NodeRecord> Nodes()
        {
            if (FailNodeListing)
            {
                Interlocked.Increment(ref _failedListings);
                throw new InvalidOperationException("membership store unavailable (test)");
            }

            return _inner.Nodes();
        }

        public NodeRecord RegisterNode(string nodeName, string publicAddress, DateTimeOffset now) => _inner.RegisterNode(nodeName, publicAddress, now);

        public bool Heartbeat(string nodeName, DateTimeOffset now) => _inner.Heartbeat(nodeName, now);

        public ShardAssignment EnsureShard(ShardKey shard, DateTimeOffset now) => _inner.EnsureShard(shard, now);

        public ShardAssignment? GetAssignment(ShardKey shard) => _inner.GetAssignment(shard);

        public IReadOnlyList<ShardAssignment> AssignmentsFor(string nodeName) => _inner.AssignmentsFor(nodeName);

        public IReadOnlyList<ShardAssignment> AllAssignments() => _inner.AllAssignments();

        public IReadOnlyList<ShardAssignment> MarkDead(string nodeName, DateTimeOffset now) => _inner.MarkDead(nodeName, now);

        public IReadOnlyList<ShardAssignment> AssignUnowned(DateTimeOffset now) => _inner.AssignUnowned(now);

        public ShardAssignment Reassign(ShardKey shard, string toNode, DateTimeOffset now) => _inner.Reassign(shard, toNode, now);

        public bool RemoveShard(ShardKey shard) => _inner.RemoveShard(shard);

        public ShardAssignment SetReplica(ShardKey shard, string? replicaNode) => _inner.SetReplica(shard, replicaNode);
    }
}
