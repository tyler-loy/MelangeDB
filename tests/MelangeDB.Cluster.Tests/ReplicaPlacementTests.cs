using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Phase 30 step 2d: every owned shard gets a warm replica on another live node, whose follower
/// keeps the shard's hot store current up to the owner's durable LSN as relayed by the hub. A
/// replica moves when its node dies, and none is placed when the deployment opts out.
/// </summary>
public class ReplicaPlacementTests
{
    private static readonly ShardKey Shard = new(1);

    private static List<string> Rows(IHotStore store, SchemaRegistry schema) =>
        [.. store.Scan(schema.Get(typeof(Mob)).Id).Select(static pair => Convert.ToHexStringLower(pair.Value.Span)).Order()];

    [Fact]
    public async Task A_shards_replica_lives_on_another_node_and_keeps_up_with_the_owner()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 3_000);
        var owner = await cluster.EnsureShardOwnedAsync(Shard.Value);
        var replica = cluster.Nodes.Single(n => n.Name != owner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.GetAssignment(Shard)!.ReplicaNode == replica.Name,
            "the hub placed the replica on the other node");

        var shard = owner.Runtime.TryGetShard(Shard)!;
        for (var i = 0; i < 5; i++)
            shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, i);

        await ClusterFixture.WaitUntilAsync(
            () => replica.Runtime.TryGetFollower(Shard) is { } follower && follower.AppliedLsn == shard.Engine.Log.DurableLsn,
            "the replica's follower caught up with the owner's durable LSN");
        var follower = replica.Runtime.TryGetFollower(Shard)!;
        Assert.Equal(Rows(shard.Engine.HotStore, shard.Engine.Schema), Rows(follower.Store!, shard.Engine.Schema));
        Assert.Null(owner.Runtime.TryGetFollower(Shard)); // Never on the owner.
    }

    [Fact]
    public async Task A_deployment_that_opts_out_gets_no_replicas()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2,
            heartbeatMs: 150,
            failureTimeoutMs: 3_000,
            extraSettings: new Dictionary<string, string?> { ["MelangeDb:Cluster:ShardReplicas"] = "0" });
        await cluster.EnsureShardOwnedAsync(Shard.Value);

        // Two rebalance ticks: placement would have happened by now.
        await Task.Delay(TestTime.Dilated(TimeSpan.FromSeconds(2.5)), TestContext.Current.CancellationToken);
        Assert.Null(cluster.Hub.Membership.GetAssignment(Shard)!.ReplicaNode);
        Assert.All(cluster.Nodes, node => Assert.Null(node.Runtime.TryGetFollower(Shard)));
    }

    [Fact]
    public async Task A_replica_whose_node_dies_moves_to_a_live_one()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 3, heartbeatMs: 150, failureTimeoutMs: 1_500);
        var owner = await cluster.EnsureShardOwnedAsync(Shard.Value);
        string? first = null;
        await ClusterFixture.WaitUntilAsync(
            () => (first = cluster.Hub.Membership.GetAssignment(Shard)!.ReplicaNode) is not null,
            "the hub placed a replica");
        owner.Runtime.TryGetShard(Shard)!.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 9);

        await cluster.StopNodeAsync(first!);
        var third = cluster.Nodes.Single(n => n.Name != owner.Name && n.Name != first).Name;
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Membership.GetAssignment(Shard)!.ReplicaNode == third,
            "the replica moved to the surviving non-owner");

        var shard = owner.Runtime.TryGetShard(Shard)!;
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(third).Runtime.TryGetFollower(Shard) is { } follower && follower.AppliedLsn == shard.Engine.Log.DurableLsn,
            "the new replica's follower caught up");
    }

    /// <summary>
    /// Phase 30 step 2e: when the owner dies, the hub gives the shard to its replica's node, which
    /// promotes the follower's projection instead of recovering from the snapshot, replays what
    /// the follower had not applied (writes the dead owner made after the last relayed ceiling),
    /// and gets a replica of its own on the remaining node.
    /// </summary>
    [Fact]
    public async Task A_dead_owners_shard_is_promoted_on_its_replicas_node_with_nothing_lost()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 3, heartbeatMs: 150, failureTimeoutMs: 1_500);
        var owner = await cluster.EnsureShardOwnedAsync(Shard.Value);
        string? replica = null;
        await ClusterFixture.WaitUntilAsync(
            () => (replica = cluster.Hub.Membership.GetAssignment(Shard)!.ReplicaNode) is not null,
            "the hub placed a replica");

        var shard = owner.Runtime.TryGetShard(Shard)!;
        for (var i = 0; i < 5; i++)
            shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, i);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(replica!).Runtime.TryGetFollower(Shard) is { } follower && follower.AppliedLsn == shard.Engine.Log.DurableLsn,
            "the follower caught up");

        // Writes the follower may not have seen: the owner dies before a heartbeat relays them.
        for (var i = 5; i < 8; i++)
            shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, i);
        var expected = Rows(shard.Engine.HotStore, shard.Engine.Schema);
        var token = cluster.Hub.Membership.GetAssignment(Shard)!.FencingToken;
        await cluster.StopNodeAsync(owner.Name);

        var successor = cluster.Node(replica!);
        await ClusterFixture.WaitUntilAsync(
            () => successor.Runtime.TryGetShard(Shard) is not null,
            "the replica's node opened the shard");
        var promoted = successor.Runtime.TryGetShard(Shard)!;
        Assert.NotNull(promoted.Engine.WarmStartedFrom);
        Assert.True(promoted.FencingToken > token);
        Assert.Null(successor.Runtime.TryGetFollower(Shard));
        Assert.Equal(expected, Rows(promoted.Engine.HotStore, promoted.Engine.Schema));

        // It writes, and the remaining node becomes its replica.
        promoted.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 99);
        var third = cluster.Nodes.Single(n => n.Name != owner.Name && n.Name != replica).Name;
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(third).Runtime.TryGetFollower(Shard) is { } follower && follower.AppliedLsn == promoted.Engine.Log.DurableLsn,
            "a new replica follows the promoted owner");
    }

    /// <summary>
    /// Cluster:ReplicaResidency: Owner leaves the owner's resolution alone; Paged pages every
    /// table, a [Table(Residency = Resident)] one and a Residency:* override included.
    /// </summary>
    [Fact]
    public void A_paged_replica_pages_every_table_and_an_owner_replica_matches_the_owner()
    {
        var schema = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables());
        var itemDef = schema.Get(typeof(ItemDef)).Id;
        MelangeDbOptions Options()
        {
            var options = new MelangeDbOptions();
            options.Residency.PerTable["Mob"] = Residency.Resident;
            return options;
        }

        var owner = Options();
        ShardNodeRuntime.ApplyReplicaResidency(owner, schema, ReplicaResidency.Owner);
        var asOwner = ResidencyResolver.Resolve(schema, owner.Residency);
        Assert.Equal(Residency.Resident, asOwner[itemDef]);
        Assert.Equal(Residency.Resident, asOwner[schema.Get(typeof(Mob)).Id]);

        var paged = Options();
        ShardNodeRuntime.ApplyReplicaResidency(paged, schema, ReplicaResidency.Paged);
        Assert.All(ResidencyResolver.Resolve(schema, paged.Residency).Values, static residency => Assert.Equal(Residency.Paged, residency));
    }
}
