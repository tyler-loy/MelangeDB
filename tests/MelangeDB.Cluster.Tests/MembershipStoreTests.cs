using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The membership contract, against the in-memory store. The Postgres-backed store passes the
/// same behavioral shape in the Postgres suite (MembershipStoreContract there).
/// </summary>
public class MembershipStoreTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.UnixEpoch;

    [Fact]
    public void A_new_shard_is_assigned_to_the_least_loaded_live_node_with_a_fresh_originator()
    {
        var store = new InMemoryMembershipStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);

        var first = store.EnsureShard(new ShardKey(1), T0);
        var second = store.EnsureShard(new ShardKey(2), T0);

        Assert.NotEqual(first.NodeName, second.NodeName);
        Assert.NotEqual(first.Originator, second.Originator);
        Assert.NotEqual(0, first.Originator); // 0 is the hub's.
        Assert.Equal(first, store.EnsureShard(new ShardKey(1), T0)); // Idempotent.
    }

    [Fact]
    public void Marking_a_node_dead_reassigns_its_shards_and_bumps_their_fencing_tokens()
    {
        var store = new InMemoryMembershipStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);
        var before = store.EnsureShard(new ShardKey(1), T0);

        var moved = store.MarkDead(before.NodeName!, T0);

        var after = Assert.Single(moved);
        Assert.NotEqual(before.NodeName, after.NodeName);
        Assert.True(after.FencingToken > before.FencingToken);
        Assert.Equal(before.Originator, after.Originator); // The originator is the shard's for life.
    }

    [Fact]
    public void A_shard_with_no_surviving_candidate_becomes_unowned_and_assigns_on_the_next_registration()
    {
        var store = new InMemoryMembershipStore();
        store.RegisterNode("a", "http://a", T0);
        var before = store.EnsureShard(new ShardKey(1), T0);

        var orphaned = Assert.Single(store.MarkDead("a", T0));
        Assert.Null(orphaned.NodeName);

        store.RegisterNode("b", "http://b", T0);
        var reassigned = Assert.Single(store.AssignUnowned(T0));
        Assert.Equal("b", reassigned.NodeName);
        Assert.True(reassigned.FencingToken > before.FencingToken);
    }

    [Fact]
    public void Heartbeats_only_succeed_for_registered_nodes()
    {
        var store = new InMemoryMembershipStore();
        Assert.False(store.Heartbeat("ghost", T0));
        store.RegisterNode("a", "http://a", T0);
        Assert.True(store.Heartbeat("a", T0.AddSeconds(1)));
        Assert.Equal(T0.AddSeconds(1), store.Nodes().Single().LastSeen);
    }
}
