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

    /// <summary>
    /// Phase 30 step 2e: a dead owner's shard goes to its replica's node, which holds the warm
    /// projection, even when another node is the least loaded. The token is bumped as for any
    /// death, and the shard has no replica until the hub places a new one.
    /// </summary>
    [Fact]
    public void A_dead_owners_shard_goes_to_its_replica_rather_than_the_least_loaded_node()
    {
        var store = new InMemoryMembershipStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);
        store.RegisterNode("c", "http://c", T0);
        var shard = new ShardKey(1);
        var before = store.EnsureShard(shard, T0);

        // The non-owner the least-loaded rule would not pick: the later name on a tie.
        var replica = new[] { "a", "b", "c" }.Where(n => n != before.NodeName).Max(StringComparer.Ordinal)!;
        store.SetReplica(shard, replica);

        var promoted = Assert.Single(store.MarkDead(before.NodeName!, T0));
        Assert.Equal(replica, promoted.NodeName);
        Assert.Equal(before.FencingToken + 1, promoted.FencingToken);
        Assert.Null(promoted.ReplicaNode);
        Assert.Equal(promoted, store.GetAssignment(shard));
    }

    [Fact]
    public void A_raised_term_passes_the_log_and_keeps_the_owner()
    {
        var store = new InMemoryMembershipStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);
        var shard = new ShardKey(7);
        var before = store.EnsureShard(shard, T0);
        var owner = before.NodeName!;

        var raised = store.RaiseTerm(shard, owner, heldToken: 1, logTerm: 4);

        Assert.Equal(before with { FencingToken = 5 }, raised);
        Assert.Equal(raised, store.GetAssignment(shard));
    }

    [Fact]
    public void An_out_of_date_term_report_changes_nothing()
    {
        var store = new InMemoryMembershipStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);
        var shard = new ShardKey(7);
        var owner = store.EnsureShard(shard, T0).NodeName!;
        var other = owner == "a" ? "b" : "a";

        Assert.Null(store.RaiseTerm(new ShardKey(8), owner, 1, 4)); // Unknown shard.
        Assert.Null(store.RaiseTerm(shard, other, 1, 4)); // Not the owner.
        Assert.Null(store.RaiseTerm(shard, owner, 2, 4)); // Not the token it holds.
        Assert.Null(store.RaiseTerm(shard, owner, 1, 1)); // The log is not past it.
        Assert.Equal(1, store.GetAssignment(shard)!.FencingToken);
    }

    [Fact]
    public void The_file_store_remembers_terms_originators_replicas_and_nodes_when_reopened()
    {
        var directory = Directory.CreateTempSubdirectory("melange-membership-").FullName;
        try
        {
            var path = Path.Combine(directory, FileMembershipStore.FileName);
            var store = new FileMembershipStore(path);
            store.RegisterNode("a", "http://a", T0);
            store.RegisterNode("b", "http://b", T0);
            var first = store.EnsureShard(new ShardKey(1), T0);
            store.EnsureShard(new ShardKey(2), T0);
            store.EnsureShard(new ShardKey(3), T0);
            store.RemoveShard(new ShardKey(3));
            var other = first.NodeName == "a" ? "b" : "a";
            store.Reassign(new ShardKey(1), other, T0);
            store.SetReplica(new ShardKey(1), first.NodeName);
            store.MarkDead("b", T0);

            var reopened = new FileMembershipStore(path);

            Assert.Equal(store.AllAssignments(), reopened.AllAssignments());
            Assert.Equal(store.Nodes(), reopened.Nodes());

            // The originator high-water mark survives too: shard 3's retired prefix is not reused.
            var fourth = reopened.EnsureShard(new ShardKey(4), T0);
            Assert.Equal(4, fourth.Originator);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void A_change_the_file_store_cannot_persist_is_not_handed_out()
    {
        var directory = Directory.CreateTempSubdirectory("melange-membership-").FullName;
        try
        {
            var path = Path.Combine(directory, FileMembershipStore.FileName);
            var store = new FileMembershipStore(path);
            store.RegisterNode("a", "http://a", T0);
            store.EnsureShard(new ShardKey(1), T0);

            // A directory where the temp file goes makes every write fail.
            Directory.CreateDirectory(path + ".tmp");
            Assert.ThrowsAny<SystemException>(() => store.EnsureShard(new ShardKey(2), T0));
            Assert.ThrowsAny<SystemException>(() => store.RegisterNode("b", "http://b", T0));
            Assert.Null(store.GetAssignment(new ShardKey(2)));
            Assert.DoesNotContain(store.Nodes(), static n => n.NodeName == "b");

            // Once the file can be written again, the originator it failed to hand out is reused.
            Directory.Delete(path + ".tmp");
            Assert.Equal(2, store.EnsureShard(new ShardKey(2), T0).Originator);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
