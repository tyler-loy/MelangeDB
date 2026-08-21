using MelangeDB.Cluster;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The membership contract against real Postgres — the production store behind the settled
/// "Postgres-backed membership, not Raft" decision. Same behavioral shape as the in-memory
/// store's tests, plus the property that justifies Postgres at all: fencing tokens and
/// originators survive a hub restart.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresMembershipTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.UnixEpoch.AddDays(20_000);

    private readonly PostgresContainerFixture _postgres;

    public PostgresMembershipTests(PostgresContainerFixture postgres) => _postgres = postgres;

    private (PostgresMembershipStore Store, MelangeDbOptions Options) CreateStore(string? schema = null)
    {
        var options = new MelangeDbOptions
        {
            Postgres =
            {
                ConnectionString = _postgres.ConnectionString,
                Schema = schema ?? PostgresContainerFixture.NewSchema(),
            },
        };
        var source = new PostgresConnectionSource(new StaticOptionsMonitor(options));
        return (new PostgresMembershipStore(source, new StaticOptionsMonitor(options)), options);
    }

    /// <summary>
    /// An originator prefixes every AutoInc id its shard mints, and those ids outlive the shard —
    /// an entity that crosses a border carries its id into the neighbour. So a removed shard's
    /// originator must be retired with it, never handed to the next shard created; otherwise the
    /// new shard re-mints ids that are still in use and "unique, not dense" stops being true.
    /// <para>
    /// This is latent until something deletes a shard row, which is why it is asserted before
    /// there is anything that does (issue #112). Deriving the next originator from the live rows
    /// — <c>MAX(originator) + 1</c> — passes every test that never deletes one.
    /// </para>
    /// </summary>
    [Fact]
    public void A_removed_shards_originator_is_retired_rather_than_handed_to_the_next_shard()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        var (store, options) = CreateStore(schema);
        store.RegisterNode("a", "http://a", T0);

        var first = store.EnsureShard(new ShardKey(1), T0);
        var second = store.EnsureShard(new ShardKey(2), T0);

        // Stand in for the reaper that does not exist yet: drop the newest shard's row, which is
        // exactly what makes MAX(originator) fall back.
        Execute(options, $"DELETE FROM {schema}.melange_cluster_shards WHERE shard = 2");

        var third = store.EnsureShard(new ShardKey(3), T0);
        Assert.NotEqual(second.Originator, third.Originator);
        Assert.NotEqual(first.Originator, third.Originator);
        Assert.True(third.Originator > second.Originator);

        // And the mark survives a hub restart, which is the reason membership is in Postgres at
        // all: a fresh store over the same schema must not rewind it either.
        var (restarted, _) = CreateStore(schema);
        var fourth = restarted.EnsureShard(new ShardKey(4), T0);
        Assert.True(fourth.Originator > third.Originator);
    }

    private static void Execute(MelangeDbOptions options, string sql)
    {
        using var connection = new Npgsql.NpgsqlConnection(options.Postgres.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    [Fact]
    public void A_new_shard_is_assigned_to_the_least_loaded_live_node_with_a_fresh_originator()
    {
        _postgres.SkipUnlessAvailable();
        var (store, _) = CreateStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);

        var first = store.EnsureShard(new ShardKey(1), T0);
        var second = store.EnsureShard(new ShardKey(2), T0);

        Assert.NotEqual(first.NodeName, second.NodeName);
        Assert.NotEqual(first.Originator, second.Originator);
        Assert.NotEqual(0, first.Originator);
        Assert.Equal(first, store.EnsureShard(new ShardKey(1), T0));
    }

    [Fact]
    public void Marking_a_node_dead_reassigns_its_shards_and_bumps_their_fencing_tokens()
    {
        _postgres.SkipUnlessAvailable();
        var (store, _) = CreateStore();
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);
        var before = store.EnsureShard(new ShardKey(1), T0);

        var moved = store.MarkDead(before.NodeName!, T0);

        var after = Assert.Single(moved);
        Assert.NotEqual(before.NodeName, after.NodeName);
        Assert.True(after.FencingToken > before.FencingToken);
        Assert.Equal(before.Originator, after.Originator);
        Assert.Equal(after, store.GetAssignment(new ShardKey(1)));
    }

    [Fact]
    public void An_orphaned_shard_assigns_on_the_next_registration()
    {
        _postgres.SkipUnlessAvailable();
        var (store, _) = CreateStore();
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
    public void Membership_state_survives_a_hub_restart_so_old_fencing_tokens_are_never_reminted()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        var (store, _) = CreateStore(schema);
        store.RegisterNode("a", "http://a", T0);
        store.RegisterNode("b", "http://b", T0);
        var minted = store.EnsureShard(new ShardKey(9), T0);
        var bumped = Assert.Single(store.MarkDead(minted.NodeName!, T0));

        // A fresh store over the same schema — the restarted hub — sees the same terms.
        var (restarted, _) = CreateStore(schema);
        Assert.Equal(bumped, restarted.GetAssignment(new ShardKey(9)));
        Assert.Equal(2, restarted.Nodes().Count);

        // And a brand-new shard's originator continues past every previously minted one.
        var next = restarted.EnsureShard(new ShardKey(10), T0);
        Assert.True(next.Originator > minted.Originator);
    }

    [Fact]
    public void Heartbeats_only_succeed_for_registered_nodes()
    {
        _postgres.SkipUnlessAvailable();
        var (store, _) = CreateStore();
        Assert.False(store.Heartbeat("ghost", T0));
        store.RegisterNode("a", "http://a", T0);
        Assert.True(store.Heartbeat("a", T0.AddSeconds(3)));
        Assert.Equal(T0.AddSeconds(3), store.Nodes().Single().LastSeen);
    }
}
