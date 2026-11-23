using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// A newly opened shard does not serve until its Replicated tables hold everything the hub had
/// committed (#283). The pushed stream reaches only shards already in service, so a shard used to
/// be published first and filled afterwards: its first calls read empty or partly applied
/// Replicated tables. Every test slows replication down, so the stream cannot win the race by
/// accident, and then looks at the shard the moment it is in service, without waiting.
/// </summary>
public class ReplicaCatchUpTests
{
    private const long Seed = 300; // More than two of the hub's 128-record slices.

    [Fact]
    public async Task A_new_shard_holds_every_replicated_row_before_its_first_call()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        SeedItemDefs(cluster);
        SlowReplication(cluster);

        await cluster.EnsureShardOwnedAsync(40);

        Assert.Equal(Rows(cluster.HubEngine, ItemDefTable(cluster)), Rows(cluster.ShardOf(40).Engine, ItemDefTable(cluster)));
        await cluster.Coordinator.ExecuteOnShardAsync(
            new ShardKey(40),
            nameof(ClusterReducers.SpawnMobIfItemDefs),
            ClusterFixture.Caller,
            [40u, Seed],
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_player_handed_off_into_a_new_shard_finds_every_replicated_row_there()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        var player = TestTokens.IdentityOf("first-into-a-new-shard");
        var origin = (await cluster.EnsureShardOwnedAsync(41)).Runtime.TryGetShard(new ShardKey(41))!;
        origin.ReducerHost.Call("GrantGold", player, 41u, 100);
        SeedItemDefs(cluster);
        SlowReplication(cluster);

        // The destination does not exist yet. An import into it is refused until its owner has it
        // open, and the transfer aborts and leaves the player on the origin; an application
        // retries, as this does. The first transfer that succeeds is the first write on the shard.
        cluster.Coordinator.EnsureShard(new ShardKey(42));
        var deadline = TestTime.Dilated(TimeSpan.FromSeconds(20));
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                await cluster.Coordinator.TransferPlayerAsync(
                    player, new ShardKey(41), new ShardKey(42), TestContext.Current.CancellationToken);
                break;
            }
            catch (Exception) when (stopwatch.Elapsed < deadline)
            {
                await Task.Delay(25, TestContext.Current.CancellationToken);
            }
        }

        var destination = cluster.ShardOf(42);
        Assert.NotNull(destination.Engine.CommittedView.Find<PlayerState>(player));
        Assert.Equal(Rows(cluster.HubEngine, ItemDefTable(cluster)), Rows(destination.Engine, ItemDefTable(cluster)));
    }

    [Fact]
    public async Task A_new_shard_whose_history_the_hub_truncated_is_caught_up_from_a_reset()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        SeedItemDefs(cluster);
        cluster.HubReducers.Call("DeleteItemDef", ClusterFixture.Caller, 7L);
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                cluster.HubEngine.TakeSnapshot();
                return cluster.HubEngine.Log.BaseLsn > 0;
            },
            "the hub truncated its log");
        SlowReplication(cluster);

        var owner = await cluster.EnsureShardOwnedAsync(43);

        var shard = cluster.ShardOf(43);
        Assert.Equal(Rows(cluster.HubEngine, ItemDefTable(cluster)), Rows(shard.Engine, ItemDefTable(cluster)));
        Assert.Null(shard.Engine.CommittedView.Find<ItemDef>(7L));
        Assert.True(owner.Runtime.ReplicaResetsApplied >= 1, "a cursor below the hub's base can only be caught up by a reset");
    }

    private static void SeedItemDefs(ClusterFixture cluster)
    {
        for (var id = 1L; id <= Seed; id++)
            cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, id, $"item-{id}");
    }

    private static void SlowReplication(ClusterFixture cluster)
    {
        foreach (var node in cluster.Nodes)
            node.Runtime.BeforeReplicaSlice = static () => Thread.Sleep(250);
    }

    private static TableId ItemDefTable(ClusterFixture cluster) => cluster.HubEngine.Schema.Get(typeof(ItemDef)).Id;

    // Under the engine lock: the stream may be applying concurrently.
    private static List<string> Rows(MelangeEngine engine, TableId table) =>
        engine.ReadConsistent(_ =>
            engine.HotStore.Scan(table).Select(static pair => $"{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}").ToList());
}
