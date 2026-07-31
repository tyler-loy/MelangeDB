using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Boundary-triggered migration at the engine level: the origin decides from its committed rows,
/// hysteresis bounds the trigger rate, companion rows travel with their anchor, and creatures
/// transfer ownership on crossing and keep ticking on exactly one node. Blocks are 4x4 chunks,
/// band 2, margin 1; the gateway's seamless-swap half has its own tests.
/// </summary>
public class SeamlessMigrationTests
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value; // Chunks x 0..3.
    private static readonly ulong BlockB = SpatialShardStrategy.ShardOfBlock(1, 0).Value; // Chunks x 4..7.

    private static Task<ClusterFixture> StartAsync(int minIntervalMs = 300) => ClusterFixture.StartAsync(
        shardNodes: 2,
        heartbeatMs: 150,
        failureTimeoutMs: 60_000,
        spatial: true,
        extraSettings: new Dictionary<string, string?>
        {
            ["MelangeDb:Cluster:HandoffMinIntervalMs"] = minIntervalMs.ToString(),
        });

    /// <summary>The shard holding the entity's authoritative (non-borrowed) rows, or null mid-transfer.</summary>
    private static ShardRuntime? OwnerOf(ClusterFixture cluster, Identity player)
    {
        foreach (var node in cluster.Nodes)
        {
            if (node.App is null)
                continue;
            foreach (var shard in node.Runtime.OwnedShards)
            {
                var runtime = node.Runtime.TryGetShard(shard);
                if (runtime is null || runtime.Engine.CommittedView.Find<PlayerPos>(player) is null)
                    continue;
                var table = runtime.Engine.Schema.Get(typeof(PlayerPos));
                var key = KeyCodec.Encode(table.PrimaryKey, player);
                if (runtime.BorrowedOwnerOf(table.Id, key) is null)
                    return runtime;
            }
        }

        return null;
    }

    /// <summary>Moves via whichever shard currently owns the player, riding out freeze windows.</summary>
    private static async Task MoveAsync(ClusterFixture cluster, Identity player, uint chunk)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var owner = OwnerOf(cluster, player);
                if (owner is not null)
                {
                    owner.ReducerHost.Call("Move", player, chunk);
                    return;
                }
            }
            catch (Exception) when (attempt < 200)
            {
                // Frozen mid-handoff, or ownership is flipping under us; the point of the retry.
            }

            Assert.True(attempt < 200, $"could not move player to chunk {chunk}");
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task The_origin_transfers_a_player_who_crosses_past_the_margin_and_companion_rows_travel_along()
    {
        await using var cluster = await StartAsync();
        var player = TestTokens.IdentityOf("walker");
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        await cluster.EnsureShardOwnedAsync(BlockB);

        shardA.ReducerHost.Call("Move", player, Chunks.Id(2, 2));
        shardA.ReducerHost.Call("EarnGold", player, 40);

        // One chunk across the line is exactly the margin: origin-sticky, no transfer.
        shardA.ReducerHost.Call("Move", player, Chunks.Id(3, 2));
        shardA.ReducerHost.Call("Move", player, Chunks.Id(4, 2));
        await Task.Delay(700, TestContext.Current.CancellationToken);
        Assert.Equal(0, cluster.Hub.Metrics.HandoffsStarted);
        Assert.NotNull(shardA.Engine.CommittedView.Find<PlayerPos>(player));

        // One more step crosses past the margin: the origin decides, and the transfer runs.
        shardA.ReducerHost.Call("Move", player, Chunks.Id(5, 2));
        await ClusterFixture.WaitUntilAsync(
            () => cluster.ShardOf(BlockB).Engine.CommittedView.Find<Pack>(player) is not null
                && OwnerOf(cluster, player)?.Shard.Value == BlockB,
            "the player and their pack transferred to the destination");

        var pack = cluster.ShardOf(BlockB).Engine.CommittedView.Find<Pack>(player);
        Assert.Equal(40, pack!.Value.Gold); // Companion state travelled intact.
        await ClusterFixture.WaitUntilAsync(
            () => cluster.HubEngine.CommittedView.Find<PlayerShardMap>(player)?.Shard == BlockB,
            "the transfer listener flipped the hub's session-to-shard map");
        Assert.Equal(1, cluster.Hub.Metrics.HandoffsCompleted);
    }

    [Fact]
    public async Task Pacing_across_a_boundary_triggers_a_bounded_number_of_handoffs_not_one_per_step()
    {
        await using var cluster = await StartAsync(minIntervalMs: 400);
        var player = TestTokens.IdentityOf("pacer");
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        await cluster.EnsureShardOwnedAsync(BlockB);
        shardA.ReducerHost.Call("Move", player, Chunks.Id(2, 2));

        // Pacing on the line itself — never deeper than the margin — triggers nothing at all.
        for (var i = 0; i < 6; i++)
        {
            await MoveAsync(cluster, player, Chunks.Id(4, 2));
            await MoveAsync(cluster, player, Chunks.Id(3, 2));
        }

        await Task.Delay(700, TestContext.Current.CancellationToken);
        Assert.Equal(0, cluster.Hub.Metrics.HandoffsStarted);

        // Pacing deeper than the margin does transfer — but bounded by the rate limit, never one
        // per crossing. Twelve deep crossings in ~1.5s under a 400ms floor stay in single digits.
        var paced = System.Diagnostics.Stopwatch.StartNew();
        for (var i = 0; i < 6; i++)
        {
            await MoveAsync(cluster, player, Chunks.Id(5, 2));
            await MoveAsync(cluster, player, Chunks.Id(2, 2));
        }

        paced.Stop();
        await Task.Delay(700, TestContext.Current.CancellationToken);
        var started = cluster.Hub.Metrics.HandoffsStarted;
        var bound = 2 + (long)(paced.Elapsed.TotalMilliseconds / 400);
        Assert.InRange(started, 1, bound);
        Assert.True(cluster.Hub.Metrics.HandoffsRateLimited > 0 || started <= 2,
            "deep pacing must actually exercise the rate limiter (or resolve in at most two transfers)");

        // And the player survived the ordeal on exactly one shard, rows intact.
        await ClusterFixture.WaitUntilAsync(
            () => OwnerOf(cluster, player) is not null,
            "the player settled on exactly one owner");
        Assert.NotNull(OwnerOf(cluster, player)!.Engine.CommittedView.Find<Pack>(player));
    }

    [Fact]
    public async Task A_creature_chasing_a_player_across_the_boundary_transfers_ownership_and_keeps_ticking_exactly_once()
    {
        await using var cluster = await StartAsync(minIntervalMs: 100);
        var player = TestTokens.IdentityOf("prey");
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardB = (await cluster.EnsureShardOwnedAsync(BlockB)).Runtime.TryGetShard(new ShardKey(BlockB))!;

        // The prey stands just inside B; A's border band lets the critter's AI read it across the
        // line — pathing toward a player in the next shard needs no ownership gymnastics, only
        // the read-only copy.
        shardB.ReducerHost.Call("Move", player, Chunks.Id(5, 2));
        shardA.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 9UL, Chunks.Id(2, 2));
        shardA.ReducerHost.Call("AggroCritter", player, 9UL);
        shardA.ReducerHost.Call("ScheduleCritterTick", ClusterFixture.Caller, 100L, 0, 0);
        shardB.ReducerHost.Call("ScheduleCritterTick", ClusterFixture.Caller, 100L, 1, 0);

        // The chase carries the critter across the boundary; on crossing, ownership transfers
        // (immediately — no margin for creatures) and the destination's tick takes over.
        var critterId = SpatialReducers.CritterId(9);
        await ClusterFixture.WaitUntilAsync(
            () => shardB.Engine.CommittedView.Find<Critter>(critterId) is { } arrived
                && arrived.ChunkId == Chunks.Id(5, 2)
                && shardB.BorrowedOwnerOf(shardB.Engine.Schema.Get(typeof(Critter)).Id, KeyOf(shardB, critterId)) is null,
            "the critter chased across the boundary and reached its target as B's own row");

        // Exactly one authoritative copy, ever after: A holds at most a read-only border shadow.
        var onA = shardA.Engine.CommittedView.Find<Critter>(critterId);
        if (onA is not null)
            Assert.NotNull(shardA.BorrowedOwnerOf(shardA.Engine.Schema.Get(typeof(Critter)).Id, KeyOf(shardA, critterId)));

        // And it never stops ticking: the destination's scheduler owns it now.
        var ticksAtArrival = shardB.Engine.CommittedView.Find<Critter>(critterId)!.Value.Ticks;
        await ClusterFixture.WaitUntilAsync(
            () => shardB.Engine.CommittedView.Find<Critter>(critterId)!.Value.Ticks > ticksAtArrival,
            "the critter keeps ticking on its new owner");
        Assert.True(cluster.Hub.Metrics.HandoffsCompleted >= 1);
    }

    private static RowKey KeyOf(ShardRuntime shard, Identity id)
    {
        var table = shard.Engine.Schema.Get(typeof(Critter));
        return KeyCodec.Encode(table.PrimaryKey, id);
    }
}
