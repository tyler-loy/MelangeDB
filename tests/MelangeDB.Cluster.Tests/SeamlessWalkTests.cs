using MelangeDB.Client;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The phase 10 acceptance bar, through the gateway with a real client: a world spanning three
/// shard nodes, a player walking a continuous path across every boundary with no disconnect, no
/// resync error, and no missing terrain — and the two kill-a-node-mid-handoff outcomes, observed
/// from the client's chair. Blocks are 4x4 chunks along the x axis; the walk is along y=2.
/// </summary>
public class SeamlessWalkTests
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value; // x 0..3
    private static readonly ulong BlockB = SpatialShardStrategy.ShardOfBlock(1, 0).Value; // x 4..7
    private static readonly ulong BlockC = SpatialShardStrategy.ShardOfBlock(2, 0).Value; // x 8..11

    // Band 3: the band must cover the distance walked during one handoff window (the derivation
    // behind the default), and this walk steps faster than the reference workload moves.
    private static Task<ClusterFixture> StartAsync(int shardNodes = 3) => ClusterFixture.StartAsync(
        shardNodes: shardNodes,
        heartbeatMs: 150,
        failureTimeoutMs: 60_000,
        spatial: true,
        extraSettings: new Dictionary<string, string?>
        {
            ["MelangeDb:Cluster:HandoffMinIntervalMs"] = "300",
            ["MelangeDb:Cluster:BorderBandChunks"] = "3",
        });

    /// <summary>Seeds one terrain row per chunk of the given block, on that block's owner.</summary>
    private static async Task SeedTerrainAsync(ClusterFixture cluster, int bx)
    {
        var owner = await cluster.EnsureShardOwnedAsync(SpatialShardStrategy.ShardOfBlock(bx, 0).Value);
        var shard = owner.Runtime.TryGetShard(SpatialShardStrategy.ShardOfBlock(bx, 0))!;
        for (var cx = bx * 4; cx < bx * 4 + 4; cx++)
        {
            for (var cy = 0; cy < 4; cy++)
                shard.ReducerHost.Call("PlaceTerrain", ClusterFixture.Caller, Chunks.Id(cx, cy));
        }
    }

    private static bool HasTerrain(MelangeSubscription terrain, int cx, int cy) =>
        terrain.Rows.Any(row => Convert.ToUInt32(row.Columns["ChunkId"]) == Chunks.Id(cx, cy));

    [Fact]
    public async Task A_client_walks_across_three_shard_nodes_with_no_disconnect_no_resync_and_no_missing_terrain()
    {
        await using var cluster = await StartAsync();
        await SeedTerrainAsync(cluster, 0);
        await SeedTerrainAsync(cluster, 1);
        await SeedTerrainAsync(cluster, 2);

        // Three blocks, three distinct nodes: the world genuinely spans the cluster.
        var owners = new[] { BlockA, BlockB, BlockC }
            .Select(shard => cluster.Hub.Membership.GetAssignment(new ShardKey(shard))!.NodeName)
            .ToArray();
        Assert.Equal(3, owners.Distinct().Count());

        await using var client = cluster.CreateClient(TestTokens.For("wanderer"));
        var player = TestTokens.IdentityOf("wanderer");
        var disconnected = false;
        var resyncErrors = new List<string>();
        client.OnDisconnected += () => disconnected = true;
        client.OnError += error => resyncErrors.Add($"{error.Code}: {error.Message}");
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var saga = new RecordingObserver();
        using var recording = cluster.Hub.Handoffs.Register(player, saga);
        await client.CallReducerAsync("Move", [Chunks.Id(1, 2)], TestContext.Current.CancellationToken);
        var terrain = await client.SubscribeAsync("SELECT * FROM Terrain", null, TestContext.Current.CancellationToken);
        var positions = await client.SubscribeAsync("SELECT * FROM PlayerPos", null, TestContext.Current.CancellationToken);

        // The continuous path: x 1 -> 10 along y=2, crossing A->B at x=4 and B->C at x=8. Every
        // step must leave the client holding its own position and the full 3x3 terrain
        // neighbourhood — served first by the origin (whose band covers the far side of each
        // line) and, after the invisible swap, by the destination.
        for (var cx = 2; cx <= 10; cx++)
        {
            await client.CallReducerAsync("Move", [Chunks.Id(cx, 2)], TestContext.Current.CancellationToken);
            var expectedX = cx;
            await ClusterFixture.WaitUntilAsync(
                () => positions.Rows.Any(row => Convert.ToUInt32(row.Columns["ChunkId"]) == Chunks.Id(expectedX, 2)),
                $"the client sees its own position at x={expectedX}");
            for (var dx = -1; dx <= 1; dx++)
            {
                for (var dy = -1; dy <= 1; dy++)
                {
                    var (nx, ny) = (expectedX + dx, 2 + dy);
                    if (nx is >= 0 and <= 11)
                    {
                        try
                        {
                            await ClusterFixture.WaitUntilAsync(
                                () => HasTerrain(terrain, nx, ny),
                                $"terrain ({nx},{ny}) visible while standing at ({expectedX},2)");
                        }
                        catch (Exception)
                        {
                            var map = cluster.HubEngine.CommittedView.Find<PlayerShardMap>(player)?.Shard;
                            var metrics = cluster.Hub.Metrics;
                            var rows = string.Join("; ", cluster.Nodes.Where(n => n.App is not null).SelectMany(
                                n => n.Runtime.OwnedShards.Select(s => (n.Name, Shard: s, Runtime: n.Runtime.TryGetShard(s)))
                                    .Where(m => m.Runtime?.Engine.CommittedView.Find<PlayerPos>(player) is not null)
                                    .Select(m =>
                                    {
                                        var pos = m.Runtime!.Engine.CommittedView.Find<PlayerPos>(player)!.Value;
                                        return $"{m.Name}/{SpatialShardStrategy.BlockOf(m.Shard)}: chunk={Chunks.At(pos.ChunkId)} steps={pos.Steps} borrowed={m.Runtime.BorrowedOwnerOf(m.Runtime.Engine.Schema.Get(typeof(PlayerPos)).Id, KeyCodec.Encode(m.Runtime.Engine.Schema.Get(typeof(PlayerPos)).PrimaryKey, player))}";
                                    })));
                            var monitors = string.Join("; ", cluster.Nodes.Where(n => n.App is not null).SelectMany(
                                n => n.Runtime.OwnedShards.Select(s => (n.Name, Shard: s, Monitor: n.Runtime.TryGetMonitor(s)))
                                    .Where(m => m.Monitor is not null)
                                    .Select(m => $"{m.Name}/{SpatialShardStrategy.BlockOf(m.Shard)}: crossed={m.Monitor!.CrossingsObserved} sent={m.Monitor.RequestsSent} strays={m.Monitor.StrayCount} sweeps={m.Monitor.SweepPasses}")));
                            Assert.Fail(
                                $"terrain ({nx},{ny}) missing at ({expectedX},2): map={map} started={metrics.HandoffsStarted} " +
                                $"completed={metrics.HandoffsCompleted} aborted={metrics.HandoffsAborted} " +
                                $"unresolved={metrics.HandoffsUnresolved} rateLimited={metrics.HandoffsRateLimited} " +
                                $"inFlight={metrics.HandoffsInFlight} requestsReceived={metrics.HandoffRequestsReceived} " +
                                $"terrainRows={terrain.Count} errors=[{string.Join("; ", resyncErrors)}] " +
                                $"rows=[{rows}] monitors=[{monitors}] events=[{string.Join("; ", saga.Events)}]");
                        }
                    }
                }
            }
        }

        // The player crossed two boundaries and ended owned by the far shard, with the client's
        // socket never dropping and no resync ever demanded of it. The trigger is commit-driven,
        // so a transfer suppressed by the rate limiter needs another step to re-fire — the wiggle
        // below is a player shifting their weight, not a workaround.
        var settled = System.Diagnostics.Stopwatch.StartNew();
        while (cluster.HubEngine.CommittedView.Find<PlayerShardMap>(player)?.Shard != BlockC)
        {
            Assert.True(settled.Elapsed < TestTime.Dilated(TimeSpan.FromSeconds(20)), "ownership must follow the walk to the far shard");
            try
            {
                await client.CallReducerAsync("Move", [Chunks.Id(10, 2)], TestContext.Current.CancellationToken);
            }
            catch (MelangeCallException)
            {
                // A step colliding with the transfer window; the next one re-fires the trigger.
            }

            await Task.Delay(150, TestContext.Current.CancellationToken);
        }
        Assert.True(cluster.Hub.Metrics.HandoffsCompleted >= 2);
        Assert.True(client.IsConnected, "the client must never observe a disconnect");
        Assert.False(disconnected);
        Assert.Empty(resyncErrors);
        Assert.Equal(0, terrain.Inconsistencies);
        Assert.Equal(0, positions.Inconsistencies);
    }

    private sealed class RecordingObserver : IPlayerHandoffObserver
    {
        public System.Collections.Concurrent.ConcurrentQueue<string> Events { get; } = new();

        public void OnApproach(ShardKey from, ShardKey to) => Events.Enqueue($"approach {B(from)}->{B(to)}");

        public void OnStarted(ShardKey from, ShardKey to) => Events.Enqueue($"started {B(from)}->{B(to)}");

        public void OnDestinationAuthoritative(ShardKey from, ShardKey to) => Events.Enqueue($"authoritative {B(from)}->{B(to)}");

        public void OnClosed(ShardKey from, ShardKey to, bool success) => Events.Enqueue($"closed {B(from)}->{B(to)} {success}");

        private static string B(ShardKey shard) => $"{SpatialShardStrategy.BlockOf(shard)}";
    }

    [Fact]
    public async Task A_reducer_call_made_mid_handoff_is_queued_invisibly_and_lands_on_the_destination()
    {
        await using var cluster = await StartAsync(shardNodes: 2);
        await SeedTerrainAsync(cluster, 0);
        await SeedTerrainAsync(cluster, 1);
        await using var client = cluster.CreateClient(TestTokens.For("queued-walker"));
        var player = TestTokens.IdentityOf("queued-walker");
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Move", [Chunks.Id(3, 2)], TestContext.Current.CancellationToken);

        // Pause the saga between freeze and import: the player is frozen everywhere, which is
        // exactly when a naive design would surface an error to the player.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reachedImport = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step == "import")
            {
                reachedImport.TrySetResult();
                await gate.Task;
            }
        };
        try
        {
            await client.CallReducerAsync("Move", [Chunks.Id(4, 2)], TestContext.Current.CancellationToken);
            await client.CallReducerAsync("Move", [Chunks.Id(5, 2)], TestContext.Current.CancellationToken);
            await reachedImport.Task.WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(15)), TestContext.Current.CancellationToken);

            // Mid-freeze call: no error, no result yet — held at the gateway.
            var held = client.CallReducerAsync("Move", [Chunks.Id(6, 2)], TestContext.Current.CancellationToken);
            await Task.Delay(400, TestContext.Current.CancellationToken);
            Assert.False(held.IsCompleted, "the mid-handoff call must be held, not rejected");

            gate.TrySetResult();
            await held.WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(20)), TestContext.Current.CancellationToken);
        }
        finally
        {
            gate.TrySetResult();
            cluster.Hub.HandoffStepHook = null;
        }

        // The held call executed on the destination, in order, after the swap.
        await ClusterFixture.WaitUntilAsync(
            () => cluster.ShardOf(BlockB).Engine.CommittedView.Find<PlayerPos>(player)?.ChunkId == Chunks.Id(6, 2),
            "the queued call landed on the destination");
    }

    [Fact]
    public async Task Killing_the_destination_mid_handoff_leaves_the_player_on_the_origin_alive_and_playable()
    {
        await using var cluster = await StartAsync(shardNodes: 2);
        await SeedTerrainAsync(cluster, 0);
        await SeedTerrainAsync(cluster, 1);
        var player = TestTokens.IdentityOf("survivor");
        var originOwner = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA))!.NodeName!;
        var destinationOwner = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockB))!.NodeName!;
        Assert.NotEqual(originOwner, destinationOwner);

        await using var client = cluster.CreateClient(TestTokens.For("survivor"));
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Move", [Chunks.Id(3, 2)], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("EarnGold", [25], TestContext.Current.CancellationToken);

        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step == "import")
                await cluster.StopNodeAsync(destinationOwner);
        };
        await client.CallReducerAsync("Move", [Chunks.Id(4, 2)], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Move", [Chunks.Id(5, 2)], TestContext.Current.CancellationToken);

        // The import's fate is unknowable, so the player freezes — until the revived
        // destination's log proves it never imported, and the origin aborts.
        var origin = cluster.Node(originOwner).Runtime.TryGetShard(new ShardKey(BlockA))!;
        await ClusterFixture.WaitUntilAsync(() => origin.PendingFreezes.Count == 1, "the freeze is pending");

        // The kill runs inside the (fire-and-forget) saga's hook; reviving before the stop
        // finished would race two apps over one log directory.
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(destinationOwner).App is null, "the destination node finished stopping");
        cluster.Hub.HandoffStepHook = null;
        await cluster.StartNodeAsync(destinationOwner);
        await ClusterFixture.WaitUntilAsync(
            () => origin.PendingFreezes.Count == 0,
            "the origin's reconciler resolved the unknowable import to an abort");

        // Owned by the origin, alive, and playable — through the same client, no reconnect. The
        // destination may hold a read-only border copy (the player stands in its band — that is
        // the visibility design working), but never an authoritative row.
        var kept = origin.Engine.CommittedView.Find<Pack>(player);
        Assert.NotNull(kept);
        Assert.Equal(25, kept!.Value.Gold);
        var destinationShard = cluster.Node(destinationOwner).Runtime.TryGetShard(new ShardKey(BlockB))!;
        if (destinationShard.Engine.CommittedView.Find<PlayerPos>(player) is not null)
        {
            var posTable = destinationShard.Engine.Schema.Get(typeof(PlayerPos));
            Assert.NotNull(destinationShard.BorrowedOwnerOf(
                posTable.Id, KeyCodec.Encode(posTable.PrimaryKey, player)));
        }
        Assert.True(client.IsConnected);
        await client.CallReducerAsync("Move", [Chunks.Id(2, 2)], TestContext.Current.CancellationToken)
            .WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(20)), TestContext.Current.CancellationToken);
        Assert.Equal(Chunks.Id(2, 2), origin.Engine.CommittedView.Find<PlayerPos>(player)!.Value.ChunkId);
    }

    [Fact]
    public async Task Killing_the_origin_mid_handoff_leaves_the_player_on_the_destination_with_no_duplicate()
    {
        await using var cluster = await StartAsync(shardNodes: 2);
        await SeedTerrainAsync(cluster, 0);
        await SeedTerrainAsync(cluster, 1);
        var player = TestTokens.IdentityOf("crosser");
        var originOwner = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA))!.NodeName!;
        var destinationOwner = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockB))!.NodeName!;

        await using var client = cluster.CreateClient(TestTokens.For("crosser"));
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Move", [Chunks.Id(3, 2)], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("EarnGold", [66], TestContext.Current.CancellationToken);

        // The import lands (destination authoritative, gateway swapped); the origin dies before
        // it can release.
        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step == "release")
                await cluster.StopNodeAsync(originOwner);
        };
        await client.CallReducerAsync("Move", [Chunks.Id(4, 2)], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Move", [Chunks.Id(5, 2)], TestContext.Current.CancellationToken);
        var destination = cluster.Node(destinationOwner).Runtime.TryGetShard(new ShardKey(BlockB))!;
        await ClusterFixture.WaitUntilAsync(
            () => destination.Engine.CommittedView.Find<Pack>(player) is not null,
            "the destination holds the imported player");
        cluster.Hub.HandoffStepHook = null;

        // Playable on the destination immediately, through the same connection.
        Assert.True(client.IsConnected);
        await client.CallReducerAsync("Move", [Chunks.Id(7, 2)], TestContext.Current.CancellationToken)
            .WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(20)), TestContext.Current.CancellationToken);
        Assert.Equal(Chunks.Id(7, 2), destination.Engine.CommittedView.Find<PlayerPos>(player)!.Value.ChunkId);
        Assert.Equal(66, destination.Engine.CommittedView.Find<Pack>(player)!.Value.Gold);

        // The revived origin replays its freeze, learns the import happened, and releases: no
        // duplicate — exactly one authoritative copy in the world. (Wait out the hook's kill
        // first; the saga that ran it is fire-and-forget.)
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(originOwner).App is null, "the origin node finished stopping");
        await cluster.StartNodeAsync(originOwner);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(originOwner).Runtime.TryGetShard(new ShardKey(BlockA)) is { } reopened
                && reopened.Engine.CommittedView.Find<PlayerPos>(player) is null,
            "the recovered origin released the transferred player");
    }
}
