using System.Net.WebSockets;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Cluster.Tests;

public sealed class SharedClusterFixture : IAsyncLifetime
{
    internal ClusterFixture Cluster { get; private set; } = null!;

    public async ValueTask InitializeAsync() => Cluster = await ClusterFixture.StartAsync();

    public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
}

[CollectionDefinition("cluster")]
public sealed class ClusterCollection : ICollectionFixture<SharedClusterFixture>;

/// <summary>
/// The phase 09 acceptance bar, against a hub plus two shard nodes running as real hosts with
/// real sockets in this process. Tests share the cluster and use disjoint instance ids.
/// </summary>
[Collection("cluster")]
public class ClusterAcceptanceTests
{
    private readonly ClusterFixture _cluster;

    public ClusterAcceptanceTests(SharedClusterFixture shared) => _cluster = shared.Cluster;

    [Fact]
    public async Task A_client_connects_to_the_gateway_and_cannot_tell_how_many_nodes_exist()
    {
        await _cluster.EnsureShardOwnedAsync(1);
        await using var client = _cluster.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // One endpoint, one protocol: a Global write, a Replicated read, and Partitioned traffic
        // all flow through the same socket, with nothing in any frame naming a node.
        await client.CallReducerAsync("SetLocation", [1u], TestContext.Current.CancellationToken);
        var spawnLsn = await client.CallReducerAsync("SpawnMob", [1u, 100], TestContext.Current.CancellationToken);
        Assert.True(spawnLsn > 0);

        var mobs = await client.SubscribeAsync(
            "SELECT * FROM Mob WHERE InstanceId = :i",
            new Dictionary<string, object?> { ["i"] = 1u },
            TestContext.Current.CancellationToken);
        Assert.Equal(1, mobs.Count);

        // A server-side commit on the owning node streams to the client through the gateway.
        _cluster.ShardOf(1).ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 50);
        await ClusterFixture.WaitUntilAsync(() => mobs.Count == 2, "the shard delta reached the client");

        _cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 7L, "pickaxe");
        var items = await client.SubscribeAsync(
            "SELECT * FROM ItemDef WHERE Id = :id",
            new Dictionary<string, object?> { ["id"] = 7L },
            TestContext.Current.CancellationToken);
        Assert.Equal(1, items.Count);

        var counterBefore = _cluster.HubEngine.CommittedView.Find<GlobalCounter>(1L)?.Value ?? 0;
        await client.CallReducerAsync("BumpGlobal", [5L], TestContext.Current.CancellationToken);
        Assert.Equal(counterBefore + 5, _cluster.HubEngine.CommittedView.Find<GlobalCounter>(1L)!.Value.Value);
    }

    [Fact]
    public async Task A_reducer_touching_only_its_own_shard_commits_locally_with_zero_cross_node_traffic()
    {
        var owner = await _cluster.EnsureShardOwnedAsync(10);
        var shard = owner.Runtime.TryGetShard(new ShardKey(10))!;
        await _cluster.QuiesceAsync();

        var before = _cluster.TotalLinkMessages();
        var lsn = shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 10u, 42);
        Assert.True(lsn > 0);
        Assert.Equal(1, shard.Engine.CommittedView.Count<Mob>());

        // Asserted by counting network messages, not by inspection: the commit generated none.
        await Task.Delay(400, TestContext.Current.CancellationToken);
        Assert.Equal(before, _cluster.TotalLinkMessages());
    }

    [Fact]
    public async Task Replicated_reference_data_is_identical_on_all_nodes_and_updates_propagate_from_the_hub()
    {
        await _cluster.EnsureShardOwnedAsync(11);
        await _cluster.EnsureShardOwnedAsync(12);
        _cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 42L, "lasgun");
        _cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 42L, "crysknife");

        var itemDef = _cluster.HubEngine.Schema.Get(typeof(ItemDef));
        var hubRows = Rows(_cluster.HubEngine, itemDef.Id);
        Assert.NotEmpty(hubRows);

        foreach (var shardValue in new ulong[] { 11, 12 })
        {
            var engine = _cluster.ShardOf(shardValue).Engine;
            await ClusterFixture.WaitUntilAsync(
                () => Rows(engine, itemDef.Id).SequenceEqual(hubRows),
                $"shard {shardValue} converged to the hub's ItemDef rows");
        }
    }

    // Scan is safe only under the engine lock: replication applies to these engines
    // concurrently, and a raw scan mid-apply throws "collection was modified".
    private static List<string> Rows(MelangeEngine engine, TableId table) =>
        engine.ReadConsistent(_ =>
            engine.HotStore.Scan(table).Select(static pair => $"{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}").ToList());

    [Fact]
    public async Task A_global_write_from_a_shard_attached_client_reaches_the_hub_and_is_visible_cluster_wide()
    {
        await _cluster.EnsureShardOwnedAsync(13);
        await using var client = _cluster.CreateClient(TestTokens.For("global-writer"));
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SetLocation", [13u], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SpawnMob", [13u, 9], TestContext.Current.CancellationToken); // Shard-attached.

        // A second observer, also through the gateway, watches the Global table live.
        await using var observer = _cluster.CreateClient(TestTokens.For("global-observer"));
        await observer.ConnectAsync(TestContext.Current.CancellationToken);
        var counters = await observer.SubscribeAsync("SELECT * FROM GlobalCounter", null, TestContext.Current.CancellationToken);

        await client.CallReducerAsync("BumpGlobal", [21L], TestContext.Current.CancellationToken);
        await ClusterFixture.WaitUntilAsync(
            () => counters.Rows.Any(static row => (long)row.Columns["Value"]! >= 21L),
            "the Global write became visible to another gateway client");
    }

    [Fact]
    public async Task Scheduled_reducers_fire_per_shard_on_the_owning_node_and_nowhere_else()
    {
        var ownerA = await _cluster.EnsureShardOwnedAsync(20);
        var ownerB = await _cluster.EnsureShardOwnedAsync(21);
        var shardA = ownerA.Runtime.TryGetShard(new ShardKey(20))!;
        var shardB = ownerB.Runtime.TryGetShard(new ShardKey(21))!;
        shardA.ReducerHost.Call("ScheduleTick", ClusterFixture.Caller, 50L);
        shardB.ReducerHost.Call("ScheduleTick", ClusterFixture.Caller, 50L);

        await ClusterFixture.WaitUntilAsync(
            () => Ticks(shardA.Engine) >= 3 && Ticks(shardB.Engine) >= 3,
            "each shard's timer fired repeatedly on its owner");

        // One timer row per shard — never duplicated by recovery or reassignment — and the hub,
        // which owns no shard, never ticks at all.
        Assert.Equal(1, shardA.Engine.CommittedView.Count<ShardTick>());
        Assert.Equal(1, shardB.Engine.CommittedView.Count<ShardTick>());
        Assert.Null(_cluster.HubEngine.CommittedView.Find<TickCount>(1L));
    }

    private static long Ticks(MelangeEngine engine) =>
        engine.CommittedView.Find<TickCount>(1L)?.Count ?? 0;

    [Fact]
    public async Task A_shard_created_on_first_visit_opens_already_holding_its_timer_rows()
    {
        // Nobody schedules anything on this shard: it is created the way a spatial world creates
        // one, by someone arriving. Before init reducers existed it would have opened with an
        // empty Local table, served reads and writes correctly, and never ticked.
        var owner = await _cluster.EnsureShardOwnedAsync(60);
        var shard = owner.Runtime.TryGetShard(new ShardKey(60))!;

        Assert.Equal(1, shard.Engine.CommittedView.Count<SeededTick>());
        Assert.NotNull(shard.Engine.CommittedView.Find<ShardSeed>(1L));

        // Per shard, not per node: the same node's other shards were seeded into their own
        // engines, and the seed is invisible from anywhere else.
        var second = await _cluster.EnsureShardOwnedAsync(61);
        Assert.Equal(1, second.Runtime.TryGetShard(new ShardKey(61))!.Engine.CommittedView.Count<SeededTick>());

        // The hub owns no shard, so a shard-executed init reducer never ran there.
        Assert.Null(_cluster.HubEngine.CommittedView.Find<ShardSeed>(1L));
    }

    [Fact]
    public async Task A_shard_spanning_transaction_trips_the_debug_check_with_a_clear_message()
    {
        var owner = await _cluster.EnsureShardOwnedAsync(40);
        var shard = owner.Runtime.TryGetShard(new ShardKey(40))!;
        var headBefore = shard.Engine.Log.HeadLsn;

        var failure = Assert.Throws<ShardSpanException>(
            () => shard.ReducerHost.Call("SpanBoth", ClusterFixture.Caller, 40u, 41u));

        Assert.Contains("must resolve to the same shard", failure.Message);
        Assert.Contains("shard:40", failure.Message);
        Assert.Contains("shard:41", failure.Message);
        Assert.Equal(headBefore, shard.Engine.Log.HeadLsn); // Aborted with zero trace.
    }

    [Fact]
    public async Task Events_published_on_a_shard_are_handled_on_the_hub_with_unchanged_handler_code()
    {
        var owner = await _cluster.EnsureShardOwnedAsync(50);
        var shard = owner.Runtime.TryGetShard(new ShardKey(50))!;
        shard.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 50u, 10);
        var mobId = shard.Engine.CommittedView.Scan<Mob>().Single().Id;
        shard.ReducerHost.Call("KillMob", ClusterFixture.Caller, mobId);

        await ClusterFixture.WaitUntilAsync(
            () => _cluster.HubReceipts.Items.Contains($"MobDied:{mobId}@50"),
            "the shard-published event reached the hub's handler");

        // Handlers run on the hub — the settled phase 09 decision — never on shard nodes.
        foreach (var node in _cluster.Nodes)
            Assert.DoesNotContain($"MobDied:{mobId}@50", node.Receipts.Items);
    }

    [Fact]
    public async Task AutoInc_ids_cannot_collide_across_shards_because_each_shard_mints_from_its_originator()
    {
        var ownerA = await _cluster.EnsureShardOwnedAsync(60);
        var ownerB = await _cluster.EnsureShardOwnedAsync(61);
        var shardA = ownerA.Runtime.TryGetShard(new ShardKey(60))!;
        var shardB = ownerB.Runtime.TryGetShard(new ShardKey(61))!;
        shardA.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 60u, 1);
        shardB.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 61u, 1);

        var idA = shardA.Engine.CommittedView.Scan<Mob>().Single().Id;
        var idB = shardB.Engine.CommittedView.Scan<Mob>().Single().Id;
        Assert.NotEqual(idA, idB);
        var originatorA = (ushort)(idA >> 47);
        var originatorB = (ushort)(idB >> 47);
        Assert.NotEqual(originatorA, originatorB);
        Assert.Equal(_cluster.Hub.Membership.GetAssignment(new ShardKey(60))!.Originator, originatorA);
        Assert.Equal(_cluster.Hub.Membership.GetAssignment(new ShardKey(61))!.Originator, originatorB);
    }

    [Fact]
    public async Task Handoff_moves_the_players_rows_and_the_player_is_never_writable_on_two_nodes_at_once()
    {
        var player = TestTokens.IdentityOf("traveller");
        var ownerFrom = await _cluster.EnsureShardOwnedAsync(30);
        await _cluster.EnsureShardOwnedAsync(31);
        var origin = ownerFrom.Runtime.TryGetShard(new ShardKey(30))!;
        origin.ReducerHost.Call("GrantGold", player, 30u, 100);
        Assert.NotNull(origin.Engine.CommittedView.Find<PlayerState>(player));

        // Between the destination's durable import and the origin's release, the origin must
        // already refuse writes to the player — writable on at most one node at every instant.
        Exception? duringTransfer = null;
        _cluster.Hub.HandoffStepHook = step =>
        {
            if (step == "release")
            {
                duringTransfer = Record.Exception(() => origin.ReducerHost.Call("GrantGold", player, 30u, 1));
            }

            return Task.CompletedTask;
        };
        try
        {
            await _cluster.Coordinator.TransferPlayerAsync(
                player, new ShardKey(30), new ShardKey(31), TestContext.Current.CancellationToken);
        }
        finally
        {
            _cluster.Hub.HandoffStepHook = null;
        }

        Assert.NotNull(duringTransfer);
        Assert.Contains("frozen mid-handoff", duringTransfer!.Message);

        var destination = _cluster.ShardOf(31);
        Assert.Null(origin.Engine.CommittedView.Find<PlayerState>(player));
        var moved = destination.Engine.CommittedView.Find<PlayerState>(player);
        Assert.NotNull(moved);
        Assert.Equal(31u, moved!.Value.InstanceId);
        Assert.Equal(100, moved.Value.Gold);
    }

    [Fact]
    public async Task The_gateway_refuses_internal_identity_assertions_from_clients()
    {
        var assertion = InternalIdentityAssertion.Mint(
            ClusterFixture.Secret, ClusterFixture.Caller, false, false, false, DateTimeOffset.UtcNow.AddMinutes(5));
        var serializer = new MessagePackFrameSerializer();
        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(_cluster.GatewayUri, TestContext.Current.CancellationToken);
        var hello = serializer.Serialize(new HelloFrame(
            MessagePackFrameSerializer.ProtocolVersion, MessagePackFrameSerializer.ProtocolVersion, assertion));
        await socket.SendAsync(hello, WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);

        var buffer = new byte[64 * 1024];
        var received = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
        var frame = serializer.Deserialize(buffer.AsSpan(0, received.Count));
        var error = Assert.IsType<ErrorFrame>(frame);
        Assert.Equal(MelangeErrorCodes.Unauthorized, error.Code);
        Assert.Contains("assertion", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task A_node_link_with_the_wrong_secret_is_refused()
    {
        using var client = new System.Net.Sockets.TcpClient();
        await client.ConnectAsync("127.0.0.1", _cluster.Hub.NodeListenPort, TestContext.Current.CancellationToken);
        var link = new NodeLink(client, new ClusterMetrics());
        var challenge = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        link.Handler = (_, type, body) =>
        {
            if (type == "challenge")
                challenge.TrySetResult(body!.Value.GetProperty("ServerNonce").GetString()!);
            return Task.FromResult<object?>(null);
        };
        link.Start();
        var nonce = await challenge.Task.WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(10)), TestContext.Current.CancellationToken);

        var failure = await Assert.ThrowsAsync<NodeLinkException>(() => link.RequestAsync(
            "auth",
            new AuthRequest("intruder", "http://127.0.0.1:1", LinkProof.NewNonce(), LinkProof.Compute("wrong-secret", nonce, "intruder")),
            TestContext.Current.CancellationToken));
        Assert.Contains("authentication failed", failure.Message);
        link.Dispose();
    }
}
