using MelangeDB.Cli;
using MelangeDB.Types;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The generated bindings, end to end: this assembly is both the server module and a typed
/// consumer of its own exported manifest, so everything here runs real generated code against the
/// real transport — typed rows off real frames, subscription helpers whose SQL the real parser
/// accepted, reducer stubs the real dispatcher decoded.
/// </summary>
public class TypedBindingsTests
{
    [Fact]
    public void The_committed_manifest_is_byte_identical_to_the_build()
    {
        // The staleness guard: the committed melange-schema.json is what the bindings in this
        // assembly were generated from, and it must match what this build of the module exports.
        // When TestTables changes, this fails until the manifest is re-exported — drift breaks
        // the build, not a client.
        var committed = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "melange-schema.json"));
        var embedded = SchemaExporter.ReadFromAssembly(typeof(Chunk).Assembly);
        Assert.Equal(embedded, committed);
    }

    [Fact]
    public async Task Typed_rows_events_stubs_and_lookups_speak_to_the_real_server()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeConnection(client);

        // The drift detector the wrapper exposes is the module's own manifest hash.
        Assert.Contains(conn.SchemaHash, SchemaExporter.ReadFromAssembly(typeof(Chunk).Assembly));

        var inserted = new List<MelangeDB.Types.PlayerState>();
        conn.Db.PlayerState.OnInsert += row => inserted.Add(row);
        await conn.Db.PlayerState.SubscribeAllAsync(TestContext.Current.CancellationToken);

        // A generated stub, through the real dispatcher, observed as a typed row: the Identity
        // column comes back as the caller's identity, not 32 anonymous bytes.
        var lsn = await conn.Reducers.SpawnAsync("Zed", 3, TestContext.Current.CancellationToken);
        Assert.True(lsn > 0);
        await TransportTestHost.WaitUntilAsync(() => inserted.Count > 0, "the typed insert to arrive");

        var player = Assert.Single(inserted);
        Assert.Equal(TransportTestHost.Caller, player.Id);
        Assert.Equal("Zed", player.Name);
        Assert.Equal(3, player.RoomId);
        Assert.Equal(0f, player.X);

        // PK lookup by Identity against the local cache.
        var found = conn.Db.PlayerState.Id.Find(TransportTestHost.Caller);
        Assert.Equal("Zed", found!.Value.Name);

        // Index lookup and typed update event.
        var updated = new List<(MelangeDB.Types.PlayerState Old, MelangeDB.Types.PlayerState New)>();
        conn.Db.PlayerState.OnUpdate += (old, now) => updated.Add((old, now));
        await conn.Reducers.MoveAsync(4.5f, TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => updated.Count > 0, "the typed update to arrive");
        Assert.Equal(0f, updated[0].Old.X);
        Assert.Equal(4.5f, updated[0].New.X);
        Assert.Equal("Zed", Assert.Single(conn.Db.PlayerState.RoomId.Filter(3)).Name);
    }

    [Fact]
    public async Task Enum_columns_and_identity_parameters_ride_typed_end_to_end()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeConnection(client);

        await conn.Db.InventoryItem.SubscribeAllAsync(TestContext.Current.CancellationToken);

        // The stub carries an Identity and an int the server casts to the enum; the row comes
        // back with the client-side re-declared enum, not a bare integer.
        await conn.Reducers.GiveItemAsync(
            TransportTestHost.Caller,
            (int)MelangeDB.Types.ContainerKind.WorldContainer,
            "lantern",
            TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => conn.Db.InventoryItem.Count > 0, "the item to arrive");

        var item = Assert.Single(conn.Db.InventoryItem.Iter());
        Assert.Equal(MelangeDB.Types.ContainerKind.WorldContainer, item.Container);
        Assert.Equal("lantern", item.ItemName);
        Assert.Equal(TransportTestHost.Caller, item.OwnerId);
    }

    [Fact]
    public async Task Generated_subscription_helpers_produce_sql_the_real_server_accepts()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 2L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 12L, new byte[] { 2 });
        host.Call("AddSkill", 7L, "mining", 100L, 3);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeConnection(client);

        // All three typed shapes, accepted by the real parser and engine: full table, range with
        // typed rescope (the terrain pattern), and equality — then an unsubscribe that leaves the
        // other subscriptions' rows alone.
        var all = await conn.Db.Skill.SubscribeAllAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, Assert.Single(conn.Db.Skill.Iter()).Level);

        var window = await conn.Db.Chunk.X.SubscribeRangeAsync(0, 10, TestContext.Current.CancellationToken);
        Assert.Equal(1, conn.Db.Chunk.Count);
        await conn.Db.Chunk.X.RescopeRangeAsync(window, 5, 15, TestContext.Current.CancellationToken);
        // Wait for convergence, not arrival: the diff's insert and delete are separate ops, and
        // under contention the arrival can land a poll-tick before the departure applies.
        await TransportTestHost.WaitUntilAsync(
            () => conn.Db.Chunk.Id.Find(2L) is not null && conn.Db.Chunk.Id.Find(1L) is null,
            "the rescope diff to converge");
        Assert.Null(conn.Db.Chunk.Id.Find(1L));

        var byPlayer = await conn.Db.Skill.PlayerNum.SubscribeAsync(7L, TestContext.Current.CancellationToken);
        await byPlayer.UnsubscribeAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, conn.Db.Skill.Count);
        await all.UnsubscribeAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, conn.Db.Skill.Count);
    }

    [Fact]
    public void Stubs_exist_for_exactly_the_client_callable_reducers()
    {
        var stubs = typeof(MelangeReducers).GetMethods()
            .Where(m => m.Name.EndsWith("Async", StringComparison.Ordinal))
            .Select(m => m.Name)
            .ToHashSet();

        // Standard reducers get stubs; the scheduled reducer (timer-row argument is server codec
        // bytes) and the lifecycle reducers (transport-fired) must not — the server already tells
        // a client naming them "unknown", and the bindings must not advertise what the server
        // refuses.
        Assert.Contains("SpawnAsync", stubs);
        Assert.Contains("NoopAsync", stubs);
        Assert.DoesNotContain("RespawnAsync", stubs);
        Assert.DoesNotContain("OnConnectAsync", stubs);
        Assert.DoesNotContain("OnDisconnectAsync", stubs);
    }
}
