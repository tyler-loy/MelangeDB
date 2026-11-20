using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A client built against an older schema keeps working against a server that changed it
/// additively (road-to-0.4 phase 28). A game client on a player's machine cannot be rebuilt in
/// lockstep with a server deploy; before this, every added column refused every subscription of
/// every client not yet updated, and every added reducer parameter refused every call.
/// </summary>
public class SchemaToleranceTests
{
    /// <summary>
    /// Bindings for <see cref="EditedChunk"/> generated before EditCount, IsEdited, and Elevation
    /// existed, and with their two columns in the other order — a reorder plus three additions,
    /// one of them between the two known columns.
    /// </summary>
    private struct OldEditedChunk
    {
        public ulong Visits;
        public long Id;
    }

    private sealed class OldEditedChunkCodec : IClientRowCodec<OldEditedChunk>
    {
        public static readonly OldEditedChunkCodec Instance = new();

        public string TableName => "EditedChunk";

        public IReadOnlyList<WireColumn> Columns { get; } =
        [
            new WireColumn("Visits", ColumnKind.UInt64),
            new WireColumn("Id", ColumnKind.Int64),
        ];

        public OldEditedChunk DecodeRow(ReadOnlySpan<byte> data)
        {
            var reader = new RowReader(data);
            var row = new OldEditedChunk { Visits = reader.ReadUInt64(), Id = reader.ReadInt64() };
            Assert.Equal(data.Length, reader.Position);
            return row;
        }

        public byte[] EncodePrimaryKey(in OldEditedChunk row) => KeyCodec.EncodeInt64(row.Id).ToArray();
    }

    /// <summary>Bindings that read a column the server never had — a removal or rename, from their side.</summary>
    private sealed class RenamedEditedChunkCodec : IClientRowCodec<OldEditedChunk>
    {
        public static readonly RenamedEditedChunkCodec Instance = new();

        public string TableName => "EditedChunk";

        public IReadOnlyList<WireColumn> Columns { get; } =
        [
            new WireColumn("Id", ColumnKind.Int64),
            new WireColumn("VisitCount", ColumnKind.UInt64),
        ];

        public OldEditedChunk DecodeRow(ReadOnlySpan<byte> data) => throw new InvalidOperationException("never reached");

        public byte[] EncodePrimaryKey(in OldEditedChunk row) => KeyCodec.EncodeInt64(row.Id).ToArray();
    }

    [Fact]
    public async Task Old_bindings_read_their_columns_from_a_table_that_gained_others()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var cache = registry.GetOrAdd(OldEditedChunkCodec.Instance);
        var updates = 0;
        cache.OnUpdate += (_, _) => Interlocked.Increment(ref updates);

        await registry.SubscribeAsync(
            OldEditedChunkCodec.Instance, "SELECT * FROM EditedChunk", null, TestContext.Current.CancellationToken);
        var lsn = host.Call("EditChunkVisited", 7L, 3u, -12, 900UL);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the insert to arrive");

        var row = Assert.Single(cache.Rows);
        Assert.Equal((7L, 900UL), (row.Id, row.Visits));

        // An update that only touches columns these bindings cannot see is no change to them.
        lsn = host.Call("EditChunkVisited", 7L, 4u, -40, 900UL);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the invisible update to arrive");
        Assert.Equal(0, Volatile.Read(ref updates));

        // One that touches a column they read is.
        lsn = host.Call("EditChunkVisited", 7L, 4u, -40, 901UL);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the visible update to arrive");
        Assert.Equal(1, Volatile.Read(ref updates));
        Assert.Equal(901UL, Assert.Single(cache.Rows).Visits);
    }

    [Fact]
    public async Task Bindings_that_read_a_column_the_server_lacks_still_refuse_by_name()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        host.Call("EditChunkVisited", 7L, 3u, -12, 900UL);

        var failure = await Assert.ThrowsAsync<MelangeSubscriptionException>(() => registry.SubscribeAsync(
            RenamedEditedChunkCodec.Instance, "SELECT * FROM EditedChunk", null, TestContext.Current.CancellationToken));

        Assert.Contains("these bindings read column 'VisitCount'", failure.Message);
    }

    [Fact]
    public async Task A_call_from_bindings_that_predate_a_defaulted_parameter_gets_the_default()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.CallReducerAsync("SpawnInRoom", ["Old"], TestContext.Current.CancellationToken);
        Assert.Equal(42, RoomOf(host));

        await client.CallReducerAsync("SpawnInRoom", ["New", 3], TestContext.Current.CancellationToken);
        Assert.Equal(3, RoomOf(host));
    }

    [Fact]
    public async Task A_call_missing_a_required_parameter_is_refused_naming_it()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var missing = await Assert.ThrowsAsync<MelangeCallException>(
            () => client.CallReducerAsync("Spawn", ["NoRoom"], TestContext.Current.CancellationToken));
        Assert.Contains("missing 'roomId'", missing.Message);
        Assert.Contains("declares a default value", missing.Message);

        var extra = await Assert.ThrowsAsync<MelangeCallException>(
            () => client.CallReducerAsync("SpawnInRoom", ["Many", 1, 2], TestContext.Current.CancellationToken));
        Assert.Contains("at most 2 (name, roomId)", extra.Message);
    }

    private static int RoomOf(TransportTestHost host)
    {
        var schema = host.Engine.Schema.Get(typeof(PlayerState));
        return host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(schema.Id)
            .Select(pair => ((PlayerState)Core.RowSerializer.Deserialize(schema, pair.Value.ToArray())).RoomId)
            .Single());
    }
}
