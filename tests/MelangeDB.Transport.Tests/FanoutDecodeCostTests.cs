using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// What the fan-out costs per subscriber, pinned where it is observable.
/// <para>
/// One committed row is evaluated for every subscriber on its table, under the engine's write
/// lock. A predicate on an indexed column used to decode the whole row per subscriber to encode
/// one column of it; a row or column policy decoded it again per subscriber to hand the policy a
/// typed row. The row is the same for all of them — only the verdict is per caller — so N
/// subscribers cost N (or 2N, or 4N) full deserializations per op, and the suite never saw it
/// because no test put more than a handful of subscribers on a table. These count decodes through
/// the table's codec: an op must decode its row a fixed number of times however many subscribers
/// watch it.
/// </para>
/// </summary>
public class FanoutDecodeCostTests : IDisposable
{
    private const int Subscribers = 60;

    private readonly string _root = Directory.CreateTempSubdirectory("melange-fanout-cost-").FullName;
    private readonly SchemaRegistry _schema = CountingCodec<PlayerState>.Registry(typeof(PlayerState), typeof(InventoryItem), typeof(AdminIdentity));
    private readonly MelangeEngine _engine;
    private readonly SubscriptionEngine _subscriptions;

    public FanoutDecodeCostTests()
    {
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot") },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.OsBuffered },
            Snapshots = { Enabled = false },
        };
        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance);
        _subscriptions = new SubscriptionEngine(_engine, telemetry: null);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A store handle Windows has not released yet; the temp reaper gets it.
        }
    }

    [Fact]
    public void An_indexed_column_predicate_decodes_the_row_once_per_op_not_once_per_subscriber()
    {
        var player = Identity.Hash("p");
        _engine.Invoke("Spawn", player, ctx => ctx.Db.Insert(new PlayerState { Id = player, RoomId = 7, Name = "p", X = 0 }));

        for (var i = 0; i < Subscribers; i++)
        {
            var query = SqlSubsetParser.Parse("SELECT * FROM PlayerState WHERE RoomId = 7", null);
            _engine.ReadConsistent(head =>
                _subscriptions.Register(new NullSink(), (uint)(i + 1), query, new SubscriptionsOptions(), head, computeInitialSet: false));
        }

        var counter = CountingCodec<PlayerState>.CounterFor(_schema, typeof(PlayerState));
        counter.Reset();
        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 7, Name = "p", X = 1 }));

        // Two rows are in play — the pre-image and the new row — so two decodes is the floor. The
        // old path decoded each of them once per subscriber: 120 here.
        Assert.InRange(counter.Deserializations, 0, 2);

        // And the verdict is still per subscriber: moving the row out of the room deletes it from
        // every one of them, without re-decoding per subscriber either.
        counter.Reset();
        var sink = new CountingSink();
        var moved = SqlSubsetParser.Parse("SELECT * FROM PlayerState WHERE RoomId = 7", null);
        _engine.ReadConsistent(head => _subscriptions.Register(sink, 999, moved, new SubscriptionsOptions(), head, computeInitialSet: false));
        _subscriptions.Fanout(Update(new PlayerState { Id = player, RoomId = 8, Name = "p", X = 1 }));
        Assert.InRange(counter.Deserializations, 0, 2);
        Assert.Equal(RowOpKind.Delete, Assert.Single(Assert.Single(sink.Frames).Updates).Ops.Single().Kind);
    }

    [Fact]
    public void Row_and_column_policies_decode_the_row_once_per_op_not_once_per_subscriber()
    {
        // Row policies on InventoryItem (two, unioned; the first is per-caller ownership) and a
        // column policy on PlayerState — the two shapes that materialize a typed row per call.
        var services = new ServiceCollection();
        services.AddSingleton<IRowPolicy<InventoryItem>, InventoryVisibility>();
        services.AddSingleton<IRowPolicy<InventoryItem>, AdminSeesAllInventory>();
        services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>();
        var policies = new PolicySet(services.BuildServiceProvider(), _schema);

        var owner = Identity.Hash("owner");
        _engine.Invoke("Give", owner, ctx => ctx.Db.Insert(new InventoryItem { Id = 1, OwnerId = owner, ItemName = "pick" }));
        _engine.Invoke("Spawn", owner, ctx => ctx.Db.Insert(new PlayerState { Id = owner, RoomId = HideoutHidesPosition.HideoutRoom, Name = "o", X = 0 }));

        for (var i = 0; i < Subscribers; i++)
        {
            // Every subscriber is a different caller, so no two verdicts can be shared — only the decode can.
            var context = new PolicyContext(Identity.Hash($"viewer{i}"), false, _engine.CommittedView);
            var itemQuery = SqlSubsetParser.Parse("SELECT * FROM InventoryItem", null);
            var playerQuery = SqlSubsetParser.Parse("SELECT * FROM PlayerState", null);
            _engine.ReadConsistent(head =>
            {
                _subscriptions.Register(new NullSink(), (uint)(2 * i + 1), itemQuery, new SubscriptionsOptions(), head, computeInitialSet: false, policies, context);
                _subscriptions.Register(new NullSink(), (uint)(2 * i + 2), playerQuery, new SubscriptionsOptions(), head, computeInitialSet: false, policies, context);
            });
        }

        var items = CountingCodec<PlayerState>.CounterFor(_schema, typeof(InventoryItem));
        var players = CountingCodec<PlayerState>.CounterFor(_schema, typeof(PlayerState));
        items.Reset();
        players.Reset();

        _subscriptions.Fanout(Update(new InventoryItem { Id = 1, OwnerId = owner, ItemName = "axe" }));
        _subscriptions.Fanout(Update(new PlayerState { Id = owner, RoomId = HideoutHidesPosition.HideoutRoom, Name = "o", X = 5 }));

        // Row policies: pre-image and new row, once each. Column policies: the same two rows,
        // intersected per subscriber from one decode each. The old path was 2N and 4N.
        Assert.InRange(items.Deserializations, 0, 2);
        Assert.InRange(players.Deserializations, 0, 2);
    }

    /// <summary>A prepared update record for one row, as the fan-out sees it — the bench's shape.</summary>
    private CommitRecord Update<TRow>(TRow row)
        where TRow : struct
    {
        var table = _schema.Get(typeof(TRow));
        var codec = (RowCodec<TRow>)table.Codec!;
        return new CommitRecord
        {
            Lsn = _engine.Log.HeadLsn + 1,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("test"),
            ReducerName = "test",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [new RowOp(RowOpKind.Update, table.Id, codec.EncodePrimaryKey(in row), codec.Serialize(in row))],
            SerializedLength = 0,
        };
    }

    private sealed class NullSink : IDeltaSink
    {
        public void EnqueueDelta(TransactionUpdateFrame frame)
        {
        }
    }

    private sealed class CountingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }
}

/// <summary>A generated codec with a counter on <see cref="Deserialize"/>; see the Core tests' twin for the rationale.</summary>
internal sealed class CountingCodec<TRow>(RowCodec<TRow> inner) : RowCodec<TRow>, IDecodeCounter
    where TRow : struct
{
    private long _deserializations;

    public long Deserializations => Interlocked.Read(ref _deserializations);

    public void Reset() => Interlocked.Exchange(ref _deserializations, 0);

    public override byte[] Serialize(in TRow row) => inner.Serialize(in row);

    public override TRow Deserialize(ReadOnlySpan<byte> data)
    {
        Interlocked.Increment(ref _deserializations);
        return inner.Deserialize(data);
    }

    public override RowKey EncodePrimaryKey(in TRow row) => inner.EncodePrimaryKey(in row);

    public override RowKey? EncodeColumn(string column, in TRow row) => inner.EncodeColumn(column, in row);

    public override void AssignAutoInc(ref TRow row, AutoIncStage stage, TableId table) => inner.AssignAutoInc(ref row, stage, table);

    public static SchemaRegistry Registry(params Type[] tables)
    {
        var wrapped = new List<TableSchema>();
        foreach (var table in new MelangeDB.Generated.MelangeModel().Tables())
        {
            if (!tables.Contains(table.RowType))
                continue;
            var codec = (RowCodec)Activator.CreateInstance(typeof(CountingCodec<>).MakeGenericType(table.RowType), table.Codec)!;
            wrapped.Add(new TableSchema(
                table.RowType, table.Name, table.Columns, table.IsPublic, table.Tier, table.Residency,
                table.Placement, table.ShardBy, table.Scheduled, codec));
        }

        return new SchemaRegistry(wrapped);
    }

    public static IDecodeCounter CounterFor(SchemaRegistry registry, Type table) => (IDecodeCounter)registry.Get(table).Codec!;
}

internal interface IDecodeCounter
{
    long Deserializations { get; }

    void Reset();
}
