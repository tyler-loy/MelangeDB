using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The subscribed half of issue #137's live-repair story. When a table has a subscriber, the commit
/// fan-out reads each op's pre-image to decide who held the row. If that pre-image is unreadable — a
/// row whose stored projection is corrupt — the read threw, and the throw escaped into the commit,
/// so a write that would <em>repair or remove</em> the poisoned row could never land: the row was
/// wedged until a restart, and any reducer touching that table (a scheduled sweep) failed with it.
/// The fan-out now treats an unreadable pre-image as "no prior row", so the repairing write commits.
/// <para>
/// The corruption is FASTER-specific (the in-memory store does not split blobs), and this layer is
/// the server, so the fault is injected with a store wrapper that throws <see cref="InvalidDataException"/>
/// on a point read of one key — exactly what the FASTER store does over a corrupt out-of-line
/// payload (its own end-to-end resilience is covered in the storage tests). This pins the one thing
/// that is server logic: the fan-out does not propagate that throw.
/// </para>
/// </summary>
public class FanoutPoisonedPreImageTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-poison-preimage-").FullName;
    private readonly SchemaRegistry _schema;
    private readonly PoisonableStore _store;
    private readonly MelangeEngine _engine;
    private readonly SubscriptionEngine _subscriptions;

    public FanoutPoisonedPreImageTests()
    {
        _schema = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables()
            .Where(t => t.RowType == typeof(PlayerState) || t.RowType == typeof(Chunk)));
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot") },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.OsBuffered },
            Snapshots = { Enabled = false },
        };
        _store = new PoisonableStore(new InMemoryHotStore(_schema));
        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance, hotStoreProvider: new Provider(_store));
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
        }
    }

    [Fact]
    public void A_repairing_update_over_an_unreadable_pre_image_commits_and_reaches_subscribers()
    {
        var id = Identity.Hash("p");
        _engine.Invoke("spawn", id, ctx => ctx.Db.Insert(new PlayerState { Id = id, RoomId = 7, Name = "p", X = 0 }));

        var table = _schema.Get(typeof(PlayerState));
        var key = ((RowCodec<PlayerState>)table.Codec!).EncodePrimaryKey(new PlayerState { Id = id });
        _store.Poison(table.Id, key);

        var sink = new CapturingSink();
        var query = SqlSubsetParser.Parse("SELECT * FROM PlayerState", null);
        _engine.ReadConsistent(head =>
            _subscriptions.Register(sink, 1, query, new SubscriptionsOptions(), head, computeInitialSet: false));

        // The pre-image of this row is now unreadable. Before the fix this threw out of Fanout and
        // failed the commit; now it is treated as "no prior row", so the update lands as an insert.
        var record = Update(table, new PlayerState { Id = id, RoomId = 7, Name = "p", X = 9 });
        var exception = Record.Exception(() => _subscriptions.Fanout(record));
        Assert.Null(exception);

        var op = Assert.Single(Assert.Single(sink.Frames).Updates).Ops.Single();
        Assert.Equal(RowOpKind.Insert, op.Kind);
    }

    [Fact]
    public void A_delete_over_an_unreadable_pre_image_commits_without_throwing()
    {
        var id = Identity.Hash("q");
        _engine.Invoke("spawn", id, ctx => ctx.Db.Insert(new PlayerState { Id = id, RoomId = 3, Name = "q", X = 0 }));

        var table = _schema.Get(typeof(PlayerState));
        var key = ((RowCodec<PlayerState>)table.Codec!).EncodePrimaryKey(new PlayerState { Id = id });
        _store.Poison(table.Id, key);

        var sink = new CapturingSink();
        var query = SqlSubsetParser.Parse("SELECT * FROM PlayerState", null);
        _engine.ReadConsistent(head =>
            _subscriptions.Register(sink, 1, query, new SubscriptionsOptions(), head, computeInitialSet: false));

        var record = new CommitRecord
        {
            Lsn = _engine.Log.HeadLsn + 1,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("t"),
            ReducerName = "t",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [new RowOp(RowOpKind.Delete, table.Id, key)],
            SerializedLength = 0,
        };

        Assert.Null(Record.Exception(() => _subscriptions.Fanout(record)));
    }

    [Fact]
    public void A_primary_key_range_initial_set_skips_an_unreadable_row()
    {
        // The initial-set gap #138 left: a PK-range subscribe materializes each key with a point
        // read, which threw on a poisoned row and killed the subscribing connection. It now skips
        // the row, exactly as the store's full-table Scan does.
        _engine.Invoke("seed", Identity.Hash("s"), ctx =>
        {
            for (var i = 1; i <= 3; i++)
                ctx.Db.Insert(new Chunk { Id = i, X = i, Data = [] });
        });

        var table = _schema.Get(typeof(Chunk));
        _store.Poison(table.Id, SchemaKeyCodec.Encode(table.PrimaryKey, 2L));

        var query = SqlSubsetParser.Parse("SELECT * FROM Chunk WHERE Id BETWEEN 1 AND 3", null);
        var initial = _engine.ReadConsistent(head =>
            _subscriptions.Register(new CapturingSink(), 1, query, new SubscriptionsOptions(), head, computeInitialSet: true).InitialSet);

        // Two rows come back — 1 and 3 — and the poisoned 2 is simply absent, no throw.
        Assert.Equal(2, initial.Rows.Count);
        Assert.DoesNotContain(SchemaKeyCodec.Encode(table.PrimaryKey, 2L), initial.Rows.Select(r => r.Key));
    }

    [Fact]
    public void A_primary_key_equality_initial_set_on_an_unreadable_row_is_empty()
    {
        _engine.Invoke("seed", Identity.Hash("s"), ctx => ctx.Db.Insert(new Chunk { Id = 42, X = 1, Data = [] }));

        var table = _schema.Get(typeof(Chunk));
        _store.Poison(table.Id, SchemaKeyCodec.Encode(table.PrimaryKey, 42L));

        var query = SqlSubsetParser.Parse("SELECT * FROM Chunk WHERE Id = 42", null);
        var initial = _engine.ReadConsistent(head =>
            _subscriptions.Register(new CapturingSink(), 1, query, new SubscriptionsOptions(), head, computeInitialSet: true).InitialSet);

        Assert.Empty(initial.Rows);
    }

    private CommitRecord Update(TableSchema table, PlayerState row)
    {
        var codec = (RowCodec<PlayerState>)table.Codec!;
        return new CommitRecord
        {
            Lsn = _engine.Log.HeadLsn + 1,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("t"),
            ReducerName = "t",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [new RowOp(RowOpKind.Update, table.Id, codec.EncodePrimaryKey(in row), codec.Serialize(in row))],
            SerializedLength = 0,
        };
    }

    private sealed class Provider(IHotStore store) : IHotStoreProvider
    {
        public HotStoreEngine Engine => HotStoreEngine.InMemory;

        public IHotStore Create(HotStoreContext context) => store;
    }

    private sealed class CapturingSink : IDeltaSink
    {
        public List<TransactionUpdateFrame> Frames { get; } = [];

        public void EnqueueDelta(TransactionUpdateFrame frame) => Frames.Add(frame);
    }

    /// <summary>
    /// An in-memory store that throws <see cref="InvalidDataException"/> on a point read of one
    /// designated key — standing in for the FASTER store reading a corrupt out-of-line payload.
    /// Every other operation delegates unchanged; a scan skips the poisoned key rather than throwing,
    /// which is how the real store's scan path already behaves.
    /// </summary>
    private sealed class PoisonableStore(IHotStore inner) : IHotStore
    {
        private readonly HashSet<(TableId, RowKey)> _poisoned = [];

        public void Poison(TableId table, in RowKey key) => _poisoned.Add((table, key));

        public ulong AppliedLsn => inner.AppliedLsn;

        public void Apply(CommitRecord record) => inner.Apply(record);

        public HotStoreStatistics Statistics() => inner.Statistics();

        public void LoadSnapshot(ulong lsn, IEnumerable<SnapshotRow> rows) => inner.LoadSnapshot(lsn, rows);

        public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row)
        {
            if (_poisoned.Contains((table, key)))
                throw new InvalidDataException($"Table {table}: key {key}: simulated corrupt out-of-line payload.");
            return inner.TryGetRow(table, key, out row);
        }

        public bool ContainsKey(TableId table, in RowKey key) => inner.ContainsKey(table, key);

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table)
        {
            foreach (var pair in inner.Scan(table))
            {
                if (!_poisoned.Contains((table, pair.Key)))
                    yield return pair;
            }
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value) =>
            inner.ScanIndex(table, column, value);

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high) =>
            inner.ScanIndexRange(table, column, low, high);

        public long Count(TableId table) => inner.Count(table);

        public IEnumerable<RowKey> ScanKeys(TableId table) => inner.ScanKeys(table);

        public IEnumerable<RowKey> ScanKeyRange(TableId table, RowKey low, RowKey high) => inner.ScanKeyRange(table, low, high);
    }
}
