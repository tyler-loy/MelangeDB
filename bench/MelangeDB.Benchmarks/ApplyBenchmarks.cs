using BenchmarkDotNet.Attributes;
using MelangeDB.Core;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Measurement gap 3: what does batching a record's ops into one version publish buy, against
/// publishing a version per op?
/// <para>
/// The in-memory store keeps rows and indexes in persistent containers so that pinning a read view
/// is a reference capture. The bill for that is paid on write: every published version costs a path
/// copy of the row map plus one of every secondary index it touched. A record's intermediate
/// versions are never observed — the whole record applies under the engine's write lock — so
/// publishing one per op was buying nothing and paying per op.
/// </para>
/// <para>
/// The comparison is one N-op record against N one-op records carrying the same ops. Indexes are a
/// parameter because they are the multiplier: the copy is per index, so the gap should widen with
/// them, and a table with no secondary index is the case where batching should barely register.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ApplyBenchmarks
{
    private SchemaRegistry _schema = null!;
    private TableId _table;
    private RowKey[] _keys = [];
    private byte[][] _rows = [];
    private ulong _lsn;

    /// <summary>Ops per record — a game tick's write set against a sweep's.</summary>
    [Params(1, 10, 100)]
    public int Ops { get; set; }

    [Params(0, 2)]
    public int Indexes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _schema = Indexes == 0
            ? SchemaRegistry.FromTypes(typeof(PlainRow))
            : SchemaRegistry.FromTypes(typeof(IndexedRow));
        var table = _schema.Tables[0];
        _table = table.Id;

        // A supply large enough that no run reuses a key while a record is still being measured.
        _keys = new RowKey[Ops * 1024];
        _rows = new byte[_keys.Length][];
        for (var i = 0; i < _keys.Length; i++)
        {
            _keys[i] = SchemaKeyCodec.Encode(table.PrimaryKey, (ulong)i);
            _rows[i] = Indexes == 0
                ? RowSerializer.Serialize(table, new PlainRow { Id = (ulong)i, Payload = [] })
                : RowSerializer.Serialize(table, new IndexedRow
                {
                    Id = (ulong)i,
                    RoomId = (ulong)(i % 64),
                    OwnerId = (ulong)(i % 128),
                });
        }
    }

    /// <summary>All of a record's ops in one apply — one version publish per table.</summary>
    [Benchmark(Description = "one batched record", Baseline = true)]
    public long BatchedRecord()
    {
        var store = NewStore();
        var ops = new List<RowOp>(Ops);
        for (var i = 0; i < Ops; i++)
            ops.Add(new RowOp(RowOpKind.Insert, _table, _keys[i], _rows[i]));
        store.Apply(Record(++_lsn, ops));
        return store.Count(_table);
    }

    /// <summary>The same ops as one record each — a version publish per op, as before.</summary>
    [Benchmark(Description = "one record per op")]
    public long RecordPerOp()
    {
        var store = NewStore();
        for (var i = 0; i < Ops; i++)
        {
            store.Apply(Record(++_lsn, [new RowOp(RowOpKind.Insert, _table, _keys[i], _rows[i])]));
        }

        return store.Count(_table);
    }

    /// <summary>
    /// A fresh store per iteration. Both cases must start from the same empty table, or whichever
    /// runs second measures inserting into a container the first one already grew.
    /// </summary>
    private InMemoryHotStore NewStore() =>
        new(_schema, ResidencyResolver.Resolve(_schema, new MelangeDbOptions().Residency));

    private static CommitRecord Record(ulong lsn, IReadOnlyList<RowOp> ops) => new()
    {
        Lsn = lsn,
        FormatVersion = 2,
        Timestamp = new Timestamp((long)lsn),
        Caller = Identity.Hash("bench"),
        ReducerName = "bench",
        Arguments = ReadOnlyMemory<byte>.Empty,
        WriteSet = ops,
        SerializedLength = 0,
    };
}
