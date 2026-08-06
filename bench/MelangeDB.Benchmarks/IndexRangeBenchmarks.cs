using BenchmarkDotNet.Attributes;
using MelangeDB.Core;

namespace MelangeDB.Benchmarks;

/// <summary>Where in the indexed key space a range benchmark's window sits.</summary>
public enum RangePosition
{
    /// <summary>At the very start of the index — nothing to skip to reach it.</summary>
    Low,

    /// <summary>In the middle.</summary>
    Middle,

    /// <summary>At the very end — everything else has to be skipped to reach it.</summary>
    High,
}

/// <summary>
/// The gate on finding #12: does a secondary-index range scan pay for where the window sits?
/// <para>
/// It should not. A range query over a sorted index is supposed to seek to the lower bound and read
/// forward, so a ten-row window costs the same whether it sits at the front of the index or the
/// back. The suite is built so that the answer is unmissable: the same window size, at three
/// positions, over an index large enough that "walk from the leftmost key and skip" and "seek"
/// differ by orders of magnitude rather than by noise.
/// </para>
/// <para>
/// <b>A rising line from Low to High is the bug.</b> A flat one is the fix. This is the rare
/// benchmark whose <i>shape</i> is the entire result — the absolute numbers say little, but a High
/// row that costs many times its Low row says the scan is walking the whole index to find its
/// window, which is what <c>ImmutableSortedDictionary</c> forces when the code merely filters its
/// enumerator.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class IndexRangeBenchmarks
{
    private const int Buckets = 10_000;
    private const int RowsPerBucket = 4;
    private const int WindowBuckets = 4;

    private SchemaRegistry _schema = null!;
    private TableSchema _table = null!;
    private IHotStore _store = null!;
    private RowKey _low;
    private RowKey _high;

    [Params(RangePosition.Low, RangePosition.Middle, RangePosition.High)]
    public RangePosition Position { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _schema = SchemaRegistry.FromTypes(typeof(RangeRow));
        _table = _schema.Tables[0];
        _store = new InMemoryHotStore(_schema, ResidencyResolver.Resolve(_schema, new MelangeDbOptions().Residency));

        var payload = new byte[64];
        var ops = new List<RowOp>(Buckets * RowsPerBucket);
        for (var bucket = 0; bucket < Buckets; bucket++)
        {
            for (var n = 0; n < RowsPerBucket; n++)
            {
                var id = (ulong)((bucket * RowsPerBucket) + n);
                ops.Add(new RowOp(
                    RowOpKind.Insert,
                    _table.Id,
                    SchemaKeyCodec.Encode(_table.PrimaryKey, id),
                    RowSerializer.Serialize(_table, new RangeRow { Id = id, Bucket = (ulong)bucket, Payload = payload })));
            }
        }

        _store.Apply(new CommitRecord
        {
            Lsn = 1,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("bench"),
            ReducerName = "bench",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = ops,
            SerializedLength = 0,
        });

        var start = Position switch
        {
            RangePosition.Low => 0,
            RangePosition.Middle => (Buckets / 2) - (WindowBuckets / 2),
            _ => Buckets - WindowBuckets,
        };
        var column = _table.Column(nameof(RangeRow.Bucket));
        _low = SchemaKeyCodec.Encode(column, (ulong)start);
        _high = SchemaKeyCodec.Encode(column, (ulong)(start + WindowBuckets - 1));
    }

    /// <summary>
    /// The same sixteen rows every time. Only where they sit in the index changes, so any difference
    /// between the parameter values is the cost of reaching them.
    /// </summary>
    [Benchmark(Description = "range over a secondary index")]
    public long ScanRange()
    {
        long bytes = 0;
        foreach (var pair in _store.ScanIndexRange(_table.Id, nameof(RangeRow.Bucket), _low, _high))
            bytes += pair.Value.Length;
        return bytes;
    }
}
