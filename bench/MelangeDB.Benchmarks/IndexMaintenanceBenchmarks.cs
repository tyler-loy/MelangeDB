using BenchmarkDotNet.Attributes;
using MelangeDB.Core;

namespace MelangeDB.Benchmarks;

/// <summary>What a benchmarked row's non-indexed columns cost to deserialize.</summary>
public enum RowColumns
{
    /// <summary>Fixed-width scalars only — a full deserialize is a handful of reads.</summary>
    Scalar,

    /// <summary>Two strings and a byte array — a full deserialize allocates and copies.</summary>
    Mixed,
}

/// <summary>
/// Measurement gap 4: what does extracting a row's indexed columns cost, and how does that scale
/// with the number of indexes?
/// <para>
/// Index maintenance runs on every write, inside the store lock, turning row bytes into one
/// order-preserving key per indexed column. Asking the codec for one column at a time deserializes
/// the <b>whole row</b> per call, so an eight-index table deserialized its rows eight times per
/// write. Finding #5 replaced that with a single pass.
/// </para>
/// <para>
/// <see cref="Columns"/> exists because the first version of this suite measured scalar-only rows
/// and reported the single pass as <i>slower</i> below eight indexes. That is a real result, and the
/// finding's own reasoning explains it: a row of fixed-width scalars costs almost nothing to
/// deserialize, so removing repeat deserializes saves less than iterating a column list costs. The
/// saving lives in the columns that must be allocated and copied on every deserialize — strings and
/// byte arrays. Both shapes are measured because reporting only the flattering one is how a
/// benchmark becomes an advertisement.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class IndexMaintenanceBenchmarks
{
    private const int WriteRows = 100;

    private SchemaRegistry _schema = null!;
    private TableSchema _table = null!;
    private RowCodec _codec = null!;
    private string[] _indexColumns = [];
    private RowKey[] _scratch = [];
    private byte[] _row = [];
    private RowKey[] _keys = [];
    private byte[][] _rows = [];
    private ulong _lsn;

    [Params(1, 4, 8)]
    public int Indexes { get; set; }

    [Params(RowColumns.Scalar, RowColumns.Mixed)]
    public RowColumns Columns { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _schema = BenchSchema.For(TableName());
        _table = _schema.Tables[0];

        // The suite is meaningless against the reflection fallback: "deserializes the whole row per
        // column" is a statement about the generated codec. BenchSchema hands back the generated
        // schema for exactly this reason, and throwing beats measuring the wrong path quietly.
        _codec = _table.Codec
            ?? throw new InvalidOperationException(
                $"{_table.Name} has no generated codec; the index benchmarks would measure the reflection fallback.");

        _indexColumns = [.. _table.Indexes.Select(i => i.Column)];
        _scratch = new RowKey[_indexColumns.Length];

        _keys = new RowKey[WriteRows * 512];
        _rows = new byte[_keys.Length][];
        for (var i = 0; i < _keys.Length; i++)
        {
            _keys[i] = SchemaKeyCodec.Encode(_table.PrimaryKey, (ulong)i);
            _rows[i] = Serialize((ulong)i);
        }

        _row = _rows[0];
    }

    /// <summary>One pass over the row bytes, filling every indexed column's key.</summary>
    [Benchmark(Description = "extract: single pass", Baseline = true), BenchmarkCategory("extract")]
    public int ExtractSinglePass()
    {
        _codec.EncodeColumnsFromBytes(_row, _indexColumns, _scratch);
        return _scratch.Length;
    }

    /// <summary>
    /// A call per indexed column, as before — each one deserializing the whole row again. This row
    /// is what finding #5 removed, kept here because a fix whose cost was never shown is a claim.
    /// </summary>
    [Benchmark(Description = "extract: one call per column"), BenchmarkCategory("extract")]
    public int ExtractPerColumn()
    {
        var total = 0;
        foreach (var column in _indexColumns)
        {
            if (_codec.EncodeColumnFromBytes(column, _row) is { } key)
                total += key.Length;
        }

        return total;
    }

    /// <summary>The extract in situ: a hundred-row insert with every index maintained.</summary>
    [Benchmark(Description = "apply 100 rows"), BenchmarkCategory("apply")]
    public long ApplyIndexedRows()
    {
        var store = new InMemoryHotStore(_schema, ResidencyResolver.Resolve(_schema, new MelangeDbOptions().Residency));
        var ops = new List<RowOp>(WriteRows);
        for (var i = 0; i < WriteRows; i++)
            ops.Add(new RowOp(RowOpKind.Insert, _table.Id, _keys[i], _rows[i]));
        store.Apply(new CommitRecord
        {
            Lsn = ++_lsn,
            FormatVersion = 2,
            Timestamp = new Timestamp((long)_lsn),
            Caller = Identity.Hash("bench"),
            ReducerName = "bench",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = ops,
            SerializedLength = 0,
        });
        return store.Count(_table.Id);
    }

    /// <summary>
    /// The mixed shape only has a one-index and an eight-index table; four would add a row type
    /// whose answer sits between two the suite already reports.
    /// </summary>
    private string TableName() => (Columns, Indexes) switch
    {
        (RowColumns.Mixed, 1) => nameof(MixedIndex1Row),
        (RowColumns.Mixed, _) => nameof(MixedIndex8Row),
        (_, 1) => nameof(Index1Row),
        (_, 4) => nameof(Index4Row),
        _ => nameof(Index8Row),
    };

    /// <summary>
    /// Values spread across the indexed columns so no index degenerates to one key holding every
    /// row — a single-entry index would make the container work disappear and flatter the result.
    /// </summary>
    private byte[] Serialize(ulong i)
    {
        var name = $"entity-{i}";
        var description = $"a description long enough that copying it costs something, for row {i}";
        var blob = new byte[64];
        return TableName() switch
        {
            nameof(MixedIndex1Row) => RowSerializer.Serialize(_table, new MixedIndex1Row
            {
                Id = i, A = i % 64, Name = name, Description = description, Blob = blob,
            }),
            nameof(MixedIndex8Row) => RowSerializer.Serialize(_table, new MixedIndex8Row
            {
                Id = i, A = i % 64, B = i % 32, C = i % 16, D = i % 8, E = i % 4, F = i % 2, G = i, H = i + 1,
                Name = name, Description = description, Blob = blob,
            }),
            nameof(Index1Row) => RowSerializer.Serialize(_table, new Index1Row
            {
                Id = i, A = i % 64, B = i % 32, C = i % 16, D = i % 8, E = i % 4, F = i % 2, G = i, H = i + 1,
            }),
            nameof(Index4Row) => RowSerializer.Serialize(_table, new Index4Row
            {
                Id = i, A = i % 64, B = i % 32, C = i % 16, D = i % 8, E = i % 4, F = i % 2, G = i, H = i + 1,
            }),
            _ => RowSerializer.Serialize(_table, new Index8Row
            {
                Id = i, A = i % 64, B = i % 32, C = i % 16, D = i % 8, E = i % 4, F = i % 2, G = i, H = i + 1,
            }),
        };
    }
}
