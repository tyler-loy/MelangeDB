using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Storage.Faster;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Measurement gap 6: what does the FASTER hash table's size cost when the row count outgrows it?
/// <para>
/// The size was a fixed 65,536 buckets regardless of the memory budget or the row count. A hash
/// table does not fail when it is too small — it degrades, quietly, by lengthening chains, and on a
/// paged table a longer chain means more records touched per lookup and more pending I/O
/// completions. That is the worst kind of ceiling: no error, no log line, just a store that gets
/// slower as the game gets more players.
/// </para>
/// <para>
/// The suite runs the same workload against a deliberately undersized table and against the size
/// the budget now derives, at row counts on both sides of the old fixed number. Below it the two
/// should agree; above it, the undersized row should separate — and where it separates is the
/// number that says whether the operator knob is worth documenting or was a fix for nothing.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class FasterHashBenchmarks : IDisposable
{
    /// <summary>Deliberately small, so chains are long at the larger row counts.</summary>
    private const long Undersized = 1L << 12;

    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private SchemaRegistry _schema = null!;
    private IHotStore _store = null!;
    private TableId _table;
    private RowKey[] _lookupOrder = [];

    [Params(50_000, 500_000)]
    public int Rows { get; set; }

    /// <summary>Zero means "derive from the memory budget" — the behaviour finding #10 introduced.</summary>
    [Params(0L, Undersized)]
    public long HashBuckets { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-hash-").FullName;
        _schema = BenchSchema.For(nameof(CommitRow));
        _table = _schema.Tables[0].Id;

        var options = new MelangeDbOptions
        {
            HotStore =
            {
                Path = Path.Combine(_root, "hot"),
                MemoryBudgetBytes = 256L * 1024 * 1024,
                HashBuckets = HashBuckets,
            },
            CommitLog = { Path = Path.Combine(_root, "log") },
        };
        Directory.CreateDirectory(options.HotStore.Path);

        _store = new FasterHotStoreProvider().Create(new HotStoreContext
        {
            Schema = _schema,
            Options = options,
            Residency = ResidencyResolver.Resolve(_schema, options.Residency),
            LoggerFactory = NullLoggerFactory.Instance,
        });
        if (_store is IDisposable disposable)
            _disposables.Add(disposable);

        var payload = new byte[96];
        var keys = new RowKey[Rows];
        var ops = new List<RowOp>(Rows);
        for (var i = 0; i < Rows; i++)
        {
            keys[i] = SchemaKeyCodec.Encode(_schema.Tables[0].PrimaryKey, (ulong)i);
            ops.Add(new RowOp(RowOpKind.Insert, _table, keys[i], payload));
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

        // Shuffled, because sequential keys hash to spread buckets and would walk the table in an
        // order no workload produces — which is exactly the access pattern a too-small hash table
        // handles best, and so the one that would hide the effect being measured.
        _lookupOrder = Shuffle(keys);
    }

    /// <summary>Ten thousand point reads in random order — the chain length shows up here.</summary>
    [Benchmark(Description = "10k random point reads")]
    public long PointReads()
    {
        long hits = 0;
        for (var i = 0; i < 10_000; i++)
        {
            if (_store.TryGetRow(_table, _lookupOrder[i], out _))
                hits++;
        }

        return hits;
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        foreach (var disposable in _disposables)
            disposable.Dispose();
        _disposables.Clear();
        GC.SuppressFinalize(this);
        try
        {
            if (_root.Length > 0)
                Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A store handle Windows has not released yet; the temp reaper gets it.
        }
    }

    private static RowKey[] Shuffle(RowKey[] source)
    {
        var copy = (RowKey[])source.Clone();
        var random = new Random(12345);
        for (var i = copy.Length - 1; i > 0; i--)
        {
            var j = random.Next(i + 1);
            (copy[i], copy[j]) = (copy[j], copy[i]);
        }

        return copy;
    }
}
