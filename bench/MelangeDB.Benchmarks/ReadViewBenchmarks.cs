using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Storage.Faster;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>Which hot store a benchmark case runs against.</summary>
public enum Engine
{
    InMemory,
    Faster,
}

/// <summary>
/// What a pinned read view costs at the store seam — the numbers the snapshot-isolation design asks
/// for, measured rather than assumed.
/// <para>
/// Three questions. <b>Opening</b> a view should be independent of how many rows the store holds,
/// or the whole mechanism is a copy wearing a different name. <b>Holding</b> one should cost the
/// write path little: the in-memory store pays nothing, because the containers were already
/// persistent, while the FASTER store pays a pre-image read per paged row written during the window
/// — the price of a hybrid log that overwrites in place. <b>Reading</b> through a view should
/// resemble reading live, and where it does not, the gap belongs in the documentation rather than in
/// a surprise.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReadViewBenchmarks : IDisposable
{
    private const int RowBytes = 96;
    private const int WriteSetRows = 100;

    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private SchemaRegistry _schema = null!;
    private IHotStore _store = null!;
    private TableId _table;
    private RowKey[] _keys = [];
    private CommitRecord[] _writes = [];
    private ulong _lsn;
    private int _next;

    [Params(Engine.InMemory, Engine.Faster)]
    public Engine Store { get; set; }

    [Params(100_000)]
    public int Rows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-").FullName;
        _schema = SchemaRegistry.FromTypes(typeof(BenchRow));
        _table = _schema.Tables[0].Id;

        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot"), MemoryBudgetBytes = 64 * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "log") },
        };
        Directory.CreateDirectory(options.HotStore.Path);

        _store = Store == Engine.Faster
            ? new FasterHotStoreProvider().Create(new HotStoreContext
            {
                Schema = _schema,
                Options = options,
                Residency = ResidencyResolver.Resolve(_schema, options.Residency),
                LoggerFactory = NullLoggerFactory.Instance,
            })
            : new InMemoryHotStore(_schema, ResidencyResolver.Resolve(_schema, options.Residency));
        if (_store is IDisposable disposable)
            _disposables.Add(disposable);

        _keys = new RowKey[Rows];
        var row = new byte[RowBytes];
        var ops = new List<RowOp>(Rows);
        for (var i = 0; i < Rows; i++)
        {
            _keys[i] = SchemaKeyCodec.Encode(_schema.Tables[0].PrimaryKey, (ulong)i);
            ops.Add(new RowOp(RowOpKind.Insert, _table, _keys[i], row));
        }

        _store.Apply(Record(++_lsn, ops));

        // A supply of distinct hundred-row transactions, so the write benchmarks never measure the
        // same key twice in a row and never allocate inside the measured region.
        _writes = new CommitRecord[512];
        for (var w = 0; w < _writes.Length; w++)
        {
            var batch = new List<RowOp>(WriteSetRows);
            for (var i = 0; i < WriteSetRows; i++)
                batch.Add(new RowOp(RowOpKind.Update, _table, _keys[((w * WriteSetRows) + i) % Rows], row));
            _writes[w] = Record((ulong)(w + 2), batch);
        }
    }

    // Every case opens and disposes its own view inside the measured region rather than through
    // IterationSetup. Two reasons, both of which silently ruined the first version of this file:
    // BenchmarkDotNet gives IterationSetup benchmarks an unroll factor of 1, so their per-operation
    // overhead is not the baseline's and the ratio column compares two different things; and the
    // undo overlay records each key at most once per view, so a view held across many iterations
    // stops capturing after the first and reports the write path as free. Opening costs tens of
    // nanoseconds against tens of microseconds of work, so including it changes nothing else.

    [Benchmark(Description = "open a read view")]
    public ulong OpenReadView()
    {
        using var view = ((IReadViewSource)_store).OpenReadView();
        return view.Lsn;
    }

    [Benchmark(Description = "apply 100 rows, no view open", Baseline = true)]
    public ulong ApplyWithNoViewOpen()
    {
        var record = Next();
        _store.Apply(record);
        return record.Lsn;
    }

    [Benchmark(Description = "apply 100 rows, a view open")]
    public ulong ApplyWithAViewOpen()
    {
        using var view = ((IReadViewSource)_store).OpenReadView();
        var record = Next();
        _store.Apply(record);
        return record.Lsn + view.Lsn;
    }

    [Benchmark(Description = "scan live")]
    public long ScanLive()
    {
        long bytes = 0;
        foreach (var pair in _store.Scan(_table))
            bytes += pair.Value.Length;
        return bytes;
    }

    [Benchmark(Description = "scan through a view")]
    public long ScanThroughAView()
    {
        using var view = ((IReadViewSource)_store).OpenReadView();
        long bytes = 0;
        foreach (var pair in view.Scan(_table))
            bytes += pair.Value.Length;
        return bytes;
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

    /// <summary>
    /// The next prepared transaction, re-stamped with a fresh LSN — a store ignores a record at or
    /// below its applied LSN, so replaying the same one would measure the early return.
    /// </summary>
    private CommitRecord Next()
    {
        var source = _writes[_next++ % _writes.Length];
        return Record(++_lsn + 1, source.WriteSet);
    }

    private CommitRecord Record(ulong lsn, IReadOnlyList<RowOp> ops)
    {
        _lsn = Math.Max(_lsn, lsn);
        return new CommitRecord
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

}
