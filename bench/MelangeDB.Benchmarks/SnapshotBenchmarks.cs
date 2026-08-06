using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Measurement gap 7, and the gate on finding #8: how long does a snapshot hold the engine's write
/// lock, and how does that scale with the resident set?
/// <para>
/// Snapshots run under the write lock today — scan every table, write the file, fsync, optionally
/// truncate — so their duration is not snapshot latency, it is a <b>global write stall</b>. Nothing
/// commits while one runs. The default interval is 100,000 transactions, so it is rare; the question
/// is whether it is also brief. "Rare and brief" needs no fix. "Rare and multi-second" is a world
/// freeze that a player experiences as the server hanging, and no percentile on the commit path will
/// show it clearly because it happens too seldom.
/// </para>
/// <para>
/// The row count is the axis because the stall should scale with it, and knowing the slope is what
/// turns "at large resident sets this is a multi-second freeze" from a plausible sentence in a review
/// into a number with a threshold. Compare against the cost of pinning a read view, which is what
/// #8 would do under the lock instead: if pinning is nanoseconds against milliseconds of writing,
/// the restructure moves essentially all of it out.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SnapshotBenchmarks : IDisposable
{
    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private SchemaRegistry _schema = null!;
    private MelangeEngine _engine = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int Rows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-snapshot-").FullName;
        _schema = SchemaRegistry.FromTypes(typeof(CommitRow));

        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot"), MemoryBudgetBytes = 1024L * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.Interval },
            // Enabled, but the automatic interval is pushed out of reach: this suite calls
            // TakeSnapshot itself, and an automatic one firing mid-measurement would land inside
            // another benchmark's timing as an unexplained outlier.
            Snapshots = { Enabled = true, TruncateLog = false, IntervalTransactions = long.MaxValue },
        };
        Directory.CreateDirectory(options.HotStore.Path);

        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance);
        _disposables.Add(_engine);

        // Seeded in batches: one transaction of a million rows would be a write set nothing in
        // production produces, and the engine's per-transaction bookkeeping would dominate the load.
        var caller = Identity.Hash("bench");
        var payload = new byte[96];
        const int batch = 5_000;
        for (var start = 0; start < Rows; start += batch)
        {
            var end = Math.Min(start + batch, Rows);
            _engine.Invoke("seed", caller, ctx =>
            {
                for (var i = start; i < end; i++)
                    ctx.Db.Insert(new CommitRow { Id = (ulong)i, Payload = payload });
            });
        }
    }

    /// <summary>
    /// A full snapshot: scan every table, write, fsync. Today this whole span holds the write lock.
    /// </summary>
    [Benchmark(Description = "take a snapshot")]
    public ulong TakeSnapshot() => _engine.TakeSnapshot() ?? 0;

    /// <summary>
    /// What #8 proposes to do under the lock instead. The ratio between this row and the one above
    /// is the fraction of the stall the restructure removes.
    /// </summary>
    [Benchmark(Description = "pin a read view (what #8 would hold the lock for)")]
    public ulong PinReadView()
    {
        if (_engine.HotStore is not IReadViewSource source)
            return 0;
        using var view = source.OpenReadView();
        return view.Lsn;
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
}
