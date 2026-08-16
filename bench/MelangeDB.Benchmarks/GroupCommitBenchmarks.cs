using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// The group-commit measurement (road-to-0.2 phase 17): sustained durable commits under
/// concurrent callers, per-commit fsync. Contention is the parameter, because contention is what
/// group commit converts into batches — while one caller's fsync is in flight the others run
/// their bodies, append buffered, and park, and the next flush covers them all. One caller is the
/// control: a lone committer pays exactly the old inline latency, so its row should match the
/// pre-phase-17 commit path. The interesting number is per-commit mean time falling as callers
/// rise while every commit remains individually durable before its Invoke returns.
/// <para>
/// The engine-level hotspot measurement (HotspotMeasurementTests, published in
/// docs/CLUSTERING.md) is the end-to-end version of this on a real shard node; this row isolates
/// the engine commit path with no cluster machinery around it.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class GroupCommitBenchmarks : IDisposable
{
    private const int CommitsPerCaller = 32;

    private string _root = string.Empty;
    private MelangeEngine _engine = null!;
    private Identity _caller;
    private long _next;

    /// <summary>Concurrent committers. One is the uncontended control; the rest form batches.</summary>
    [Params(1, 4, 16)]
    public int Callers { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-group-").FullName;
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot"), MemoryBudgetBytes = 64 * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.OnCommit },
            Snapshots = { Enabled = false },
        };
        Directory.CreateDirectory(options.HotStore.Path);
        _engine = new MelangeEngine(options, BenchSchema.For(nameof(CommitRow)), NullLoggerFactory.Instance);
        _caller = Identity.Hash("bench");
    }

    [Benchmark(Description = "durable commits under contention", OperationsPerInvoke = 16 * CommitsPerCaller)]
    public void DurableCommits()
    {
        // Every invocation commits the same total regardless of Callers, so per-op numbers
        // compare across rows: 16 × 32 commits split over however many threads are contending.
        var perCaller = 16 * CommitsPerCaller / Callers;
        var threads = Enumerable.Range(0, Callers).Select(_ => new Thread(() =>
        {
            for (var i = 0; i < perCaller; i++)
            {
                var id = (ulong)Interlocked.Increment(ref _next);
                _engine.Invoke("bench", _caller, ctx => ctx.Db.Insert(new CommitRow { Id = id, Payload = [] }));
            }
        })).ToArray();
        foreach (var thread in threads)
            thread.Start();
        foreach (var thread in threads)
            thread.Join();
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _engine?.Dispose();
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
