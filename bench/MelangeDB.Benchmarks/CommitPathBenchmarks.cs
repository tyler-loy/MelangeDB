using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Measurement gap 1: under interval fsync, where does a commit's time and allocation actually go?
/// <para>
/// The engine reports body / commit / fsync / post-commit, but post-commit is one blob and the
/// allocation is not attributed at all — which is exactly the question finding #6 asks before
/// anything gets pooled. Six sites allocate on every commit, and pooling all of them on principle
/// is how a commit path acquires six new lifetime bugs to fix a cost that lived in one of them.
/// </para>
/// <para>
/// The suite answers by layers rather than by profiler: encode the payload alone, append it, apply
/// it to the store, and run the whole commit. Each row is a strict subset of the one below it, so
/// the differences attribute the cost — and the <b>allocated</b> column, not the time column, is the
/// one this suite exists for. Fsync policy is a parameter because the answer inverts between them:
/// under <c>OnCommit</c> the disk dominates everything and none of this matters, which is itself
/// worth having on the page.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CommitPathBenchmarks : IDisposable
{
    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private SchemaRegistry _schema = null!;
    private MelangeEngine _engine = null!;
    private IHotStore _store = null!;
    private FileCommitLog _log = null!;
    private TableId _table;
    private Identity _caller;
    private CommitRequest _request;
    private CommitRecord[] _records = [];
    private ulong _lsn;
    private ulong _storeLsn;
    private int _next;
    private int _applied;

    /// <summary>
    /// Write-set size. One row is the game tick's shape; a hundred is a sweep's. The per-op sites
    /// (row serialize, RowKey, the log's op loop) scale with this and the per-commit sites do not,
    /// which is how the two groups separate without a profiler.
    /// </summary>
    [Params(1, 10, 100)]
    public int Rows { get; set; }

    [Params(FsyncPolicy.Interval, FsyncPolicy.OnCommit)]
    public FsyncPolicy Fsync { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-commit-").FullName;
        _schema = BenchSchema.For(nameof(CommitRow));
        _table = _schema.Tables[0].Id;
        _caller = Identity.Hash("bench");

        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot"), MemoryBudgetBytes = 64 * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = Fsync },
            Snapshots = { Enabled = false },
        };
        Directory.CreateDirectory(options.HotStore.Path);

        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance);
        _disposables.Add(_engine);

        // A second, independent log and store for the layer benchmarks, so appending and applying
        // never disturb the engine the full-commit case measures.
        var layerOptions = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "layer-hot"), MemoryBudgetBytes = 64 * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "layer-log"), FsyncPolicy = Fsync },
        };
        Directory.CreateDirectory(layerOptions.HotStore.Path);
        _log = new FileCommitLog(layerOptions.CommitLog, NullLogger<FileCommitLog>.Instance, telemetry: null);
        _disposables.Add(_log);
        _store = new InMemoryHotStore(_schema, ResidencyResolver.Resolve(_schema, layerOptions.Residency));
        if (_store is IDisposable storeDisposable)
            _disposables.Add(storeDisposable);

        var payload = new byte[96];
        var ops = new List<RowOp>(Rows);
        for (var i = 0; i < Rows; i++)
            ops.Add(new RowOp(RowOpKind.Insert, _table, Key((ulong)i), payload));
        _request = new CommitRequest(new Timestamp(1), _caller, "bench", ReadOnlyMemory<byte>.Empty, ops);

        // Distinct records for the apply case: a store ignores a record at or below its applied
        // LSN, so replaying one would measure the early return rather than the write.
        _records = new CommitRecord[512];
        for (var r = 0; r < _records.Length; r++)
        {
            var batch = new List<RowOp>(Rows);
            for (var i = 0; i < Rows; i++)
                batch.Add(new RowOp(RowOpKind.Insert, _table, Key((ulong)((r * Rows) + i)), payload));
            _records[r] = Record((ulong)(r + 1), batch);
        }
    }

    /// <summary>
    /// The log payload alone: <c>MemoryStream</c> + <c>BinaryWriter</c> + a final <c>ToArray</c>
    /// copy. Finding #6's first candidate, and the only one whose whole cost this row reports.
    /// </summary>
    [Benchmark(Description = "encode the log payload")]
    public int EncodePayload()
    {
        var length = LogRecordCodec.WritePayload(++_lsn, in _request, out var buffer);
        LogRecordCodec.Release(buffer);
        return length;
    }

    /// <summary>Encode plus framing, CRC, and the write — the disk's share appears here.</summary>
    [Benchmark(Description = "append to the log")]
    public ulong AppendToLog() => _log.Append(_request).Lsn;

    /// <summary>
    /// The store side: one version publish per table, plus index maintenance.
    /// <para>
    /// The record is re-stamped with a fresh LSN rather than replayed as prepared. A store ignores
    /// any record at or below its applied LSN, so the second pass over a fixed array measures the
    /// early return — which is how the first version of this row reported a whole apply as 3.5 ns
    /// and looked like a triumph instead of a bug.
    /// </para>
    /// </summary>
    [Benchmark(Description = "apply to the store")]
    public ulong ApplyToStore()
    {
        var source = _records[_applied++ % _records.Length];
        var record = Record(++_storeLsn, source.WriteSet);
        _store.Apply(record);
        return record.Lsn;
    }

    /// <summary>
    /// The whole thing through the public entry point: body, write-set collapse, guards, append,
    /// fsync, observers, apply. Everything above is a subset of this row.
    /// </summary>
    [Benchmark(Description = "full commit", Baseline = true)]
    public ulong FullCommit()
    {
        var start = _next++ * Rows;
        return _engine.Invoke("bench", _caller, ctx =>
        {
            for (var i = 0; i < Rows; i++)
                ctx.Db.Insert(new CommitRow { Id = (ulong)(start + i), Payload = [] });
        });
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

    private RowKey Key(ulong value) => SchemaKeyCodec.Encode(_schema.Tables[0].PrimaryKey, value);

    private CommitRecord Record(ulong lsn, IReadOnlyList<RowOp> ops) => new()
    {
        Lsn = lsn,
        FormatVersion = 2,
        Timestamp = new Timestamp((long)lsn),
        Caller = _caller,
        ReducerName = "bench",
        Arguments = ReadOnlyMemory<byte>.Empty,
        WriteSet = ops,
        SerializedLength = 0,
    };
}
