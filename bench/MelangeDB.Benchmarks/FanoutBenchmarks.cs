using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Measurement gap 2: fix the write set at one row and vary the number of subscribers watching that
/// table. Does the fan-out's cost live in matching subscriptions, or in producing wire values?
/// <para>
/// This is the suite that matters most, because fan-out runs <b>under the engine's write lock</b>.
/// Work repeated per subscriber there is not a local cost — it is a global stall, and it scales with
/// exactly the thing a successful game does more of. The published single-shard ceiling of ~52,000
/// commits/s under interval fsync is a claim about this loop.
/// </para>
/// <para>
/// <see cref="DistinctProjections"/> is the lever. At 1, every subscriber asks for the same columns
/// and the fan-out decodes the row once and hands the same map to all of them. At 8, eight decodes
/// are unavoidable. The gap between the two rows at 500 subscribers is what the memo buys — and if
/// the two rows were equal, the memo would be dead code.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class FanoutBenchmarks : IDisposable
{
    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private SchemaRegistry _schema = null!;
    private MelangeEngine _engine = null!;
    private SubscriptionEngine _subscriptions = null!;
    private CommitRecord[] _records = [];
    private int _next;

    [Params(1, 10, 100, 500)]
    public int Subscribers { get; set; }

    /// <summary>How many different column sets the subscribers ask for. 1 means all of them share.</summary>
    [Params(1, 8)]
    public int DistinctProjections { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-fanout-").FullName;
        _schema = SchemaRegistry.FromTypes(typeof(FanoutRow));

        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot"), MemoryBudgetBytes = 64 * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.Interval },
            Snapshots = { Enabled = false },
        };
        Directory.CreateDirectory(options.HotStore.Path);

        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance);
        _disposables.Add(_engine);
        _subscriptions = new SubscriptionEngine(_engine, telemetry: null);

        // One row for the deltas to be about. Fan-out reads the pre-image from the store, so the
        // row has to exist or every op measures the insert path instead of the update path.
        _engine.Invoke("seed", Identity.Hash("bench"), ctx =>
            ctx.Db.Insert(new FanoutRow { Id = 1, Name = "subject", X = 0f, Y = 0f, Z = 0f, Health = 100, Level = 1 }));

        var projections = Projections(DistinctProjections);
        for (var i = 0; i < Subscribers; i++)
        {
            var query = SqlSubsetParser.Parse($"SELECT {projections[i % projections.Length]} FROM FanoutRow", null);
            var sink = new NullSink();
            _engine.ReadConsistent(head =>
                _subscriptions.Register(sink, (uint)(i + 1), query, new SubscriptionsOptions(), head, computeInitialSet: false));
        }

        // Prepared records rather than live commits: this suite measures Fanout, and running a
        // whole transaction around it would bury the signal under the log and the store.
        var table = _schema.Tables[0];
        _records = new CommitRecord[512];
        var key = SchemaKeyCodec.Encode(table.PrimaryKey, 1UL);
        for (var r = 0; r < _records.Length; r++)
        {
            var row = RowSerializer.Serialize(table, new FanoutRow
            {
                Id = 1,
                Name = "subject",
                X = r,
                Y = r + 1,
                Z = r + 2,
                Health = 100,
                Level = 1,
            });
            _records[r] = new CommitRecord
            {
                Lsn = (ulong)(r + 100),
                FormatVersion = 2,
                Timestamp = new Timestamp(r + 100),
                Caller = Identity.Hash("bench"),
                ReducerName = "bench",
                Arguments = ReadOnlyMemory<byte>.Empty,
                WriteSet = [new RowOp(RowOpKind.Update, table.Id, key, row)],
                SerializedLength = 0,
            };
        }
    }

    /// <summary>One row changed, N subscribers watching — the shape of a game tick.</summary>
    [Benchmark(Description = "fan out one row")]
    public ulong FanoutOneRow()
    {
        var record = _records[_next++ % _records.Length];
        _subscriptions.Fanout(record);
        return record.Lsn;
    }

    /// <summary>
    /// Column sets for the subscribers to ask for. Every entry names Id so each is a legal
    /// projection of the same table, and they differ from each other so the memo cannot collapse
    /// them.
    /// </summary>
    private static string[] Projections(int count) => count == 1
        ? ["*"]
        : [.. new[] { "X", "Y", "Z", "Name", "Health", "Level", "X, Y", "X, Y, Z" }
            .Take(count)
            .Select(columns => $"Id, {columns}")];

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
    /// A sink that keeps nothing. Holding the frames would measure a growing list and eventually the
    /// GC, neither of which is the fan-out's cost.
    /// </summary>
    private sealed class NullSink : IDeltaSink
    {
        public void EnqueueDelta(TransactionUpdateFrame frame)
        {
            // Deliberately empty: the frame's construction is the measurement, not its delivery.
        }
    }
}
