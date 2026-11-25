using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// One row of a hundred-column table changes while N callers watch it through a column policy
/// (#315): the owner sees everything, everyone else the same row without three columns.
/// <para>
/// Policies are per caller, so these subscriptions never share a group's verdict; what they can
/// share is the work behind equal answers. With two answers in play, the fan-out's cost per op
/// should be about two visible-column sets and two projections, not N of each.
/// </para>
/// <para>
/// <see cref="Mask"/> is the policy's style. <c>Cached</c> returns one static mask instance, as
/// the policy docs ask; <c>PerCall</c> writes <c>ColumnMask.All.Except(...)</c> inline, which
/// builds a new array on every call, as the docs' own example does.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ColumnPolicyFanoutBenchmarks : IDisposable
{
    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private MelangeEngine _engine = null!;
    private SubscriptionEngine _subscriptions = null!;
    private CommitRecord[] _records = [];
    private int _next;

    [Params(100, 500)]
    public int Subscribers { get; set; }

    [Params(MaskStyle.Cached, MaskStyle.PerCall)]
    public MaskStyle Mask { get; set; }

    public enum MaskStyle
    {
        Cached,
        PerCall,
    }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-policy-fanout-").FullName;
        var schema = BenchSchema.For(nameof(PolicyRow));
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot"), MemoryBudgetBytes = 64 * 1024 * 1024 },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.Interval },
            Snapshots = { Enabled = false },
        };
        Directory.CreateDirectory(options.HotStore.Path);

        _engine = new MelangeEngine(options, schema, NullLoggerFactory.Instance);
        _disposables.Add(_engine);
        _subscriptions = new SubscriptionEngine(_engine, telemetry: null);

        var owner = Identity.Hash("owner");
        _engine.Invoke("seed", owner, ctx => ctx.Db.Insert(new PolicyRow { Id = 1, Owner = owner }));

        var services = new ServiceCollection();
        if (Mask == MaskStyle.Cached)
            services.AddSingleton<IColumnPolicy<PolicyRow>, CachedMaskPolicy>();
        else
            services.AddSingleton<IColumnPolicy<PolicyRow>, PerCallMaskPolicy>();
        var policies = new PolicySet(services.BuildServiceProvider(), schema);

        for (var i = 0; i < Subscribers; i++)
        {
            var caller = i == 0 ? owner : Identity.Hash($"viewer{i}");
            var context = new PolicyContext(caller, false, _engine.CommittedView);
            var query = SqlSubsetParser.Parse("SELECT * FROM PolicyRow", null);
            var id = (uint)(i + 1);
            _engine.ReadConsistent(head =>
                _subscriptions.Register(new NullSink(), id, query, new SubscriptionsOptions(), head, computeInitialSet: false, policies, context));
        }

        // A position update, the shape a game tick repeats: visible to every caller.
        var table = schema.Tables[0];
        var key = SchemaKeyCodec.Encode(table.PrimaryKey, 1UL);
        _records = new CommitRecord[512];
        for (var r = 0; r < _records.Length; r++)
        {
            var row = RowSerializer.Serialize(table, new PolicyRow { Id = 1, Owner = owner, X = r, Y = r + 1, Z = r + 2 });
            _records[r] = new CommitRecord
            {
                Lsn = (ulong)(r + 100),
                FormatVersion = 2,
                Timestamp = new Timestamp(r + 100),
                Caller = owner,
                ReducerName = "bench",
                Arguments = ReadOnlyMemory<byte>.Empty,
                WriteSet = [new RowOp(RowOpKind.Update, table.Id, key, row)],
                SerializedLength = 0,
            };
        }
    }

    [Benchmark(Description = "fan out one row through a column policy")]
    public ulong FanoutOneRow()
    {
        var record = _records[_next++ % _records.Length];
        _subscriptions.Fanout(record);
        return record.Lsn;
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

    private sealed class CachedMaskPolicy : IColumnPolicy<PolicyRow>
    {
        private static readonly ColumnMask Others = ColumnMask.All.Except(nameof(PolicyRow.C00), nameof(PolicyRow.C01), nameof(PolicyRow.C02));

        public ColumnMask VisibleTo(in PolicyRow row, PolicyContext ctx) =>
            row.Owner == ctx.Caller ? ColumnMask.All : Others;
    }

    private sealed class PerCallMaskPolicy : IColumnPolicy<PolicyRow>
    {
        public ColumnMask VisibleTo(in PolicyRow row, PolicyContext ctx) =>
            row.Owner == ctx.Caller
                ? ColumnMask.All
                : ColumnMask.All.Except(nameof(PolicyRow.C00), nameof(PolicyRow.C01), nameof(PolicyRow.C02));
    }

    /// <summary>A sink that keeps nothing: the frame's construction is the measurement.</summary>
    private sealed class NullSink : IDeltaSink
    {
        public void EnqueueDelta(TransactionUpdateFrame frame)
        {
            // Deliberately empty.
        }
    }
}
