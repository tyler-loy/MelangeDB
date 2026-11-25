using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Benchmarks;

/// <summary>
/// The other half of #315: a row policy that reads the caller's own row
/// (<c>ctx.Db.Find&lt;ViewerRow&gt;(ctx.Caller)</c>) to decide what it sees. The fan-out judges it
/// for every subscriber on both sides of every op, so an uncached lookup costs a store read and a
/// full decode of a wide row 2 × ops × subscribers times per record.
/// <para>
/// <see cref="OpsPerRecord"/> is the lever. A game's scheduled tick moves many rows in one
/// transaction, so one record carries many ops; within it, committed state is fixed and each
/// caller's row need be decoded only once. A cached column policy (owner sees all, everyone else
/// loses three columns) applies too, as in the issue's workload.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RowPolicyFanoutBenchmarks : IDisposable
{
    private readonly List<IDisposable> _disposables = [];
    private string _root = string.Empty;
    private MelangeEngine _engine = null!;
    private SubscriptionEngine _subscriptions = null!;
    private CommitRecord[] _records = [];
    private int _next;

    [Params(100, 500)]
    public int Subscribers { get; set; }

    /// <summary>Rows each record moves: one write, or a tick moving many entities at once.</summary>
    [Params(1, 20)]
    public int OpsPerRecord { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _root = Directory.CreateTempSubdirectory("melange-bench-row-policy-fanout-").FullName;
        var schema = BenchSchema.For(nameof(PolicyRow), nameof(ViewerRow));
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
        for (var r = 1; r <= OpsPerRecord; r++)
        {
            var id = (ulong)r;
            _engine.Invoke("seed", owner, ctx => ctx.Db.Insert(new PolicyRow { Id = id, Owner = owner }));
        }

        var services = new ServiceCollection();
        services.AddSingleton<IColumnPolicy<PolicyRow>, CachedMaskPolicy>();
        services.AddSingleton<IRowPolicy<PolicyRow>, SameRoomPolicy>();
        var policies = new PolicySet(services.BuildServiceProvider(), schema);

        for (var i = 0; i < Subscribers; i++)
        {
            var caller = i == 0 ? owner : Identity.Hash($"viewer{i}");
            _engine.Invoke("seed", caller, ctx => ctx.Db.Insert(new ViewerRow { Id = caller, Room = 0, Name = "viewer" }));
            var context = new PolicyContext(caller, false, _engine.CommittedView);
            var query = SqlSubsetParser.Parse("SELECT * FROM PolicyRow", null);
            var subscription = (uint)(i + 1);
            _engine.ReadConsistent(head =>
                _subscriptions.Register(new NullSink(), subscription, query, new SubscriptionsOptions(), head, computeInitialSet: false, policies, context));
        }

        var table = schema.Get(typeof(PolicyRow));
        _records = new CommitRecord[256];
        for (var r = 0; r < _records.Length; r++)
        {
            var ops = new List<RowOp>(OpsPerRecord);
            for (var o = 1; o <= OpsPerRecord; o++)
            {
                var row = RowSerializer.Serialize(table, new PolicyRow { Id = (ulong)o, Owner = owner, X = r, Y = r + 1, Z = r + 2 });
                ops.Add(new RowOp(RowOpKind.Update, table.Id, SchemaKeyCodec.Encode(table.PrimaryKey, (ulong)o), row));
            }

            _records[r] = new CommitRecord
            {
                Lsn = (ulong)(r + 100),
                FormatVersion = 2,
                Timestamp = new Timestamp(r + 100),
                Caller = owner,
                ReducerName = "bench",
                Arguments = ReadOnlyMemory<byte>.Empty,
                WriteSet = ops,
                SerializedLength = 0,
            };
        }
    }

    [Benchmark(Description = "fan out one record through a row policy that reads the caller's row")]
    public ulong FanoutOneRecord()
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

    private sealed class SameRoomPolicy : IRowPolicy<PolicyRow>
    {
        public bool IsVisibleTo(in PolicyRow row, PolicyContext ctx) =>
            ctx.Db.Find<ViewerRow>(ctx.Caller) is { } viewer && viewer.Room == row.C03;
    }

    private sealed class CachedMaskPolicy : IColumnPolicy<PolicyRow>
    {
        private static readonly ColumnMask Others = ColumnMask.All.Except(nameof(PolicyRow.C00), nameof(PolicyRow.C01), nameof(PolicyRow.C02));

        public ColumnMask VisibleTo(in PolicyRow row, PolicyContext ctx) =>
            row.Owner == ctx.Caller ? ColumnMask.All : Others;
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
