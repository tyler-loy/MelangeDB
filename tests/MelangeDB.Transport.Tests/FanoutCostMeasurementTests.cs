using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Road-to-0.4 phase 31, step 2a: where a subscriber's share of the fan-out goes. The load test
/// measured about 2–3 µs per subscriber per commit under the write lock; this splits that among
/// the parts the fan-out does per subscriber, by layering them one at a time over the same commit
/// stream:
/// <list type="number">
/// <item>the evaluation itself (verdicts, op lists, the frame), into a sink that drops the frame;</item>
/// <item>plus <c>Measure</c>, which the socket connection runs under the lock for backpressure;</item>
/// <item>plus the connection's enqueue and send-signal release, with a send loop per subscriber waiting on it;</item>
/// <item>plus the delta-rows counter with a listener attached, as under an OpenTelemetry exporter.</item>
/// </list>
/// And, for scale, what each send loop spends serializing its copy off the lock. Shipped as a test
/// so the split regenerates on any machine; the recorded figures are in
/// docs/road-to-0.4/plan-phase-31.md. The only assertion is a sanity ceiling.
/// </summary>
public sealed class FanoutCostMeasurementTests(ITestOutputHelper output) : IDisposable
{
    private const int Subscribers = 200;
    private const int Commits = 2_000;
    private const int Trials = 5;

    private readonly string _root = Directory.CreateTempSubdirectory("melange-fanout-measure-").FullName;
    private readonly List<MelangeEngine> _engines = [];

    public void Dispose()
    {
        foreach (var engine in _engines)
            engine.Dispose();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A store handle Windows has not released yet; the temp reaper gets it.
        }
    }

    [Fact]
    public async Task Measure_what_one_subscriber_costs_the_fanout_per_commit()
    {
        var serializer = new MessagePackFrameSerializer();
        var results = new List<(string Name, double PerSubscriberNs, double AllocatedBytes)>();

        // Two predicate shapes: the load test's whole-table subscription, and an equality on an
        // indexed column, which is a real verdict per subscriber.
        using var listened = ListenedTelemetry(out var listener);
        using var _ = listener;
        foreach (var query in new[] { "SELECT * FROM PlayerState", "SELECT * FROM PlayerState WHERE RoomId = 7" })
        {
            // Each layer runs Trials times, the layers interleaved trial by trial, and the median
            // is kept: a single run of each was dominated by run order and JIT tiering.
            var trials = new Dictionary<string, List<(double Ns, double Bytes)>>(StringComparer.Ordinal);
            void Record(string layer, (double Ns, double Bytes) run) =>
                (trials.TryGetValue(layer, out var list) ? list : trials[layer] = []).Add(run);

            long frames = 0;
            double encoded = 0;
            for (var trial = 0; trial < Trials; trial++)
            {
                Record("baseline", Run(query, subscribers: 0, _ => new DropSink(), telemetry: null));
                Record("evaluate, drop the frame", Run(query, Subscribers, _ => new DropSink(), null));
                Record("evaluate + delta-rows counter (listened)", Run(query, Subscribers, _ => new DropSink(), listened));
                Record("+ Measure", Run(query, Subscribers, _ => new MeasureSink(serializer), null));
                var queued = new List<QueueSink>();
                Record("+ Measure, enqueue, signal under the lock (before step 2d)", Run(query, Subscribers, _ =>
                {
                    var sink = new QueueSink(serializer, wakeUnderLock: true);
                    queued.Add(sink);
                    return sink;
                }, null));
                Record("+ Measure, enqueue; signal from the pool (step 2d)", Run(query, Subscribers, _ =>
                {
                    var sink = new QueueSink(serializer, wakeUnderLock: false);
                    queued.Add(sink);
                    return sink;
                }, null));
                foreach (var sink in queued)
                    await sink.DisposeAsync();
                encoded += queued.Sum(static s => s.EncodedNs);
                frames += queued.Sum(static s => s.Encoded);
            }

            var baseline = Median(trials["baseline"]);
            foreach (var (layer, runs) in trials.Where(static t => t.Key != "baseline"))
                results.Add(Per($"[{query}] {layer}", Median(runs), baseline));
            output.WriteLine($"[{query}] off the lock, each send loop's serialize: {(frames == 0 ? 0 : encoded / frames):F0} ns per frame over {frames} frames");
            output.WriteLine($"[{query}] fixed cost per commit with no subscribers: {baseline.Ns:F0} ns");
        }

        foreach (var (name, perSubscriber, allocated) in results)
            output.WriteLine($"{name}: {perSubscriber:F0} ns and {allocated:F0} B allocated per subscriber per commit");

        // A sanity ceiling, two orders of magnitude above the load test's 2–3 µs.
        Assert.All(results, r => Assert.True(r.PerSubscriberNs < 300_000, $"{r.Name}: {r.PerSubscriberNs:F0} ns"));

        static (string, double, double) Per(string name, (double Ns, double Bytes) run, (double Ns, double Bytes) baseline) =>
            (name, (run.Ns - baseline.Ns) / Subscribers, (run.Bytes - baseline.Bytes) / Subscribers);

        static (double Ns, double Bytes) Median(List<(double Ns, double Bytes)> runs)
        {
            var ordered = runs.OrderBy(static r => r.Ns).ToList();
            return ordered[ordered.Count / 2];
        }
    }

    /// <summary>Mean time and bytes allocated per <c>Fanout</c> call, after a warm-up.</summary>
    private (double Ns, double Bytes) Run(string sql, int subscribers, Func<int, IDeltaSink> sinkFor, ServerTelemetry? telemetry)
    {
        var path = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        var engine = new MelangeEngine(
            new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(path, "hot") },
                CommitLog = { Path = Path.Combine(path, "log"), FsyncPolicy = FsyncPolicy.OsBuffered },
                Snapshots = { Enabled = false },
            },
            new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables().Where(static t => t.RowType == typeof(PlayerState))),
            NullLoggerFactory.Instance);
        _engines.Add(engine);
        var fanout = new SubscriptionEngine(engine, telemetry);

        var player = Identity.Hash("mover");
        engine.Invoke("Spawn", player, ctx => ctx.Db.Insert(new PlayerState { Id = player, RoomId = 7, Name = "mover", X = 0 }));
        var query = SqlSubsetParser.Parse(sql, null);
        for (var i = 0; i < subscribers; i++)
        {
            var sink = sinkFor(i);
            engine.ReadConsistent(head =>
                fanout.Register(sink, (uint)(i + 1), query, new SubscriptionsOptions(), head, computeInitialSet: false));
        }

        // The store is never applied to, so every record sees the same pre-image: a steady stream
        // of in-place updates, the load test's Move.
        var records = Enumerable.Range(0, 64)
            .Select(x => Update(engine, new PlayerState { Id = player, RoomId = 7, Name = "mover", X = x + 1 }))
            .ToArray();
        for (var i = 0; i < Commits / 4; i++)
            fanout.Fanout(records[i % records.Length]);

        var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        var clock = Stopwatch.StartNew();
        for (var i = 0; i < Commits; i++)
            fanout.Fanout(records[i % records.Length]);
        clock.Stop();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        return (clock.Elapsed.TotalNanoseconds / Commits, (double)allocated / Commits);
    }

    private static CommitRecord Update(MelangeEngine engine, PlayerState row)
    {
        var table = engine.Schema.Get(typeof(PlayerState));
        var codec = (RowCodec<PlayerState>)table.Codec!;
        return new CommitRecord
        {
            Lsn = engine.Log.HeadLsn + 1,
            FormatVersion = 2,
            Timestamp = new Timestamp(1),
            Caller = Identity.Hash("mover"),
            ReducerName = "Move",
            Arguments = ReadOnlyMemory<byte>.Empty,
            WriteSet = [new RowOp(RowOpKind.Update, table.Id, codec.EncodePrimaryKey(in row), codec.Serialize(in row))],
            SerializedLength = 0,
        };
    }

    /// <summary>The server's telemetry with a listener on its counters, as an exporter attaches one.</summary>
    private static ServerTelemetry ListenedTelemetry(out MeterListener listener)
    {
        var telemetry = new ServerTelemetry(static () => new TelemetryOptions(), static () => 0, static () => []);
        listener = new MeterListener
        {
            InstrumentPublished = static (instrument, l) =>
            {
                if (instrument.Meter.Name == "MelangeDB")
                    l.EnableMeasurementEvents(instrument);
            },
        };
        long sink = 0;
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => sink += value);
        listener.SetMeasurementEventCallback<int>((_, value, _, _) => sink += value);
        listener.Start();
        return telemetry;
    }

    private sealed class DropSink : IDeltaSink
    {
        public void EnqueueDelta(TransactionUpdateFrame frame)
        {
        }
    }

    private sealed class MeasureSink(IMelangeSerializer serializer) : IDeltaSink
    {
        private long _bytes;

        public void EnqueueDelta(TransactionUpdateFrame frame) => _bytes += serializer.Measure(frame);
    }

    /// <summary>
    /// <c>MelangeSocketConnection.EnqueueDelta</c>'s work under the lock, with a send loop waiting
    /// on the signal the way the connection's does, serializing each frame as it wakes.
    /// </summary>
    private sealed class QueueSink : IDeltaSink, IAsyncDisposable
    {
        private readonly IMelangeSerializer _serializer;
        private readonly ConcurrentQueue<(TransactionUpdateFrame Frame, int Length)> _queue = new();
        private readonly SemaphoreSlim _signal = new(0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _sender;
        private long _buffered;

        private readonly bool _wakeUnderLock;

        /// <param name="wakeUnderLock">
        /// True reproduces the connection before step 2d, which released the signal inside every
        /// enqueue; false is the connection now, which queues quietly and is woken from the pool.
        /// </param>
        public QueueSink(IMelangeSerializer serializer, bool wakeUnderLock)
        {
            _serializer = serializer;
            _wakeUnderLock = wakeUnderLock;
            _sender = Task.Run(SendLoopAsync);
        }

        public long Encoded { get; private set; }

        public double EncodedNs { get; private set; }

        public void EnqueueDelta(TransactionUpdateFrame frame)
        {
            Queue(frame);
            _signal.Release();
        }

        public void EnqueueDeltaQuietly(TransactionUpdateFrame frame)
        {
            Queue(frame);
            if (_wakeUnderLock)
                _signal.Release();
        }

        public void Wake()
        {
            if (!_wakeUnderLock)
                _signal.Release();
        }

        private void Queue(TransactionUpdateFrame frame)
        {
            var length = _serializer.Measure(frame);
            Interlocked.Add(ref _buffered, length);
            _queue.Enqueue((frame, length));
        }

        private async Task SendLoopAsync()
        {
            try
            {
                while (true)
                {
                    await _signal.WaitAsync(_stop.Token).ConfigureAwait(false);
                    while (_queue.TryDequeue(out var pending))
                    {
                        var started = Stopwatch.GetTimestamp();
                        _ = _serializer.Serialize(pending.Frame);
                        EncodedNs += Stopwatch.GetElapsedTime(started).TotalNanoseconds;
                        Encoded++;
                        Interlocked.Add(ref _buffered, -pending.Length);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await _sender;
            _stop.Dispose();
            _signal.Dispose();
        }
    }
}
