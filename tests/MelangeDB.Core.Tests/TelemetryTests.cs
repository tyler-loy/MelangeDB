using System.Diagnostics;
using System.Diagnostics.Metrics;
using Xunit;

namespace MelangeDB.Core.Tests;

[Collection("Telemetry")]
public class TelemetryTests : IDisposable
{
    private readonly EngineHarness _harness = new();
    private readonly List<Activity> _stopped = [];
    private readonly ActivityListener _listener;

    public TelemetryTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "MelangeDB",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                lock (_stopped)
                {
                    _stopped.Add(activity);
                }
            },
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _harness.Dispose();
    }

    private List<Activity> Stopped()
    {
        lock (_stopped)
        {
            return [.. _stopped];
        }
    }

    [Fact]
    public void Reducer_span_carries_name_outcome_and_caller_and_commit_span_has_fsync_child()
    {
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        var spans = Stopped();
        var reducer = Assert.Single(spans, a => a.OperationName == "melange.reducer");
        Assert.Equal("Join", reducer.GetTagItem("melange.reducer.name"));
        Assert.Equal("commit", reducer.GetTagItem("melange.outcome"));
        Assert.Equal(1, reducer.GetTagItem("melange.writeset.rows"));
        Assert.Equal(EngineHarness.Caller.ToString(), reducer.GetTagItem("melange.caller"));

        var commit = Assert.Single(spans, a => a.OperationName == "melange.commit");
        Assert.Equal(reducer.SpanId, commit.ParentSpanId);
        Assert.Equal(1L, commit.GetTagItem("melange.lsn"));
        Assert.True((int)commit.GetTagItem("melange.writeset.bytes")! > 0);

        var fsync = Assert.Single(spans, a => a.OperationName == "melange.fsync");
        Assert.Equal(commit.SpanId, fsync.ParentSpanId);

        var apply = Assert.Single(spans, a => a.OperationName == "melange.apply");
        Assert.Equal("hot-store", apply.GetTagItem("melange.applier"));
    }

    [Fact]
    public void Abort_and_rejection_set_the_outcome_attribute()
    {
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Crash", _ => throw new InvalidOperationException("boom")));
        Assert.Throws<RejectedException>(() => _harness.Invoke("Deny", _ => throw new RejectedException("no")));

        var reducers = Stopped().Where(a => a.OperationName == "melange.reducer").ToList();
        Assert.Equal(2, reducers.Count);
        Assert.Equal("abort", reducers[0].GetTagItem("melange.outcome"));
        Assert.Equal(ActivityStatusCode.Error, reducers[0].Status);
        Assert.Equal("rejected", reducers[1].GetTagItem("melange.outcome"));
        Assert.DoesNotContain(Stopped(), a => a.OperationName == "melange.commit");
    }

    [Fact]
    public void Caller_identity_can_be_excluded_from_spans()
    {
        using var quiet = new EngineHarness();
        quiet.Options.Telemetry.IncludeCallerIdentity = false;
        quiet.Restart();
        quiet.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        var reducer = Stopped().Last(a => a.OperationName == "melange.reducer");
        Assert.Null(reducer.GetTagItem("melange.caller"));
    }

    [Fact]
    public void Transactions_counter_has_reducer_and_outcome_dimensions_and_never_caller()
    {
        var measurements = new List<(long Value, Dictionary<string, object?> Tags)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "MelangeDB" && instrument.Name == "melange.transactions")
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            lock (measurements)
            {
                measurements.Add((value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value)));
            }
        });
        listener.Start();

        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));
        Assert.Throws<RejectedException>(() => _harness.Invoke("Deny", _ => throw new RejectedException("no")));

        lock (measurements)
        {
            var commit = Assert.Single(measurements, m => Equals(m.Tags.GetValueOrDefault("outcome"), "commit"));
            Assert.Equal("Join", commit.Tags["reducer"]);
            var rejected = Assert.Single(measurements, m => Equals(m.Tags.GetValueOrDefault("outcome"), "rejected"));
            Assert.Equal("Deny", rejected.Tags["reducer"]);

            // Caller identity is unbounded: spans only, never a metric dimension.
            Assert.All(measurements, m => Assert.DoesNotContain("caller", m.Tags.Keys));
            Assert.All(measurements, m => Assert.Equal(1L, m.Value));
        }
    }

    [Fact]
    public void Applier_lag_is_nonzero_while_paused_and_zero_after_resume()
    {
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "MelangeDB" && instrument.Name is "melange.applier.lag" or "melange.log.head_lsn")
                    l.EnableMeasurementEvents(instrument);
            },
        };
        var gauges = new Dictionary<string, long>();
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            lock (gauges)
            {
                var applier = tags.ToArray().FirstOrDefault(t => t.Key == "applier").Value as string;
                gauges[applier is null ? instrument.Name : $"{instrument.Name}/{applier}"] = value;
            }
        });
        listener.Start();

        _harness.Invoke("One", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1], Kind = ChunkKind.Rock }));
        _harness.Engine.Appliers.Pause("hot-store");
        _harness.Invoke("Two", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 2, Data = [2], Kind = ChunkKind.Rock }));
        _harness.Invoke("Three", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 3, Data = [3], Kind = ChunkKind.Rock }));

        listener.RecordObservableInstruments();
        lock (gauges)
        {
            Assert.Equal(2, gauges["melange.applier.lag/hot-store"]);
            Assert.Equal(3, gauges["melange.log.head_lsn"]);
        }

        _harness.Engine.Appliers.Resume("hot-store");
        listener.RecordObservableInstruments();
        lock (gauges)
        {
            Assert.Equal(0, gauges["melange.applier.lag/hot-store"]);
        }
    }

    [Fact]
    public void Disabled_telemetry_emits_no_spans()
    {
        using var silent = new EngineHarness(telemetryEnabled: false);
        var before = Stopped().Count;
        silent.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));
        Assert.Equal(before, Stopped().Count);
    }
}
