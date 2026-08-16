using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
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
    public void Reducer_span_carries_name_outcome_and_caller_and_the_fsync_span_lands_on_the_flushing_transaction()
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

        // The fsync runs in the durability wait, after the commit span closed with the write lock:
        // it parents to the reducer span of whichever transaction performed the flush — here the
        // lone committer, which flushed for itself.
        var fsync = Assert.Single(spans, a => a.OperationName == "melange.fsync");
        Assert.Equal(reducer.SpanId, fsync.ParentSpanId);

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

    [Fact]
    public void Slow_reducer_event_splits_the_duration_into_body_commit_and_post_commit()
    {
        _harness.Options.Telemetry.SlowReducerMs = 0; // Every transaction is "slow".
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        var slow = SlowReducerEvent();
        var duration = Tag(slow, "melange.duration_ms");
        var body = Tag(slow, "melange.body_ms");
        var commit = Tag(slow, "melange.commit_ms");
        var postCommit = Tag(slow, "melange.post_commit_ms");

        Assert.Equal(1, slow.Tags.Single(t => t.Key == "melange.writeset.rows").Value);
        Assert.All([body, commit, postCommit], part => Assert.True(part >= 0));
        Assert.True(body + commit + postCommit <= duration, $"{body}+{commit}+{postCommit} > {duration}");
        // OnCommit is this harness's policy, so durability cost is attributable and reported: the
        // wait this caller experienced, which sits beside the append (melange.commit_ms no longer
        // contains it — the group-commit split moved durability out of the locked commit).
        Assert.True(Tag(slow, "melange.fsync_ms") <= duration);
    }

    [Fact]
    public void A_slow_commit_observer_lands_on_post_commit_and_not_on_the_reducer_body()
    {
        // The reason body time is measured rather than derived: observers, appliers, and automatic
        // snapshots all run after the append but inside the same span, so (duration - commit) would
        // charge this observer's 60ms to a reducer body that did nothing.
        _harness.Options.Telemetry.SlowReducerMs = 0;
        _harness.Engine.AddCommitObserver(new SleepyObserver(TimeSpan.FromMilliseconds(60)));
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        var slow = SlowReducerEvent();
        Assert.True(Tag(slow, "melange.post_commit_ms") >= 55, "the observer's cost belongs to post-commit");
        Assert.True(Tag(slow, "melange.body_ms") < 30, "an idle body must not inherit the observer's cost");
    }

    [Fact]
    public void The_fsync_field_is_absent_rather_than_zero_when_the_policy_defers_the_flush()
    {
        using var deferred = new EngineHarness(FsyncPolicy.Interval);
        deferred.Options.Telemetry.SlowReducerMs = 0;
        deferred.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        var slow = SlowReducerEvent();
        Assert.Contains(slow.Tags, t => t.Key == "melange.commit_ms");
        // Zero would read as "the disk was instant"; the flush simply did not happen here.
        Assert.DoesNotContain(slow.Tags, t => t.Key == "melange.fsync_ms");
    }

    [Fact]
    public void The_1003_log_record_carries_the_split_as_structured_fields()
    {
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 0;
        logged.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        var entry = logs.Single(1003);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal("Join", entry.Fields["Reducer"]);
        Assert.Equal(1, entry.Fields["Rows"]);
        Assert.True(entry.Number("BodyMs") + entry.Number("CommitMs") + entry.Number("PostCommitMs")
            <= entry.Number("DurationMs"));
        Assert.Contains("body", entry.Message);
        Assert.Contains("fsync", entry.Message);
    }

    [Fact]
    public void A_deferred_fsync_policy_logs_1003_under_its_own_event_name_with_no_fsync_field()
    {
        using var logs = new LogCapture();
        using var deferred = new EngineHarness(FsyncPolicy.OsBuffered, loggerFactory: logs);
        deferred.Options.Telemetry.SlowReducerMs = 0;
        deferred.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));

        // Alerts key on the id, which is why it stays 1003; the name says why a number is missing.
        var entry = logs.Single(1003);
        Assert.Equal("SlowReducerDeferredFsync", entry.EventName);
        Assert.DoesNotContain("FsyncMs", entry.Fields.Keys);
        Assert.Contains("fsync deferred by CommitLog:FsyncPolicy", entry.Message);
    }

    [Fact]
    public void A_slow_reducer_that_throws_still_warns_because_the_lock_was_held_just_as_long()
    {
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 0;

        Assert.Throws<InvalidOperationException>(() => logged.Invoke("Crash", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" });
            throw new InvalidOperationException("boom");
        }));

        var entry = logs.Single(1003);
        Assert.Equal("SlowReducerAborted", entry.EventName);
        Assert.Equal("abort", entry.Fields["Outcome"]);
        Assert.True(entry.Number("BodyMs") > 0);
        // Nothing was appended, so there is nothing to attribute — and a zero would be averaged.
        Assert.DoesNotContain("CommitMs", entry.Fields.Keys);
        Assert.DoesNotContain("PostCommitMs", entry.Fields.Keys);
        Assert.Contains("nothing appended", entry.Message);
    }

    [Fact]
    public void A_slow_rejection_warns_too_and_the_outcome_field_is_what_separates_them()
    {
        // A rejection is an ordinary outcome — a player tried something illegal — but it costs the
        // same lock time, and "rejections are cheap" is the assumption that makes a validating
        // reducer expensive. It warns; the outcome field is how an alert can choose to ignore it.
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 0;

        Assert.Throws<RejectedException>(() => logged.Invoke("Deny", _ => throw new RejectedException("illegal move")));

        var entry = logs.Single(1003);
        Assert.Equal("rejected", entry.Fields["Outcome"]);
        Assert.Equal("Deny", entry.Fields["Reducer"]);
    }

    [Fact]
    public void An_abort_carries_the_slow_reducer_span_event_with_the_outcome_and_no_commit_split()
    {
        _harness.Options.Telemetry.SlowReducerMs = 0;
        Assert.Throws<RejectedException>(() => _harness.Invoke("Deny", _ => throw new RejectedException("no")));

        var slow = SlowReducerEvent();
        Assert.Equal("rejected", slow.Tags.Single(t => t.Key == "melange.outcome").Value);
        Assert.Equal(0, slow.Tags.Single(t => t.Key == "melange.writeset.rows").Value);
        Assert.True(Tag(slow, "melange.body_ms") <= Tag(slow, "melange.duration_ms"));
        Assert.DoesNotContain(slow.Tags, t => t.Key == "melange.commit_ms");
    }

    [Fact]
    public void A_fast_abort_stays_silent_so_ordinary_rejections_do_not_become_noise()
    {
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 5_000;

        Assert.Throws<RejectedException>(() => logged.Invoke("Deny", _ => throw new RejectedException("no")));

        Assert.DoesNotContain(logs.Entries, e => e.EventId == 1003);
    }

    [Fact]
    public void A_commit_guard_that_rejects_slowly_is_not_charged_to_the_reducer_body()
    {
        // The cluster span check runs inside the same try as the body, so a transaction refused for
        // touching two shards aborts having paid for both. Body time stays the body's.
        using var logs = new LogCapture();
        using var logged = new EngineHarness(loggerFactory: logs);
        logged.Options.Telemetry.SlowReducerMs = 0;
        logged.Engine.AddCommitGuard(new SleepyGuard(TimeSpan.FromMilliseconds(60)));

        Assert.Throws<RejectedException>(() => logged.Invoke("SpanBoth", ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" })));

        var entry = logs.Single(1003);
        Assert.True(entry.Number("DurationMs") >= 55, "the guard's cost is part of the stall");
        Assert.True(entry.Number("BodyMs") < 30, "but it is not the body's");
    }

    private ActivityEvent SlowReducerEvent()
    {
        var reducer = Stopped().Last(a => a.OperationName == "melange.reducer");
        return Assert.Single(reducer.Events, e => e.Name == "melange.slow_reducer");
    }

    private static double Tag(ActivityEvent slow, string key) =>
        Assert.IsType<double>(slow.Tags.Single(t => t.Key == key).Value);

    private sealed class SleepyObserver(TimeSpan delay) : ICommitObserver
    {
        public void OnCommit(CommitRecord record) => Thread.Sleep(delay);
    }

    private sealed class SleepyGuard(TimeSpan delay) : ICommitGuard
    {
        public void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin)
        {
            Thread.Sleep(delay);
            throw new RejectedException("this write set spans two shards");
        }
    }
}
