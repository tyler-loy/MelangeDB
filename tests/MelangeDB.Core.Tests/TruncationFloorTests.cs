using System.Diagnostics.Metrics;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Truncation-floor observability (road-to-0.2 phase 18): every floor has a name, every truncation
/// decision says which one governed — including the decision that removes nothing, the one that
/// used to be perfectly silent — and the gauges pair the named floors with the live head, so "why
/// is the log not truncating" is a look rather than a debugger session.
/// </summary>
[Collection("Telemetry")]
public class TruncationFloorTests : IDisposable
{
    private readonly LogCapture _logs = new();
    private readonly EngineHarness _harness;

    public TruncationFloorTests()
    {
        _harness = new EngineHarness(loggerFactory: _logs);

        // The retention window is a floor like any other and has its own test; the rest of these
        // want it out of the way, exactly as every other truncation test in the suite does.
        _harness.Options.Resume.RetentionWindowSeconds = 0;
    }

    public void Dispose() => _harness.Dispose();

    private void Commit(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var id = Identity.Hash($"player-{Guid.NewGuid()}");
            _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = id, RoomId = 1, X = 1, Y = 2, Name = "p" }));
        }
    }

    private TruncationFloorReport Snapshot()
    {
        Assert.NotNull(_harness.Engine.TakeSnapshot());
        var report = _harness.Engine.TruncationFloors;
        Assert.NotNull(report);
        return report;
    }

    private static ulong FloorOf(TruncationFloorReport report, string name) =>
        Assert.Single(report.Floors, f => f.Name == name).Lsn;

    [Fact]
    public void Truncation_names_the_snapshot_as_its_governing_floor_when_nothing_is_holding_the_log()
    {
        Commit(5);
        var report = Snapshot();

        // Nothing is behind, so the snapshot itself is the binding constraint — which is what a
        // healthy log looks like, and is why "snapshot" is a floor name rather than an implicit
        // ceiling nobody can see.
        Assert.Equal(TruncationFloorNames.Snapshot, report.Governing.Name);
        Assert.Equal(5UL, report.Governing.Lsn);
        Assert.Equal(5UL, report.EffectiveFloor);
        Assert.Equal(0, report.PinnedRecords);
        Assert.Equal(5UL, FloorOf(report, "hot-store"));
        Assert.Equal(5UL, FloorOf(report, TruncationFloorNames.ResumeWindow));

        // A floor that answers null holds nothing and is absent, rather than reported at some
        // sentinel LSN an operator would have to learn to ignore.
        Assert.DoesNotContain(report.Floors, f => f.Name == TruncationFloorNames.BackupPin);

        var truncated = _logs.Single(1503);
        Assert.Equal(TruncationFloorNames.Snapshot, truncated.Fields["FloorName"]);
        Assert.Equal(0d, truncated.Number("PinnedRecords"));
        Assert.True(truncated.Number("LogBytes") > 0);
    }

    [Fact]
    public void A_floor_that_stops_moving_is_named_in_the_truncation_log_line()
    {
        _harness.Engine.AddTruncationFloor(TruncationFloorNames.EventBus, () => 3UL);
        Commit(10);
        var report = Snapshot();

        Assert.Equal(TruncationFloorNames.EventBus, report.Governing.Name);
        Assert.Equal(3UL, report.EffectiveFloor);
        Assert.Equal(7, report.PinnedRecords);
        Assert.Equal(3UL, _harness.Engine.Log.BaseLsn);

        var truncated = _logs.Single(1503);
        Assert.Equal(TruncationFloorNames.EventBus, truncated.Fields["FloorName"]);
        Assert.Equal(3d, truncated.Number("FloorLsn"));
        Assert.Equal(7d, truncated.Number("PinnedRecords"));
    }

    [Fact]
    public void A_truncation_that_removes_nothing_says_which_floor_pinned_it()
    {
        // The crashed-subscriber shape: a holder that never checkpoints again. Today's symptom is
        // log growth; this line is the cause, at the moment the growth is decided.
        _harness.Engine.AddTruncationFloor(TruncationFloorNames.EventBus, () => 0UL);
        Commit(10);
        var report = Snapshot();

        Assert.Equal(TruncationFloorNames.EventBus, report.Governing.Name);
        Assert.Equal(0UL, report.EffectiveFloor);
        Assert.Equal(10, report.PinnedRecords);
        Assert.Equal(0UL, _harness.Engine.Log.BaseLsn);

        Assert.DoesNotContain(_logs.Entries, e => e.EventId == 1503);
        var pinned = _logs.Single(1510);
        Assert.Equal(TruncationFloorNames.EventBus, pinned.Fields["FloorName"]);
        Assert.Equal(0d, pinned.Number("FloorLsn"));
        Assert.Equal(10d, pinned.Number("HeadLsn"));
        Assert.Equal(10d, pinned.Number("PinnedRecords"));
        Assert.True(pinned.Number("LogBytes") > 0);
    }

    [Fact]
    public void An_unnamed_floor_registration_reports_as_unnamed()
    {
        // The pre-1.0 overload survives. A third-party floor that never named itself still shows
        // up — as "unnamed", which is itself the diagnosis.
        _harness.Engine.AddTruncationFloor(() => 4UL);
        Commit(10);
        var report = Snapshot();

        Assert.Equal(TruncationFloorNames.Unnamed, report.Governing.Name);
        Assert.Equal(4UL, FloorOf(report, TruncationFloorNames.Unnamed));
    }

    [Fact]
    public void The_resume_window_is_a_floor_with_a_name_like_any_other()
    {
        // Every record is younger than the window, so a reconnecting client could still ask for
        // the first one: nothing may be removed, and the reason has a name.
        _harness.Options.Resume.RetentionWindowSeconds = 300;
        Commit(5);
        var report = Snapshot();

        Assert.Equal(TruncationFloorNames.ResumeWindow, report.Governing.Name);
        Assert.Equal(0UL, report.EffectiveFloor);
        Assert.Equal(TruncationFloorNames.ResumeWindow, _logs.Single(1510).Fields["FloorName"]);
    }

    [Fact]
    public void Two_floors_sharing_a_name_report_the_lower_of_the_two()
    {
        // Two shards' freeze markers, or two unnamed third-party floors: a duplicated tag set
        // would let the last writer win arbitrarily, so the gauge reports the one actually holding
        // the log. The report itself keeps both readings.
        _harness.Engine.AddTruncationFloor(TruncationFloorNames.ShardFreeze, () => 7UL);
        _harness.Engine.AddTruncationFloor(TruncationFloorNames.ShardFreeze, () => 4UL);
        Commit(10);
        var report = Snapshot();

        Assert.Equal(2, report.Floors.Count(f => f.Name == TruncationFloorNames.ShardFreeze));
        Assert.Equal(4UL, ReadGauges()[("melange.log.truncation_floor", TruncationFloorNames.ShardFreeze)]);
    }

    [Fact]
    public void The_gauges_publish_every_floor_and_the_distance_the_head_has_travelled_since()
    {
        _harness.Engine.AddTruncationFloor(TruncationFloorNames.EventBus, () => 3UL);
        Commit(10);
        Snapshot();

        // Commits after the decision are the point: the floors are a cached reading, but the head
        // is live, so the pinned distance grows while the stuck holder stands still.
        Commit(5);
        var gauges = ReadGauges();
        Assert.Equal(3UL, gauges[("melange.log.truncation_floor", TruncationFloorNames.EventBus)]);
        Assert.Equal(10UL, gauges[("melange.log.truncation_floor", TruncationFloorNames.Snapshot)]);
        Assert.Equal(12UL, gauges[("melange.log.pinned_records", string.Empty)]);
    }

    [Fact]
    public void Floors_are_evaluated_only_when_truncation_is_decided()
    {
        // Floor providers run under the write lock, read engine state unguarded, and one of them
        // (the cluster's borrowed-sidecar refresh) writes a file. Evaluating them from a metrics
        // scrape would race the pin list and rewrite that sidecar every scrape.
        var evaluations = 0;
        _harness.Engine.AddTruncationFloor(TruncationFloorNames.EventBus, () =>
        {
            evaluations++;
            return 3UL;
        });
        Commit(10);
        Snapshot();
        Assert.Equal(1, evaluations);

        for (var i = 0; i < 3; i++)
            ReadGauges();
        Assert.Equal(1, evaluations);
    }

    [Fact]
    public void There_is_no_report_and_no_gauge_until_a_truncation_decision_is_made()
    {
        // An absent series says "never evaluated"; a zero would say "healthy". Only one of those
        // is true of a log nothing is compacting.
        _harness.Options.Snapshots.TruncateLog = false;
        Commit(5);
        Assert.NotNull(_harness.Engine.TakeSnapshot());

        Assert.Null(_harness.Engine.TruncationFloors);
        var gauges = ReadGauges();
        Assert.DoesNotContain(gauges.Keys, k => k.Instrument == "melange.log.truncation_floor");
        Assert.DoesNotContain(gauges.Keys, k => k.Instrument == "melange.log.pinned_records");
    }

    private static Dictionary<(string Instrument, string Floor), ulong> ReadGauges()
    {
        var gauges = new Dictionary<(string Instrument, string Floor), ulong>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "MelangeDB"
                && instrument.Name is "melange.log.truncation_floor" or "melange.log.pinned_records")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var floor = string.Empty;
            foreach (var tag in tags)
            {
                if (tag.Key == "floor")
                    floor = (string)tag.Value!;
            }

            gauges[(instrument.Name, floor)] = (ulong)value;
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return gauges;
    }
}
