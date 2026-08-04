using System.Diagnostics;
using System.Diagnostics.Metrics;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The scheduler's contract, driven entirely by a hand-cranked clock: transactional scheduling,
/// interval cadence, one-shot consumption, restart recovery from the log, the documented overrun
/// policies, and the not-client-callable gate.
/// </summary>
public class SchedulerTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-scheduler-").FullName;
    private readonly ManualTimeProvider _time = new();

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private async Task<IHost> StartHostAsync(IDictionary<string, string?>? settings = null)
    {
        var host = TestApp.Build(_root, settings, builder => builder.Services.AddSingleton<TimeProvider>(_time));
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static SchedulerProbe Probe(IHost host) => host.Services.GetRequiredService<SchedulerProbe>();

    private Timestamp In(TimeSpan fromNow) => Timestamp.FromDateTimeOffset(_time.GetUtcNow() + fromNow);

    [Fact]
    public async Task A_repeating_timer_fires_on_its_interval_and_its_work_commits()
    {
        using var host = await StartHostAsync();
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 7);

        _time.Advance(TimeSpan.FromSeconds(9));
        Assert.Equal(0, Probe(host).WorldTicks);

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, Probe(host).WorldTicks);

        _time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(4, Probe(host).WorldTicks);

        host.Engine().Invoke("Verify", TestApp.Caller, ctx =>
        {
            Assert.Equal(4, ctx.Db.TickLog.Iter().Count());
            Assert.All(ctx.Db.TickLog.Iter(), entry => Assert.StartsWith("tick:", entry.Entry));
        });
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_repeating_fire_that_writes_nothing_appends_nothing()
    {
        // The write-amplification answer, asserted: the next fire derives from the interval, so
        // an idle tick is free — no reschedule row write, no bookkeeping-only fsynced commit.
        using var host = await StartHostAsync();
        Probe(host).WriteRows = false;
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 5_000L, 0);
        var head = host.Engine().Log.HeadLsn;

        _time.Advance(TimeSpan.FromSeconds(50));
        Assert.Equal(10, Probe(host).WorldTicks);
        Assert.Equal(head, host.Engine().Log.HeadLsn);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_one_shot_fires_exactly_once_and_deletes_its_row_in_the_same_commit()
    {
        using var host = await StartHostAsync();
        host.Reducers().Call("ScheduleOnce", TestApp.Caller, In(TimeSpan.FromSeconds(30)), "boom");
        var head = host.Engine().Log.HeadLsn;

        _time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(1, Probe(host).OneShots);

        // One record for the fire, carrying the reducer's insert and the timer's delete together.
        var record = Assert.Single(host.Engine().Log.ReadFrom(head + 1));
        Assert.Equal("RunOnce", record.ReducerName);
        Assert.Equal(MelangeScheduler.Caller, record.Caller);
        Assert.Equal(2, record.WriteSet.Count);
        Assert.Contains(record.WriteSet, op => op.Kind == RowOpKind.Insert);
        Assert.Contains(record.WriteSet, op => op.Kind == RowOpKind.Delete);

        _time.Advance(TimeSpan.FromMinutes(5));
        Assert.Equal(1, Probe(host).OneShots);
        host.Engine().Invoke("Verify", TestApp.Caller, ctx => Assert.Empty(ctx.Db.OneShotTimer.Iter()));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_rolled_back_reducer_schedules_nothing()
    {
        using var host = await StartHostAsync();
        Assert.Throws<RejectedException>(() =>
            host.Reducers().Call("ScheduleOnceAndThrow", TestApp.Caller, In(TimeSpan.FromSeconds(1))));

        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(0, Probe(host).OneShots);
        host.Engine().Invoke("Verify", TestApp.Caller, ctx => Assert.Empty(ctx.Db.OneShotTimer.Iter()));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Restart_resumes_timers_from_the_log_with_no_duplicates_and_no_losses()
    {
        using (var host = await StartHostAsync())
        {
            host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 1);
            host.Reducers().Call("ScheduleOnce", TestApp.Caller, In(TimeSpan.FromSeconds(5)), "early");
            host.Reducers().Call("ScheduleOnce", TestApp.Caller, In(TimeSpan.FromSeconds(60)), "late");
            _time.Advance(TimeSpan.FromSeconds(12));
            Assert.Equal(1, Probe(host).WorldTicks);
            Assert.Equal(1, Probe(host).OneShots);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        // Forty seconds of downtime: the "late" one-shot (due at +60s) is still pending, the
        // "early" one already fired and must not fire again, and the repeating tick is overdue.
        _time.Advance(TimeSpan.FromSeconds(40));

        using (var host = await StartHostAsync())
        {
            // FireOnce (the default): the overdue repeating timer fired once at recovery.
            Assert.Equal(1, Probe(host).WorldTicks);
            Assert.Equal(0, Probe(host).OneShots);

            _time.Advance(TimeSpan.FromSeconds(8));
            Assert.Equal(1, Probe(host).OneShots);

            host.Engine().Invoke("Verify", TestApp.Caller, ctx =>
            {
                Assert.Equal(1, ctx.Db.TickLog.Iter().Count(e => e.Entry == "once:early"));
                Assert.Equal(1, ctx.Db.TickLog.Iter().Count(e => e.Entry == "once:late"));
                Assert.Empty(ctx.Db.OneShotTimer.Iter());
                Assert.Single(ctx.Db.WorldTickTimer.Iter());
            });

            // The cadence resumed from recovery: the next interval fire still happens.
            _time.Advance(TimeSpan.FromSeconds(10));
            Assert.Equal(2, Probe(host).WorldTicks);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task CatchUpAll_fires_once_per_missed_interval_where_FireOnce_fires_once()
    {
        using (var host = await StartHostAsync())
        {
            host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 1);
            _time.Advance(TimeSpan.FromSeconds(10));
            Assert.Equal(1, Probe(host).WorldTicks);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        // Down for 50 seconds. The last log record is the tick fire that just happened, so the
        // recovery anchor sits at that fire: five whole intervals were missed.
        _time.Advance(TimeSpan.FromSeconds(50));

        using (var host = await StartHostAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Scheduler:CatchUpAfterDowntime"] = "CatchUpAll",
        }))
        {
            Assert.Equal(5, Probe(host).WorldTicks);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Skip_is_the_default_overrun_policy_and_resumes_one_interval_after_the_slow_tick()
    {
        using var host = await StartHostAsync();
        var overruns = 0L;
        using var listener = OverrunListener(() => overruns++);

        var probe = Probe(host);
        var slowOnce = true;
        probe.OnWorldTick = _ =>
        {
            if (slowOnce)
            {
                slowOnce = false;
                _time.Advance(TimeSpan.FromSeconds(25));
            }
        };

        host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 1);
        _time.Advance(TimeSpan.FromSeconds(10));

        // The tick at +10s ran 25 seconds long, missing the fires at +20s and +30s. Skip drops
        // them: nothing runs until one full interval after the slow tick completed.
        Assert.Equal(1, probe.WorldTicks);
        Assert.Equal(1, Interlocked.Read(ref overruns));

        _time.Advance(TimeSpan.FromSeconds(9));
        Assert.Equal(1, probe.WorldTicks);
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(2, probe.WorldTicks);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task RunImmediately_replays_every_missed_fire_and_Coalesce_collapses_them_into_one()
    {
        using (var host = await StartHostAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Scheduler:OverrunPolicy"] = "RunImmediately",
        }))
        {
            var probe = Probe(host);
            var slowOnce = true;
            probe.OnWorldTick = _ =>
            {
                if (slowOnce)
                {
                    slowOnce = false;
                    _time.Advance(TimeSpan.FromSeconds(25));
                }
            };
            host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 1);
            _time.Advance(TimeSpan.FromSeconds(10));

            // The slow tick plus its two missed fires, replayed back to back.
            Assert.Equal(3, probe.WorldTicks);
            host.Reducers().Call("CancelWorldTicks", TestApp.Caller);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        using (var host = await StartHostAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Scheduler:OverrunPolicy"] = "Coalesce",
        }))
        {
            var probe = Probe(host);
            var slowOnce = true;
            probe.OnWorldTick = _ =>
            {
                if (slowOnce)
                {
                    slowOnce = false;
                    _time.Advance(TimeSpan.FromSeconds(25));
                }
            };
            host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 1);
            _time.Advance(TimeSpan.FromSeconds(10));

            // The slow tick plus one coalesced catch-up fire covering both missed fires.
            Assert.Equal(2, probe.WorldTicks);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Disabling_the_scheduler_stops_fires_and_reenabling_is_live()
    {
        using var host = await StartHostAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Scheduler:Enabled"] = "false",
        });
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 1);

        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(0, Probe(host).WorldTicks);

        host.ReloadWith("MelangeDb:Scheduler:Enabled", "true");
        Assert.True(Probe(host).WorldTicks >= 1);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Rewriting_a_timer_row_reschedules_it_and_deleting_it_unschedules_it()
    {
        using var host = await StartHostAsync();
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 60_000L, 1);
        ulong id = 0;
        host.Engine().Invoke("Verify", TestApp.Caller, ctx => id = ctx.Db.WorldTickTimer.Iter().Single().Id);

        // Tighten the interval mid-flight: the next fire derives from the rewritten row.
        host.Reducers().Call("RescheduleTick", TestApp.Caller, id, 5_000L);
        _time.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(1, Probe(host).WorldTicks);

        host.Reducers().Call("CancelWorldTicks", TestApp.Caller);
        _time.Advance(TimeSpan.FromMinutes(5));
        Assert.Equal(1, Probe(host).WorldTicks);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_throwing_one_shot_aborts_with_its_row_intact_and_does_not_hot_loop()
    {
        using var host = await StartHostAsync();
        Probe(host).ThrowOnOneShot = true;
        host.Reducers().Call("ScheduleOnce", TestApp.Caller, In(TimeSpan.FromSeconds(1)), "fails");
        var head = host.Engine().Log.HeadLsn;

        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(0, Probe(host).OneShots);
        Assert.Equal(head, host.Engine().Log.HeadLsn);

        // The abort kept the row — it is data — so a restart arms it again.
        host.Engine().Invoke("Verify", TestApp.Caller, ctx => Assert.Single(ctx.Db.OneShotTimer.Iter()));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Scheduled_reducers_answer_unknown_to_clients_and_are_absent_from_the_unpoliced_report()
    {
        using var host = await StartHostAsync();
        var thrown = Assert.Throws<ArgumentException>(() => host.Reducers().Call(
            "WorldTick",
            TestApp.Caller,
            ConnectionId.None,
            ReducerArguments.Encode(),
            source: CallSource.Client()));
        Assert.Contains("No reducer named 'WorldTick'", thrown.Message);

        Assert.DoesNotContain("WorldTick", host.Reducers().UnpolicedReducers);
        Assert.DoesNotContain("RunOnce", host.Reducers().UnpolicedReducers);
        Assert.Contains("AddNote", host.Reducers().UnpolicedReducers);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_tick_starts_its_own_trace_and_the_reducer_span_parents_to_it()
    {
        var ticks = new List<Activity>();
        var reducers = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "MelangeDB",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                lock (ticks)
                {
                    if (activity.OperationName == "melange.scheduler.tick")
                        ticks.Add(activity);
                    if (activity.OperationName == "melange.reducer" && Equals(activity.GetTagItem("melange.reducer.name"), "RunOnce"))
                        reducers.Add(activity);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        using var host = await StartHostAsync();
        host.Reducers().Call("ScheduleOnce", TestApp.Caller, In(TimeSpan.FromSeconds(1)), "traced");
        _time.Advance(TimeSpan.FromSeconds(1));

        lock (ticks)
        {
            var tick = Assert.Single(ticks, t => Equals(t.GetTagItem("melange.reducer.name"), "RunOnce"));
            Assert.Null(tick.Parent);
            var reducer = Assert.Single(reducers);
            Assert.Equal(tick.SpanId, reducer.ParentSpanId);
            Assert.Equal(tick.TraceId, reducer.TraceId);
        }

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task An_init_reducer_seeds_a_fresh_database_and_a_restart_does_not_seed_it_again()
    {
        var probe = new SchedulerProbe { SeedOnInit = true };
        IHost Build() => TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });

        using (var host = Build())
        {
            await host.StartAsync(TestContext.Current.CancellationToken);

            // Seeded before the scheduler started, so the timer is in the pending set from the
            // first instant rather than arriving as an observed commit beside it.
            Assert.Equal(1, host.Engine().CommittedView.Count<WorldTickTimer>());
            _time.Advance(TimeSpan.FromSeconds(10));
            Assert.Equal(1, probe.WorldTicks);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        using (var restarted = Build())
        {
            await restarted.StartAsync(TestContext.Current.CancellationToken);

            // The log has a head now, so this database is not fresh: seeding again would double
            // every timer on every restart.
            Assert.Equal(1, probe.InitFires);
            Assert.Equal(1, restarted.Engine().CommittedView.Count<WorldTickTimer>());
            await restarted.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    private static MeterListener OverrunListener(Action onOverrun)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "MelangeDB" && instrument.Name == "melange.scheduler.overruns")
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
        {
            for (long i = 0; i < measurement; i++)
                onOverrun();
        });
        listener.Start();
        return listener;
    }
}
