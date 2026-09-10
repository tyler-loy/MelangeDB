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
        var thrown = Assert.Throws<UnknownReducerException>(() => host.Reducers().Call(
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

    [Fact]
    public async Task Two_scheduled_tables_both_keep_firing_after_a_restart()
    {
        // The reference-workload report: after a restart of a world with several scheduled tables,
        // one keeps firing and the rest go silent. Two interval timers on their own tables, both
        // alive on the first boot, both must still fire after a plain restart.
        var probe = new SchedulerProbe();
        IHost Build() => TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });

        using (var host = Build())
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            host.Reducers().Call("ScheduleTick", TestApp.Caller, 2_000L, 0);
            host.Reducers().Call("ScheduleSecond", TestApp.Caller, 5_000L);

            _time.Advance(TimeSpan.FromSeconds(20));
            Assert.True(probe.WorldTicks > 0, "world tick never fired on the first boot");
            Assert.True(probe.SecondTicks > 0, "second tick never fired on the first boot");
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        var worldBefore = probe.WorldTicks;
        var secondBefore = probe.SecondTicks;

        using (var restarted = Build())
        {
            await restarted.StartAsync(TestContext.Current.CancellationToken);

            // Both timers are in the log; both must resume after the restart.
            Assert.Equal(1, restarted.Engine().CommittedView.Count<WorldTickTimer>());
            Assert.Equal(1, restarted.Engine().CommittedView.Count<SecondTickTimer>());

            _time.Advance(TimeSpan.FromSeconds(20));
            Assert.True(probe.WorldTicks > worldBefore, "world tick stopped firing after the restart");
            Assert.True(probe.SecondTicks > secondBefore, "second tick stopped firing after the restart");
            await restarted.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Two_scheduled_tables_both_keep_firing_after_a_restart_from_a_snapshot()
    {
        // A migrated store restarts from the sealing snapshot the migration took, not from a full
        // log replay. This is the same two-timer restart, but the store is snapshotted (and the log
        // truncated) before the stop, so the restart bootstraps from the snapshot — the shape a
        // post-migration restart actually has.
        var probe = new SchedulerProbe();
        IHost Build() => TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });

        using (var host = Build())
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            host.Reducers().Call("ScheduleTick", TestApp.Caller, 2_000L, 0);
            host.Reducers().Call("ScheduleSecond", TestApp.Caller, 5_000L);

            _time.Advance(TimeSpan.FromSeconds(20));
            Assert.True(probe.WorldTicks > 0 && probe.SecondTicks > 0);

            // Seal a snapshot (and truncate) so the next boot bootstraps from it, like a migrated store.
            host.Engine().TakeSnapshot();
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        var worldBefore = probe.WorldTicks;
        var secondBefore = probe.SecondTicks;

        using (var restarted = Build())
        {
            await restarted.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(1, restarted.Engine().CommittedView.Count<WorldTickTimer>());
            Assert.Equal(1, restarted.Engine().CommittedView.Count<SecondTickTimer>());

            _time.Advance(TimeSpan.FromSeconds(20));
            Assert.True(probe.WorldTicks > worldBefore, "world tick stopped firing after a snapshot restart");
            Assert.True(probe.SecondTicks > secondBefore, "second tick stopped firing after a snapshot restart");
            await restarted.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task The_scheduler_never_re_arms_for_a_sub_resolution_residual()
    {
        // The hot re-arm spin: a platform timer wakes a fraction of a millisecond before its due
        // time, the drain finds nothing due, and the scheduler re-arms on the tiny remainder —
        // waking early again on a smaller one, tens of times a second, until the due time passes.
        // A residual too small to wait for is armed as zero instead, and the drain fires: the
        // tolerance is what ends the loop. Here a 1ms-interval timer would ask for a 1ms re-arm.
        var probe = new SchedulerProbe();
        using var host = TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });
        await host.StartAsync(TestContext.Current.CancellationToken);

        host.Reducers().Call("ScheduleTick", TestApp.Caller, 1L, 0); // 1ms interval — the residual case.

        // Every finite re-arm the scheduler made is either "now" — the 1ms residual, armed as zero
        // so the drain fires it — or a delay long enough to be worth waiting for. Never a
        // sub-tolerance positive delay, which is the one that spins.
        var tolerance = TimeSpan.FromMilliseconds(2);
        Assert.DoesNotContain(_time.ArmDelays, d => d > TimeSpan.Zero && d < tolerance);
        Assert.Contains(_time.ArmDelays, d => d == TimeSpan.Zero);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_sixty_hertz_interval_holds_its_cadence_instead_of_overrunning()
    {
        // Issue #154. The re-arm delay used to be floored at 16ms "the platform timer's practical
        // resolution", which is fine against a 10s sweep and fatal against a 16.67ms simulation
        // tick: a tick that costs 1ms leaves a 15.67ms residual, the floor rounds it up to 16, and
        // the wake lands 0.33ms after the next due time. That lateness accumulates one residual at
        // a time until a fire is a whole interval behind, and the scheduler charges the resulting
        // overrun to the reducer — which did nothing wrong and was long since finished. (On Windows
        // it is worse than the arithmetic: a 16ms request rounds to two 15.625ms quanta, ~31ms.)
        //
        // The cadence must therefore survive a tick whose body costs less than the interval.
        using var host = await StartHostAsync();
        var overruns = 0L;
        using var listener = OverrunListener(() => overruns++);

        var probe = Probe(host);
        probe.OnWorldTick = _ => _time.Advance(TimeSpan.FromMilliseconds(1)); // A 1ms body.

        var interval = TimeSpan.FromSeconds(1.0 / 60.0);
        host.Engine().Invoke("ScheduleSixtyHertz", TestApp.Caller, ctx => ctx.Db.WorldTickTimer.Insert(new WorldTickTimer
        {
            ScheduledAt = ScheduleAt.Interval(interval),
            Payload = 60,
        }));

        _time.Advance(TimeSpan.FromSeconds(1));

        // A full second of a 60Hz timer whose body costs 1ms: 60 fires, none of them late. Before
        // the fix this drifted 0.33ms per fire and logged its first 1301 inside the same second.
        Assert.Equal(60, probe.WorldTicks);
        Assert.Equal(0, Interlocked.Read(ref overruns));

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_fire_may_be_early_but_never_by_more_than_the_tolerance()
    {
        // The other half of #154: what the scheduler now does with a residual instead of inflating
        // it. A tick that eats all but 1.67ms of its own interval leaves nothing worth waiting for,
        // so the next fire happens on the same wake rather than a timer quantum later. Early, by
        // less than the tolerance, and — the part that matters — still anchored: the fire after it
        // is scheduled from the due time, not from when the early one actually ran.
        using var host = await StartHostAsync();
        var probe = Probe(host);
        var fires = new List<DateTimeOffset>();
        probe.OnWorldTick = _ =>
        {
            fires.Add(_time.GetUtcNow());
            _time.Advance(TimeSpan.FromMilliseconds(15)); // Body eats 15 of the 16.67ms interval.
        };

        var interval = TimeSpan.FromSeconds(1.0 / 60.0);
        var scheduled = _time.GetUtcNow();
        host.Engine().Invoke("ScheduleSixtyHertz", TestApp.Caller, ctx => ctx.Db.WorldTickTimer.Insert(new WorldTickTimer
        {
            ScheduledAt = ScheduleAt.Interval(interval),
            Payload = 60,
        }));

        _time.Advance(TimeSpan.FromMilliseconds(100));

        Assert.NotEmpty(fires);
        for (var i = 0; i < fires.Count; i++)
        {
            // Fire i was due at scheduled + (i + 1) intervals. Never late, never more than the
            // tolerance early — so the cadence is the interval's, not the platform quantum's.
            var due = scheduled + interval * (i + 1);
            Assert.InRange(fires[i], due - TimeSpan.FromMilliseconds(2), due);
        }

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_faulting_drain_re_arms_and_keeps_firing_instead_of_wedging()
    {
        // The prod stall: the dispatch drain threw, and because Rearm ran AFTER the try/finally the
        // timer was left unarmed — only the next commit's OnCommit re-armed it, so on an idle world
        // the scheduler sat idle for tens of seconds between fires. Rearm now runs in the finally,
        // and a faulting drain is swallowed and logged rather than wedging every timer. A fault
        // injected into one drain must not stop the timer from firing on the next tick.
        var probe = new SchedulerProbe();
        using var host = TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });
        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 5_000L, 0);

        _time.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(1, probe.WorldTicks);

        // The next drain throws once, then clears itself.
        var scheduler = host.Services.GetRequiredService<MelangeScheduler>();
        scheduler.DrainFaultInjection = () =>
        {
            scheduler.DrainFaultInjection = null;
            throw new InvalidOperationException("injected drain fault");
        };

        // Advancing triggers the faulting drain. It must not propagate (no crash), and the timer
        // must still be armed afterward — so the next interval fires.
        var thrown = Record.Exception(() => _time.Advance(TimeSpan.FromSeconds(5)));
        Assert.Null(thrown);
        Assert.Equal(2, probe.WorldTicks);

        // And it keeps going on the interval after the fault, not just the one recovery fire.
        _time.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(3, probe.WorldTicks);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_re_entrant_drain_request_is_not_dropped()
    {
        // The prod idle-stall: a ProcessDueFires that re-enters while another drain holds the guard
        // used to return having armed nothing, dropping its re-arm. On a committing world the next
        // commit's OnCommit re-armed within milliseconds and hid it; on an idle world nothing
        // committed, so the timer went dark. The guard now records the request and the in-flight
        // drain honours it — so a re-entrant call causes a rescan (the drain runs again) rather than
        // being lost. Observed by counting drains: the injected re-entry must produce a second one.
        var probe = new SchedulerProbe();
        using var host = TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });
        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 0);

        var scheduler = host.Services.GetRequiredService<MelangeScheduler>();
        var drains = 0;
        scheduler.DrainFaultInjection = () =>
        {
            drains++;
            if (drains == 1)
                scheduler.PumpForTest(); // re-enter while this drain holds the guard
        };

        // The timer fires, the drain runs, and its first iteration re-enters the scheduler. With the
        // request honoured, the drain runs a second time (the rescan); with the old bare guard the
        // re-entry was dropped and the drain ran exactly once.
        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(drains >= 2, $"a re-entrant drain request was dropped: the drain ran {drains} time(s), no rescan");

        scheduler.DrainFaultInjection = null;
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Stopping_waits_for_a_fire_that_is_already_in_flight()
    {
        // Issue #145. Stop() used to set a flag and dispose the timer, neither of which recalls a
        // fire already running on the timer's thread. That fire then committed after the host had
        // drained, checkpointed, and logged the LSN it flushed at — the two unaccounted LSNs in the
        // report. Stop now waits it out, so the fire is counted in the announced LSN.
        var probe = new SchedulerProbe();
        using var host = TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });
        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 0);

        var scheduler = host.Services.GetRequiredService<MelangeScheduler>();
        using var inDrain = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        scheduler.DrainFaultInjection = () =>
        {
            scheduler.DrainFaultInjection = null;
            inDrain.Set();
            release.Wait(TimeSpan.FromSeconds(30));
        };

        // Hold a drain open on another thread, so Stop() meets a fire genuinely in flight.
        var firing = Task.Run(() => scheduler.PumpForTest(), TestContext.Current.CancellationToken);
        Assert.True(inDrain.Wait(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken), "the drain never started");

        var stopping = Task.Run(() => scheduler.Stop(), TestContext.Current.CancellationToken);
        var raced = await Task.WhenAny(stopping, Task.Delay(500, TestContext.Current.CancellationToken));
        Assert.NotSame(stopping, raced);

        release.Set();
        await stopping.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await firing;

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task The_lsn_the_host_announces_at_shutdown_is_the_last_one()
    {
        // The property the report is really about: whatever the stop line says was flushed, nothing
        // appends after it. Here the scheduler is mid-fire when shutdown begins — the exact shape of
        // issue #145 — and the head must not move once StopAsync has returned.
        var probe = new SchedulerProbe();
        using var host = TestApp.Build(_root, null, builder =>
        {
            builder.Services.AddSingleton<TimeProvider>(_time);
            builder.Services.AddSingleton(probe);
        });
        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Reducers().Call("ScheduleTick", TestApp.Caller, 10_000L, 0);
        _time.Advance(TimeSpan.FromSeconds(10));

        await host.StopAsync(TestContext.Current.CancellationToken);
        var announced = host.Engine().Log.HeadLsn;

        // A scheduled fire arriving now is exactly the race: past its own stopping check, queued on
        // the write lock, landing after the flush. It is refused, and the head stands.
        Assert.Throws<TransientRejectionException>(() =>
            host.Engine().Invoke("LateFire", TestApp.Caller, ctx => ctx.Db.Insert(new TickLog
            {
                Id = 0,
                Entry = "after-the-announcement",
            })));
        Assert.Equal(announced, host.Engine().Log.HeadLsn);
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
