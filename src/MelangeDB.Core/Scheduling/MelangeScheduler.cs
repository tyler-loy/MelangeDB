using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// Fires timer rows: a projection consumer over the tables declaring <c>Scheduled</c>, driven by
/// an initial scan at startup plus the engine's commit-observer seam — never by polling. All
/// pending fires live in memory and are rebuilt from current rows on restart; a repeating timer's
/// next fire derives from its interval, so an idle tick appends nothing to the log. Dispatch is a
/// single-threaded loop over one <see cref="TimeProvider"/> timer armed at the earliest due entry
/// — reducer transactions serialize on the engine's write lock regardless, so a worker pool would
/// buy nothing (see <c>Scheduler:MaxConcurrentTicks</c>). Its failure mode is deliberate: one
/// slow tick delays every other timer, made visible by <c>melange.scheduler.overruns</c>.
/// </summary>
public sealed class MelangeScheduler : ICommitObserver, IDisposable
{
    /// <summary>The identity scheduled fires run as — what <c>ctx.Caller</c> is inside a tick.</summary>
    public static Identity Caller { get; } = Identity.Hash("melange/scheduler");

    private readonly MelangeEngine _engine;
    private readonly MelangeReducerHost _host;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly Lock _lock = new();
    private readonly Dictionary<TableId, TimerTable> _tables = [];

    // The floor on a re-arm delay: the platform timer's practical resolution. Below this a
    // positive delay is re-armed to it, so an early wake cannot spin the fire/re-arm loop. See Rearm.
    private static readonly TimeSpan RearmFloor = TimeSpan.FromMilliseconds(16);
    private ITimer? _timer;
    private IDisposable? _reload;
    private int _processing;
    private bool _rescanRequested;

    /// <summary>Test-only: invoked at the top of each drain iteration; throwing exercises the fault path.</summary>
    internal Action? DrainFaultInjection { get; set; }

    /// <summary>Test-only: runs a drain the way the timer callback does, to exercise re-entrancy.</summary>
    internal void PumpForTest() => ProcessDueFires();
    private volatile bool _started;
    private volatile bool _stopped;

    public MelangeScheduler(
        MelangeEngine engine,
        MelangeReducerHost host,
        IOptionsMonitor<MelangeDbOptions> options,
        ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _host = host;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<MelangeScheduler>();
    }

    /// <summary>
    /// Builds the pending set from current timer rows, registers as a commit observer, and fires
    /// anything already due — including downtime catch-up per <c>Scheduler:CatchUpAfterDowntime</c>.
    /// Throws when a scheduled table names an unregistered reducer, so the omission fails startup
    /// rather than silently never ticking.
    /// </summary>
    internal void Start()
    {
        var scheduled = _engine.Schema.Tables.Where(table => table.Scheduled is not null).ToList();
        if (scheduled.Count == 0)
            return;
        foreach (var table in scheduled)
        {
            if (_host.Reducers.All(descriptor => descriptor.Name != table.Scheduled))
            {
                throw new InvalidOperationException(
                    $"Table '{table.Name}' schedules reducer '{table.Scheduled}', which is not registered. " +
                    "Register it with AddReducersFrom or remove the Scheduled declaration.");
            }
        }

        var catchUp = _options.CurrentValue.Scheduler.CatchUpAfterDowntime;
        var now = _time.GetUtcNow();
        var anchor = _engine.RecoveredTailTimestamp?.ToDateTimeOffset() ?? now;
        if (anchor > now)
            anchor = now;

        // Scan and observer registration happen under one write-lock hold, so no commit can slip
        // between the snapshot and the stream of observed records.
        _engine.ReadConsistent(_ =>
        {
            lock (_lock)
            {
                foreach (var schema in scheduled)
                {
                    var table = new TimerTable(schema, ScheduleAtIndexOf(schema));
                    _tables.Add(schema.Id, table);
                    foreach (var (key, row) in _engine.HotStore.Scan(schema.Id))
                        table.Entries[key] = RecoveredEntry(table, key, row.ToArray(), anchor, now, catchUp);
                }
            }

            _engine.AddCommitObserver(this);
        });

        // What the pending set looks like at start: how many timers each scheduled table
        // registered, and for the earliest, how far out its first fire is. A scheduled table that
        // scanned zero rows, or whose next fire is implausibly far out, is diagnosable here rather
        // than by inferring it from a reducer's side effects. Info for the summary, Debug for the
        // per-table and per-entry detail (enable MelangeDB's scheduler logger at Debug to get it).
        lock (_lock)
        {
            var totalTimers = 0;
            foreach (var table in _tables.Values)
            {
                totalTimers += table.Entries.Count;
                LogMessages.SchedulerTableRegistered(_logger, table.Schema.Name, table.Schema.Scheduled!, table.Entries.Count);
                foreach (var entry in table.Entries.Values)
                {
                    LogMessages.SchedulerEntryRegistered(
                        _logger,
                        table.Schema.Name,
                        (entry.Due - now).TotalMilliseconds,
                        entry.Interval?.TotalMilliseconds ?? -1,
                        entry.CatchUpRemaining);
                }
            }

            LogMessages.SchedulerStarted(_logger, totalTimers, _tables.Count, anchor, now);
        }

        _timer = _time.CreateTimer(
            static state => ((MelangeScheduler)state!).ProcessDueFires(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
        _started = true;
        _reload = _options.OnChange(_ => ProcessDueFires());
        ProcessDueFires();
    }

    /// <summary>Stops arming and firing. Pending timer rows are data; they survive to the next start.</summary>
    internal void Stop()
    {
        _stopped = true;
        _reload?.Dispose();
        _timer?.Dispose();
    }

    public void Dispose() => Stop();

    /// <summary>
    /// Maintains the pending set from committed timer-row ops — the same records every other
    /// projection consumes. Runs under the engine's write lock; it must stay cheap and never fire.
    /// </summary>
    public void OnCommit(CommitRecord record)
    {
        if (!_started || _stopped)
            return;
        var touched = false;
        lock (_lock)
        {
            foreach (var op in record.WriteSet)
            {
                if (!_tables.TryGetValue(op.Table, out var table))
                    continue;
                touched = true;
                if (op.Kind == RowOpKind.Delete)
                {
                    table.Entries.Remove(op.Key);
                    continue;
                }

                var row = op.Row.ToArray();
                var schedule = ReadScheduleAt(table.Schema, table.ScheduleAtIndex, row);
                var committedAt = record.Timestamp.ToDateTimeOffset();
                var due = schedule.IsInterval
                    ? committedAt + ClampInterval(schedule.Every)
                    : schedule.DueAt.ToDateTimeOffset();
                if (table.Entries.TryGetValue(op.Key, out var entry))
                {
                    entry.Row = row;
                    entry.Interval = schedule.IsInterval ? ClampInterval(schedule.Every) : null;
                    entry.Due = due;
                    entry.CatchUpRemaining = 0;
                    entry.Generation++;
                }
                else
                {
                    table.Entries[op.Key] = new TimerEntry(op.Key, row)
                    {
                        Interval = schedule.IsInterval ? ClampInterval(schedule.Every) : null,
                        Due = due,
                    };
                }
            }
        }

        if (touched)
            Rearm();
    }

    /// <summary>
    /// The dispatch loop: repeatedly fire the earliest due entry until none remain, then re-arm.
    /// Reentrant invocations (a fire advancing a manual clock, an options reload mid-loop) fold
    /// into the running loop, which re-scans after every fire.
    /// </summary>
    /// <summary>
    /// The dispatch loop: fire every due entry, then re-arm the timer at the next earliest.
    /// <para>
    /// Only one drain runs at a time, and it never loses a wake. Every caller (the timer callback,
    /// an options reload, a start) requests a scan; whoever wins the guard drains and keeps draining
    /// while any request arrived while it was working. A caller that finds the guard already held
    /// does <b>not</b> just return and drop its re-arm — it leaves the request flag set, and the
    /// in-flight drainer picks it up before it exits. That closes the bug behind the reference
    /// workload's idle stall: the bare guard dropped the re-entrant caller's re-arm, and on a world
    /// that was committing (players online) the very next commit's <see cref="OnCommit"/> re-armed
    /// within milliseconds, hiding it — but on an <em>idle</em> world, where the only commits come
    /// from scheduled ticks, the thing that would re-arm the scheduler is the tick the scheduler
    /// just failed to run, so it stayed dark until some incidental commit, firing erratically for
    /// tens of seconds at a time with the process otherwise idle.
    /// </para>
    /// </summary>
    private void ProcessDueFires()
    {
        if (!_started || _stopped)
            return;

        // Request a scan, then drain while requests keep arriving. The flag is set before the guard
        // is tested, so a caller that loses the guard has already recorded its request for the
        // winner to honour.
        Volatile.Write(ref _rescanRequested, true);
        while (Volatile.Read(ref _rescanRequested))
        {
            if (Interlocked.Exchange(ref _processing, 1) == 1)
            {
                // Another drain holds the guard; it will see the flag we set above before it exits,
                // so this request is not lost. (This is the ~per-minute re-entry the stall traced to.)
                LogMessages.SchedulerDrainReentered(_logger);
                return;
            }

            try
            {
                Volatile.Write(ref _rescanRequested, false);
                try
                {
                    Drain();
                }
                catch (Exception exception)
                {
                    // One faulting drain must not wedge every timer, and must not skip the re-arm
                    // below. It also must not crash the process: the timer callback (see Start) is a
                    // bare delegate with no handler of its own, so an exception propagating out of
                    // here would terminate the process rather than fail one tick. Caught, logged,
                    // and re-armed. Latent-safe: the fire path contains its own telemetry, so a
                    // drain reaches here only on something genuinely unexpected.
                    LogMessages.SchedulerDrainFaulted(_logger, exception);
                }

                Rearm();
            }
            finally
            {
                Interlocked.Exchange(ref _processing, 0);
            }

            // Loop: if a caller set the flag while we were draining or re-arming, drain again.
        }
    }

    /// <summary>Fires every currently-due entry, earliest first, until none remain.</summary>
    private void Drain()
    {
        DrainFaultInjection?.Invoke();
        while (!_stopped)
        {
            var options = _options.CurrentValue.Scheduler;
            if (!options.Enabled)
                break;
            TimerTable? dueTable = null;
            TimerEntry? dueEntry = null;
            lock (_lock)
            {
                var now = _time.GetUtcNow();
                foreach (var table in _tables.Values)
                {
                    foreach (var entry in table.Entries.Values)
                    {
                        if (entry.Due <= now && (dueEntry is null || entry.Due < dueEntry.Due))
                        {
                            dueTable = table;
                            dueEntry = entry;
                        }
                    }
                }
            }

            if (dueEntry is null)
            {
                LogMessages.SchedulerNothingDue(_logger, EarliestDelayMillis());
                break;
            }

            LogMessages.SchedulerFiring(_logger, dueTable!.Schema.Name);

            // Never fire while holding the state lock: the fire takes the engine's write lock, and
            // committing threads inside that lock call OnCommit, which takes the state lock —
            // holding both here would be a lock-order inversion.
            Fire(dueTable!, dueEntry, options.OverrunPolicy);
        }
    }

    private void Fire(TimerTable table, TimerEntry entry, SchedulerOverrunPolicy policy)
    {
        var generation = entry.Generation;
        var reducerName = table.Schema.Scheduled!;

        // Every telemetry interaction is contained, so none can propagate out of the fire, skip the
        // reschedule (leaving the timer's Due un-advanced, which would then re-fire in a hot loop)
        // or fault the drain. Telemetry must never affect dispatch — a metric or span export that
        // throws (an OTEL exporter in a bad state, a meter disposed under load) is swallowed and
        // logged (EventId 1312), and the reschedule always runs.
        Activity? tick = null;
        SafeTelemetry(() => tick = _engine.Telemetry?.StartSchedulerTick(reducerName));
        var started = Stopwatch.GetTimestamp();
        var failed = false;
        try
        {
            try
            {
                _host.CallScheduled(table.Schema, entry.Key, entry.Row, deleteOnFire: entry.Interval is null);
            }
            catch (Exception exception)
            {
                failed = true;
                SafeTelemetry(() => tick?.SetStatus(ActivityStatusCode.Error, exception.Message));
                LogMessages.TickFailed(_logger, reducerName, exception);
            }

            SafeTelemetry(() => _engine.Telemetry?.RecordSchedulerTick(reducerName, Stopwatch.GetElapsedTime(started).TotalMilliseconds));

            lock (_lock)
            {
                // A successful one-shot removed its own row — the observer already dropped the entry.
                // A commit during the fire that rewrote this timer bumped the generation and owns the
                // schedule now. Either way there is nothing left to reschedule here.
                if (!table.Entries.TryGetValue(entry.Key, out var current)
                    || !ReferenceEquals(current, entry)
                    || current.Generation != generation)
                {
                    return;
                }

                if (entry.Interval is not { } interval)
                {
                    // A one-shot whose fire aborted: the row survives (nothing committed), but
                    // retrying on a hot loop would re-fail forever. Drop it from the pending set;
                    // the row is still data, so a restart re-arms it.
                    table.Entries.Remove(entry.Key);
                    return;
                }

                var now = _time.GetUtcNow();
                if (!failed && entry.CatchUpRemaining > 0)
                {
                    entry.CatchUpRemaining--;
                    entry.Due = now;
                    return;
                }

                entry.CatchUpRemaining = 0;
                var next = entry.Due + interval;
                if (next <= now)
                {
                    var missed = (now - entry.Due).Ticks / interval.Ticks;
                    SafeTelemetry(() => _engine.Telemetry?.RecordSchedulerOverrun(reducerName));
                    LogMessages.Overrun(_logger, reducerName, missed, policy);
                    entry.Due = policy switch
                    {
                        SchedulerOverrunPolicy.RunImmediately => next,
                        SchedulerOverrunPolicy.Coalesce => now,
                        _ => now + interval,
                    };
                }
                else
                {
                    entry.Due = next;
                }

                LogMessages.SchedulerRescheduled(_logger, reducerName, (entry.Due - now).TotalMilliseconds, interval.TotalMilliseconds);
            }
        }
        finally
        {
            SafeTelemetry(() => tick?.Dispose());
        }
    }

    /// <summary>Runs one telemetry interaction, swallowing and logging any fault so it never reaches dispatch.</summary>
    private void SafeTelemetry(Action telemetry)
    {
        try
        {
            telemetry();
        }
        catch (Exception exception)
        {
            LogMessages.SchedulerTelemetryFaulted(_logger, exception);
        }
    }

    private void Rearm()
    {
        if (_stopped)
            return;

        // While disabled the timer stays unarmed — re-arming an overdue entry would spin the
        // fire/skip loop. The options-reload subscription re-arms when the scheduler comes back.
        if (!_options.CurrentValue.Scheduler.Enabled)
            return;
        lock (_lock)
        {
            DateTimeOffset? earliest = null;
            foreach (var table in _tables.Values)
            {
                foreach (var entry in table.Entries.Values)
                {
                    if (earliest is null || entry.Due < earliest)
                        earliest = entry.Due;
                }
            }

            if (earliest is null)
                return;
            var delay = earliest.Value - _time.GetUtcNow();
            if (delay < TimeSpan.Zero)
                delay = TimeSpan.Zero;
            else if (delay > TimeSpan.Zero && delay < RearmFloor)
            {
                // Never re-arm for a sub-resolution residual. A platform timer routinely wakes a
                // fraction of a millisecond before the delay it was given; the drain then finds
                // nothing due (the entry's time has not quite arrived) and, without this floor,
                // re-arms on the tiny remainder — waking early again on a smaller remainder, tens
                // of times a second, until the due time finally passes. Waiting at least the
                // timer's own resolution guarantees the next wake lands at or after the due time,
                // so it fires rather than spinning. A fire is therefore at most one resolution
                // late, which is nothing against any real interval.
                delay = RearmFloor;
            }

            LogMessages.SchedulerRearmed(_logger, delay.TotalMilliseconds);
            _timer?.Change(delay, Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Milliseconds until the earliest pending fire, or <c>-1</c> when nothing is pending. Caller holds <see cref="_lock"/>.</summary>
    private double EarliestDelayMillis()
    {
        DateTimeOffset? earliest = null;
        foreach (var table in _tables.Values)
        {
            foreach (var entry in table.Entries.Values)
            {
                if (earliest is null || entry.Due < earliest)
                    earliest = entry.Due;
            }
        }

        return earliest is null ? -1 : (earliest.Value - _time.GetUtcNow()).TotalMilliseconds;
    }

    private TimerEntry RecoveredEntry(
        TimerTable table,
        RowKey key,
        byte[] row,
        DateTimeOffset anchor,
        DateTimeOffset now,
        SchedulerCatchUp catchUp)
    {
        var schedule = ReadScheduleAt(table.Schema, table.ScheduleAtIndex, row);
        if (!schedule.IsInterval)
        {
            // An overdue instant simply fires promptly; firing once is what a one-shot means.
            return new TimerEntry(key, row) { Due = schedule.DueAt.ToDateTimeOffset() };
        }

        // Repeating timers persist no per-fire bookkeeping, so downtime is measured from the
        // recovered log's tail: the moment the world last moved. Not overdue means the cadence
        // resumes from that anchor; overdue fires once (FireOnce) or once per missed interval
        // (CatchUpAll), then resumes from now.
        var interval = ClampInterval(schedule.Every);
        var due = anchor + interval;
        var catchUpRemaining = 0;
        if (due <= now)
        {
            var missed = 1 + ((now - due).Ticks / interval.Ticks);
            var fires = catchUp == SchedulerCatchUp.CatchUpAll ? missed : 1;
            due = now;
            catchUpRemaining = (int)Math.Min(fires - 1, int.MaxValue);
        }

        return new TimerEntry(key, row)
        {
            Interval = interval,
            Due = due,
            CatchUpRemaining = catchUpRemaining,
        };
    }

    private static int ScheduleAtIndexOf(TableSchema schema)
    {
        for (var i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Kind == ColumnKind.ScheduleAt)
                return i;
        }

        throw new InvalidOperationException($"Table '{schema.Name}' has no ScheduleAt column.");
    }

    private static ScheduleAt ReadScheduleAt(TableSchema schema, int columnIndex, ReadOnlySpan<byte> row)
    {
        var reader = new RowReader(row);
        for (var i = 0; i < columnIndex; i++)
        {
            switch (schema.Columns[i].Kind)
            {
                case ColumnKind.Bool or ColumnKind.Int8 or ColumnKind.UInt8:
                    _ = reader.ReadUInt8();
                    break;
                case ColumnKind.Int16 or ColumnKind.UInt16:
                    _ = reader.ReadUInt16();
                    break;
                case ColumnKind.Int32 or ColumnKind.UInt32 or ColumnKind.Float32:
                    _ = reader.ReadUInt32();
                    break;
                case ColumnKind.Int64 or ColumnKind.UInt64 or ColumnKind.Float64 or ColumnKind.Timestamp:
                    _ = reader.ReadUInt64();
                    break;
                case ColumnKind.Identity:
                    _ = reader.ReadIdentity();
                    break;
                case ColumnKind.String:
                    _ = reader.ReadString();
                    break;
                case ColumnKind.Bytes:
                    _ = reader.ReadBytes();
                    break;
                case ColumnKind.ScheduleAt:
                    _ = reader.ReadScheduleAt();
                    break;
                default:
                    // Every ColumnKind must advance the reader, or the columns after it read from
                    // the wrong offset and the ScheduleAt at the end decodes as garbage — silently,
                    // with no fire and no error. A kind this switch does not handle (a future 17th)
                    // must fail loudly here rather than mis-time every timer on the table.
                    throw new NotSupportedException(
                        $"Table '{schema.Name}': column '{schema.Columns[i].Name}' has kind {schema.Columns[i].Kind}, "
                        + "which the scheduler's ScheduleAt walk does not handle; the timer cadence cannot be read.");
            }
        }

        return reader.ReadScheduleAt();
    }

    private static TimeSpan ClampInterval(TimeSpan every) =>
        every > TimeSpan.Zero ? every : TimeSpan.FromMilliseconds(1);

    private sealed class TimerTable(TableSchema schema, int scheduleAtIndex)
    {
        public TableSchema Schema { get; } = schema;

        public int ScheduleAtIndex { get; } = scheduleAtIndex;

        public Dictionary<RowKey, TimerEntry> Entries { get; } = [];
    }

    private sealed class TimerEntry(RowKey key, byte[] row)
    {
        public RowKey Key { get; } = key;

        public byte[] Row { get; set; } = row;

        public TimeSpan? Interval { get; set; }

        public DateTimeOffset Due { get; set; }

        public int CatchUpRemaining { get; set; }

        public int Generation { get; set; }
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, long, SchedulerOverrunPolicy, Exception?> OverrunMessage =
            LoggerMessage.Define<string, long, SchedulerOverrunPolicy>(
                LogLevel.Warning,
                new EventId(1301, "SchedulerOverrun"),
                "Scheduled reducer '{Reducer}' overran its interval by {Missed} fire(s); applying Scheduler:OverrunPolicy {Policy}.");

        public static void Overrun(ILogger logger, string reducer, long missed, SchedulerOverrunPolicy policy) =>
            OverrunMessage(logger, reducer, missed, policy, null);

        private static readonly Action<ILogger, string, Exception?> TickFailedMessage =
            LoggerMessage.Define<string>(
                LogLevel.Error,
                new EventId(1302, "SchedulerTickFailed"),
                "Scheduled reducer '{Reducer}' threw; the tick aborted with nothing appended.");

        public static void TickFailed(ILogger logger, string reducer, Exception failure) =>
            TickFailedMessage(logger, reducer, failure);

        private static readonly Action<ILogger, int, int, DateTimeOffset, DateTimeOffset, Exception?> SchedulerStartedMessage =
            LoggerMessage.Define<int, int, DateTimeOffset, DateTimeOffset>(
                LogLevel.Information,
                new EventId(1303, "SchedulerStarted"),
                "Scheduler started: {Timers} timer(s) across {Tables} scheduled table(s); downtime anchor {Anchor}, now {Now}.");

        public static void SchedulerStarted(ILogger logger, int timers, int tables, DateTimeOffset anchor, DateTimeOffset now) =>
            SchedulerStartedMessage(logger, timers, tables, anchor, now, null);

        private static readonly Action<ILogger, string, string, int, Exception?> SchedulerTableRegisteredMessage =
            LoggerMessage.Define<string, string, int>(
                LogLevel.Debug,
                new EventId(1304, "SchedulerTableRegistered"),
                "Scheduled table '{Table}' (reducer '{Reducer}') registered {Timers} timer(s) at start.");

        public static void SchedulerTableRegistered(ILogger logger, string table, string reducer, int timers) =>
            SchedulerTableRegisteredMessage(logger, table, reducer, timers, null);

        private static readonly Action<ILogger, string, double, double, int, Exception?> SchedulerEntryRegisteredMessage =
            LoggerMessage.Define<string, double, double, int>(
                LogLevel.Debug,
                new EventId(1305, "SchedulerEntryRegistered"),
                "Timer on '{Table}': first fire in {DueInMs}ms, interval {IntervalMs}ms (-1 = one-shot), catch-up {CatchUp}.");

        public static void SchedulerEntryRegistered(ILogger logger, string table, double dueInMs, double intervalMs, int catchUp) =>
            SchedulerEntryRegisteredMessage(logger, table, dueInMs, intervalMs, catchUp, null);

        private static readonly Action<ILogger, string, Exception?> SchedulerFiringMessage =
            LoggerMessage.Define<string>(
                LogLevel.Debug,
                new EventId(1306, "SchedulerFiring"),
                "Scheduler firing the due timer on '{Table}'.");

        public static void SchedulerFiring(ILogger logger, string table) =>
            SchedulerFiringMessage(logger, table, null);

        private static readonly Action<ILogger, double, Exception?> SchedulerNothingDueMessage =
            LoggerMessage.Define<double>(
                LogLevel.Debug,
                new EventId(1307, "SchedulerNothingDue"),
                "Scheduler drain found nothing due; earliest pending fire in {EarliestMs}ms (-1 = no timers pending).");

        public static void SchedulerNothingDue(ILogger logger, double earliestMs) =>
            SchedulerNothingDueMessage(logger, earliestMs, null);

        private static readonly Action<ILogger, double, Exception?> SchedulerRearmedMessage =
            LoggerMessage.Define<double>(
                LogLevel.Debug,
                new EventId(1308, "SchedulerRearmed"),
                "Scheduler re-armed its timer to fire in {DelayMs}ms.");

        public static void SchedulerRearmed(ILogger logger, double delayMs) =>
            SchedulerRearmedMessage(logger, delayMs, null);

        private static readonly Action<ILogger, string, double, double, Exception?> SchedulerRescheduledMessage =
            LoggerMessage.Define<string, double, double>(
                LogLevel.Debug,
                new EventId(1309, "SchedulerRescheduled"),
                "Timer '{Reducer}' rescheduled its next fire to {NextInMs}ms out (interval {IntervalMs}ms).");

        public static void SchedulerRescheduled(ILogger logger, string reducer, double nextInMs, double intervalMs) =>
            SchedulerRescheduledMessage(logger, reducer, nextInMs, intervalMs, null);

        private static readonly Action<ILogger, Exception?> SchedulerDrainReenteredMessage =
            LoggerMessage.Define(
                LogLevel.Debug,
                new EventId(1310, "SchedulerDrainReentered"),
                "A scheduler drain re-entered while one was already running and returned; the in-flight drain will re-arm.");

        public static void SchedulerDrainReentered(ILogger logger) =>
            SchedulerDrainReenteredMessage(logger, null);

        private static readonly Action<ILogger, Exception?> SchedulerDrainFaultedMessage =
            LoggerMessage.Define(
                LogLevel.Error,
                new EventId(1311, "SchedulerDrainFaulted"),
                "A scheduler drain threw; it was swallowed and the timer re-armed so one bad tick does not wedge every timer. See the exception.");

        public static void SchedulerDrainFaulted(ILogger logger, Exception exception) =>
            SchedulerDrainFaultedMessage(logger, exception);

        private static readonly Action<ILogger, Exception?> SchedulerTelemetryFaultedMessage =
            LoggerMessage.Define(
                LogLevel.Error,
                new EventId(1312, "SchedulerTelemetryFaulted"),
                "A scheduler telemetry call threw and was ignored so it could not affect dispatch. See the exception.");

        public static void SchedulerTelemetryFaulted(ILogger logger, Exception exception) =>
            SchedulerTelemetryFaultedMessage(logger, exception);
    }
}
