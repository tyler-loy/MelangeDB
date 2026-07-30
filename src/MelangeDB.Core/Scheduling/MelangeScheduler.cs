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
    private ITimer? _timer;
    private IDisposable? _reload;
    private int _processing;
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
    private void ProcessDueFires()
    {
        if (!_started || _stopped)
            return;
        if (Interlocked.Exchange(ref _processing, 1) == 1)
            return;
        try
        {
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
                    break;

                // Never fire while holding the state lock: the fire takes the engine's write
                // lock, and committing threads inside that lock call OnCommit, which takes the
                // state lock — holding both here would be a lock-order inversion.
                Fire(dueTable!, dueEntry, options.OverrunPolicy);
            }
        }
        finally
        {
            Interlocked.Exchange(ref _processing, 0);
        }

        Rearm();
    }

    private void Fire(TimerTable table, TimerEntry entry, SchedulerOverrunPolicy policy)
    {
        var generation = entry.Generation;
        var reducerName = table.Schema.Scheduled!;
        using var tick = _engine.Telemetry?.StartSchedulerTick(reducerName);
        var started = Stopwatch.GetTimestamp();
        var failed = false;
        try
        {
            _host.CallScheduled(table.Schema, entry.Key, entry.Row, deleteOnFire: entry.Interval is null);
        }
        catch (Exception exception)
        {
            failed = true;
            tick?.SetStatus(ActivityStatusCode.Error, exception.Message);
            LogMessages.TickFailed(_logger, reducerName, exception);
        }

        _engine.Telemetry?.RecordSchedulerTick(reducerName, Stopwatch.GetElapsedTime(started).TotalMilliseconds);

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
                _engine.Telemetry?.RecordSchedulerOverrun(reducerName);
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
            _timer?.Change(delay, Timeout.InfiniteTimeSpan);
        }
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
    }
}
