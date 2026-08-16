using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace MelangeDB.Core;

/// <summary>
/// MelangeDB's built-in signals: one <see cref="ActivitySource"/> and one <see cref="Meter"/>, both
/// named <c>MelangeDB</c>, with zero telemetry package references — both types live in the
/// framework, and the host chooses exporters. Names and cardinality rules are contract; see
/// docs/OBSERVABILITY.md. Caller identity goes on spans, never on metric dimensions.
/// </summary>
internal sealed class EngineTelemetry : IDisposable
{
    /// <summary>The one source name. Hosts subscribe with <c>AddSource("MelangeDB")</c> / <c>AddMeter("MelangeDB")</c>.</summary>
    public const string SourceName = "MelangeDB";

    private static readonly ActivitySource Source = new(SourceName);

    private readonly TelemetryOptions _options;
    private readonly Meter _meter;
    private readonly Counter<long> _transactions;
    private readonly Histogram<double> _reducerDuration;
    private readonly Histogram<double> _commitDuration;
    private readonly Histogram<double> _fsyncDuration;
    private readonly Histogram<long> _groupCommitBatch;
    private readonly Histogram<long> _writeSetRows;
    private readonly Counter<long> _rateLimited;
    private readonly Counter<long> _schedulerOverruns;
    private readonly Histogram<double> _schedulerTickDuration;
    private readonly Counter<long> _deadLettered;
    private Func<long>? _eventQueueDepth;
    private Func<HotStoreStatistics>? _storeStatistics;

    public EngineTelemetry(TelemetryOptions options, Func<ulong> headLsn, Func<IEnumerable<(string Applier, long Lag)>> applierLags)
    {
        _options = options;
        _meter = new Meter(SourceName);
        _transactions = _meter.CreateCounter<long>("melange.transactions", "{tx}", "Committed, aborted, and rejected transactions.");
        _reducerDuration = _meter.CreateHistogram<double>("melange.reducer.duration", "ms", "Reducer body plus commit duration.");
        _commitDuration = _meter.CreateHistogram<double>("melange.commit.duration", "ms", "Log append duration; the durability wait is melange.fsync.duration's.");
        _fsyncDuration = _meter.CreateHistogram<double>("melange.fsync.duration", "ms", "Durability flush duration.");
        _groupCommitBatch = _meter.CreateHistogram<long>("melange.log.group_commit.batch_size", "{record}", "Records made durable per fsync under OnCommit — the distribution's shape is group commit working.");
        _writeSetRows = _meter.CreateHistogram<long>("melange.writeset.rows", "{row}", "Collapsed row ops per transaction.");
        _rateLimited = _meter.CreateCounter<long>("melange.ratelimit.rejected", "{call}", "Client reducer calls rejected by the rate limiter before any transaction opened.");
        _schedulerOverruns = _meter.CreateCounter<long>("melange.scheduler.overruns", "{tick}", "Ticks that ran past their timer's interval — the death-spiral early warning.");
        _schedulerTickDuration = _meter.CreateHistogram<double>("melange.scheduler.tick.duration", "ms", "Scheduled fire duration, dispatch and transaction included.");
        _deadLettered = _meter.CreateCounter<long>("melange.events.deadlettered", "{event}", "Events whose delivery exhausted its retries and was recorded to the dead-letter path.");
        _meter.CreateObservableGauge("melange.events.queue_depth", () => _eventQueueDepth?.Invoke() ?? 0L, "{event}", "Events held in the bus's in-memory delivery window.");
        _meter.CreateObservableGauge("melange.log.head_lsn", () => (long)headLsn(), "{lsn}", "LSN of the newest log record.");
        _meter.CreateObservableGauge(
            "melange.applier.lag",
            () => applierLags().Select(l => new Measurement<long>(l.Lag, new KeyValuePair<string, object?>("applier", l.Applier))),
            "{tx}",
            "Transactions between the log head and each applier's checkpoint.");
        _meter.CreateObservableGauge(
            "melange.store.resident_bytes",
            () => StoreMeasurements(static t => t.ResidentBytes),
            "By",
            "Managed bytes each table pins in memory — full rows for resident tables, bookkeeping for paged ones.");
        _meter.CreateObservableCounter(
            "melange.store.page_faults",
            () => StoreMeasurements(static t => t.PageFaults),
            "{fault}",
            "Row reads served from disk instead of the buffer pool.");
        _meter.CreateObservableCounter(
            "melange.store.scan_rows",
            () => StoreMeasurements(static t => t.RowsScanned),
            "{row}",
            "Rows returned by full table scans.");
    }

    private IEnumerable<Measurement<long>> StoreMeasurements(Func<HotStoreTableStatistics, long> select)
    {
        if (_storeStatistics is null)
            yield break;
        foreach (var table in _storeStatistics().Tables)
            yield return new Measurement<long>(select(table), new KeyValuePair<string, object?>("table", table.Name));
    }

    /// <summary>Wires the hot store's per-table statistics into the <c>melange.store.*</c> instruments.</summary>
    public void SetHotStoreStatisticsProvider(Func<HotStoreStatistics> provider) => _storeStatistics = provider;

    public Activity? StartReducer(
        string reducerName,
        Identity caller,
        IReadOnlyList<object?>? arguments,
        ReadOnlyMemory<byte> encodedArguments,
        ActivityContext parentContext = default)
    {
        // A transport-propagated traceparent parents the reducer span directly, so a click in a
        // game client and the server-side transaction are one trace.
        var activity = parentContext == default
            ? Source.StartActivity("melange.reducer")
            : Source.StartActivity("melange.reducer", ActivityKind.Server, parentContext);
        if (activity is null)
            return null;
        activity.SetTag("melange.reducer.name", reducerName);
        if (_options.IncludeCallerIdentity)
            activity.SetTag("melange.caller", caller.ToString());
        if (_options.IncludeReducerArguments)
        {
            // In-process calls carry boxed values worth formatting; encoded dispatch carries the
            // wire payload, tagged as bounded hex so the opt-in still works for real traffic.
            if (arguments is { Count: > 0 })
                activity.SetTag("melange.reducer.args", string.Join(", ", arguments.Select(a => a?.ToString() ?? "null")));
            else if (!encodedArguments.IsEmpty)
                activity.SetTag("melange.reducer.args", FormatEncodedArguments(encodedArguments.Span));
        }

        return activity;
    }

    private static string FormatEncodedArguments(ReadOnlySpan<byte> encoded)
    {
        const int capBytes = 256;
        return encoded.Length <= capBytes
            ? Convert.ToHexStringLower(encoded)
            : $"{Convert.ToHexStringLower(encoded[..capBytes])}… ({encoded.Length} bytes)";
    }

    public Activity? StartCommit() => Source.StartActivity("melange.commit");

    public Activity? StartFsync() => Source.StartActivity("melange.fsync");

    public Activity? StartApply(string applier)
    {
        var activity = Source.StartActivity("melange.apply");
        activity?.SetTag("melange.applier", applier);
        return activity;
    }

    public void RecordTransaction(string reducerName, string outcome, double durationMs, int writeSetRows)
    {
        var reducerTag = new KeyValuePair<string, object?>("reducer", reducerName);
        _transactions.Add(1, reducerTag, new KeyValuePair<string, object?>("outcome", outcome));
        _reducerDuration.Record(durationMs, reducerTag);
        _writeSetRows.Record(writeSetRows, reducerTag);
    }

    public void RecordRateLimited(string reducerName) =>
        _rateLimited.Add(1, new KeyValuePair<string, object?>("reducer", reducerName));

    /// <summary>
    /// Starts a <c>melange.scheduler.tick</c> span. A tick has no client parent, so it starts a
    /// new trace; any ambient activity is detached first so the root is unconditional.
    /// </summary>
    public Activity? StartSchedulerTick(string reducerName)
    {
        if (Activity.Current is not null)
            Activity.Current = null;
        var activity = Source.StartActivity("melange.scheduler.tick");
        activity?.SetTag("melange.reducer.name", reducerName);
        return activity;
    }

    public void RecordSchedulerOverrun(string reducerName) =>
        _schedulerOverruns.Add(1, new KeyValuePair<string, object?>("reducer", reducerName));

    public void RecordSchedulerTick(string reducerName, double durationMs) =>
        _schedulerTickDuration.Record(durationMs, new KeyValuePair<string, object?>("reducer", reducerName));

    /// <summary>Wires the delivery window's depth into the <c>melange.events.queue_depth</c> gauge.</summary>
    public void SetEventQueueDepthProvider(Func<long> provider) => _eventQueueDepth = provider;

    /// <summary>
    /// Starts a <c>melange.event.handle</c> span. A handler runs after — possibly long after — the
    /// emitting transaction, so its span is a new trace <em>linked</em> to the emitter, never
    /// parented under it: a child span would distort the reducer's duration and produce traces
    /// that never close.
    /// </summary>
    public Activity? StartEventHandle(string eventType, string handler, ActivityContext emitterContext)
    {
        if (Activity.Current is not null)
            Activity.Current = null;
        var links = emitterContext == default ? null : new[] { new ActivityLink(emitterContext) };
        var activity = Source.StartActivity("melange.event.handle", ActivityKind.Internal, default(ActivityContext), links: links);
        activity?.SetTag("melange.event.type", eventType);
        activity?.SetTag("melange.handler", handler);
        return activity;
    }

    public void RecordDeadLettered(string eventType) =>
        _deadLettered.Add(1, new KeyValuePair<string, object?>("event_type", eventType));

    public void RecordCommitDuration(double durationMs) => _commitDuration.Record(durationMs);

    public void RecordFsyncDuration(double durationMs) => _fsyncDuration.Record(durationMs);

    public void RecordGroupCommitBatch(long records) => _groupCommitBatch.Record(records);

    public void Dispose() => _meter.Dispose();
}
