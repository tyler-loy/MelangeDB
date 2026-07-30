using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>Shared runtime state between the hosted service and the health checks.</summary>
internal sealed class MelangeDbRuntimeState
{
    private volatile bool _started;

    public bool Started
    {
        get => _started;
        set => _started = value;
    }
}

/// <summary>
/// Owns the engine's lifetime inside a generic host. Startup constructs the engine — which opens
/// the commit log, replays it into the projections, and recovers the AutoInc sequences — and wires
/// the options-reload bridge so live keys take effect without a restart. Shutdown stops accepting
/// calls, drains the in-flight invocation, advances every applier to the log head, and flushes.
/// </summary>
internal sealed partial class MelangeDbHostedService : IHostedService
{
    private readonly IServiceProvider _provider;
    private readonly MelangeDbRuntimeState _state;
    private readonly IOptionsMonitor<MelangeDbOptions> _monitor;
    private readonly ILogger<MelangeDbHostedService> _logger;
    private MelangeEngine? _engine;
    private MelangeScheduler? _scheduler;
    private MelangeEventBus? _eventBus;
    private IDisposable? _reloadBridge;

    public MelangeDbHostedService(
        IServiceProvider provider,
        MelangeDbRuntimeState state,
        IOptionsMonitor<MelangeDbOptions> monitor,
        ILogger<MelangeDbHostedService> logger)
    {
        _provider = provider;
        _state = state;
        _monitor = monitor;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Resolving the engine is startup: its constructor runs log recovery and projection rebuild.
        var engine = (MelangeEngine)_provider.GetService(typeof(MelangeEngine))!;
        _engine = engine;
        _reloadBridge = _monitor.OnChange(next =>
        {
            CopyLiveKeys(next, engine.Options);
            ApplyResidencyOverrides(next, engine.Options, engine);
        });
        ReportUnpolicedReducers();

        // The event bus starts before the scheduler so a recovered timer's first fire can already
        // publish; its subscribers catch up from their checkpoints against the recovered log.
        _eventBus = (MelangeEventBus?)_provider.GetService(typeof(MelangeEventBus));
        _eventBus?.Start();

        // Log truncation must never pass the slowest live event subscriber; the bus's minimum
        // checkpoint is registered as a truncation floor the snapshot path consults.
        if (_eventBus is { } bus)
            engine.AddTruncationFloor(() => bus.MinimumLiveCheckpointLsn);

        // Scheduling starts only after recovery finished: the pending set is rebuilt from the
        // recovered timer rows, and overdue timers fire per Scheduler:CatchUpAfterDowntime.
        _scheduler = (MelangeScheduler?)_provider.GetService(typeof(MelangeScheduler));
        _scheduler?.Start();

        // A relational table with no relational tier is a projection that will never exist. The
        // rows still live in the hot store, so nothing is lost — but the declared intent isn't
        // being served, and silence here would look exactly like a working deployment.
        if (string.IsNullOrEmpty(_monitor.CurrentValue.Postgres.ConnectionString))
        {
            var relational = engine.Schema.Tables.Where(t => t.Tier == StorageTier.Relational).Select(t => t.Name).ToList();
            if (relational.Count > 0)
                LogRelationalWithoutPostgres(_logger, relational.Count, string.Join(", ", relational));
        }

        _state.Started = true;
        LogStarted(_logger, engine.Log.HeadLsn, engine.Schema.Tables.Count);
        return Task.CompletedTask;
    }

    /// <summary>
    /// The unpoliced-reducer report: every client-callable reducer with no authorization policy,
    /// as a startup artifact rather than a code-review question. <c>Warn</c> logs the list;
    /// <c>Fail</c> refuses to start.
    /// </summary>
    private void ReportUnpolicedReducers()
    {
        var mode = _monitor.CurrentValue.Policies.UnpolicedReducerReport;
        if (mode == UnpolicedReducerReport.Off)
            return;
        var host = (MelangeReducerHost?)_provider.GetService(typeof(MelangeReducerHost));
        if (host is null || host.UnpolicedReducers is not { Count: > 0 } unpoliced)
            return;

        var list = string.Join(", ", unpoliced);
        if (mode == UnpolicedReducerReport.Fail)
        {
            throw new InvalidOperationException(
                $"Policies:UnpolicedReducerReport is Fail and {unpoliced.Count} client-callable reducer(s) declare no " +
                $"authorization policy: {list}. Attach [Reducer(Policy = typeof(...))] to each, or lower the report to Warn.");
        }

        LogUnpolicedReducers(_logger, unpoliced.Count, list);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _reloadBridge?.Dispose();
        _scheduler?.Stop();

        // The bus stops before reducer dispatch closes, so an in-flight handler that calls a
        // reducer still can; an event mid-delivery redelivers from its checkpoint next start.
        _eventBus?.Stop();
        if (_engine is { } engine)
        {
            ((MelangeReducerHost?)_provider.GetService(typeof(MelangeReducerHost)))?.SignalStopping();
            engine.Drain();
            engine.Checkpoint();
            LogStopped(_logger, engine.Log.HeadLsn);
        }

        _state.Started = false;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Copies the live-reloadable keys from a freshly bound options instance onto the instance the
    /// engine reads per operation. Restart-only keys (paths, Telemetry:Enabled) are deliberately
    /// not copied; see docs/CONFIGURATION.md for each key's reload semantic.
    /// </summary>
    private static void CopyLiveKeys(MelangeDbOptions next, MelangeDbOptions live)
    {
        live.CommitLog.FsyncPolicy = next.CommitLog.FsyncPolicy;
        live.CommitLog.FsyncIntervalMs = next.CommitLog.FsyncIntervalMs;
        live.Telemetry.IncludeCallerIdentity = next.Telemetry.IncludeCallerIdentity;
        live.Telemetry.IncludeReducerArguments = next.Telemetry.IncludeReducerArguments;
        live.Telemetry.SlowReducerMs = next.Telemetry.SlowReducerMs;
        live.Telemetry.DeltaSpanSampleRatio = next.Telemetry.DeltaSpanSampleRatio;
        live.Transport.MaxMessageBytes = next.Transport.MaxMessageBytes;
        live.Transport.HeartbeatIntervalMs = next.Transport.HeartbeatIntervalMs;
        live.Transport.HeartbeatTimeoutMs = next.Transport.HeartbeatTimeoutMs;
        live.Transport.MaxInitialSetChunkBytes = next.Transport.MaxInitialSetChunkBytes;
        live.Resume.RetentionWindowSeconds = next.Resume.RetentionWindowSeconds;
        live.Subscriptions.MaxPerConnection = next.Subscriptions.MaxPerConnection;
        live.Subscriptions.BackpressurePolicy = next.Subscriptions.BackpressurePolicy;
        live.Subscriptions.MaxBufferedBytes = next.Subscriptions.MaxBufferedBytes;
        live.Subscriptions.MaxRowsPerSubscription = next.Subscriptions.MaxRowsPerSubscription;
        live.Subscriptions.MaxBytesPerSubscription = next.Subscriptions.MaxBytesPerSubscription;
        live.Subscriptions.MaxRangeSpan = next.Subscriptions.MaxRangeSpan;
        live.Subscriptions.RequirePredicateOn = next.Subscriptions.RequirePredicateOn;
        live.Events.MaxQueueDepth = next.Events.MaxQueueDepth;
        live.Events.HandlerRetries = next.Events.HandlerRetries;
        live.Events.RetryBackoffMs = next.Events.RetryBackoffMs;
        live.Events.MaxPublishDepth = next.Events.MaxPublishDepth;
        live.Events.SubscriberExpirySeconds = next.Events.SubscriberExpirySeconds;
        live.CommitLog.GroupCommit = next.CommitLog.GroupCommit;
        live.Snapshots.Enabled = next.Snapshots.Enabled;
        live.Snapshots.IntervalTransactions = next.Snapshots.IntervalTransactions;
        live.Snapshots.TruncateLog = next.Snapshots.TruncateLog;
    }

    /// <summary>
    /// Applies changed <c>Residency:&lt;TableName&gt;</c> overrides to the running store — the
    /// register's <c>careful</c> semantic: the change takes effect at runtime, but pinning a table
    /// faults it wholly in and unpinning one migrates it to the buffer pool, so it is applied
    /// per changed table rather than wholesale.
    /// </summary>
    private void ApplyResidencyOverrides(MelangeDbOptions next, MelangeDbOptions live, MelangeEngine engine)
    {
        if (engine.HotStore is not IResidencyControl control)
            return;
        foreach (var (table, residency) in next.Residency.PerTable)
        {
            if (live.Residency.PerTable.TryGetValue(table, out var current) && current == residency)
                continue;
            live.Residency.PerTable[table] = residency;
            try
            {
                control.ApplyResidency(table, residency);
            }
            catch (Exception exception)
            {
                LogResidencyChangeFailed(_logger, table, residency, exception);
            }
        }
    }

    [LoggerMessage(EventId = 1507, EventName = "ResidencyChangeFailed", Level = LogLevel.Error,
        Message = "Applying the Residency:{Table} override ({Residency}) to the running store failed; the table keeps its previous residency until restart.")]
    private static partial void LogResidencyChangeFailed(ILogger logger, string table, Residency residency, Exception exception);

    [LoggerMessage(EventId = 1607, EventName = "RelationalTablesWithoutPostgres", Level = LogLevel.Warning,
        Message = "{Count} table(s) declare Tier = Relational but no Postgres:ConnectionString is configured: {Tables}. " +
            "Rows stay in the hot store; the relational projection (and ad-hoc SQL aggregates) will not exist until AddPostgres(...) is configured.")]
    private static partial void LogRelationalWithoutPostgres(ILogger logger, int count, string tables);

    [LoggerMessage(EventId = 1101, EventName = "MelangeStarted", Level = LogLevel.Information,
        Message = "MelangeDB started: recovered to LSN {HeadLsn} with {TableCount} table(s).")]
    private static partial void LogStarted(ILogger logger, ulong headLsn, int tableCount);

    [LoggerMessage(EventId = 1102, EventName = "MelangeStopped", Level = LogLevel.Information,
        Message = "MelangeDB stopped: drained, checkpointed, and flushed at LSN {HeadLsn}.")]
    private static partial void LogStopped(ILogger logger, ulong headLsn);

    [LoggerMessage(EventId = 1104, EventName = "UnpolicedReducers", Level = LogLevel.Warning,
        Message = "{Count} client-callable reducer(s) declare no authorization policy: {Reducers}. " +
            "Each is callable by any authenticated client (Policies:DefaultReducerPosture).")]
    private static partial void LogUnpolicedReducers(ILogger logger, int count, string reducers);
}
