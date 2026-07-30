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
        _reloadBridge = _monitor.OnChange(next => CopyLiveKeys(next, engine.Options));
        ReportUnpolicedReducers();

        // The event bus starts before the scheduler so a recovered timer's first fire can already
        // publish; its subscribers catch up from their checkpoints against the recovered log.
        _eventBus = (MelangeEventBus?)_provider.GetService(typeof(MelangeEventBus));
        _eventBus?.Start();

        // Scheduling starts only after recovery finished: the pending set is rebuilt from the
        // recovered timer rows, and overdue timers fire per Scheduler:CatchUpAfterDowntime.
        _scheduler = (MelangeScheduler?)_provider.GetService(typeof(MelangeScheduler));
        _scheduler?.Start();
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
    }

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
