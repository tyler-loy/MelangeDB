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
        _state.Started = true;
        LogStarted(_logger, engine.Log.HeadLsn, engine.Schema.Tables.Count);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _reloadBridge?.Dispose();
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
    }

    [LoggerMessage(EventId = 1101, EventName = "MelangeStarted", Level = LogLevel.Information,
        Message = "MelangeDB started: recovered to LSN {HeadLsn} with {TableCount} table(s).")]
    private static partial void LogStarted(ILogger logger, ulong headLsn, int tableCount);

    [LoggerMessage(EventId = 1102, EventName = "MelangeStopped", Level = LogLevel.Information,
        Message = "MelangeDB stopped: drained, checkpointed, and flushed at LSN {HeadLsn}.")]
    private static partial void LogStopped(ILogger logger, ulong headLsn);
}
