using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Core;

/// <summary>
/// The phase-01 composition root and dispatcher: opens the commit log, rebuilds projections and
/// AutoInc sequences from it, and invokes reducers. One invocation is one transaction — the
/// reducer mutates through the overlay, and a single atomic log append is the commit point.
/// Return means commit; throw means abort with zero trace. Phase 02's host integration wraps this
/// behind <c>AddMelangeDb</c>.
/// </summary>
public sealed class MelangeEngine : IDisposable
{
    private readonly MelangeDbOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly FileCommitLog _log;
    private readonly AutoIncSequencer _sequencer;
    private readonly EngineTelemetry? _telemetry;
    private readonly Lock _writeLock = new();
    private readonly ThreadLocal<bool> _inReducer = new();
    private bool _disposed;

    public MelangeEngine(
        MelangeDbOptions options,
        SchemaRegistry schema,
        ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schema);
        _options = options;
        Schema = schema;
        _time = timeProvider ?? TimeProvider.System;
        var loggers = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = loggers.CreateLogger<MelangeEngine>();

        Directory.CreateDirectory(options.HotStore.Path);
        _telemetry = options.Telemetry.Enabled
            ? new EngineTelemetry(
                options.Telemetry,
                () => _log?.HeadLsn ?? 0UL,
                () => Appliers?.Lags() ?? [])
            : null;
        try
        {
            _log = new FileCommitLog(options.CommitLog, loggers.CreateLogger<FileCommitLog>(), _telemetry);
            _sequencer = new AutoIncSequencer();
            var store = new InMemoryHotStore(schema);

            // Recovery: one pass over the log rebuilds the projection and re-observes every durably
            // allocated AutoInc id, so replay never reassigns different ids.
            foreach (var record in _log.ReadFrom(1))
            {
                store.Apply(record);
                _sequencer.Observe(record, schema);
            }

            HotStore = store;
            Appliers = new ApplierPipeline(_log, _telemetry);
            Appliers.Register(new HotStoreApplier(store));
        }
        catch
        {
            _log?.Dispose();
            _telemetry?.Dispose();
            throw;
        }
    }

    public SchemaRegistry Schema { get; }

    public ICommitLog Log => _log;

    /// <summary>The options instance the engine reads live keys from; the host's reload bridge mutates it.</summary>
    internal MelangeDbOptions Options => _options;

    /// <summary>The commit log's poisoned-state failure, if any — the melange-log health signal.</summary>
    internal Exception? LogFailure => _log.Failure;

    public IHotStore HotStore { get; }

    public ApplierPipeline Appliers { get; }

    /// <summary>
    /// Invokes a reducer body as one transaction. <paramref name="reducerName"/> and
    /// <paramref name="arguments"/> are recorded as log metadata for audit; the write set is the
    /// authoritative payload. Nested invocations are forbidden and throw.
    /// </summary>
    public void Invoke(
        string reducerName,
        Identity caller,
        Action<ReducerContext> body,
        IReadOnlyList<object?>? arguments = null,
        ConnectionId connectionId = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(reducerName);
        ArgumentNullException.ThrowIfNull(body);
        if (_inReducer.Value)
        {
            throw new InvalidOperationException(
                "Nested reducer calls are forbidden: a reducer must not invoke another reducer. " +
                "Extract shared logic into a plain method both reducers call.");
        }

        lock (_writeLock)
        {
            _inReducer.Value = true;
            try
            {
                InvokeCore(reducerName, caller, body, arguments, ArgsCodec.Encode(arguments), connectionId);
            }
            finally
            {
                _inReducer.Value = false;
            }
        }
    }

    /// <summary>
    /// Invokes a reducer body with pre-encoded arguments — the generated dispatch path, which
    /// decoded (and validated) the same bytes before this call.
    /// </summary>
    public void Invoke(
        string reducerName,
        Identity caller,
        ReadOnlyMemory<byte> encodedArguments,
        Action<ReducerContext> body,
        ConnectionId connectionId = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(reducerName);
        ArgumentNullException.ThrowIfNull(body);
        if (_inReducer.Value)
        {
            throw new InvalidOperationException(
                "Nested reducer calls are forbidden: a reducer must not invoke another reducer. " +
                "Extract shared logic into a plain method both reducers call.");
        }

        lock (_writeLock)
        {
            _inReducer.Value = true;
            try
            {
                InvokeCore(reducerName, caller, body, arguments: null, encodedArguments, connectionId);
            }
            finally
            {
                _inReducer.Value = false;
            }
        }
    }

    /// <summary>Blocks until any in-flight invocation has completed. Used by graceful shutdown.</summary>
    public void Drain()
    {
        lock (_writeLock)
        {
        }
    }

    /// <summary>
    /// Advances every unpaused applier to the log head and forces the log to stable storage —
    /// graceful shutdown's flush-and-checkpoint step.
    /// </summary>
    public void Checkpoint()
    {
        Appliers.CatchUpAll();
        _log.FlushToDisk();
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _log.Dispose();
        _telemetry?.Dispose();
        _inReducer.Dispose();
    }

    private void InvokeCore(
        string reducerName,
        Identity caller,
        Action<ReducerContext> body,
        IReadOnlyList<object?>? arguments,
        ReadOnlyMemory<byte> encodedArguments,
        ConnectionId connectionId)
    {
        using var activity = _telemetry?.StartReducer(reducerName, caller, arguments);
        var started = Stopwatch.GetTimestamp();
        var timestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
        var writeSet = new WriteSet();
        var stage = _sequencer.BeginStage();
        var random = new Random(unchecked((int)timestamp.UnixTimeMicroseconds ^ caller.GetHashCode()));
        var context = new ReducerContext(caller, connectionId, timestamp, random, new TransactionDb(Schema, HotStore, writeSet, stage));

        try
        {
            body(context);
        }
        catch (Exception exception)
        {
            // Abort: nothing was appended, the write set is discarded, and the allocation stage
            // was never committed — zero trace, no consumed AutoInc value.
            var outcome = exception is RejectedException ? "rejected" : "abort";
            activity?.SetTag("melange.outcome", outcome);
            activity?.SetTag("melange.writeset.rows", 0);
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            _telemetry?.RecordTransaction(reducerName, outcome, Elapsed(started), 0);
            throw;
        }

        var ops = writeSet.ToOps();
        if (ops.Count > 0)
        {
            using (var commit = _telemetry?.StartCommit())
            {
                var commitStarted = Stopwatch.GetTimestamp();
                var record = _log.Append(new CommitRequest(timestamp, caller, reducerName, encodedArguments, ops));
                _telemetry?.RecordCommitDuration(Elapsed(commitStarted));
                commit?.SetTag("melange.lsn", (long)record.Lsn);
                commit?.SetTag("melange.writeset.bytes", record.SerializedLength);
                stage.Commit();
                Appliers.NotifyAppended(record);
            }
        }

        activity?.SetTag("melange.outcome", "commit");
        activity?.SetTag("melange.writeset.rows", ops.Count);
        var elapsed = Elapsed(started);
        _telemetry?.RecordTransaction(reducerName, "commit", elapsed, ops.Count);
        if (elapsed > _options.Telemetry.SlowReducerMs)
        {
            activity?.AddEvent(new ActivityEvent(
                "melange.slow_reducer",
                tags: new ActivityTagsCollection { ["melange.duration_ms"] = elapsed }));
            LogMessages.SlowReducer(_logger, reducerName, elapsed, _options.Telemetry.SlowReducerMs);
        }
    }

    private static double Elapsed(long startedTimestamp) =>
        Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds;

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, double, int, Exception?> SlowReducerMessage =
            LoggerMessage.Define<string, double, int>(
                LogLevel.Warning,
                new EventId(1003, "SlowReducer"),
                "Reducer '{Reducer}' took {DurationMs:F1}ms, over the Telemetry:SlowReducerMs threshold of {ThresholdMs}ms.");

        public static void SlowReducer(ILogger logger, string reducer, double durationMs, int thresholdMs) =>
            SlowReducerMessage(logger, reducer, durationMs, thresholdMs, null);
    }
}
