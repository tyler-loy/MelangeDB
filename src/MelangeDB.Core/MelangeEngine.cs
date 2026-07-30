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
                InvokeCore(reducerName, caller, body, arguments, connectionId);
            }
            finally
            {
                _inReducer.Value = false;
            }
        }
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
                var record = _log.Append(new CommitRequest(timestamp, caller, reducerName, ArgsCodec.Encode(arguments), ops));
                _telemetry?.RecordCommitDuration(Elapsed(commitStarted));
                commit?.SetTag("melange.lsn", (long)record.Lsn);
                commit?.SetTag("melange.writeset.bytes", record.SerializedLength);
                stage.Commit();
                Appliers.NotifyAppended(record);
            }
        }

        activity?.SetTag("melange.outcome", "commit");
        activity?.SetTag("melange.writeset.rows", ops.Count);
        _telemetry?.RecordTransaction(reducerName, "commit", Elapsed(started), ops.Count);
    }

    private static double Elapsed(long startedTimestamp) =>
        Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds;
}
