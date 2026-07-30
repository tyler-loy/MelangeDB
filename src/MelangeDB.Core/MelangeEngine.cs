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
    private readonly List<ICommitObserver> _commitObservers = [];
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
            // allocated AutoInc id, so replay never reassigns different ids. The tail record's
            // timestamp is kept as the scheduler's downtime anchor — when the world last moved.
            foreach (var record in _log.ReadFrom(1))
            {
                store.Apply(record);
                _sequencer.Observe(record, schema);
                RecoveredTailTimestamp = record.Timestamp;
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

    /// <summary>
    /// The timestamp of the newest record recovered at startup, or null for an empty log — the
    /// scheduler's approximation of when the process went down.
    /// </summary>
    internal Timestamp? RecoveredTailTimestamp { get; private set; }

    public IHotStore HotStore { get; }

    public ApplierPipeline Appliers { get; }

    /// <summary>The engine's telemetry, if enabled — shared with the dispatch path's rate limiter.</summary>
    internal EngineTelemetry? Telemetry => _telemetry;

    /// <summary>
    /// A read-only <see cref="IDbView"/> over committed state — what policies evaluate against.
    /// Reads see the hot store with no overlay: during commit fan-out (which runs before the store
    /// applies) that is the pre-transaction committed state, never a partially applied write set.
    /// </summary>
    public IDbView CommittedView =>
        _committedView ??= new CommittedReadView(Schema, HotStore);

    private IDbView? _committedView;

    /// <summary>
    /// Invokes a reducer body as one transaction. <paramref name="reducerName"/> and
    /// <paramref name="arguments"/> are recorded as log metadata for audit; the write set is the
    /// authoritative payload. Nested invocations are forbidden and throw.
    /// </summary>
    public ulong Invoke(
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
                return InvokeCore(reducerName, caller, body, arguments, ArgsCodec.Encode(arguments), connectionId);
            }
            finally
            {
                _inReducer.Value = false;
            }
        }
    }

    /// <summary>
    /// Invokes a reducer body with pre-encoded arguments — the generated dispatch path, which
    /// decoded (and validated) the same bytes before this call. <paramref name="parentContext"/>
    /// parents the reducer span when a transport propagated a caller's trace context.
    /// </summary>
    public ulong Invoke(
        string reducerName,
        Identity caller,
        ReadOnlyMemory<byte> encodedArguments,
        Action<ReducerContext> body,
        ConnectionId connectionId = default,
        ActivityContext parentContext = default)
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
                return InvokeCore(reducerName, caller, body, arguments: null, encodedArguments, connectionId, parentContext);
            }
            finally
            {
                _inReducer.Value = false;
            }
        }
    }

    /// <summary>
    /// Registers a commit observer. It sees every record committed after registration, in LSN
    /// order, under the write lock and before any applier advances — see
    /// <see cref="ICommitObserver"/> for the pre-image guarantee.
    /// </summary>
    public void AddCommitObserver(ICommitObserver observer)
    {
        ArgumentNullException.ThrowIfNull(observer);
        lock (_writeLock)
        {
            _commitObservers.Add(observer);
        }
    }

    /// <summary>
    /// Runs a read under the write lock, handing it the head LSN the read is consistent at. No
    /// commit — and no commit observer — runs concurrently, so state observed here plus every
    /// observed record after that LSN is a gap-free, duplicate-free view. This is the anchor a
    /// subscription's initial set shares with its delta stream; keep the body cheap, because every
    /// reducer call waits behind it.
    /// </summary>
    public T ReadConsistent<T>(Func<ulong, T> read)
    {
        ArgumentNullException.ThrowIfNull(read);
        lock (_writeLock)
        {
            return read(_log.HeadLsn);
        }
    }

    /// <summary>Runs an action under the write lock; see <see cref="ReadConsistent{T}"/>.</summary>
    public void ReadConsistent(Action<ulong> read)
    {
        ArgumentNullException.ThrowIfNull(read);
        lock (_writeLock)
        {
            read(_log.HeadLsn);
        }
    }

    /// <summary>
    /// Appends one large write set as one transaction — the bulk ingestion path, one log record
    /// for the whole batch rather than one per row. Rows are upserts built from boxed column
    /// values keyed by name; zero or missing <c>[AutoInc]</c> columns are allocated, explicit
    /// values observed. Returns null when <paramref name="rows"/> is empty. Unique indexes are
    /// checked against committed state, not within the batch — the batch is the loader's to keep
    /// consistent.
    /// </summary>
    public CommitRecord? BulkInsert(Identity caller, IReadOnlyList<BulkRow> rows, ConnectionId connectionId = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (rows.Count == 0)
            return null;
        if (_inReducer.Value)
            throw new InvalidOperationException("Bulk ingestion cannot run inside a reducer.");

        lock (_writeLock)
        {
            using var activity = _telemetry?.StartReducer(BulkReducerName, caller, arguments: null, encodedArguments: default);
            var started = Stopwatch.GetTimestamp();
            var timestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
            var writeSet = new WriteSet();
            var stage = _sequencer.BeginStage();
            try
            {
                foreach (var row in rows)
                    StageBulkRow(row, writeSet, stage);
            }
            catch (Exception exception)
            {
                activity?.SetTag("melange.outcome", "rejected");
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                _telemetry?.RecordTransaction(BulkReducerName, "rejected", Elapsed(started), 0);
                throw;
            }

            var ops = writeSet.ToOps();
            var record = _log.Append(new CommitRequest(timestamp, caller, BulkReducerName, ReadOnlyMemory<byte>.Empty, ops));
            stage.Commit();
            NotifyCommitObservers(record);
            Appliers.NotifyAppended(record);
            activity?.SetTag("melange.outcome", "commit");
            activity?.SetTag("melange.writeset.rows", ops.Count);
            _telemetry?.RecordTransaction(BulkReducerName, "commit", Elapsed(started), ops.Count);
            return record;
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

    private const string BulkReducerName = "melange/bulk";

    private void StageBulkRow(in BulkRow row, WriteSet writeSet, AutoIncStage stage)
    {
        if (!Schema.TryGetByName(row.Table, out var schema))
            throw new ArgumentException($"No table named '{row.Table}' is registered.");

        var values = new Dictionary<string, object?>(row.Columns.Count, StringComparer.Ordinal);
        foreach (var (name, value) in row.Columns)
        {
            var column = schema.Column(name);
            values[name] = RowSerializer.CoerceValue(schema, column, value);
        }

        foreach (var autoInc in schema.AutoIncColumns)
        {
            var current = values.TryGetValue(autoInc.Name, out var supplied) ? AutoIncSequencer.ToUInt64(supplied) : 0UL;
            if (current is 0 or null)
                values[autoInc.Name] = autoInc.Kind == ColumnKind.Int64 ? (long)stage.Allocate(schema.Id) : stage.Allocate(schema.Id);
            else if (current is { } explicitId)
                stage.ObserveExplicit(schema.Id, explicitId);
        }

        values.TryGetValue(schema.PrimaryKey.Name, out var pkValue);
        if (pkValue is null)
            throw new ArgumentException($"Table '{schema.Name}': bulk row is missing primary key column '{schema.PrimaryKey.Name}'.");

        var key = KeyCodec.Encode(schema.PrimaryKey, pkValue);
        var exists = writeSet.TryGetPending(schema.Id, key, out var pending)
            ? pending.Kind != RowOpKind.Delete
            : HotStore.TryGetRow(schema.Id, key, out _);
        var bytes = RowSerializer.SerializeValues(schema, values);
        writeSet.Stage(new RowOp(exists ? RowOpKind.Update : RowOpKind.Insert, schema.Id, key, bytes));
    }

    private void NotifyCommitObservers(CommitRecord record)
    {
        foreach (var observer in _commitObservers)
        {
            try
            {
                observer.OnCommit(record);
            }
            catch (Exception exception)
            {
                // The transaction is durable; an observer failure must not undo or poison it.
                LogMessages.CommitObserverFailed(_logger, record.Lsn, exception);
            }
        }
    }

    private ulong InvokeCore(
        string reducerName,
        Identity caller,
        Action<ReducerContext> body,
        IReadOnlyList<object?>? arguments,
        ReadOnlyMemory<byte> encodedArguments,
        ConnectionId connectionId,
        ActivityContext parentContext = default)
    {
        using var activity = _telemetry?.StartReducer(reducerName, caller, arguments, encodedArguments, parentContext);
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

        ulong committedLsn = 0;
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
                NotifyCommitObservers(record);
                Appliers.NotifyAppended(record);
                committedLsn = record.Lsn;
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

        return committedLsn;
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

        private static readonly Action<ILogger, ulong, Exception?> CommitObserverFailedMessage =
            LoggerMessage.Define<ulong>(
                LogLevel.Error,
                new EventId(1005, "CommitObserverFailed"),
                "A commit observer threw for LSN {Lsn}; the transaction is committed and unaffected.");

        public static void CommitObserverFailed(ILogger logger, ulong lsn, Exception failure) =>
            CommitObserverFailedMessage(logger, lsn, failure);
    }
}

/// <summary>One bulk-ingested row: a table name and boxed column values keyed by column name.</summary>
public readonly record struct BulkRow(string Table, IReadOnlyDictionary<string, object?> Columns);
