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
public sealed partial class MelangeEngine : IDisposable
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
    private readonly List<ICommitGuard> _commitGuards = [];
    private readonly List<Func<ulong?>> _truncationFloors = [];
    private TableAccessGuard? _tableGuard;
    private readonly IDisposable? _storeLifetime;
    private long _commitsSinceSnapshot;
    private Timestamp? _tailTimestamp;
    private bool _disposed;

    public MelangeEngine(
        MelangeDbOptions options,
        SchemaRegistry schema,
        ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null,
        IHotStoreProvider? hotStoreProvider = null,
        ushort originator = 0)
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
            _sequencer = new AutoIncSequencer(originator);
            SnapshotPath = Path.Combine(options.CommitLog.Path, SnapshotFile.FileName);
            var store = CreateStore(options, schema, hotStoreProvider, loggers);
            _storeLifetime = store as IDisposable;

            // Recovery: the snapshot (when one exists) bootstraps the projection and the AutoInc
            // sequences at its LSN, then one pass over the log tail rebuilds the rest — replaying
            // re-observes every durably allocated AutoInc id, so replay never reassigns different
            // ids. The tail record's timestamp is kept as the scheduler's downtime anchor — when
            // the world last moved.
            var replayFrom = RecoverSnapshot(store);
            foreach (var record in _log.ReadFrom(replayFrom))
            {
                store.Apply(record);
                _sequencer.Observe(record, schema);
                RecoveredTailTimestamp = record.Timestamp;
            }

            _tailTimestamp = RecoveredTailTimestamp;
            HotStore = store;
            Appliers = new ApplierPipeline(_log, _telemetry);
            Appliers.Register(new HotStoreApplier(store));
            _telemetry?.SetHotStoreStatisticsProvider(store.Statistics);
            if (options.Residency.ReportOnStartup)
                ReportResidency(store);
        }
        catch
        {
            _storeLifetime?.Dispose();
            _log?.Dispose();
            _telemetry?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Selects the hot store per <c>HotStore:Engine</c>: selection by registration, not by path —
    /// <c>Auto</c> picks the registered provider when one exists, else the in-memory store, and
    /// asking for an engine whose package is not registered fails loudly rather than silently
    /// substituting.
    /// </summary>
    private IHotStore CreateStore(
        MelangeDbOptions options,
        SchemaRegistry schema,
        IHotStoreProvider? provider,
        ILoggerFactory loggers)
    {
        var residency = ResidencyResolver.Resolve(schema, options.Residency);
        var engine = options.HotStore.Engine;
        if (engine == HotStoreEngine.InMemory || (engine == HotStoreEngine.Auto && provider is null))
            return new InMemoryHotStore(schema, residency);
        if (provider is null || (engine != HotStoreEngine.Auto && provider.Engine != engine))
        {
            throw new InvalidOperationException(
                $"HotStore:Engine is {engine} but no matching store provider is registered. " +
                "Reference the storage package and register it on the builder (UseFasterHotStore()), " +
                "or set HotStore:Engine to InMemory or Auto.");
        }

        return provider.Create(new HotStoreContext
        {
            Schema = schema,
            Options = options,
            Residency = residency,
            LoggerFactory = loggers,
        });
    }

    /// <summary>
    /// Loads the snapshot if a valid one exists, returning the LSN log replay resumes from. A
    /// snapshot from another log epoch is stale and ignored — unless the log has been truncated,
    /// in which case state below the base is gone and recovery must fail loudly rather than
    /// silently rebuild a partial world.
    /// </summary>
    private ulong RecoverSnapshot(IHotStore store)
    {
        if (!File.Exists(SnapshotPath))
        {
            if (_log.BaseLsn > 0)
            {
                throw new InvalidDataException(
                    $"The commit log was truncated up to LSN {_log.BaseLsn} but no snapshot exists at " +
                    $"'{SnapshotPath}'. The truncated history is unrecoverable; restore the snapshot from backup.");
            }

            return 1;
        }

        using var reader = SnapshotFile.Open(SnapshotPath);
        var header = reader.Header;
        if (header.Epoch != _log.EpochId)
        {
            if (_log.BaseLsn > 0)
            {
                throw new InvalidDataException(
                    $"Snapshot '{SnapshotPath}' belongs to log epoch {header.Epoch}, but the truncated log's " +
                    $"epoch is {_log.EpochId}. The truncated history is unrecoverable; restore from backup.");
            }

            LogMessages.StaleSnapshotIgnored(_logger, SnapshotPath, header.Epoch, _log.EpochId);
            return 1;
        }

        if (header.Lsn < _log.BaseLsn)
        {
            throw new InvalidDataException(
                $"Snapshot '{SnapshotPath}' captures LSN {header.Lsn} but the log was truncated up to " +
                $"LSN {_log.BaseLsn}; records between the two are gone. Restore from backup.");
        }

        store.LoadSnapshot(header.Lsn, reader.Rows());
        foreach (var (table, next) in header.Sequences)
            _sequencer.RestoreSequence(table, next);
        RecoveredTailTimestamp = header.Timestamp;
        return header.Lsn + 1;
    }

    public SchemaRegistry Schema { get; }

    public ICommitLog Log => _log;

    /// <summary>The full path of the current snapshot file, beside the log.</summary>
    public string SnapshotPath { get; }

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
        _committedView ??= new CommittedReadView(Schema, HotStore, _tableGuard);

    private IDbView? _committedView;

    /// <summary>
    /// Invokes a reducer body as one transaction. <paramref name="reducerName"/> and
    /// <paramref name="arguments"/> are recorded as log metadata for audit; the write set is the
    /// authoritative payload. Nested invocations are forbidden and throw.
    /// <para>
    /// The engine's single write lock is held across the entire call — body, commit guards,
    /// append and fsync, commit observers, and any automatic snapshot the commit triggers — so time
    /// spent in the body is global write latency: no other transaction on this engine can start
    /// until it returns. Readers are unaffected (<see cref="CommittedView"/> takes no lock). Window
    /// long sweeps across many short transactions rather than running one long one.
    /// </para>
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
    /// parents the reducer span when a transport propagated a caller's trace context. Holds the
    /// write lock across the whole call, as the overload above describes.
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
    /// Registers a commit guard: it validates every subsequent transaction's collapsed write set
    /// at the commit point, before the append, and a throw aborts with zero trace. The cluster
    /// layer's seam; see <see cref="ICommitGuard"/>.
    /// </summary>
    public void AddCommitGuard(ICommitGuard guard)
    {
        ArgumentNullException.ThrowIfNull(guard);
        lock (_writeLock)
        {
            _commitGuards.Add(guard);
        }
    }

    /// <summary>
    /// Installs the table-access guard consulted by every transactional and committed read view —
    /// the cluster layer's placement visibility rule. Set once, before the engine serves calls;
    /// null (the default) means every registered table is accessible, which is the whole
    /// single-node behavior.
    /// </summary>
    public void SetTableAccessGuard(TableAccessGuard? guard)
    {
        lock (_writeLock)
        {
            _tableGuard = guard;
            _committedView = null; // Rebuilt with the guard on next access.
        }
    }

    /// <summary>
    /// Appends one externally produced write set as a single committed record — the cluster's
    /// replication and handoff apply path. Not a reducer: no DI scope, no policies, no rate
    /// limits; <paramref name="reducerName"/> should carry a reserved <c>melange/</c> name.
    /// <paramref name="reconcile"/> rewrites ops against current committed state so re-applying
    /// after a crash is idempotent — an insert of an existing key becomes an update, a delete of a
    /// missing key is dropped. <paramref name="alwaysAppend"/> appends even an empty write set:
    /// saga markers must reach the log to be recoverable. Returns null when nothing was appended.
    /// </summary>
    public CommitRecord? ApplyInternal(
        string reducerName,
        Identity caller,
        IReadOnlyList<RowOp> ops,
        ReadOnlyMemory<byte> arguments = default,
        bool reconcile = false,
        bool alwaysAppend = false)
    {
        ArgumentException.ThrowIfNullOrEmpty(reducerName);
        ArgumentNullException.ThrowIfNull(ops);
        if (_inReducer.Value)
            throw new InvalidOperationException("ApplyInternal cannot run inside a reducer.");

        lock (_writeLock)
        {
            var timestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
            var effective = reconcile ? ReconcileOps(ops) : ops;
            RunCommitGuards(reducerName, effective, CommitOrigin.Internal);
            if (effective.Count == 0 && !alwaysAppend)
                return null;
            var record = _log.Append(new CommitRequest(timestamp, caller, reducerName, arguments, effective));

            // Runtime observation mirrors what recovery replay will re-observe, so AutoInc
            // behavior is identical before and after a restart. Foreign-originator ids are
            // filtered out by the sequencer itself.
            _sequencer.Observe(record, Schema);
            NotifyCommitObservers(record);
            Appliers.NotifyAppended(record);
            AfterCommit(timestamp);
            return record;
        }
    }

    /// <summary>
    /// Rewrites externally produced ops against current committed state so they apply cleanly:
    /// the at-least-once import paths re-deliver after a crash, and the second delivery must be a
    /// no-op-shaped update rather than a duplicate-key insert.
    /// </summary>
    private IReadOnlyList<RowOp> ReconcileOps(IReadOnlyList<RowOp> ops)
    {
        var effective = new List<RowOp>(ops.Count);

        // Existence must track the batch's own effects, not just the store: a border batch
        // routinely carries several ops for one hot key, and judging each against pre-batch
        // state alone would log Insert, Insert, Insert for one row — the store upserts, but
        // subscription fan-out faithfully reports the logged kinds, so every observer holding
        // the row would see duplicate inserts (and count them as cache inconsistencies).
        Dictionary<(TableId Table, RowKey Key), bool>? batchState = null;
        foreach (var op in ops)
        {
            var key = (op.Table, op.Key);
            var exists = batchState?.TryGetValue(key, out var inBatch) is true
                ? inBatch
                : HotStore.TryGetRow(op.Table, op.Key, out _);
            switch (op.Kind)
            {
                case RowOpKind.Delete when exists:
                    effective.Add(op);
                    (batchState ??= [])[key] = false;
                    break;
                case RowOpKind.Delete:
                    break;
                case RowOpKind.Insert or RowOpKind.Update:
                    effective.Add(new RowOp(exists ? RowOpKind.Update : RowOpKind.Insert, op.Table, op.Key, op.Row));
                    (batchState ??= [])[key] = true;
                    break;
            }
        }

        return effective;
    }

    private void RunCommitGuards(string reducerName, IReadOnlyList<RowOp> ops, CommitOrigin origin)
    {
        foreach (var guard in _commitGuards)
            guard.Validate(reducerName, ops, origin);
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
    /// consistent. Deliberately ungated: callers on the wire are gated at the HTTP endpoint
    /// (<c>Bulk:Enabled</c> plus the <c>Bulk:OwnerRole</c> claim); direct engine callers are the
    /// host's own code and are trusted.
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
            IReadOnlyList<RowOp> ops;
            try
            {
                foreach (var row in rows)
                    StageBulkRow(row, writeSet, stage);
                ops = writeSet.ToOps();
                RunCommitGuards(BulkReducerName, ops, CommitOrigin.Bulk);
            }
            catch (Exception exception)
            {
                activity?.SetTag("melange.outcome", "rejected");
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                _telemetry?.RecordTransaction(BulkReducerName, "rejected", Elapsed(started), 0);
                throw;
            }

            var record = _log.Append(new CommitRequest(timestamp, caller, BulkReducerName, ReadOnlyMemory<byte>.Empty, ops));
            stage.Commit();
            NotifyCommitObservers(record);
            Appliers.NotifyAppended(record);
            AfterCommit(timestamp);
            activity?.SetTag("melange.outcome", "commit");
            activity?.SetTag("melange.writeset.rows", ops.Count);
            _telemetry?.RecordTransaction(BulkReducerName, "commit", Elapsed(started), ops.Count);
            return record;
        }
    }

    /// <summary>
    /// Registers a truncation floor: a provider of the highest LSN log compaction may remove from
    /// that consumer's perspective (its checkpoint). Null means the consumer pins nothing. The
    /// event bus registers <c>MinimumLiveCheckpointLsn</c> here so truncation never strands a
    /// subscriber that is merely behind.
    /// </summary>
    public void AddTruncationFloor(Func<ulong?> floor)
    {
        ArgumentNullException.ThrowIfNull(floor);
        lock (_writeLock)
        {
            _truncationFloors.Add(floor);
        }
    }

    /// <summary>
    /// Takes a snapshot at the current head LSN and, when <c>Snapshots:TruncateLog</c> is on,
    /// truncates the log behind it — never past the slowest applier checkpoint, the slowest live
    /// event-subscriber checkpoint, or the Resume retention window. Runs under the write lock, so
    /// the capture is consistent at one LSN; commits wait behind it. Returns the snapshot LSN, or
    /// null when snapshots are disabled or there is nothing to capture.
    /// </summary>
    public ulong? TakeSnapshot()
    {
        lock (_writeLock)
        {
            return TakeSnapshotCore();
        }
    }

    private ulong? TakeSnapshotCore()
    {
        if (!_options.Snapshots.Enabled)
            return null;
        var lsn = _log.HeadLsn;
        if (lsn == 0)
            return null;

        var header = new SnapshotFile.Header
        {
            Epoch = _log.EpochId,
            Lsn = lsn,
            Timestamp = _tailTimestamp ?? Timestamp.FromDateTimeOffset(_time.GetUtcNow()),
            Sequences = [.. _sequencer.ExportSequences()],
        };
        SnapshotFile.Write(SnapshotPath, header, Schema.Tables.Select(t => (t.Id, HotStore.Scan(t.Id))));
        _commitsSinceSnapshot = 0;
        LogMessages.SnapshotWritten(_logger, lsn, SnapshotPath);
        if (_options.Snapshots.TruncateLog)
            TruncateLogCore(lsn);
        return lsn;
    }

    /// <summary>
    /// The truncation floors, applied in one place so no configuration can override them: the
    /// snapshot LSN itself, every applier's checkpoint, every registered floor (live event
    /// subscribers), and the Resume retention window — a reconnecting client's gap must stay
    /// servable from the log for <c>Resume:RetentionWindowSeconds</c>.
    /// </summary>
    private void TruncateLogCore(ulong snapshotLsn)
    {
        var floor = snapshotLsn;
        foreach (var applier in Appliers.Appliers)
            floor = Math.Min(floor, applier.AppliedLsn);
        foreach (var provider in _truncationFloors)
        {
            if (provider() is { } pinned)
                floor = Math.Min(floor, pinned);
        }

        var retentionCutoff = _time.GetUtcNow().AddSeconds(-_options.Resume.RetentionWindowSeconds);
        var cutoffMicros = Timestamp.FromDateTimeOffset(retentionCutoff).UnixTimeMicroseconds;
        foreach (var record in _log.ReadFrom(_log.BaseLsn + 1))
        {
            if (record.Lsn > floor)
                break;
            if (record.Timestamp.UnixTimeMicroseconds >= cutoffMicros)
            {
                floor = Math.Min(floor, record.Lsn - 1);
                break;
            }
        }

        if (floor <= _log.BaseLsn)
            return;
        _log.TruncateBefore(floor);
        LogMessages.LogTruncated(_logger, floor, snapshotLsn);
    }

    /// <summary>
    /// The startup residency report (EventId 1501): each resident table's row count and measured
    /// bytes, the buffer-pool cap, and the total they sum to. The memory budget is a declared,
    /// computable artifact — this makes it an observed one.
    /// </summary>
    private void ReportResidency(IHotStore store)
    {
        var statistics = store.Statistics();
        var lines = new System.Text.StringBuilder();
        long residentBytes = 0;
        long overheadBytes = 0;
        var residentTables = 0;
        foreach (var table in statistics.Tables)
        {
            if (table.Residency == Residency.Resident)
            {
                residentTables++;
                residentBytes += table.ResidentBytes;
                lines.Append($"\n  {table.Name}: {table.RowCount} row(s), {table.ResidentBytes} bytes resident");
            }
            else
            {
                overheadBytes += table.ResidentBytes;
            }
        }

        var total = residentBytes + overheadBytes + statistics.BufferPoolCapacityBytes;
        LogMessages.ResidencyReport(
            _logger, residentTables, residentBytes, overheadBytes, statistics.BufferPoolCapacityBytes, total, lines.ToString());
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
        _storeLifetime?.Dispose();
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

        var key = SchemaKeyCodec.Encode(schema.PrimaryKey, pkValue);
        var exists = writeSet.TryGetPending(schema.Id, key, out var pending)
            ? pending.Kind != RowOpKind.Delete
            : HotStore.TryGetRow(schema.Id, key, out _);
        var bytes = RowSerializer.SerializeValues(schema, values);
        writeSet.Stage(new RowOp(exists ? RowOpKind.Update : RowOpKind.Insert, schema.Id, key, bytes));
    }

    /// <summary>
    /// Post-commit bookkeeping under the write lock: the tail timestamp for the next snapshot's
    /// downtime anchor, and the automatic snapshot trigger. A snapshot failure must not fail the
    /// committed transaction — the commit is durable in the log regardless.
    /// </summary>
    private void AfterCommit(Timestamp timestamp)
    {
        _tailTimestamp = timestamp;
        _commitsSinceSnapshot++;
        if (!_options.Snapshots.Enabled || _commitsSinceSnapshot < _options.Snapshots.IntervalTransactions)
            return;
        try
        {
            TakeSnapshotCore();
        }
        catch (Exception exception)
        {
            _commitsSinceSnapshot = 0; // Back off a full interval rather than failing every commit.
            LogMessages.SnapshotFailed(_logger, exception);
        }
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
        var events = new EventStage(_options.Events);
        var context = new ReducerContext(caller, connectionId, timestamp, random, new TransactionDb(Schema, HotStore, writeSet, stage, _tableGuard), events);

        IReadOnlyList<RowOp> ops;
        double bodyMs;
        try
        {
            // Measured directly rather than as (total - commit): everything after the append —
            // commit observers, applier notification, an automatic snapshot — is inside the same
            // span, and subtracting would charge all of it to the module's reducer body.
            var bodyStarted = Stopwatch.GetTimestamp();
            body(context);
            bodyMs = Elapsed(bodyStarted);
            ops = writeSet.ToOps();
            RunCommitGuards(reducerName, ops, CommitOrigin.Reducer);
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
        var commitMs = 0d;
        double? fsyncMs = null;
        var postCommitMs = 0d;
        if (ops.Count > 0 || events.Events is { Count: > 0 })
        {
            using (var commit = _telemetry?.StartCommit())
            {
                var commitStarted = Stopwatch.GetTimestamp();
                var record = _log.Append(new CommitRequest(timestamp, caller, reducerName, encodedArguments, ops, events.Events));
                commitMs = Elapsed(commitStarted);
                fsyncMs = _log.LastAppendFsyncMilliseconds;
                _telemetry?.RecordCommitDuration(commitMs);
                commit?.SetTag("melange.lsn", (long)record.Lsn);
                commit?.SetTag("melange.writeset.bytes", record.SerializedLength);
                var postCommitStarted = Stopwatch.GetTimestamp();
                stage.Commit();
                NotifyCommitObservers(record);
                Appliers.NotifyAppended(record);
                AfterCommit(timestamp);
                postCommitMs = Elapsed(postCommitStarted);
                committedLsn = record.Lsn;
            }
        }

        activity?.SetTag("melange.outcome", "commit");
        activity?.SetTag("melange.writeset.rows", ops.Count);
        var elapsed = Elapsed(started);
        _telemetry?.RecordTransaction(reducerName, "commit", elapsed, ops.Count);
        if (elapsed > _options.Telemetry.SlowReducerMs)
            WarnSlowReducer(activity, reducerName, elapsed, bodyMs, commitMs, fsyncMs, postCommitMs, ops.Count);

        return committedLsn;
    }

    /// <summary>
    /// Reports one over-threshold transaction split into the parts that fail for different reasons:
    /// a wide body is the module's problem, a slow commit is the disk's, and a slow post-commit is
    /// an observer or an automatic snapshot. Undifferentiated, the same warning covers all three and
    /// tells the reader only where to start looking.
    /// </summary>
    private void WarnSlowReducer(
        Activity? activity,
        string reducerName,
        double elapsed,
        double bodyMs,
        double commitMs,
        double? fsyncMs,
        double postCommitMs,
        int rows)
    {
        if (activity is not null)
        {
            var tags = new ActivityTagsCollection
            {
                ["melange.duration_ms"] = elapsed,
                ["melange.body_ms"] = bodyMs,
                ["melange.commit_ms"] = commitMs,
                ["melange.post_commit_ms"] = postCommitMs,
                ["melange.writeset.rows"] = rows,
            };
            // Absent, not zero: under a deferred fsync policy there was no flush to attribute, and
            // a zero would read as "the disk was instant".
            if (fsyncMs is { } fsync)
                tags["melange.fsync_ms"] = fsync;
            activity.AddEvent(new ActivityEvent("melange.slow_reducer", tags: tags));
        }

        var threshold = _options.Telemetry.SlowReducerMs;
        if (fsyncMs is { } inlineFsync)
            LogMessages.SlowReducer(_logger, reducerName, elapsed, threshold, bodyMs, commitMs, inlineFsync, postCommitMs, rows);
        else
            LogMessages.SlowReducerDeferredFsync(_logger, reducerName, elapsed, threshold, bodyMs, commitMs, postCommitMs, rows);
    }

    private static double Elapsed(long startedTimestamp) =>
        Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds;

    private static partial class LogMessages
    {
        // Source-generated rather than LoggerMessage.Define like its siblings below: the split
        // carries more than the six type arguments Define offers, and every part has to stay a
        // structured field or an alert cannot key on the actionable half.

        /// <summary>1003, in-line fsync: the whole split, including what durability cost.</summary>
        [LoggerMessage(
            EventId = 1003,
            EventName = "SlowReducer",
            Level = LogLevel.Warning,
            Message = "Reducer '{Reducer}' took {DurationMs:F1}ms, over the Telemetry:SlowReducerMs threshold of " +
                      "{ThresholdMs}ms — body {BodyMs:F1}ms, commit {CommitMs:F1}ms (fsync {FsyncMs:F1}ms), " +
                      "post-commit {PostCommitMs:F1}ms, {Rows} row ops.")]
        public static partial void SlowReducer(
            ILogger logger,
            string reducer,
            double durationMs,
            int thresholdMs,
            double bodyMs,
            double commitMs,
            double fsyncMs,
            double postCommitMs,
            int rows);

        /// <summary>
        /// 1003 under a deferred fsync policy. The flush happened on a timer thread or not at all,
        /// so there is no fsync field: omitting it says "not measured here", where a zero would say
        /// "the disk was instant". Same event id — alerts key on 1003 — but its own event name,
        /// because "this deployment defers durability" is itself worth reading off the line.
        /// </summary>
        [LoggerMessage(
            EventId = 1003,
            EventName = "SlowReducerDeferredFsync",
            Level = LogLevel.Warning,
            Message = "Reducer '{Reducer}' took {DurationMs:F1}ms, over the Telemetry:SlowReducerMs threshold of " +
                      "{ThresholdMs}ms — body {BodyMs:F1}ms, commit {CommitMs:F1}ms (fsync deferred by " +
                      "CommitLog:FsyncPolicy), post-commit {PostCommitMs:F1}ms, {Rows} row ops.")]
        public static partial void SlowReducerDeferredFsync(
            ILogger logger,
            string reducer,
            double durationMs,
            int thresholdMs,
            double bodyMs,
            double commitMs,
            double postCommitMs,
            int rows);

        private static readonly Action<ILogger, ulong, Exception?> CommitObserverFailedMessage =
            LoggerMessage.Define<ulong>(
                LogLevel.Error,
                new EventId(1005, "CommitObserverFailed"),
                "A commit observer threw for LSN {Lsn}; the transaction is committed and unaffected.");

        public static void CommitObserverFailed(ILogger logger, ulong lsn, Exception failure) =>
            CommitObserverFailedMessage(logger, lsn, failure);

        private static readonly Action<ILogger, int, long, long, long, long, string, Exception?> ResidencyReportMessage =
            LoggerMessage.Define<int, long, long, long, long, string>(
                LogLevel.Information,
                new EventId(1501, "ResidencyReport"),
                "Residency report: {ResidentTables} resident table(s) holding {ResidentBytes} bytes, " +
                "{OverheadBytes} bytes of paged-table bookkeeping, buffer-pool cap {BufferPoolBytes} bytes — " +
                "total declared footprint {TotalBytes} bytes.{Tables}");

        public static void ResidencyReport(
            ILogger logger, int residentTables, long residentBytes, long overheadBytes, long bufferPoolBytes, long totalBytes, string tables) =>
            ResidencyReportMessage(logger, residentTables, residentBytes, overheadBytes, bufferPoolBytes, totalBytes, tables, null);

        private static readonly Action<ILogger, ulong, string, Exception?> SnapshotWrittenMessage =
            LoggerMessage.Define<ulong, string>(
                LogLevel.Information,
                new EventId(1502, "SnapshotWritten"),
                "Snapshot captured at LSN {Lsn} to '{Path}'.");

        public static void SnapshotWritten(ILogger logger, ulong lsn, string path) =>
            SnapshotWrittenMessage(logger, lsn, path, null);

        private static readonly Action<ILogger, ulong, ulong, Exception?> LogTruncatedMessage =
            LoggerMessage.Define<ulong, ulong>(
                LogLevel.Information,
                new EventId(1503, "LogTruncated"),
                "Commit log truncated up to LSN {Floor} behind the snapshot at LSN {SnapshotLsn}; " +
                "the floor is the minimum of the snapshot, every applier checkpoint, every live " +
                "event-subscriber checkpoint, and the Resume retention window.");

        public static void LogTruncated(ILogger logger, ulong floor, ulong snapshotLsn) =>
            LogTruncatedMessage(logger, floor, snapshotLsn, null);

        private static readonly Action<ILogger, Exception?> SnapshotFailedMessage =
            LoggerMessage.Define(
                LogLevel.Error,
                new EventId(1504, "SnapshotFailed"),
                "Automatic snapshot failed; the committed transaction is durable and unaffected. " +
                "The next attempt is one full Snapshots:IntervalTransactions away.");

        public static void SnapshotFailed(ILogger logger, Exception failure) =>
            SnapshotFailedMessage(logger, failure);

        private static readonly Action<ILogger, string, Guid, Guid, Exception?> StaleSnapshotIgnoredMessage =
            LoggerMessage.Define<string, Guid, Guid>(
                LogLevel.Warning,
                new EventId(1506, "StaleSnapshotIgnored"),
                "Snapshot '{Path}' belongs to log epoch {SnapshotEpoch}, not the current epoch {LogEpoch}; " +
                "ignored and recovery replays the full log.");

        public static void StaleSnapshotIgnored(ILogger logger, string path, Guid snapshotEpoch, Guid logEpoch) =>
            StaleSnapshotIgnoredMessage(logger, path, snapshotEpoch, logEpoch, null);
    }
}

/// <summary>One bulk-ingested row: a table name and boxed column values keyed by column name.</summary>
public readonly record struct BulkRow(string Table, IReadOnlyDictionary<string, object?> Columns);
