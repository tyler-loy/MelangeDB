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
    // Null when the configured hot store does not offer pinned reads. Snapshot-isolated reducers
    // then run serialized instead — see InvokeCore's fallback, which says so once and loudly.
    private IReadViewSource? _readViewSource;
    private int _snapshotUnavailableReported;
    private long _commitsSinceSnapshot;

    /// <summary>Set to 1 while a snapshot is writing, so two never race on the same temporary file.</summary>
    private int _snapshotWriting;

    /// <summary>
    /// A snapshot captured by an automatic trigger under the write lock, waiting for the committing
    /// thread to release the lock and write it.
    /// </summary>
    private PendingSnapshot? _deferredSnapshot;
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
            // the world last moved. A store that can take the whole replay through builders does
            // (no read view can exist yet, so a version per row would be built for nobody); the
            // in-memory store's LoadSnapshot has its own bulk path either way.
            var bulk = store as IBulkRecovery;
            bulk?.BeginRecovery();
            var replayFrom = RecoverSnapshot(store);
            foreach (var record in _log.ReadFrom(replayFrom))
            {
                store.Apply(record);
                _sequencer.Observe(record, schema);
                RecoveredTailTimestamp = record.Timestamp;
            }

            bulk?.CompleteRecovery();

            _tailTimestamp = RecoveredTailTimestamp;
            HotStore = store;
            _readViewSource = store as IReadViewSource;
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

        try
        {
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
        finally
        {
            // An automatic snapshot captured under the lock is written here, with the lock released.
            // In a finally because a capture that is never completed leaks its pin, which holds
            // container versions alive for the life of the process.
            CompleteDeferredSnapshot();
        }
    }

    /// <summary>
    /// Invokes a reducer body with pre-encoded arguments — the generated dispatch path, which
    /// decoded (and validated) the same bytes before this call. <paramref name="parentContext"/>
    /// parents the reducer span when a transport propagated a caller's trace context.
    /// <para>
    /// Holds the write lock across the whole call as the overload above describes, unless
    /// <paramref name="isolation"/> is <see cref="Isolation.Snapshot"/> — then the body runs outside
    /// the lock against a read view pinned at one LSN, and only reconcile, the guards, and the
    /// append serialize. Read <see cref="Isolation"/> before declaring that: the failure mode of
    /// declaring it on a read-modify-write body is lost writes with no error anywhere.
    /// </para>
    /// </summary>
    public ulong Invoke(
        string reducerName,
        Identity caller,
        ReadOnlyMemory<byte> encodedArguments,
        Action<ReducerContext> body,
        ConnectionId connectionId = default,
        ActivityContext parentContext = default,
        Isolation isolation = Isolation.Serialized)
    {
        ArgumentException.ThrowIfNullOrEmpty(reducerName);
        ArgumentNullException.ThrowIfNull(body);
        if (_inReducer.Value)
        {
            throw new InvalidOperationException(
                "Nested reducer calls are forbidden: a reducer must not invoke another reducer. " +
                "Extract shared logic into a plain method both reducers call.");
        }

        if (isolation == Isolation.Snapshot && ReadViewOrFallback() is { } source)
        {
            try
            {
                _inReducer.Value = true;
                try
                {
                    return InvokeSnapshot(reducerName, caller, body, encodedArguments, connectionId, parentContext, source);
                }
                finally
                {
                    _inReducer.Value = false;
                }
            }
            finally
            {
                CompleteDeferredSnapshot();
            }
        }

        try
        {
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
        finally
        {
            CompleteDeferredSnapshot();
        }
    }

    /// <summary>
    /// The pinned-read capability, or null when the configured store has none — in which case a
    /// snapshot-isolated reducer runs serialized and the engine says so once.
    /// <para>
    /// Degrading rather than failing is deliberate. Isolation is a <em>latency</em> property, not a
    /// semantic one: a body written for snapshot isolation is still correct when run serialized,
    /// just slower and holding the lock. Refusing to start would turn a performance feature into a
    /// hard dependency on a store capability that <see cref="IReadViewSource"/> deliberately makes
    /// optional. Degrading <em>silently</em> is the option that is actually wrong, which is why this
    /// warns rather than merely not-crashing.
    /// </para>
    /// </summary>
    private IReadViewSource? ReadViewOrFallback()
    {
        if (_readViewSource is { } source)
            return source;
        if (Interlocked.Exchange(ref _snapshotUnavailableReported, 1) == 0)
            LogMessages.SnapshotIsolationUnavailable(_logger, HotStore.GetType().Name);
        return null;
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

        CommitRecord record;
        lock (_writeLock)
        {
            var timestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
            var effective = reconcile ? ReconcileOps(ops) : ops;
            RunCommitGuards(reducerName, effective, CommitOrigin.Internal);
            if (effective.Count == 0 && !alwaysAppend)
                return null;
            record = _log.Append(new CommitRequest(timestamp, caller, reducerName, arguments, effective));

            // Runtime observation mirrors what recovery replay will re-observe, so AutoInc
            // behavior is identical before and after a restart. Foreign-originator ids are
            // filtered out by the sequencer itself.
            _sequencer.Observe(record, Schema);
            NotifyCommitObservers(record);
            Appliers.NotifyAppended(record);
            AfterCommit(timestamp);
        }

        CompleteDeferredSnapshot();
        return record;
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

        CommitRecord bulkRecord;
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
            bulkRecord = record;
        }

        CompleteDeferredSnapshot();
        return bulkRecord;
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
        PendingSnapshot? pending;
        lock (_writeLock)
        {
            pending = BeginSnapshot();
        }

        return pending is null ? null : CompleteSnapshot(pending);
    }

    /// <summary>
    /// The part of a snapshot that must run under the write lock: read the head LSN, capture the
    /// header, and pin a read view at that LSN. All of it is cheap — the pin is a reference capture,
    /// because the stores keep their containers persistent for exactly this.
    /// <para>
    /// Returns null when snapshots are off, when there is nothing to capture, or when another
    /// snapshot is already writing. That last case is not a lost snapshot: the interval counter is
    /// only reset by a snapshot that actually begins, so the next commit past the threshold tries
    /// again.
    /// </para>
    /// </summary>
    private PendingSnapshot? BeginSnapshot()
    {
        if (!_options.Snapshots.Enabled)
            return null;
        var lsn = _log.HeadLsn;
        if (lsn == 0)
            return null;

        // One writer at a time, or two snapshots race on the same temporary file.
        if (Interlocked.CompareExchange(ref _snapshotWriting, 1, 0) != 0)
        {
            LogMessages.SnapshotAlreadyRunning(_logger, lsn);
            return null;
        }

        var header = new SnapshotFile.Header
        {
            Epoch = _log.EpochId,
            Lsn = lsn,
            Timestamp = _tailTimestamp ?? Timestamp.FromDateTimeOffset(_time.GetUtcNow()),
            Sequences = [.. _sequencer.ExportSequences()],
        };

        // A store with no pinned-read capability has no way to give a consistent view outside the
        // lock, so it keeps the old behaviour and writes under it. Everything else writes from a pin.
        var view = _readViewSource?.OpenReadView();
        _commitsSinceSnapshot = 0;
        return new PendingSnapshot(lsn, header, view);
    }

    /// <summary>
    /// The expensive part, run <b>outside</b> the write lock: scan every table through the pinned
    /// view and write the snapshot file. Commits proceed while this runs, and land after the
    /// snapshot's LSN — the pin is what makes that safe.
    /// <para>
    /// Truncation re-takes the lock afterwards. Evaluating the retention floors later than the
    /// capture is safe in the only direction that matters: floors advance, and the result is capped
    /// by the snapshot's own LSN, so a later evaluation can never truncate more than an earlier one
    /// would have.
    /// </para>
    /// </summary>
    private ulong CompleteSnapshot(PendingSnapshot pending)
    {
        try
        {
            var tables = pending.View is { } view
                ? Schema.Tables.Select(t => (t.Id, view.Scan(t.Id)))
                : Schema.Tables.Select(t => (t.Id, HotStore.Scan(t.Id)));
            SnapshotFile.Write(SnapshotPath, pending.Header, tables);
            LogMessages.SnapshotWritten(_logger, pending.Lsn, SnapshotPath);

            if (_options.Snapshots.TruncateLog)
            {
                lock (_writeLock)
                {
                    TruncateLogCore(pending.Lsn);
                }
            }

            return pending.Lsn;
        }
        finally
        {
            pending.View?.Dispose();
            Volatile.Write(ref _snapshotWriting, 0);
        }
    }

    /// <summary>
    /// A snapshot captured under the write lock and not yet written. The view pins the store at
    /// <see cref="Lsn"/>; it is null only for a store with no pinned-read capability, which writes
    /// under the lock as before.
    /// </summary>
    private sealed record PendingSnapshot(ulong Lsn, SnapshotFile.Header Header, IHotStoreReadView? View);

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
            // Only the cheap half runs here — this is under the write lock. The commit that crossed
            // the threshold writes the file on its way out, after releasing it.
            _deferredSnapshot = BeginSnapshot();
        }
        catch (Exception exception)
        {
            _commitsSinceSnapshot = 0; // Back off a full interval rather than failing every commit.
            LogMessages.SnapshotFailed(_logger, exception);
        }
    }

    /// <summary>
    /// Writes the snapshot an automatic trigger captured, now that the write lock is released. Runs
    /// on the committing thread rather than a background one, so a snapshot is still finished before
    /// the call that triggered it returns — what changes is that other transactions no longer wait
    /// for it.
    /// <para>
    /// A snapshot failure must never fail the transaction that triggered it: the commit is already
    /// durable in the log, and the snapshot is only an optimisation of replay.
    /// </para>
    /// </summary>
    private void CompleteDeferredSnapshot()
    {
        if (Interlocked.Exchange(ref _deferredSnapshot, null) is not { } pending)
            return;
        try
        {
            CompleteSnapshot(pending);
        }
        catch (Exception exception)
        {
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
        activity?.SetTag("melange.isolation", "serialized");
        var started = Stopwatch.GetTimestamp();
        var timestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
        var writeSet = new WriteSet();
        var stage = _sequencer.BeginStage();
        var random = new Random(unchecked((int)timestamp.UnixTimeMicroseconds ^ caller.GetHashCode()));
        var events = new EventStage(_options.Events);
        var context = new ReducerContext(caller, connectionId, timestamp, random, new TransactionDb(Schema, HotStore, writeSet, stage, _tableGuard), events);

        IReadOnlyList<RowOp> ops;
        // Measured directly rather than as (total - commit): everything after the append — commit
        // observers, applier notification, an automatic snapshot — is inside the same span, and
        // subtracting would charge all of it to the module's reducer body. Declared out here so the
        // abort path can report it too; null there means the body itself is what threw.
        var bodyStarted = Stopwatch.GetTimestamp();
        double? bodyMs = null;
        try
        {
            body(context);
            bodyMs = Elapsed(bodyStarted);
            ops = writeSet.ToOps();
            RunCommitGuards(reducerName, ops, CommitOrigin.Reducer);
        }
        catch (Exception exception)
        {
            // Abort: nothing was appended, the write set is discarded, and the allocation stage
            // was never committed — zero trace, no consumed AutoInc value. The write lock was still
            // held for the whole of it, so a slow abort is reported exactly like a slow commit.
            var outcome = exception is RejectedException ? "rejected" : "abort";
            // The part before the whole: reading the body's clock after the transaction's would let
            // the work between the two readings push a trivial abort's body past its own duration.
            var abortedBodyMs = bodyMs ?? Elapsed(bodyStarted);
            var abortedAfter = Elapsed(started);
            activity?.SetTag("melange.outcome", outcome);
            activity?.SetTag("melange.writeset.rows", 0);
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            _telemetry?.RecordTransaction(reducerName, outcome, abortedAfter, 0);
            // The whole of a serialized transaction is the locked portion: `started` is read inside
            // the lock, so this duration and the lock hold are the same interval.
            if (abortedAfter > _options.Telemetry.SlowReducerMs)
                WarnSlowAbort(activity, reducerName, outcome, abortedAfter, abortedBodyMs, abortedAfter, Isolation.Serialized);
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
        // Locked and total are the same interval here, by construction: `started` is read inside
        // the lock. The threshold is on the locked portion at every isolation level, and for a
        // serialized transaction that is the whole transaction — so this path's behaviour is
        // exactly what it was before snapshot isolation existed.
        if (elapsed > _options.Telemetry.SlowReducerMs)
            WarnSlowReducer(activity, reducerName, elapsed, bodyMs.GetValueOrDefault(), commitMs, fsyncMs, postCommitMs, ops.Count, elapsed, Isolation.Serialized);

        return committedLsn;
    }

    /// <summary>
    /// Invokes a reducer body under <see cref="Isolation.Snapshot"/>: the body runs <em>outside</em>
    /// the write lock against an <see cref="IHotStoreReadView"/> pinned at one LSN, and only the
    /// commit — reconcile, the guards, the append, and the post-commit fan-out — serializes.
    /// <para>
    /// Two things differ from the serialized path beyond where the lock sits, and both are forced by
    /// the body no longer being alone. AutoInc ids are <b>reserved as allocated</b> rather than
    /// staged, because two concurrent bodies staging against one sequence hand out the same id. And
    /// the write set is <b>reconciled</b> against committed state before the guards see it, because
    /// the body decided against a view that may since have moved: an update of a row someone deleted
    /// becomes an insert, a delete of a row already gone drops. Reconcile fixes op shape, never op
    /// value — it cannot rescue a lost increment, which is why the eligibility rule on
    /// <see cref="Isolation"/> is the feature's first documentation and not its last.
    /// </para>
    /// </summary>
    private ulong InvokeSnapshot(
        string reducerName,
        Identity caller,
        Action<ReducerContext> body,
        ReadOnlyMemory<byte> encodedArguments,
        ConnectionId connectionId,
        ActivityContext parentContext,
        IReadViewSource source)
    {
        using var activity = _telemetry?.StartReducer(reducerName, caller, arguments: null, encodedArguments, parentContext);
        activity?.SetTag("melange.isolation", "snapshot");
        var started = Stopwatch.GetTimestamp();

        // What the body sees. The log record gets its own timestamp at append time below: a body
        // that ran for 200 ms would otherwise stamp a record 200 ms older than one a serialized
        // transaction appended meanwhile, putting the log's timestamps out of order against its own
        // LSNs. The body still wants a stable start-of-transaction clock — a scheduled sweep derives
        // its next fire from it, and deriving that from commit time would drift the cadence by
        // however long the body happened to take.
        var bodyTimestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
        var writeSet = new WriteSet();
        var stage = _sequencer.BeginStage(reserveEagerly: true);
        var random = new Random(unchecked((int)bodyTimestamp.UnixTimeMicroseconds ^ caller.GetHashCode()));
        var events = new EventStage(_options.Events);

        IReadOnlyList<RowOp> ops;
        var bodyStarted = Stopwatch.GetTimestamp();
        double? bodyMs = null;
        try
        {
            // Disposed before the lock is taken: the pin is the body's, and holding it across the
            // commit would keep versions alive for no reader.
            using (var view = source.OpenReadView())
            {
                var context = new ReducerContext(
                    caller,
                    connectionId,
                    bodyTimestamp,
                    random,
                    new TransactionDb(Schema, view, writeSet, stage, _tableGuard),
                    events);
                body(context);
            }

            bodyMs = Elapsed(bodyStarted);
            ops = writeSet.ToOps();
        }
        catch (Exception exception)
        {
            // Aborted in the body, which held no lock — so unlike a serialized abort this one
            // stalled nobody, and the locked portion it reports is zero.
            ReportAbort(activity, reducerName, exception, Elapsed(started), bodyMs ?? Elapsed(bodyStarted), lockedMs: 0);
            throw;
        }

        ulong committedLsn = 0;
        var commitMs = 0d;
        double? fsyncMs = null;
        var postCommitMs = 0d;
        var lockedStarted = Stopwatch.GetTimestamp();
        double lockedMs;
        try
        {
            lock (_writeLock)
            {
                // Measured from inside: waiting for the lock is not holding it, and billing the wait
                // to this transaction would blame the queue on whoever happened to be last in it.
                lockedStarted = Stopwatch.GetTimestamp();
                ops = ReconcileOps(ops);
                RunCommitGuards(reducerName, ops, CommitOrigin.Reducer);
                if (ops.Count > 0 || events.Events is { Count: > 0 })
                {
                    using var commit = _telemetry?.StartCommit();
                    var commitStarted = Stopwatch.GetTimestamp();
                    var record = _log.Append(new CommitRequest(
                        Timestamp.FromDateTimeOffset(_time.GetUtcNow()), caller, reducerName, encodedArguments, ops, events.Events));
                    commitMs = Elapsed(commitStarted);
                    fsyncMs = _log.LastAppendFsyncMilliseconds;
                    _telemetry?.RecordCommitDuration(commitMs);
                    commit?.SetTag("melange.lsn", (long)record.Lsn);
                    commit?.SetTag("melange.writeset.bytes", record.SerializedLength);
                    var postCommitStarted = Stopwatch.GetTimestamp();
                    stage.Commit();
                    NotifyCommitObservers(record);
                    Appliers.NotifyAppended(record);
                    AfterCommit(record.Timestamp);
                    postCommitMs = Elapsed(postCommitStarted);
                    committedLsn = record.Lsn;
                }

                lockedMs = Elapsed(lockedStarted);
            }
        }
        catch (Exception exception)
        {
            // A guard rejected, or the append failed. The body's work is discarded; the ids it
            // reserved are not returned to the sequence, which is what "unique, not dense" buys.
            ReportAbort(activity, reducerName, exception, Elapsed(started), bodyMs.GetValueOrDefault(), Elapsed(lockedStarted));
            throw;
        }

        activity?.SetTag("melange.outcome", "commit");
        activity?.SetTag("melange.writeset.rows", ops.Count);
        var elapsed = Elapsed(started);
        _telemetry?.RecordTransaction(reducerName, "commit", elapsed, ops.Count);
        if (lockedMs > _options.Telemetry.SlowReducerMs)
            WarnSlowReducer(activity, reducerName, elapsed, bodyMs.GetValueOrDefault(), commitMs, fsyncMs, postCommitMs, ops.Count, lockedMs, Isolation.Snapshot);

        return committedLsn;
    }

    /// <summary>Shared abort reporting for the snapshot path's two failure points.</summary>
    private void ReportAbort(
        Activity? activity,
        string reducerName,
        Exception exception,
        double elapsed,
        double bodyMs,
        double lockedMs)
    {
        var outcome = exception is RejectedException ? "rejected" : "abort";
        activity?.SetTag("melange.outcome", outcome);
        activity?.SetTag("melange.writeset.rows", 0);
        activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
        _telemetry?.RecordTransaction(reducerName, outcome, elapsed, 0);
        if (lockedMs > _options.Telemetry.SlowReducerMs)
            WarnSlowAbort(activity, reducerName, outcome, elapsed, bodyMs, lockedMs, Isolation.Snapshot);
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
        int rows,
        double lockedMs,
        Isolation isolation)
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
                // The number the threshold actually fired on, and the one an alert about write
                // latency wants. For a serialized transaction it equals melange.duration_ms; for a
                // snapshot one the gap between them is the stall the feature removed.
                ["melange.locked_ms"] = lockedMs,
                ["melange.isolation"] = IsolationTag(isolation),
            };
            // Absent, not zero: under a deferred fsync policy there was no flush to attribute, and
            // a zero would read as "the disk was instant".
            if (fsyncMs is { } fsync)
                tags["melange.fsync_ms"] = fsync;
            activity.AddEvent(new ActivityEvent("melange.slow_reducer", tags: tags));
        }

        var threshold = _options.Telemetry.SlowReducerMs;
        var tag = IsolationTag(isolation);
        if (fsyncMs is { } inlineFsync)
            LogMessages.SlowReducer(_logger, reducerName, elapsed, threshold, bodyMs, commitMs, inlineFsync, postCommitMs, rows, lockedMs, tag);
        else
            LogMessages.SlowReducerDeferredFsync(_logger, reducerName, elapsed, threshold, bodyMs, commitMs, postCommitMs, rows, lockedMs, tag);
    }

    private static string IsolationTag(Isolation isolation) =>
        isolation == Isolation.Snapshot ? "snapshot" : "serialized";

    /// <summary>
    /// Reports one over-threshold transaction that aborted. For a serialized transaction, rolling
    /// back costs nothing and buys nothing: the write lock was held for the full duration either
    /// way, so a reducer that walks five thousand rows and then rejects the move stalls every other
    /// writer exactly as long as one that commits. For a snapshot transaction that threw in its
    /// body, the locked portion is zero and it stalled nobody — which is why the threshold is on
    /// <paramref name="lockedMs"/> and not on the total. Only the parts that happened are reported —
    /// there is no commit, no fsync, and no post-commit to attribute, and zeroes would invite the
    /// reader to average them.
    /// </summary>
    private void WarnSlowAbort(
        Activity? activity,
        string reducerName,
        string outcome,
        double elapsed,
        double bodyMs,
        double lockedMs,
        Isolation isolation)
    {
        activity?.AddEvent(new ActivityEvent(
            "melange.slow_reducer",
            tags: new ActivityTagsCollection
            {
                ["melange.duration_ms"] = elapsed,
                ["melange.body_ms"] = bodyMs,
                ["melange.outcome"] = outcome,
                ["melange.writeset.rows"] = 0,
                ["melange.locked_ms"] = lockedMs,
                ["melange.isolation"] = IsolationTag(isolation),
            }));
        LogMessages.SlowReducerAborted(
            _logger, reducerName, elapsed, outcome, _options.Telemetry.SlowReducerMs, bodyMs, lockedMs, IsolationTag(isolation));
    }

    private static double Elapsed(long startedTimestamp) =>
        Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds;

    private static partial class LogMessages
    {
        // Source-generated rather than LoggerMessage.Define like its siblings below: the split
        // carries more than the six type arguments Define offers, and every part has to stay a
        // structured field or an alert cannot key on the actionable half.

        /// <summary>
        /// 1003, in-line fsync: the whole split, including what durability cost.
        /// <para>
        /// <c>LockedMs</c> is the number the threshold fired on and the one that is global write
        /// latency; <c>DurationMs</c> is the whole transaction. Under
        /// <c>Isolation.Serialized</c> they are equal by construction. Under
        /// <c>Isolation.Snapshot</c> the gap between them is exactly the stall the isolation level
        /// removed, which makes a 500 ms serialized transaction and a 500 ms snapshot one
        /// distinguishable on the line rather than only in a trace.
        /// </para>
        /// </summary>
        [LoggerMessage(
            EventId = 1003,
            EventName = "SlowReducer",
            Level = LogLevel.Warning,
            Message = "Reducer '{Reducer}' held the write lock {LockedMs:F1}ms, over the Telemetry:SlowReducerMs " +
                      "threshold of {ThresholdMs}ms — {Isolation} isolation, {DurationMs:F1}ms in total, body " +
                      "{BodyMs:F1}ms, commit {CommitMs:F1}ms (fsync {FsyncMs:F1}ms), post-commit " +
                      "{PostCommitMs:F1}ms, {Rows} row ops.")]
        public static partial void SlowReducer(
            ILogger logger,
            string reducer,
            double durationMs,
            int thresholdMs,
            double bodyMs,
            double commitMs,
            double fsyncMs,
            double postCommitMs,
            int rows,
            double lockedMs,
            string isolation);

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
            Message = "Reducer '{Reducer}' held the write lock {LockedMs:F1}ms, over the Telemetry:SlowReducerMs " +
                      "threshold of {ThresholdMs}ms — {Isolation} isolation, {DurationMs:F1}ms in total, body " +
                      "{BodyMs:F1}ms, commit {CommitMs:F1}ms (fsync deferred by CommitLog:FsyncPolicy), " +
                      "post-commit {PostCommitMs:F1}ms, {Rows} row ops.")]
        public static partial void SlowReducerDeferredFsync(
            ILogger logger,
            string reducer,
            double durationMs,
            int thresholdMs,
            double bodyMs,
            double commitMs,
            double postCommitMs,
            int rows,
            double lockedMs,
            string isolation);

        /// <summary>
        /// 1003 for a transaction that aborted. Nothing was appended, so there is no commit, fsync,
        /// or post-commit field. <c>Outcome</c> separates a bug (<c>abort</c>) from an ordinary
        /// refusal (<c>rejected</c>) that happened to be expensive.
        /// <para>
        /// A serialized transaction held the lock for its whole duration whether it committed or
        /// not, so <c>LockedMs</c> equals <c>DurationMs</c> there. A snapshot transaction that threw
        /// in its body held nothing — it does not reach this warning at all, since the threshold is
        /// on the locked portion; one that was rejected by a guard reports only the commit attempt.
        /// </para>
        /// </summary>
        [LoggerMessage(
            EventId = 1003,
            EventName = "SlowReducerAborted",
            Level = LogLevel.Warning,
            Message = "Reducer '{Reducer}' held the write lock {LockedMs:F1}ms and then {Outcome}, over the " +
                      "Telemetry:SlowReducerMs threshold of {ThresholdMs}ms — {Isolation} isolation, " +
                      "{DurationMs:F1}ms in total, body {BodyMs:F1}ms, nothing appended.")]
        public static partial void SlowReducerAborted(
            ILogger logger,
            string reducer,
            double durationMs,
            string outcome,
            int thresholdMs,
            double bodyMs,
            double lockedMs,
            string isolation);

        /// <summary>
        /// 1004: a reducer declared <c>Isolation.Snapshot</c> but the configured hot store offers no
        /// pinned reads, so it ran serialized instead. Once per engine — the point is that the
        /// degradation is stated, not that every call restates it.
        /// </summary>
        [LoggerMessage(
            EventId = 1004,
            EventName = "SnapshotIsolationUnavailable",
            Level = LogLevel.Warning,
            Message = "A reducer declared Isolation.Snapshot but the configured hot store '{Store}' does not " +
                      "implement IReadViewSource, so snapshot-isolated reducers on this engine run serialized " +
                      "and hold the write lock for their whole body. They remain correct; they are not faster. " +
                      "This is reported once.")]
        public static partial void SnapshotIsolationUnavailable(ILogger logger, string store);

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

        private static readonly Action<ILogger, ulong, Exception?> SnapshotAlreadyRunningMessage =
            LoggerMessage.Define<ulong>(
                LogLevel.Debug,
                new EventId(1509, "SnapshotAlreadyRunning"),
                "Snapshot at LSN {Lsn} skipped: another snapshot is still writing. Snapshots write " +
                "outside the write lock, so an interval short enough to overlap one is the signal — " +
                "raise Snapshots:IntervalTransactions rather than treating this as an error.");

        public static void SnapshotAlreadyRunning(ILogger logger, ulong lsn) =>
            SnapshotAlreadyRunningMessage(logger, lsn, null);

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
