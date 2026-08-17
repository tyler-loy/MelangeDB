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
    private readonly ShapeResolution _shapes;
    private readonly EngineTelemetry? _telemetry;
    private readonly Lock _writeLock = new();
    private readonly ThreadLocal<bool> _inReducer = new();
    private readonly List<ICommitObserver> _commitObservers = [];
    private readonly List<ICommitGuard> _commitGuards = [];
    private readonly List<(string Name, Func<ulong?> Provider)> _truncationFloors = [];
    private readonly List<TruncationPin> _truncationPins = [];

    /// <summary>The last truncation decision's floors; null until one has been made. See <see cref="TruncationFloors"/>.</summary>
    private TruncationFloorReport? _floorReport;
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

    /// <summary>Cumulative Stopwatch ticks of write-lock work; see <see cref="WriteLockBusyTicks"/>.</summary>
    private long _writeLockBusyTicks;

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
                () => Appliers?.Lags() ?? [],
                () => _floorReport)
            : null;
        try
        {
            SnapshotPath = Path.Combine(options.CommitLog.Path, SnapshotFile.FileName);

            // The snapshot's LSN is the log's durable floor: recovery uses it to tell damaged
            // committed history (fatal) from a crash's torn tail (truncated) — under buffered
            // group commit a tail can span several records. Peeked before the log opens because
            // recovery is where the distinction is drawn.
            _log = new FileCommitLog(
                options.CommitLog,
                loggers.CreateLogger<FileCommitLog>(),
                _telemetry,
                SnapshotFile.DurableFloor(options.CommitLog.Path));
            _sequencer = new AutoIncSequencer(originator);

            // Shape governance before any row byte is interpreted: load (or adopt) the shape
            // sidecar, refuse a destructive schema change, and hold the additive transform that
            // recovery routes every snapshot row and tail record through. See ShapeGuard.
            _shapes = ShapeGuard.Resolve(options.CommitLog.Path, schema, _log.BaseLsn);
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
                var replay = _shapes.TransformRecord(record);
                store.Apply(replay);
                _sequencer.Observe(replay, schema);
                RecoveredTailTimestamp = replay.Timestamp;
            }

            bulk?.CompleteRecovery();

            _tailTimestamp = RecoveredTailTimestamp;
            HotStore = store;
            _readViewSource = store as IReadViewSource;
            Appliers = new ApplierPipeline(_log, _telemetry, _shapes.TransformRecord, _log.ReadUncapped);
            Appliers.Register(new HotStoreApplier(store));
            _telemetry?.SetHotStoreStatisticsProvider(store.Statistics);
            _truncationFloors.Add((TruncationFloorNames.BackupPin, PinnedTruncationFloor));
            if (options.Residency.ReportOnStartup)
                ReportResidency(store);
            CompleteShapeMigration(store);
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
    /// Finishes an additive schema migration after recovery has rebuilt the projections under the
    /// booting schema. The order is the crash-safety argument: an empty-write-set <em>marker
    /// record</em> is appended first, so the new shape's reign begins at an LSN no existing row
    /// was written under; the sidecar entry is saved second (a crash between the two leaves a
    /// harmless empty record and the next boot simply re-migrates); the immediate snapshot comes
    /// last and lands <em>at</em> the marker's LSN, which is what makes every snapshot's shape
    /// unambiguously the one governing its own LSN — without the marker, a snapshot taken at
    /// exactly the pre-migration head could be either shape and recovery could not tell.
    /// </summary>
    private void CompleteShapeMigration(IHotStore store)
    {
        if (!_shapes.MigrationPending)
            return;

        var request = new CommitRequest(
            Timestamp.FromDateTimeOffset(_time.GetUtcNow()),
            ShapeMarkerCaller,
            ShapeMarkerReducer,
            ReadOnlyMemory<byte>.Empty,
            WriteSet: []);
        CommitRecord marker;
        lock (_writeLock)
        {
            marker = _log.Append(request);
            store.Apply(marker);
        }

        // The sidecar must never claim a reign whose marker a crash could untell: durable first,
        // then the entry. (The reverse crash order — marker durable, sidecar unsaved — is the
        // harmless one the design already accepts: an empty record, and the next boot re-migrates.)
        _log.WaitDurable(marker.Lsn);
        _shapes.History.Append(ShapeHistory.EntryOf(Schema, marker.Lsn));
        _shapes.History.Save(_shapes.SidecarPath);
        LogMessages.ShapeMigrated(_logger, string.Join("; ", _shapes.Changes), marker.Lsn);

        // Seal the migration so the transform never runs for these rows again. Correctness does
        // not depend on it — every boot decodes by LSN through the history — so a disabled
        // snapshotter or a snapshot failure just means the next boot transforms again.
        TakeSnapshot();
    }

    /// <summary>What a shape-migration marker record runs as; the melange/init precedent.</summary>
    internal static Identity ShapeMarkerCaller { get; } = Identity.Hash("melange/shape");

    /// <summary>The marker's reducer name — metadata for audit, never replayed, like every reducer name.</summary>
    internal const string ShapeMarkerReducer = "__melange/shape";

    /// <summary>The shape history and transform, for readers that decode records below the current reign.</summary>
    internal ShapeResolution Shapes => _shapes;

    /// <summary>
    /// Re-encodes a record's rows to the booting schema's shape, or returns it unchanged when its
    /// shape already matches — one LSN compare in the common case. Every reader that decodes row
    /// bytes from records it did not just watch commit (a lagging applier's own catch-up loop, a
    /// resume replay) must route records through this: a record written before the last additive
    /// schema migration carries its columns in the old order, and decoding it under the current
    /// schema without this call reads plausible garbage. Pipeline-driven appliers get it
    /// automatically; decoupled readers call it themselves.
    /// </summary>
    public CommitRecord TransformToCurrentShape(CommitRecord record) => _shapes.TransformRecord(record);

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

        store.LoadSnapshot(header.Lsn, _shapes.TransformSnapshotRows(header.Lsn, reader.Rows()));
        foreach (var (table, next) in header.Sequences)
            _sequencer.RestoreSequence(table, next);
        RecoveredTailTimestamp = header.Timestamp;
        return header.Lsn + 1;
    }

    public SchemaRegistry Schema { get; }

    public ICommitLog Log => _log;

    /// <summary>
    /// The log as the file it is — path, buffer flush, base sidecar — for the online backup's
    /// direct file walk. Internal on purpose: <see cref="Log"/> is the abstraction everything
    /// else programs against.
    /// </summary>
    internal FileCommitLog LogFile => _log;

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
    /// The engine's single write lock is held across body, commit guards, the buffered append,
    /// commit observers, and the store apply — so time spent in the body is global write latency:
    /// no other transaction on this engine can start until it commits. The durability wait runs
    /// <em>after</em> the lock releases, which is what lets concurrent commits share fsyncs under
    /// <see cref="FsyncPolicy.OnCommit"/> (group commit) — while this call still returns only once
    /// its record is durable, exactly as before. Readers are unaffected
    /// (<see cref="CommittedView"/> takes no lock). Window long sweeps across many short
    /// transactions rather than running one long one.
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
            var lockStarted = Stopwatch.GetTimestamp();
            try
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
            finally
            {
                Interlocked.Add(ref _writeLockBusyTicks, Stopwatch.GetTimestamp() - lockStarted);
            }
        }

        // The commit point, outside the lock — the group-commit split; see InvokeCore.
        _log.WaitDurable(record.Lsn);
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
            var started = Stopwatch.GetTimestamp();
            try
            {
                return read(_log.HeadLsn);
            }
            finally
            {
                Interlocked.Add(ref _writeLockBusyTicks, Stopwatch.GetTimestamp() - started);
            }
        }
    }

    /// <summary>Runs an action under the write lock; see <see cref="ReadConsistent{T}"/>.</summary>
    public void ReadConsistent(Action<ulong> read)
    {
        ArgumentNullException.ThrowIfNull(read);
        lock (_writeLock)
        {
            var started = Stopwatch.GetTimestamp();
            try
            {
                read(_log.HeadLsn);
            }
            finally
            {
                Interlocked.Add(ref _writeLockBusyTicks, Stopwatch.GetTimestamp() - started);
            }
        }
    }

    /// <summary>
    /// Cumulative Stopwatch ticks the write lock has been held for work: reducer transactions
    /// (whole body under <see cref="Isolation.Serialized"/>, commit portion under
    /// <see cref="Isolation.Snapshot"/>), internal applies, bulk ingestion, and consistent reads.
    /// Monotonic — sample twice and divide the delta by the elapsed Stopwatch ticks for the
    /// engine's commit-loop utilization over the interval. This is the saturation signal the
    /// cluster's load view carries: the published hotspot ceilings (docs/CLUSTERING.md) are
    /// ceilings on exactly this resource. Time spent <em>waiting</em> for the lock is deliberately
    /// not counted — a queue is evidence of saturation, not more capacity spent.
    /// </summary>
    public long WriteLockBusyTicks => Interlocked.Read(ref _writeLockBusyTicks);

    /// <summary>Accumulates into <see cref="WriteLockBusyTicks"/> from construction to disposal.</summary>
    private BusyScope TrackWriteLockBusy() => new(this);

    private readonly struct BusyScope(MelangeEngine engine) : IDisposable
    {
        private readonly long _started = Stopwatch.GetTimestamp();

        public void Dispose() => Interlocked.Add(ref engine._writeLockBusyTicks, Stopwatch.GetTimestamp() - _started);
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
            using var busy = TrackWriteLockBusy();
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

        // The commit point, outside the lock — the group-commit split; see InvokeCore.
        _log.WaitDurable(bulkRecord.Lsn);
        CompleteDeferredSnapshot();
        return bulkRecord;
    }

    /// <summary>
    /// Registers a named truncation floor: a provider of the highest LSN log compaction may remove
    /// from that consumer's perspective (its checkpoint). Null means the consumer pins nothing. The
    /// event bus registers <c>MinimumLiveCheckpointLsn</c> here so truncation never strands a
    /// subscriber that is merely behind.
    /// <para>
    /// The name is what makes a pinned log diagnosable — it is the <c>melange.log.truncation_floor</c>
    /// tag, the floor named in every truncation's log line, and the holder the
    /// <c>melange-retention</c> health check points at. Use a stable mechanism name, not a
    /// per-instance one: it is a metric dimension. <see cref="TruncationFloorNames"/> holds the
    /// built-in set.
    /// </para>
    /// </summary>
    public void AddTruncationFloor(string name, Func<ulong?> floor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(floor);
        lock (_writeLock)
        {
            _truncationFloors.Add((name, floor));
        }
    }

    /// <summary>
    /// Registers an unnamed truncation floor — <see cref="AddTruncationFloor(string, Func{ulong?})"/>
    /// under <see cref="TruncationFloorNames.Unnamed"/>. Kept rather than broken pre-1.0 for the
    /// sake of one string: a third-party floor that never names itself still has to show up in the
    /// report, and showing up as "unnamed" is itself the diagnosis.
    /// </summary>
    public void AddTruncationFloor(Func<ulong?> floor) => AddTruncationFloor(TruncationFloorNames.Unnamed, floor);

    /// <summary>
    /// The floors as they stood at the last truncation decision, or null if none has been made —
    /// snapshots disabled, <c>Snapshots:TruncateLog</c> off, or simply no snapshot yet. Read by
    /// the <c>melange.log.*</c> gauges and the <c>melange-retention</c> health check; see
    /// <see cref="TruncationFloorReport"/> for why it is cached rather than evaluated on demand.
    /// </summary>
    public TruncationFloorReport? TruncationFloors => _floorReport;

    /// <summary>
    /// Pins log truncation at the current base for the lifetime of the returned handle: while any
    /// pin is held, compaction removes nothing beyond where it already stood. This is the bounded
    /// counterpart to <see cref="AddTruncationFloor"/> — a floor is a permanent registration whose
    /// provider decides per tick, a pin is a scoped lease with an explicit release — and it exists
    /// for the online backup, which must stream a snapshot and the records above it while commits
    /// continue. Taken under the write lock so no truncation can interleave between reading the
    /// base and pinning it.
    /// </summary>
    public IDisposable PinTruncation()
    {
        lock (_writeLock)
        {
            var pin = new TruncationPin(this, _log.BaseLsn);
            _truncationPins.Add(pin);
            return pin;
        }
    }

    /// <summary>The pins' collective floor; registered in the constructor beside the other floors.</summary>
    private ulong? PinnedTruncationFloor()
    {
        // Callers hold the write lock: floors are only evaluated inside TruncateLogCore.
        if (_truncationPins.Count == 0)
            return null;
        var floor = ulong.MaxValue;
        foreach (var pin in _truncationPins)
            floor = Math.Min(floor, pin.BaseLsn);
        return floor;
    }

    private sealed class TruncationPin(MelangeEngine engine, ulong baseLsn) : IDisposable
    {
        public ulong BaseLsn { get; } = baseLsn;

        public void Dispose()
        {
            lock (engine._writeLock)
            {
                engine._truncationPins.Remove(this);
            }
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
            // A snapshot file at LSN N is a durability claim about N: recovery trusts it as the
            // log's durable floor, and a crash after the file lands but before the log's N is on
            // disk would boot a snapshot ahead of its own log and re-mint LSNs the snapshot
            // already holds. So the log goes durable through N first — under every policy, which
            // also closes the same (pre-existing) window for Interval and OsBuffered at the cost
            // of one fsync per snapshot. If the log cannot promise it (poisoned mid-flush), the
            // snapshot is abandoned rather than written ahead of durability.
            if (_log.FsyncedLsn < pending.Lsn)
                _log.FlushToDisk();
            if (_log.FsyncedLsn < pending.Lsn)
            {
                throw new InvalidOperationException(
                    $"The snapshot at LSN {pending.Lsn} was abandoned: the commit log could not be " +
                    "made durable through it. See the melange-log health check for the log's failure.");
            }

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
    /// subscribers, backup pins, cluster handoff markers), and the Resume retention window — a
    /// reconnecting client's gap must stay servable from the log for
    /// <c>Resume:RetentionWindowSeconds</c>.
    /// <para>
    /// Every floor is named and the whole reading is kept (<see cref="TruncationFloors"/>), because
    /// the operator's question is never "what is the floor" but "who is holding it". The decision
    /// logs either way — a truncation that removes nothing <em>because</em> a floor pinned it is
    /// the interesting case, and it used to be perfectly silent.
    /// </para>
    /// </summary>
    private void TruncateLogCore(ulong snapshotLsn)
    {
        // The snapshot is the ceiling and the first candidate, so a healthy log reports "snapshot"
        // as its governing floor: nothing is holding anything back.
        var floors = new List<TruncationFloor>(_truncationFloors.Count + Appliers.Appliers.Count + 2)
        {
            new(TruncationFloorNames.Snapshot, snapshotLsn),
        };
        var floor = snapshotLsn;
        foreach (var applier in Appliers.Appliers)
        {
            floors.Add(new TruncationFloor(applier.Name, applier.AppliedLsn));
            floor = Math.Min(floor, applier.AppliedLsn);
        }

        foreach (var (name, provider) in _truncationFloors)
        {
            if (provider() is not { } pinned)
                continue;
            floors.Add(new TruncationFloor(name, pinned));
            floor = Math.Min(floor, pinned);
        }

        // The retention window is scanned only as far as the floor the other holders already set —
        // the record that binds it, if any, is the oldest one still inside the window. When nothing
        // binds, the reading is the ceiling it scanned to: "permits removing at least this much",
        // which keeps the floor's tag present rather than flickering in and out of the metric.
        var resumeFloor = floor;
        var retentionCutoff = _time.GetUtcNow().AddSeconds(-_options.Resume.RetentionWindowSeconds);
        var cutoffMicros = Timestamp.FromDateTimeOffset(retentionCutoff).UnixTimeMicroseconds;
        foreach (var record in _log.ReadFrom(_log.BaseLsn + 1))
        {
            if (record.Lsn > floor)
                break;
            if (record.Timestamp.UnixTimeMicroseconds >= cutoffMicros)
            {
                resumeFloor = record.Lsn - 1;
                break;
            }
        }

        floors.Add(new TruncationFloor(TruncationFloorNames.ResumeWindow, resumeFloor));
        floor = Math.Min(floor, resumeFloor);

        var governing = floors[0];
        foreach (var candidate in floors)
        {
            if (candidate.Lsn < governing.Lsn)
                governing = candidate;
        }

        var head = _log.HeadLsn;
        var pinnedRecords = head > floor ? head - floor : 0;
        if (floor <= _log.BaseLsn)
        {
            // Nothing removable. Either the log is already compacted to the floor, or a holder has
            // stopped moving — the same shape of line, because the operator cannot tell which from
            // silence.
            _floorReport = new TruncationFloorReport(snapshotLsn, head, _log.BaseLsn, floor, governing, floors);
            LogMessages.LogTruncationPinned(
                _logger, governing.Name, governing.Lsn, _log.BaseLsn, head, pinnedRecords, _log.FileLengthBytes);
            return;
        }

        _log.TruncateBefore(floor);
        _floorReport = new TruncationFloorReport(snapshotLsn, head, _log.BaseLsn, floor, governing, floors);
        LogMessages.LogTruncated(
            _logger, floor, snapshotLsn, governing.Name, governing.Lsn, pinnedRecords, _log.FileLengthBytes);
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
        ulong committedLsn = 0;
        var commitMs = 0d;
        var postCommitMs = 0d;
        double committedBodyMs;
        double lockedMs;
        long started;
        int rowCount;
        lock (_writeLock)
        {
            using var busy = TrackWriteLockBusy();
            // Read inside the lock: waiting for the lock is not holding it, and billing the wait
            // to this transaction would blame the queue on whoever happened to be last in it.
            started = Stopwatch.GetTimestamp();
            var timestamp = Timestamp.FromDateTimeOffset(_time.GetUtcNow());
            var writeSet = new WriteSet();
            var stage = _sequencer.BeginStage();
            var random = new Random(unchecked((int)timestamp.UnixTimeMicroseconds ^ caller.GetHashCode()));
            var events = new EventStage(_options.Events);
            var context = new ReducerContext(caller, connectionId, timestamp, random, new TransactionDb(Schema, HotStore, writeSet, stage, _tableGuard), events);

            IReadOnlyList<RowOp> ops;
            // Measured directly rather than as (total - commit): everything after the append —
            // commit observers, applier notification, an automatic snapshot — is inside the same
            // span, and subtracting would charge all of it to the module's reducer body. Declared
            // out here so the abort path can report it too; null there means the body itself is
            // what threw.
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
                // was never committed — zero trace, no consumed AutoInc value. The write lock was
                // still held for the whole of it, so a slow abort is reported exactly like a slow
                // commit.
                var outcome = exception is RejectedException ? "rejected" : "abort";
                // The part before the whole: reading the body's clock after the transaction's would
                // let the work between the two readings push a trivial abort's body past its own
                // duration.
                var abortedBodyMs = bodyMs ?? Elapsed(bodyStarted);
                var abortedAfter = Elapsed(started);
                activity?.SetTag("melange.outcome", outcome);
                activity?.SetTag("melange.writeset.rows", 0);
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                _telemetry?.RecordTransaction(reducerName, outcome, abortedAfter, 0);
                // An abort has no durability wait, so its whole is its locked portion.
                if (abortedAfter > _options.Telemetry.SlowReducerMs)
                    WarnSlowAbort(activity, reducerName, outcome, abortedAfter, abortedBodyMs, abortedAfter, Isolation.Serialized);
                throw;
            }

            if (ops.Count > 0 || events.Events is { Count: > 0 })
            {
                using var commit = _telemetry?.StartCommit();
                var commitStarted = Stopwatch.GetTimestamp();
                var record = _log.Append(new CommitRequest(timestamp, caller, reducerName, encodedArguments, ops, events.Events));
                commitMs = Elapsed(commitStarted);
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

            rowCount = ops.Count;
            committedBodyMs = bodyMs.GetValueOrDefault();
            lockedMs = Elapsed(started);
        }

        // The commit point, outside the lock: the record is buffered, and this is where it becomes
        // durable — the next transaction's body runs while this one waits, which is what forms
        // fsync batches. Everything ordering-sensitive (observers before the store applied, the
        // store applied before the lock released) already happened; only the caller's return gates
        // on durability. A durability failure throws here: the transaction is reported failed even
        // though the in-memory projection applied it, and the poisoned log stops every later
        // commit until a restart reconverges state from the durable prefix.
        double? durabilityWaitMs = null;
        if (committedLsn != 0)
            durabilityWaitMs = _log.WaitDurable(committedLsn);

        activity?.SetTag("melange.outcome", "commit");
        activity?.SetTag("melange.writeset.rows", rowCount);
        var elapsed = Elapsed(started);
        _telemetry?.RecordTransaction(reducerName, "commit", elapsed, rowCount);
        if (elapsed > _options.Telemetry.SlowReducerMs)
            WarnSlowReducer(activity, reducerName, elapsed, committedBodyMs, commitMs, durabilityWaitMs, postCommitMs, rowCount, lockedMs, Isolation.Serialized);

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
                using var busy = TrackWriteLockBusy();
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

            // The commit point, outside the lock — the group-commit split; see InvokeCore, which
            // explains why only the caller's return (never the fan-out ordering) gates on it.
            if (committedLsn != 0)
                fsyncMs = _log.WaitDurable(committedLsn);
        }
        catch (Exception exception)
        {
            // A guard rejected, the append failed, or the batch fsync covering this record failed.
            // The body's work is discarded; the ids it reserved are not returned to the sequence,
            // which is what "unique, not dense" buys.
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
            // The durability wait this caller actually experienced — under group commit that is
            // the batch's cost from this transaction's seat, not a private fsync. Absent, not
            // zero, when the policy defers durability off the commit path entirely (Interval,
            // OsBuffered): there was no wait to attribute, and a zero would read as "the disk
            // was instant".
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

        private static readonly Action<ILogger, string, ulong, Exception?> ShapeMigratedMessage =
            LoggerMessage.Define<string, ulong>(
                LogLevel.Warning,
                new EventId(1006, "ShapeMigrated"),
                "Additive schema migration: {Changes}. Existing rows were rebuilt under the new shape; " +
                "the new shape governs from LSN {MarkerLsn}. Automatic must never mean silent — this is that line.");

        public static void ShapeMigrated(ILogger logger, string changes, ulong markerLsn) =>
            ShapeMigratedMessage(logger, changes, markerLsn, null);

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

        private static readonly Action<ILogger, ulong, ulong, string, ulong, ulong, long, Exception?> LogTruncatedMessage =
            LoggerMessage.Define<ulong, ulong, string, ulong, ulong, long>(
                LogLevel.Information,
                new EventId(1503, "LogTruncated"),
                "Commit log truncated up to LSN {Floor} behind the snapshot at LSN {SnapshotLsn}; " +
                "governing floor '{FloorName}' at LSN {FloorLsn}, {PinnedRecords} record(s) behind " +
                "the head. Log file is now {LogBytes} byte(s).");

        public static void LogTruncated(
            ILogger logger, ulong floor, ulong snapshotLsn, string floorName, ulong floorLsn, ulong pinnedRecords, long logBytes) =>
            LogTruncatedMessage(logger, floor, snapshotLsn, floorName, floorLsn, pinnedRecords, logBytes, null);

        private static readonly Action<ILogger, string, ulong, ulong, ulong, ulong, long, Exception?> LogTruncationPinnedMessage =
            LoggerMessage.Define<string, ulong, ulong, ulong, ulong, long>(
                LogLevel.Information,
                new EventId(1510, "LogTruncationPinned"),
                "Commit log truncation removed nothing: floor '{FloorName}' at LSN {FloorLsn} holds " +
                "the base at LSN {BaseLsn}, with the head at LSN {HeadLsn} — {PinnedRecords} " +
                "record(s), {LogBytes} byte(s) pinned. Everything older than the floor stays on " +
                "disk until that holder checkpoints past it.");

        public static void LogTruncationPinned(
            ILogger logger, string floorName, ulong floorLsn, ulong baseLsn, ulong headLsn, ulong pinnedRecords, long logBytes) =>
            LogTruncationPinnedMessage(logger, floorName, floorLsn, baseLsn, headLsn, pinnedRecords, logBytes, null);

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
