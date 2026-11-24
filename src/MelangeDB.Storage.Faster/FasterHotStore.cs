using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using MelangeDB.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// The paged hot store: paged tables live in a storage engine's key-value spaces (FASTER hybrid logs
/// by default; <c>HotStore:PagedEngine</c> selects another, see <see cref="IPagedKv"/>) whose
/// in-memory portion is capped by <c>HotStore:MemoryBudgetBytes</c>, so the working set bounds memory instead of the
/// dataset; resident tables are pinned wholly in managed memory outside that budget. Large
/// <c>byte[]</c> payloads are stored out of line in a second hybrid log, so a blob table's main
/// records stay small and scanning by key faults no blobs. Store-owned indexes: the key directory
/// and every secondary index (equality and range) are kept in managed memory beside the data, with
/// each row's indexed values recorded on its directory entry so index maintenance never reads the
/// old row back from disk.
///
/// <para>Recovery is the engine's, not FASTER's — a settled phase 07 trade. The commit log (plus
/// its snapshot) is the source of truth and this store is a projection, rebuilt through
/// <see cref="LoadSnapshot"/> and log replay on <b>every</b> start, clean shutdown included:
/// FASTER's checkpoint/recovery machinery stays entirely out of the picture, one recovery story
/// covers both store engines, and crash consistency is inherited from the log. The cost is startup
/// time proportional to snapshot size — sequential I/O, seconds at the reference scale.</para>
///
/// <para><b>Pinned reads</b> (<see cref="IReadViewSource"/>) are split the way the store itself is.
/// Everything in managed memory — the key directory, the secondary indexes, and a resident table's
/// rows — is held in persistent containers, so a view captures those by reference exactly as the
/// in-memory store does. A <em>paged</em> row's payload cannot be captured that way: it lives in the
/// hybrid log, where an upsert overwrites in place and leaves no old version to read. Those are
/// covered by an undo overlay instead — while any view is open, a write to a paged row first stashes
/// the row's pre-image on every view that has not already recorded one. The cost is therefore
/// proportional to <em>writes during the window</em>, not to table size: a sweep scanning a million
/// rows while fifty change pays for fifty.</para>
/// </summary>
public sealed class FasterHotStore : IHotStore, IResidencyControl, IReadViewSource, IBulkRecovery, IDisposable
{
    private readonly SchemaRegistry _registry;
    private readonly ILogger _logger;
    private readonly long _autoThresholdBytes;
    private readonly Lock _lock = new();
    private readonly Dictionary<TableId, TableState> _tables = [];
    private readonly List<ReadView> _openViews = [];

    private readonly IPagedKv _main;
    private readonly IPagedKv? _blob;
    private readonly long _bufferPoolCapacityBytes;

    /// <summary>Bytes a compaction step may copy before it gives the store lock back.</summary>
    private const long CompactionStepBytes = 1 << 20;

    /// <summary>How long the compaction thread sleeps when no space has anything to reclaim.</summary>
    private static readonly TimeSpan CompactionIdle = TimeSpan.FromMilliseconds(100);

    private readonly HotStoreCompactionOptions _compaction;
    private readonly string _hotStorePath;

    /// <summary>The lowest compaction trigger ratio: the setting's floor, and what pressure compacts at.</summary>
    private const double MinTriggerRatio = 1.1;
    private readonly CancellationTokenSource _compactionStop = new();
    private readonly Thread? _compactor;

    // Per space (0 main, 1 blob): compaction steps taken, and the time they held the store lock.
    private readonly long[] _compactionSteps = new long[2];
    private readonly long[] _compactionLockTicks = new long[2];

    /// <summary>Reused buffer for composed store and blob keys; see <see cref="StoreKey"/>.</summary>
    private byte[] _keyScratch = new byte[64];
    private bool _disposed;

    public FasterHotStore(HotStoreContext context)
        : this(context, FasterPagedKvFactory.Instance)
    {
    }

    /// <summary>
    /// The store over another engine's key-value spaces: everything but where the paged bytes live
    /// is this class's own. FASTER is the shipped engine; the constructor is how a bake-off swaps it.
    /// </summary>
    internal FasterHotStore(HotStoreContext context, IPagedKvFactory engine)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(engine);
        _registry = context.Schema;
        _logger = context.LoggerFactory.CreateLogger<FasterHotStore>();
        EngineName = engine.EngineName;
        LogMessages.PagedEngine(_logger, engine.EngineName);
        _autoThresholdBytes = context.Options.Residency.AutoThresholdBytes;

        var anyBlobColumns = context.Schema.Tables.Any(RowBlobSplitter.HasBytesColumns);
        var budget = Math.Max(context.Options.HotStore.MemoryBudgetBytes, 2L << 20);
        var totalBits = Math.Clamp(63 - BitOperations.LeadingZeroCount((ulong)budget), 21, 34);
        var mainMemoryBits = anyBlobColumns ? totalBits - 1 : totalBits;
        var blobMemoryBits = totalBits - 1;
        var hashBuckets = HashBuckets(context.Options.HotStore);

        // The store is rebuilt from snapshot + log replay on every start, so files left by a
        // previous run (clean or crashed) are stale projections; each space opens empty.
        var path = context.Options.HotStore.Path;
        Directory.CreateDirectory(path);
        _main = engine.Open(path, PagedKvSpace.Main, 1L << mainMemoryBits, hashBuckets);
        if (anyBlobColumns)
            _blob = engine.Open(path, PagedKvSpace.Blob, 1L << blobMemoryBits, hashBuckets);
        _bufferPoolCapacityBytes = _main.CapacityBytes + (_blob?.CapacityBytes ?? 0);

        foreach (var table in context.Schema.Tables)
        {
            var declared = context.Residency.GetValueOrDefault(table.Id, table.Residency);
            _tables.Add(table.Id, new TableState(table, declared, EmptyTrees));
        }

        _compaction = context.Options.HotStore.Compaction;
        if (!(_compaction.TriggerRatio >= MinTriggerRatio))
            throw new ArgumentOutOfRangeException(nameof(context), _compaction.TriggerRatio, "HotStore:Compaction:TriggerRatio must be at least 1.1.");
        if (_compaction.MaxLogBytes < 0 || _compaction.MinFreeDiskBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(context), "HotStore:Compaction:MaxLogBytes and MinFreeDiskBytes must not be negative.");
        _hotStorePath = Path.GetFullPath(path);
        if (_compaction.MaxBytesPerSecond < 0)
            throw new ArgumentOutOfRangeException(nameof(context), _compaction.MaxBytesPerSecond, "HotStore:Compaction:MaxBytesPerSecond must not be negative.");
        if (_compaction.Enabled && (_main is ICompactingPagedKv || _blob is ICompactingPagedKv))
        {
            _compactor = new Thread(CompactionLoop) { IsBackground = true, Name = "melange-hotstore-compaction" };
            _compactor.Start();
        }
    }

    public ulong AppliedLsn { get; private set; }

    /// <summary>The kind of ordered bookkeeping new tables start with (see <see cref="TableTrees"/>).</summary>
    private static TableTrees EmptyTrees { get; } = ManagedTableTrees.Create(0);

    /// <summary>Publishes every table's open write (a no-op for tables the write did not touch, and during recovery).</summary>
    private void PublishWrites()
    {
        foreach (var table in _tables.Values)
            table.Publish();
    }

    /// <summary>The engine under the paged tables (EventId 1512 logs it at construction).</summary>
    internal string EngineName { get; }

    public void LoadSnapshot(ulong lsn, IEnumerable<SnapshotRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        lock (_lock)
        {
            if (AppliedLsn != 0)
                throw new InvalidOperationException("A snapshot loads only into an empty store, before any record applies.");
            try
            {
                foreach (var row in rows)
                {
                    if (_tables.TryGetValue(row.Table, out var table))
                        PutRow(table, row.Key, row.Row);
                }
            }
            finally
            {
                PublishWrites();
            }

            AppliedLsn = lsn;
        }
    }

    /// <summary>
    /// Enters recovery's bulk mode — see <see cref="IBulkRecovery"/>. The managed state (resident
    /// rows, the key directory, secondary indexes) accumulates in builders; FASTER upserts are
    /// unchanged, since the hybrid log was never the cost. Recovery replays 269MB logs one
    /// single-op record at a time, and each op was paying a path copy of its table's containers
    /// for a version no reader could observe.
    /// </summary>
    public void BeginRecovery()
    {
        lock (_lock)
        {
            if (_openViews.Count > 0)
                throw new InvalidOperationException("Recovery bulk mode cannot begin while a read view is open.");
            foreach (var table in _tables.Values)
                table.BeginBulk();
        }
    }

    public void CompleteRecovery()
    {
        lock (_lock)
        {
            foreach (var table in _tables.Values)
                table.CompleteBulk();
        }
    }

    public void Apply(CommitRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        lock (_lock)
        {
            if (record.Lsn <= AppliedLsn)
                return;
            try
            {
                foreach (var op in record.WriteSet)
                {
                    if (!_tables.TryGetValue(op.Table, out var table))
                        continue; // A table this projection doesn't know; nothing to project.
                    if (op.Kind == RowOpKind.Delete)
                        DeleteRow(table, op.Key);
                    else
                        PutRow(table, op.Key, op.Row);
                }
            }
            finally
            {
                // One version per record, so a reader never sees half of one. Published even when
                // an op threw: the ops before it reached the paged engine, and the engine closes to
                // writes on an apply failure rather than build on a partial record.
                PublishWrites();
            }

            AppliedLsn = record.Lsn;
        }
    }

    public bool ContainsKey(TableId table, in RowKey key)
    {
        if (!_tables.TryGetValue(table, out var state))
            return false;

        // The key directory (paged) or the resident row map answers this — no FASTER read, no
        // out-of-line payload. The version is immutable, so no lock is needed, exactly as the
        // resident TryGetRow fast path reasons.
        var version = state.Current;
        return version.Contains(key);
    }

    public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row)
    {
        if (!_tables.TryGetValue(table, out var state))
        {
            row = default;
            return false;
        }

        // One volatile read of an immutable version. A resident table's rows are in managed memory,
        // so answering from this version needs no lock at all — and this is the read the engine's
        // fan-out makes per op, under the *engine* write lock, to fetch a pre-image. Taking the
        // store lock there was contention on the one path that can least afford it.
        //
        // A concurrent demotion cannot make this stale: the version is captured once, and either it
        // is the resident one (which has the rows) or it is not, in which case the paged path runs.
        var version = state.Current;
        if (version.IsResident)
        {
            var found = version.ResidentRows.TryGetValue(key, out var resident);
            row = found ? resident : default;
            return found;
        }

        // The paged path keeps the lock, and not for the session's sake: the hybrid log overwrites
        // in place, so the directory probe and the record read must be atomic against a write, or a
        // reader can hold a directory entry whose record a concurrent delete has already removed.
        lock (_lock)
        {
            var bytes = ReadRow(state, key);
            row = bytes;
            return bytes is not null;
        }
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table)
    {
        TableState? resident = null;
        lock (_lock)
        {
            if (_tables.TryGetValue(table, out var state) && state.IsResident)
                resident = state;
        }

        if (resident is not null)
        {
            // Residency's whole promise: a resident scan iterates managed memory directly, the
            // same shape as the in-memory store — no key snapshot, no per-row lookup.
            foreach (var pair in resident.ResidentRows)
            {
                resident.AddRowsScanned(1);
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(pair.Key, pair.Value);
            }

            yield break;
        }

        foreach (var pair in ScanInBatches(ScanKeys(table), (keys, rows) =>
                 {
                     lock (_lock)
                     {
                         if (!_tables.TryGetValue(table, out var state))
                             return false;
                         state.AddRowsScanned(ReadRowsBatched(state, state.Current, keys, rows, skipUnreadable: true));
                         return true;
                     }
                 }))
        {
            yield return pair;
        }
    }

    // The index lives in the table's immutable version, so collecting a key list needs no lock —
    // only materializing the rows behind those keys does, and only for a paged table.

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
    {
        if (!_tables.TryGetValue(table, out var state))
            return [];
        var version = state.Current;
        return MaterializeRows(state, version, [.. version.Trees.IndexEqual(state.IndexPosition(column), value)]);
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
    {
        if (!_tables.TryGetValue(table, out var state))
            return [];
        var version = state.Current;
        return MaterializeRows(state, version, [.. version.Trees.IndexRange(state.IndexPosition(column), low, high)]);
    }

    public long Count(TableId table) =>
        _tables.TryGetValue(table, out var state) ? state.Current.RowCount : 0;

    /// <summary>
    /// The keys, lazily, off the version captured under the lock.
    /// <para>
    /// This used to copy every key into an array before returning — <c>[.. Current.Keys]</c> — on
    /// the theory that the caller should not hold the lock while it enumerates. It does not need
    /// to: the version is immutable, so capturing the reference is enough and reading it needs no
    /// lock at all. What the copy actually bought was an allocation the size of the table on every
    /// call, on the large-object heap for anything past a few thousand rows, taken while holding
    /// the store lock — and a subscription re-scoping over a million-row table did it once per
    /// subscription, under the engine's write lock, which is to say with every writer stopped.
    /// </para>
    /// </summary>
    public IEnumerable<RowKey> ScanKeys(TableId table)
    {
        lock (_lock)
        {
            return _tables.TryGetValue(table, out var state) ? state.Current.Keys : [];
        }
    }

    public IEnumerable<RowKey> ScanKeyRange(TableId table, RowKey low, RowKey high)
    {
        lock (_lock)
        {
            return _tables.TryGetValue(table, out var state) ? state.Current.KeyRange(low, high) : [];
        }
    }

    /// <summary>
    /// Every table's counters, read <b>without</b> the store lock. A cluster node calls this from
    /// its heartbeat (the load sample), and the store lock is held across each paged read, disk
    /// I/O included, so a long scan over a store larger than its buffer pool made the heartbeat
    /// queue behind it (#199). Nothing here needs the lock. The table map is fixed at
    /// construction, the current version is published volatile, and the counters are 64-bit values
    /// written only while a commit applies, which happens under the engine's write lock. That lock
    /// is the one a caller needing consistency across tables already holds
    /// (<c>MelangeEngine.ReadConsistent</c>, as the load sample does). The scan and fault counters
    /// are advisory and may run a read behind.
    /// </summary>
    public HotStoreStatistics Statistics()
    {
        var tables = new List<HotStoreTableStatistics>(_tables.Count);
        foreach (var schema in _registry.Tables)
        {
            var state = _tables[schema.Id];
            tables.Add(new HotStoreTableStatistics(
                schema.Id,
                schema.Name,
                state.IsResident ? Residency.Resident : Residency.Paged,
                state.RowCount,
                state.ResidentBytes,
                state.PageFaults,
                state.RowsScanned));
        }

        HotStoreLogStatistics[] logs = _blob is null
            ? [LogStatistics(0, "main", _main)]
            : [LogStatistics(0, "main", _main), LogStatistics(1, "blob", _blob)];
        return new HotStoreStatistics { Tables = tables, BufferPoolCapacityBytes = _bufferPoolCapacityBytes, Logs = logs };
    }

    /// <summary>
    /// Whether compaction must run flat out: the logs over <c>HotStore:Compaction:MaxLogBytes</c>, or the
    /// hot store's disk under <c>MinFreeDiskBytes</c>. Checked at most once a second, on the compaction
    /// thread. Each condition warns when it starts (EventIds 1519, 1518), every five minutes while it
    /// lasts, and logs once when it clears.
    /// </summary>
    private sealed class CompactionPressure(FasterHotStore store, IPagedKv[] spaces)
    {
        private static readonly TimeSpan CheckEvery = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan RepeatEvery = TimeSpan.FromMinutes(5);

        private readonly System.Diagnostics.Stopwatch _clock = System.Diagnostics.Stopwatch.StartNew();
        private TimeSpan _lastCheck = -CheckEvery;
        private TimeSpan? _overSince;
        private TimeSpan? _lowSince;
        private TimeSpan _overWarned;
        private TimeSpan _lowWarned;
        private bool _urgent;

        public bool Check()
        {
            var now = _clock.Elapsed;
            if (now - _lastCheck < CheckEvery)
                return _urgent;
            _lastCheck = now;

            var options = store._compaction;
            var over = false;
            if (options.MaxLogBytes > 0)
            {
                long log = 0, live = 0;
                foreach (var space in spaces)
                {
                    var statistics = space.Statistics;
                    log += Math.Max(0, statistics.LogBytes);
                    live += Math.Max(0, statistics.LiveBytes);
                }

                over = log > options.MaxLogBytes;
                if (Track(over, ref _overSince, ref _overWarned, now))
                    LogMessages.LogOverCap(store._logger, log, options.MaxLogBytes, live);
                else if (!over && _overSince is null && _overWarned != TimeSpan.Zero)
                {
                    _overWarned = TimeSpan.Zero;
                    LogMessages.LogUnderCap(store._logger, log, options.MaxLogBytes);
                }
            }

            var low = false;
            if (options.MinFreeDiskBytes > 0 && FreeBytes(store._hotStorePath) is { } free)
            {
                low = free < options.MinFreeDiskBytes;
                if (Track(low, ref _lowSince, ref _lowWarned, now))
                    LogMessages.DiskSpaceLow(store._logger, store._hotStorePath, free, options.MinFreeDiskBytes);
                else if (!low && _lowSince is null && _lowWarned != TimeSpan.Zero)
                {
                    _lowWarned = TimeSpan.Zero;
                    LogMessages.DiskSpaceRecovered(store._logger, store._hotStorePath, free, options.MinFreeDiskBytes);
                }
            }

            _urgent = over || low;
            return _urgent;
        }

        /// <summary>Tracks one condition; true when it should warn now (it began, or five minutes passed).</summary>
        private static bool Track(bool active, ref TimeSpan? since, ref TimeSpan warned, TimeSpan now)
        {
            if (!active)
            {
                since = null;
                return false;
            }

            since ??= now;
            if (warned != TimeSpan.Zero && now - warned < RepeatEvery)
                return false;
            warned = now == TimeSpan.Zero ? TimeSpan.FromTicks(1) : now;
            return true;
        }

        /// <summary>Free bytes on the file system holding <paramref name="path"/>, or null if it cannot be read.</summary>
        private static long? FreeBytes(string path)
        {
            try
            {
                return new DriveInfo(OperatingSystem.IsWindows() ? Path.GetPathRoot(path)! : path).AvailableFreeSpace;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or ArgumentException)
            {
                return null;
            }
        }
    }

    /// <summary>A space's engine statistics, named, with the compaction thread's own counters when it compacts.</summary>
    private HotStoreLogStatistics LogStatistics(int index, string space, IPagedKv kv)
    {
        var statistics = kv.Statistics with { Space = space };
        return kv is ICompactingPagedKv
            ? statistics with
            {
                CompactionSteps = Volatile.Read(ref _compactionSteps[index]),
                CompactionLockTime = TimeSpan.FromTicks(Volatile.Read(ref _compactionLockTicks[index])),
            }
            : statistics;
    }

    public void ApplyResidency(string tableName, Residency residency)
    {
        ArgumentException.ThrowIfNullOrEmpty(tableName);
        lock (_lock)
        {
            if (!_registry.TryGetByName(tableName, out var schema))
                throw new ArgumentException($"No table named '{tableName}' is registered.", nameof(tableName));
            var state = _tables[schema.Id];
            state.Declared = residency;
            var wantResident = residency == Residency.Resident
                || (residency == Residency.Auto && state.ResidentDataBytes <= _autoThresholdBytes);
            if (wantResident == state.IsResident)
                return;
            try
            {
                if (wantResident)
                    Promote(state);
                else
                    Demote(state);
            }
            finally
            {
                PublishWrites();
            }

            LogMessages.ResidencyChanged(_logger, tableName, residency, state.IsResident ? "resident" : "paged");
        }
    }

    public void Dispose()
    {
        // The compaction thread reads a segment's mapped pages off the lock, so it must be gone
        // before the spaces unmap them. It takes the lock itself, so it is joined outside it. The
        // token source is never disposed: it has no timer, and a second Dispose cancels it again.
        _compactionStop.Cancel();
        if (_compactor is not null && Thread.CurrentThread != _compactor)
            _compactor.Join();

        lock (_lock)
        {
            if (_disposed)
                return;
            _disposed = true;
            _main.Dispose();
            _blob?.Dispose();
        }
    }

    /// <summary>
    /// The compaction thread (#287): for each space that compacts, warm the next step's pages off the
    /// lock, then run one bounded step under it, so writers and readers wait at most one step. Sleeps
    /// while nothing is due. A segment it cannot read is logged and left; anything else stops
    /// compaction (logged) rather than the process, and the store carries on without it.
    /// </summary>
    private void CompactionLoop()
    {
        var spaces = new List<(int Index, string Name, ICompactingPagedKv Space)>(2);
        if (_main is ICompactingPagedKv main)
            spaces.Add((0, "main", main));
        if (_blob is ICompactingPagedKv blob)
            spaces.Add((1, "blob", blob));

        var token = _compactionStop.Token;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var copied = 0L;
        var pressure = new CompactionPressure(this, spaces.Select(static s => (IPagedKv)s.Space).ToArray());
        try
        {
            while (!token.IsCancellationRequested)
            {
                // Over the log cap or short of disk, compact as hard as possible: the lowest ratio, no rate cap.
                var urgent = pressure.Check();
                var ratio = urgent ? MinTriggerRatio : _compaction.TriggerRatio;
                var busy = false;
                foreach (var (index, name, space) in spaces)
                {
                    space.PrepareCompactionStep();
                    CompactionStep step;
                    lock (_lock)
                    {
                        if (_disposed)
                            return;
                        var held = System.Diagnostics.Stopwatch.GetTimestamp();
                        try
                        {
                            step = space.Compact(ratio, CompactionStepBytes);
                        }
                        catch (InvalidDataException fault)
                        {
                            LogMessages.CompactionSkippedSegment(_logger, name, fault.Message);
                            busy = true;
                            continue;
                        }
                        finally
                        {
                            var elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(held).Ticks;
                            Volatile.Write(ref _compactionLockTicks[index], _compactionLockTicks[index] + elapsed);
                        }
                    }

                    // A step that found nothing due is a probe, not compaction: only work is counted.
                    if (step.MoreWork || step.SegmentsDropped > 0)
                        Volatile.Write(ref _compactionSteps[index], _compactionSteps[index] + 1);

                    if (step.SegmentsDropped > 0)
                        LogMessages.LogSpaceReclaimed(_logger, name, step.ReclaimedBytes, step.SegmentsDropped);
                    busy |= step.MoreWork;
                    copied += step.RelocatedBytes;
                }

                if (!busy)
                {
                    token.WaitHandle.WaitOne(CompactionIdle);
                    continue;
                }

                // Under a cap, run no faster than it allows; uncapped, still let a waiting writer in.
                if (_compaction.MaxBytesPerSecond > 0 && !urgent)
                {
                    var ahead = TimeSpan.FromSeconds((double)copied / _compaction.MaxBytesPerSecond) - clock.Elapsed;
                    if (ahead > TimeSpan.Zero && token.WaitHandle.WaitOne(ahead))
                        return;
                }
                else
                {
                    Thread.Yield();
                }
            }
        }
        catch (Exception exception) when (!token.IsCancellationRequested)
        {
            LogMessages.CompactionStopped(_logger, exception);
        }
    }

    /// <summary>
    /// Captures every table's current version and registers the view for pre-image capture — see
    /// <see cref="IReadViewSource.OpenReadView"/>. Under the store lock, so the captured versions
    /// share one LSN rather than straddling an apply.
    /// </summary>
    public IHotStoreReadView OpenReadView()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var captured = new Dictionary<TableId, PinnedTable>(_tables.Count);
            foreach (var (id, state) in _tables)
                captured.Add(id, new PinnedTable(state, state.Current, state.ResidencyEpoch));
            var view = new ReadView(this, AppliedLsn, captured);
            _openViews.Add(view);
            return view;
        }
    }

    /// <summary>
    /// Stashes a paged row's pre-image on every open view that has not already recorded one, before
    /// the write that would destroy it. Only paged rows need this: a resident table's rows live in
    /// the versioned containers a view already pinned. Called under the store lock, on the write
    /// path, and it costs a row read — so it costs nothing at all while no view is open, which is
    /// the overwhelmingly common case.
    /// </summary>
    private void CapturePreImage(TableState table, in RowKey key)
    {
        if (_openViews.Count == 0)
            return;
        var id = table.Schema.Id;
        var epoch = table.ResidencyEpoch;
        byte[]? preImage = null;
        var read = false;
        foreach (var view in _openViews)
        {
            if (!view.WantsPreImage(id, key, epoch))
                continue;
            if (!read)
            {
                preImage = ReadRow(table, key);
                read = true;
            }

            view.CapturePreImage(id, key, preImage);
        }
    }

    private void CloseReadView(ReadView view)
    {
        lock (_lock)
        {
            _openViews.Remove(view);
        }
    }

    /// <summary>One table as a read view sees it: the owner, the pinned version, and the tier it was pinned in.</summary>
    private readonly record struct PinnedTable(TableState Owner, TableVersion Version, int ResidencyEpoch);

    /// <summary>
    /// A read view over versions captured at one LSN, plus the undo overlay covering paged rows
    /// whose payloads were overwritten since.
    /// <para>
    /// A <b>resident</b> table reads entirely from the pinned version, with no lock and no overlay —
    /// which is the case worth optimising, since a table a sweep scans hot is exactly the one to
    /// declare <see cref="Residency.Resident"/>. A <b>paged</b> table's row payload lives in the
    /// hybrid log behind a single FASTER session, so each row read takes the store lock for its own
    /// duration. That still frees the engine's write lock for the whole body, which is the point;
    /// it does not make paged reads concurrent with writers, which a per-view FASTER session would.
    /// </para>
    /// </summary>
    private sealed class ReadView(FasterHotStore store, ulong lsn, Dictionary<TableId, PinnedTable> tables)
        : IHotStoreReadView
    {
        private readonly Dictionary<UndoKey, byte[]?> _undo = [];
        private bool _disposed;

        public ulong Lsn => lsn;

        /// <summary>Whether this view still needs the pre-image of a row about to be overwritten.</summary>
        public bool WantsPreImage(TableId table, in RowKey key, int residencyEpoch) =>
            !_disposed
            && tables.TryGetValue(table, out var pinned)
            && pinned.ResidencyEpoch == residencyEpoch
            && !_undo.ContainsKey(new UndoKey(table, key));

        public void CapturePreImage(TableId table, in RowKey key, byte[]? preImage) =>
            _undo[new UndoKey(table, key)] = preImage;

        public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row)
        {
            if (Pin(table) is not { } pinned)
            {
                row = default;
                return false;
            }

            var bytes = Read(pinned, key);
            row = bytes;
            return bytes is not null;
        }

        public bool ContainsKey(TableId table, in RowKey key) =>
            Pin(table) is { } pinned
            && pinned.Version.Contains(key);

        public long Count(TableId table) => Pin(table) is { } pinned ? pinned.Version.RowCount : 0;

        public IEnumerable<RowKey> ScanKeys(TableId table) => Pin(table) is { } pinned ? pinned.Version.Keys : [];

        public IEnumerable<RowKey> ScanKeyRange(TableId table, RowKey low, RowKey high) =>
            Pin(table) is { } pinned ? pinned.Version.KeyRange(low, high) : [];

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table)
        {
            if (Pin(table) is not { } pinned)
                return [];
            return pinned.Version.IsResident
                ? Resident(pinned)
                : Materialize(pinned, pinned.Version.Trees.Keys);
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
        {
            if (Pin(table) is not { } pinned)
                return [];
            return Materialize(pinned, pinned.Version.Trees.IndexEqual(pinned.Owner.IndexPosition(column), value));
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
        {
            if (Pin(table) is not { } pinned)
                return [];
            return Materialize(pinned, pinned.Version.Trees.IndexRange(pinned.Owner.IndexPosition(column), low, high));
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            store.CloseReadView(this);
            _undo.Clear();
            tables.Clear();
        }

        private IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Resident(PinnedTable pinned)
        {
            var scanned = 0L;
            try
            {
                foreach (var pair in pinned.Version.ResidentRows)
                {
                    scanned++;
                    yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(pair.Key, pair.Value);
                }
            }
            finally
            {
                pinned.Owner.AddRowsScanned(scanned);
            }
        }

        private IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Materialize(PinnedTable pinned, IEnumerable<RowKey> keys)
        {
            if (pinned.Version.IsResident)
            {
                foreach (var key in keys)
                {
                    ThrowIfUnusable(pinned);
                    if (Read(pinned, key) is { } bytes)
                    {
                        pinned.Owner.AddRowsScanned(1);
                        yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, bytes);
                    }
                }

                yield break;
            }

            // Paged: a block of keys at a time, the overlay probes and the store reads under one
            // hold of the store lock so no write slips between them (as Read below), with the
            // store's reads for the block in flight together rather than one at a time (#212).
            var toRead = new RowKey[ScanBatchRows];
            var slots = new int[ScanBatchRows];
            var read = new byte[]?[ScanBatchRows];
            foreach (var pair in store.ScanInBatches(keys, (batch, rows) =>
                     {
                         lock (store._lock)
                         {
                             ThrowIfUnusable(pinned);
                             var pending = 0;
                             for (var i = 0; i < batch.Length; i++)
                             {
                                 rows[i] = null;
                                 if (!pinned.Version.Contains(batch[i]))
                                     continue;
                                 if (_undo.TryGetValue(new UndoKey(pinned.Owner.Schema.Id, batch[i]), out var preImage))
                                 {
                                     rows[i] = preImage;
                                     continue;
                                 }

                                 toRead[pending] = batch[i];
                                 slots[pending++] = i;
                             }

                             // A view is a consistent read: an unreadable row is a fault, not a skip.
                             store.ReadRowsBatched(pinned.Owner, pinned.Version, toRead.AsSpan(0, pending), read, skipUnreadable: false);
                             for (var j = 0; j < pending; j++)
                                 rows[slots[j]] = read[j];
                             var found = 0;
                             for (var i = 0; i < batch.Length; i++)
                             {
                                 if (rows[i] is not null)
                                     found++;
                             }

                             pinned.Owner.AddRowsScanned(found);
                             return true;
                         }
                     }))
            {
                yield return pair;
            }
        }

        /// <summary>
        /// The row as of <see cref="Lsn"/>: the overlay first, because an entry there means the live
        /// payload has already been overwritten; otherwise the store, which still holds it.
        /// </summary>
        private byte[]? Read(PinnedTable pinned, in RowKey key)
        {
            ThrowIfUnusable(pinned);
            if (!pinned.Version.Contains(key))
                return null;
            if (pinned.Version.IsResident)
                return pinned.Version.ResidentRows.TryGetValue(key, out var resident) ? resident : null;

            // Both the overlay probe and the store read happen under the store lock, so a write
            // cannot slip between them and hand back a payload from after the pin.
            lock (store._lock)
            {
                return _undo.TryGetValue(new UndoKey(pinned.Owner.Schema.Id, key), out var preImage)
                    ? preImage
                    : store.ReadRow(pinned.Owner, pinned.Version, key);
            }
        }

        private PinnedTable? Pin(TableId table)
        {
            ObjectDisposedException.ThrowIf(_disposed, typeof(IHotStoreReadView));
            if (!tables.TryGetValue(table, out var pinned))
                return null;
            ThrowIfUnusable(pinned);
            return pinned;
        }

        private static void ThrowIfUnusable(PinnedTable pinned)
        {
            if (pinned.Owner.ResidencyEpoch == pinned.ResidencyEpoch)
                return;
            throw new InvalidOperationException(
                $"Table '{pinned.Owner.Schema.Name}' changed residency tier while this read view was open, so the " +
                "state it pinned no longer describes where the rows live. Reopen the view; if this fires during " +
                "normal operation, an Auto-residency table is crossing Residency:AutoThresholdBytes under load — " +
                "declare it Resident or Paged.");
        }

        private readonly record struct UndoKey(TableId Table, RowKey Key);
    }

    /// <summary>
    /// Reads the rows behind a key list. A resident version answers from managed memory with no
    /// lock; a paged one takes the store lock per row, because the hybrid log overwrites in place
    /// and the directory probe and record read have to be atomic against a concurrent write.
    /// </summary>
    private IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> MaterializeRows(
        TableState state, TableVersion version, RowKey[] keys)
    {
        if (version.IsResident)
        {
            foreach (var key in keys)
            {
                if (version.ResidentRows.TryGetValue(key, out var resident))
                {
                    state.AddRowsScanned(1);
                    yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, resident);
                }
            }

            yield break;
        }

        foreach (var pair in ScanInBatches(keys, (batch, rows) =>
                 {
                     lock (_lock)
                     {
                         state.AddRowsScanned(ReadRowsBatched(state, version, batch, rows, skipUnreadable: true));
                         return true;
                     }
                 }))
        {
            yield return pair;
        }
    }

    /// <summary>
    /// Test-only: overwrites one paged row's out-of-line payload with bytes of a length that
    /// disagrees with the main record's framing — the on-disk state issue #137 observed, where a
    /// blob and its main record came from different versions. The public API cannot produce it (a
    /// write splits and upserts both together, and recovery rebuilds both from one log record), so
    /// the store's resilience to it is only testable by forcing it. Composes and upserts through
    /// the same paths a real write uses, under the same lock.
    /// </summary>
    internal void CorruptBlobPayloadForTest(TableId table, in RowKey key, int ordinal, byte[] payload)
    {
        lock (_lock)
        {
            _blob!.Upsert(BlobKey(table, key, ordinal), payload);
        }
    }

    /// <summary>Test-only: corrupts one paged row's main record in a MelangeLog space, where it lives now.</summary>
    internal void CorruptPagedRowForTest(TableId table, in RowKey key)
    {
        lock (_lock)
        {
            ((MelangeLogPagedKv)_main).CorruptForTest(StoreKey(table, key));
        }
    }

    private byte[]? ReadRow(TableState table, in RowKey key) => ReadRow(table, table.Current, key);

    private byte[]? ReadRow(TableState table, TableVersion version, in RowKey key)
    {
        // A disposed store's devices are deleteOnClose: its log files are gone, and every paged
        // read would come back "main record missing" or "out-of-line payload missing", which reads
        // as corruption and which the scan path below would quietly skip as unreadable rows. A
        // read view that outlived its store (a snapshot still running when the shard was released
        // under it, #199) must hear that the store is gone, not that its data is. Every caller
        // holds the store lock, and Dispose sets the flag under it.
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (version.IsResident)
            return version.ResidentRows.TryGetValue(key, out var resident) ? resident : null;
        if (!version.Trees.TryGetEntry(key, out var entry))
            return null;

        var rowKey = key;
        var main = ReadValue(_main, StoreKey(table.Schema.Id, rowKey), table)
            ?? throw new InvalidDataException(
                $"Table '{table.Schema.Name}': key {rowKey} is in the directory but its main record is missing.");
        if (entry.BlobMask == 0)
            return main;

        // Every integrity fault the join can raise — a payload whose length disagrees with the
        // main record's framing, or a missing payload — is rethrown carrying the key, because the
        // caller that has to act on it (an operator repairing one row, or the scan wrapper logging
        // it) needs the key, and RowBlobSplitter has only the ordinal. The splitter's own message,
        // which names the ordinal and the two lengths, is preserved as the reason.
        try
        {
            return RowBlobSplitter.Join(table.Schema, main, entry.BlobMask, ordinal =>
                ReadValue(_blob!, BlobKey(table.Schema.Id, rowKey, ordinal), table)
                ?? throw new InvalidDataException(
                    $"out-of-line payload for bytes-column ordinal {ordinal} is missing."));
        }
        catch (InvalidDataException fault)
        {
            throw new InvalidDataException($"Table '{table.Schema.Name}': key {rowKey}: {fault.Message}", fault);
        }
    }

    /// <summary>Rows per batched scan read; see <see cref="ReadRowsBatched"/>.</summary>
    private const int ScanBatchRows = 128;

    /// <summary>
    /// Feeds keys to <paramref name="readBatch"/> a block at a time and yields the rows it found, in
    /// key order. <paramref name="readBatch"/> fills one slot per key (null for absent) and returns
    /// false to end the scan, the way a dropped table ends one.
    /// </summary>
    private IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanInBatches(
        IEnumerable<RowKey> keys, Func<RowKey[], byte[]?[], bool> readBatch)
    {
        var batch = new RowKey[ScanBatchRows];
        var rows = new byte[]?[ScanBatchRows];
        var count = 0;
        foreach (var key in keys)
        {
            batch[count++] = key;
            if (count < ScanBatchRows)
                continue;
            if (!readBatch(batch, rows))
                yield break;
            for (var i = 0; i < count; i++)
            {
                if (rows[i] is { } bytes)
                    yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(batch[i], bytes);
            }

            count = 0;
        }

        if (count == 0)
            yield break;
        var tail = batch[..count];
        var tailRows = new byte[]?[count];
        if (!readBatch(tail, tailRows))
            yield break;
        for (var i = 0; i < count; i++)
        {
            if (tailRows[i] is { } bytes)
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(tail[i], bytes);
        }
    }

    /// <summary>
    /// Reads a block of rows with their FASTER reads in flight together: every main record is
    /// issued, then completed in one wait, then every out-of-line payload the same way. A paged
    /// scan used to read one row at a time, each read a synchronous wait on a random disk read,
    /// which made a million-row snapshot or border reset take four to five minutes (#212). The
    /// order rows come back in, the directory as the source of truth, and the fault messages are
    /// <see cref="ReadRow(TableState, TableVersion, in RowKey)"/>'s.
    /// <para>
    /// The caller holds the store lock, as for any paged read. A row that is absent from the
    /// version gets null. An unreadable one is logged and gets null when
    /// <paramref name="skipUnreadable"/> (a scan's rule, see <see cref="TryReadRowForScan"/>), and
    /// throws otherwise. Returns how many rows were found.
    /// </para>
    /// </summary>
    private int ReadRowsBatched(TableState table, TableVersion version, ReadOnlySpan<RowKey> keys, byte[]?[] rows, bool skipUnreadable)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var count = keys.Length;
        var found = 0;
        if (version.IsResident)
        {
            for (var i = 0; i < count; i++)
            {
                rows[i] = version.ResidentRows.TryGetValue(keys[i], out var resident) ? resident : null;
                if (rows[i] is not null)
                    found++;
            }

            return found;
        }

        try
        {
            var entries = new DirectoryEntry?[count];
            var mains = new byte[]?[count];
            var mainKeys = new byte[count][];
            var mainSlots = new int[count];
            var mainReads = 0;
            for (var i = 0; i < count; i++)
            {
                rows[i] = null;
                if (!version.Trees.TryGetEntry(keys[i], out var entry))
                    continue;
                entries[i] = entry;
                mainKeys[mainReads] = StoreKey(table.Schema.Id, keys[i]).ToArray();
                mainSlots[mainReads++] = i;
            }

            var mainValues = new byte[]?[mainReads];
            table.PageFaults += _main.ReadMany(mainKeys.AsSpan(0, mainReads), mainValues);
            for (var j = 0; j < mainReads; j++)
                mains[mainSlots[j]] = mainValues[j];

            var blobs = new Dictionary<(int Row, int Ordinal), byte[]>();
            var blobKeys = new List<byte[]>();
            var blobSlots = new List<(int Row, int Ordinal)>();
            for (var i = 0; i < count; i++)
            {
                if (entries[i] is not { } entry || entry.BlobMask == 0 || mains[i] is null)
                    continue;
                var mask = entry.BlobMask;
                for (var ordinal = 0; mask != 0; ordinal++, mask >>= 1)
                {
                    if ((mask & 1) == 0)
                        continue;
                    blobKeys.Add(BlobKey(table.Schema.Id, keys[i], ordinal).ToArray());
                    blobSlots.Add((i, ordinal));
                }
            }

            if (blobKeys.Count > 0)
            {
                var blobValues = new byte[]?[blobKeys.Count];
                table.PageFaults += _blob!.ReadMany(blobKeys.ToArray(), blobValues);
                for (var j = 0; j < blobValues.Length; j++)
                {
                    if (blobValues[j] is { } payload)
                        blobs[blobSlots[j]] = payload;
                }
            }

            for (var i = 0; i < count; i++)
            {
                if (entries[i] is not { } entry)
                    continue;
                try
                {
                    rows[i] = JoinRow(table, keys[i], entry, mains[i], i, blobs);
                    found++;
                }
                catch (InvalidDataException fault) when (skipUnreadable)
                {
                    LogMessages.UnreadableRowSkipped(_logger, table.Schema.Name, keys[i].ToString(), fault.Message);
                }
            }

            return found;
        }
        catch (InvalidDataException) when (skipUnreadable)
        {
            // An engine that verifies records as it reads them (MelangeLog's checksums) fails a
            // batched read whole when one record is bad. A scan skips unreadable rows, not batches:
            // read this batch again a row at a time, so only the bad one is logged and left out.
            found = 0;
            for (var i = 0; i < count; i++)
            {
                rows[i] = null;
                try
                {
                    rows[i] = ReadRow(table, version, keys[i]);
                }
                catch (InvalidDataException fault)
                {
                    LogMessages.UnreadableRowSkipped(_logger, table.Schema.Name, keys[i].ToString(), fault.Message);
                }

                if (rows[i] is not null)
                    found++;
            }

            return found;
        }
    }

    private static byte[] JoinRow(
        TableState table, in RowKey key, DirectoryEntry entry, byte[]? main, int row, Dictionary<(int Row, int Ordinal), byte[]> blobs)
    {
        if (main is null)
        {
            throw new InvalidDataException(
                $"Table '{table.Schema.Name}': key {key} is in the directory but its main record is missing.");
        }

        if (entry.BlobMask == 0)
            return main;
        try
        {
            return RowBlobSplitter.Join(table.Schema, main, entry.BlobMask, ordinal =>
                blobs.TryGetValue((row, ordinal), out var payload)
                    ? payload
                    : throw new InvalidDataException($"out-of-line payload for bytes-column ordinal {ordinal} is missing."));
        }
        catch (InvalidDataException fault)
        {
            throw new InvalidDataException($"Table '{table.Schema.Name}': key {key}: {fault.Message}", fault);
        }
    }

    /// <summary>
    /// Reads a row for a scan, turning an unreadable one into "absent" rather than an exception.
    /// A scan that materializes rows — a subscription's initial set, an index range — must not let
    /// one corrupt row take the whole operation, and through it the connection that asked for it,
    /// down with it: the failure mode issue #137 reported, where a single row whose out-of-line
    /// payload disagreed with its main record threw out of <see cref="Scan"/> and killed every
    /// client whose first subscription scanned it. The row is logged with its key (EventId 1509)
    /// and skipped; a point read (<see cref="TryGetRow"/>) still surfaces the fault, because a
    /// caller asking for one row by key is owed the truth about that row rather than a silent miss.
    /// </summary>
    private byte[]? TryReadRowForScan(TableState table, in RowKey key)
    {
        try
        {
            return ReadRow(table, key);
        }
        catch (InvalidDataException fault)
        {
            LogMessages.UnreadableRowSkipped(_logger, table.Schema.Name, key.ToString(), fault.Message);
            return null;
        }
    }

    private void PutRow(TableState table, in RowKey key, ReadOnlyMemory<byte> row)
    {
        if (table.IsResident)
        {
            table.PutResident(key, row.ToArray());
            if (table.Declared == Residency.Auto && table.ResidentDataBytes > _autoThresholdBytes)
            {
                Demote(table);
                LogMessages.AutoResidencyDemoted(_logger, table.Schema.Name, table.ResidentDataBytes, _autoThresholdBytes);
            }

            return;
        }

        PutPaged(table, key, row);
    }

    private void PutPaged(TableState table, in RowKey key, ReadOnlyMemory<byte> row)
    {
        var indexValues = table.EncodeIndexValues(row.Span);
        var (main, mask, blobs) = RowBlobSplitter.Split(table.Schema, row);

        CapturePreImage(table, key);
        if (table.TryGetDirectory(key, out var previous))
        {
            table.Unindex(key, previous.IndexValues);

            // Blob records overwrite in place by key; only columns that stopped being
            // out of line need explicit deletion.
            var stale = previous.BlobMask & ~mask;
            for (var ordinal = 0; stale != 0; ordinal++, stale >>= 1)
            {
                if ((stale & 1) != 0)
                    _blob!.Delete(BlobKey(table.Schema.Id, key, ordinal));
            }
        }

        _main.Upsert(StoreKey(table.Schema.Id, key), main.Span);
        if (blobs is not null)
        {
            foreach (var (ordinal, payload) in blobs)
                _blob!.Upsert(BlobKey(table.Schema.Id, key, ordinal), payload.Span);
        }

        table.PutDirectory(key, new DirectoryEntry(mask, indexValues), previous);
        table.Index(key, indexValues);
    }

    private void DeleteRow(TableState table, in RowKey key)
    {
        if (table.IsResident)
        {
            table.RemoveResident(key);
            return;
        }

        if (!table.TryGetDirectory(key, out var entry))
            return;
        CapturePreImage(table, key);
        table.Unindex(key, entry.IndexValues);
        _main.Delete(StoreKey(table.Schema.Id, key));
        var mask = entry.BlobMask;
        for (var ordinal = 0; mask != 0; ordinal++, mask >>= 1)
        {
            if ((mask & 1) != 0)
                _blob!.Delete(BlobKey(table.Schema.Id, key, ordinal));
        }

        table.RemoveDirectory(key, entry);
    }

    /// <summary>
    /// Migrates a resident table into the paged tier: rows move into the hybrid log, the managed
    /// dictionary is dropped, and only the key directory and indexes stay pinned.
    /// </summary>
    private void Demote(TableState table)
    {
        var rows = table.SnapshotResidentRows();
        table.BeginPaged();
        foreach (var (key, bytes) in rows)
            PutPaged(table, key, bytes);
    }

    /// <summary>
    /// Pins a paged table wholly into managed memory — one deliberate faulting pass over its rows —
    /// then deletes the paged records it read. Nothing could read them again (the directory is the
    /// authority, and a read view pinned on the paged tier refuses reads once the tier changes), but
    /// left in place they would count as live in the engine's log forever, and the log space they
    /// hold could never be reclaimed.
    /// </summary>
    private void Promote(TableState table)
    {
        var keys = table.SnapshotKeys();
        var rows = new List<(RowKey Key, byte[] Bytes)>(keys.Length);
        foreach (var key in keys)
        {
            if (ReadRow(table, key) is { } bytes)
                rows.Add((key, bytes));
        }

        foreach (var key in keys)
        {
            if (!table.TryGetDirectory(key, out var entry))
                continue;
            _main.Delete(StoreKey(table.Schema.Id, key));
            var mask = entry.BlobMask;
            for (var ordinal = 0; mask != 0; ordinal++, mask >>= 1)
            {
                if ((mask & 1) != 0)
                    _blob!.Delete(BlobKey(table.Schema.Id, key, ordinal));
            }
        }

        table.BeginResident();
        foreach (var (key, bytes) in rows)
            table.PutResident(key, bytes);
    }

    /// <summary>Reads one value from a space, counting a read that waited on storage against the table.</summary>
    private static byte[]? ReadValue(IPagedKv space, ReadOnlySpan<byte> key, TableState faultAccounting)
    {
        var value = space.Read(key, out var faulted);
        if (faulted)
            faultAccounting.PageFaults++;
        return value;
    }

    /// <summary>
    /// Composes a store key into the scratch buffer. Every upsert, delete, and read used to
    /// allocate a fresh array for a key that is dead the moment the call returns; under a heavy
    /// apply rate that is pure garbage. The buffer is safe to share because every path that
    /// composes a key holds the store lock -- which is also what makes the single session safe --
    /// and the span is consumed before the next key is composed.
    /// </summary>
    private ReadOnlySpan<byte> StoreKey(TableId table, in RowKey key)
    {
        var bytes = Scratch(4 + key.Length);
        BinaryPrimitives.WriteUInt32BigEndian(bytes, table.Value);
        key.Span.CopyTo(bytes[4..]);
        return bytes;
    }

    private ReadOnlySpan<byte> BlobKey(TableId table, in RowKey key, int bytesColumnOrdinal)
    {
        var bytes = Scratch(4 + key.Length + 1);
        BinaryPrimitives.WriteUInt32BigEndian(bytes, table.Value);
        key.Span.CopyTo(bytes[4..]);
        bytes[^1] = checked((byte)bytesColumnOrdinal);
        return bytes;
    }

    private Span<byte> Scratch(int length)
    {
        if (_keyScratch.Length < length)
            _keyScratch = new byte[Math.Max(length, _keyScratch.Length * 2)];
        return _keyScratch.AsSpan(0, length);
    }

    /// <summary>
    /// Hash buckets for the FASTER index: the operator's figure when set, else derived from the
    /// memory budget. A bucket holds several entries, so the target is roughly one per few
    /// expected records; the budget is the only proxy for record count the store has at
    /// construction, since the rows themselves arrive later by replay.
    /// <para>
    /// The previous fixed 65,536 sized the index for about a quarter of a million records whatever
    /// the budget said. Past that, chains lengthen and a lookup that should be one probe becomes
    /// several -- each one a candidate for a pending completion on a paged table.
    /// </para>
    /// </summary>
    private static long HashBuckets(HotStoreOptions options)
    {
        // A record averages a few hundred bytes and a bucket serves several, so budget/1024 keeps
        // the index proportional to what the buffer pool can hold without dwarfing it.
        var target = options.HashBuckets > 0 ? options.HashBuckets : Math.Max(options.MemoryBudgetBytes, 2L << 20) / 1024;
        var clamped = Math.Clamp(target, 1L << 16, 1L << 26);
        return BitOperations.IsPow2(clamped) ? clamped : 1L << (64 - BitOperations.LeadingZeroCount((ulong)clamped));
    }

    /// <summary>
    /// One immutable version of a table's managed-memory state: which tier it is in, a resident
    /// table's rows, and the table's ordered bookkeeping (a paged table's key directory and every
    /// table's secondary indexes, <see cref="TableTrees"/>). A commit record publishes one new
    /// version; a read view holds an older one. Row payloads and index-value arrays are shared
    /// across versions — they are replaced, never mutated.
    /// </summary>
    private sealed class TableVersion(bool isResident, RowDirectory<byte[]> residentRows, TableTrees trees)
    {
        public bool IsResident { get; } = isResident;

        public RowDirectory<byte[]> ResidentRows { get; } = residentRows;

        public TableTrees Trees { get; } = trees;

        public long RowCount => IsResident ? ResidentRows.Count : Trees.Count;

        public IEnumerable<RowKey> Keys => IsResident ? ResidentRows.Keys : Trees.Keys;

        /// <summary>The keys in [low, high], sought rather than scanned to; see IHotStoreReader.ScanKeyRange.</summary>
        public IEnumerable<RowKey> KeyRange(RowKey low, RowKey high) =>
            IsResident ? ResidentRows.KeyRange(low, high) : Trees.KeyRange(low, high);

        public bool Contains(in RowKey key) => IsResident ? ResidentRows.ContainsKey(key) : Trees.Contains(key);
    }

    private sealed class TableState
    {
        private readonly Dictionary<string, int> _indexPositions = new(StringComparer.Ordinal);

        /// <summary>Indexed column names in index order — the codec's one-pass encode reads this per put.</summary>
        private readonly string[] _indexColumns;
        private TableVersion _current;

        /// <summary>
        /// The next version while a write is open: everything a commit record (or a migration, a
        /// snapshot load, or recovery's whole replay) changes, published once by <see cref="Publish"/>
        /// or <see cref="CompleteBulk"/>. The write path reads through it, so a record's later ops
        /// see its earlier ones; readers see <see cref="Current"/> and so never a half-applied record.
        /// </summary>
        private Pending? _pending;

        public TableState(TableSchema schema, Residency declared, TableTrees emptyTrees)
        {
            var bytesColumns = schema.Columns.Count(c => c.Kind == ColumnKind.Bytes);
            if (bytesColumns > 32)
            {
                throw new NotSupportedException(
                    $"Table '{schema.Name}' has {bytesColumns} byte[] columns; the out-of-line blob mask supports at most 32.");
            }

            Schema = schema;
            Declared = declared;
            _indexColumns = new string[schema.Indexes.Count];
            for (var i = 0; i < schema.Indexes.Count; i++)
            {
                _indexPositions[schema.Indexes[i].Column] = i;
                _indexColumns[i] = schema.Indexes[i].Column;
            }

            _current = new TableVersion(
                declared is Residency.Resident or Residency.Auto,
                RowDirectory<byte[]>.Empty,
                emptyTrees.Empty(schema.Indexes.Count));
        }

        public TableSchema Schema { get; }

        public Residency Declared { get; set; }

        /// <summary>
        /// The table's current version. Written under the store lock and read by a pinned view
        /// without one, so publication is volatile: a reader must never see a torn reference.
        /// </summary>
        public TableVersion Current
        {
            get => Volatile.Read(ref _current);
            private set => Volatile.Write(ref _current, value);
        }

        /// <summary>
        /// Bumped by every tier migration. A read view captures it and refuses to serve reads once
        /// it moves: a promote rewrites where rows live and a demote overwrites their hybrid-log
        /// records, so a view pinned across one would be answering from bookkeeping that no longer
        /// describes the data. Failing loudly beats a plausible wrong answer.
        /// </summary>
        public int ResidencyEpoch { get; private set; }

        /// <summary>Whether the table's rows are currently pinned in managed memory.</summary>
        public bool IsResident => _pending?.IsResident ?? Current.IsResident;

        public RowDirectory<byte[]> ResidentRows => Current.ResidentRows;

        public long PageFaults { get; set; }

        public long RowsScanned
        {
            get => Interlocked.Read(ref _rowsScanned);
            set => Interlocked.Exchange(ref _rowsScanned, value);
        }

        private long _rowsScanned;

        /// <summary>
        /// Publishes scan progress. A resident table's rows are read from a pinned view with no lock
        /// held, so several threads can be scanning the same table at once and this counter is the
        /// one piece of shared state they touch.
        /// </summary>
        public void AddRowsScanned(long count)
        {
            if (count != 0)
                Interlocked.Add(ref _rowsScanned, count);
        }

        /// <summary>Row data bytes held in managed memory — what the Auto threshold compares against.</summary>
        public long ResidentDataBytes { get; private set; }

        /// <summary>Bookkeeping bytes (keys, directory entries, index values) held in managed memory.</summary>
        public long OverheadBytes { get; private set; }

        public long ResidentBytes => ResidentDataBytes + OverheadBytes;

        public long RowCount => Current.RowCount;

        public int IndexPosition(string column) =>
            _indexPositions.TryGetValue(column, out var position)
                ? position
                : throw new ArgumentException($"Table {Schema.Id} has no index on column '{column}'.", nameof(column));

        public RowKey[] SnapshotKeys() => [.. Current.Keys];

        /// <summary>A row's bytes if resident — the write path's read, so it sees the open write.</summary>
        public bool TryGetResident(in RowKey key, out byte[] bytes)
        {
            if (_pending is { } pending)
            {
                return pending.BulkRows is { } bulkRows
                    ? bulkRows.TryGetValue(key, out bytes!)
                    : pending.Rows.TryGetValue(key, out bytes!);
            }

            return Current.ResidentRows.TryGetValue(key, out bytes!);
        }

        /// <summary>A paged row's directory entry — the write path's read, so it sees the open write.</summary>
        public bool TryGetDirectory(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry) =>
            _pending is { } pending
                ? pending.Trees.TryGetEntry(key, out entry)
                : Current.Trees.TryGetEntry(key, out entry);

        /// <summary>The resident rows materialized for a tier migration, which is about to reset them.</summary>
        public KeyValuePair<RowKey, byte[]>[] SnapshotResidentRows()
        {
            if (_pending is { } pending)
                return pending.BulkRows is { } bulkRows ? [.. bulkRows] : [.. pending.Rows];
            return [.. Current.ResidentRows];
        }

        public void PutResident(RowKey key, byte[] bytes)
        {
            var pending = Write();
            if (TryGetResident(key, out var previous))
            {
                Unindex(key, EncodeIndexValues(previous));
                ResidentDataBytes -= previous.Length;
            }
            else
            {
                OverheadBytes += key.Length + 32;
            }

            ResidentDataBytes += bytes.Length;
            if (pending.BulkRows is { } bulkRows)
                bulkRows[key] = bytes;
            else
                pending.Rows = pending.Rows.SetItem(key, bytes);
            Index(key, EncodeIndexValues(bytes));
        }

        public void RemoveResident(RowKey key)
        {
            if (!TryGetResident(key, out var previous))
                return;
            var pending = Write();
            ResidentDataBytes -= previous.Length;
            OverheadBytes -= key.Length + 32;
            if (pending.BulkRows is { } bulkRows)
                bulkRows.Remove(key);
            else
                pending.Rows = pending.Rows.Remove(key);
            Unindex(key, EncodeIndexValues(previous));
        }

        public void PutDirectory(RowKey key, DirectoryEntry entry, DirectoryEntry? previous)
        {
            if (previous is not null)
                OverheadBytes -= EntryOverhead(previous);
            else
                OverheadBytes += key.Length + 32;
            OverheadBytes += EntryOverhead(entry);
            Write().Trees.SetEntry(key, entry);
        }

        public void RemoveDirectory(RowKey key, DirectoryEntry entry)
        {
            OverheadBytes -= EntryOverhead(entry) + key.Length + 32;
            Write().Trees.RemoveEntry(key);
        }

        /// <summary>Drops the resident rows and switches to paged bookkeeping; the caller re-puts the rows.</summary>
        public void BeginPaged() => BeginTier(isResident: false);

        /// <summary>Drops the paged bookkeeping and switches to resident storage; the caller re-puts the rows.</summary>
        public void BeginResident() => BeginTier(isResident: true);

        private void BeginTier(bool isResident)
        {
            ResidentDataBytes = 0;
            OverheadBytes = 0;
            ResidencyEpoch++;

            // The migration restarts from empty inside the open write (recovery's included), and
            // nothing publishes until that write does.
            var bulk = _pending?.BulkRows is not null;
            _pending?.Trees.Dispose();
            var empty = Current.Trees.Empty(_indexColumns.Length);
            _pending = new Pending
            {
                IsResident = isResident,
                Rows = RowDirectory<byte[]>.Empty,
                BulkRows = bulk ? RowDirectory<byte[]>.Empty.ToBuilder() : null,
                Trees = empty.BeginEdit(bulk),
            };
        }

        /// <summary>
        /// Enters bulk mode: one write held open for recovery's whole replay, seeded from the
        /// current version, published by <see cref="CompleteBulk"/>.
        /// </summary>
        public void BeginBulk()
        {
            Publish();
            var current = Current;
            _pending = new Pending
            {
                IsResident = current.IsResident,
                Rows = current.ResidentRows,
                BulkRows = current.ResidentRows.ToBuilder(),
                Trees = current.Trees.BeginEdit(bulk: true),
            };
        }

        /// <summary>Publishes recovery's write and leaves bulk mode.</summary>
        public void CompleteBulk()
        {
            if (_pending is not { BulkRows: { } bulkRows } pending)
                return;
            _pending = null;
            Current = new TableVersion(pending.IsResident, bulkRows.ToImmutable(), pending.Trees.Commit());
        }

        /// <summary>
        /// Publishes the open write, if any, as the table's next version. Recovery's bulk write is
        /// left open: it publishes once, at <see cref="CompleteBulk"/>.
        /// </summary>
        public void Publish()
        {
            if (_pending is not { BulkRows: null } pending)
                return;
            _pending = null;
            Current = new TableVersion(pending.IsResident, pending.Rows, pending.Trees.Commit());
        }

        /// <summary>The open write, opened on first use from the current version.</summary>
        private Pending Write()
        {
            if (_pending is { } pending)
                return pending;
            var current = Current;
            return _pending = new Pending
            {
                IsResident = current.IsResident,
                Rows = current.ResidentRows,
                Trees = current.Trees.BeginEdit(bulk: false),
            };
        }

        private sealed class Pending
        {
            public required bool IsResident { get; init; }

            /// <summary>The resident rows as of this write, path-copied per op (an ordinary write).</summary>
            public required RowDirectory<byte[]> Rows { get; set; }

            /// <summary>Recovery's mutable resident rows, assembled once at <see cref="CompleteBulk"/>; null outside bulk mode.</summary>
            public RowDirectory<byte[]>.Builder? BulkRows { get; init; }

            public required TableTreesEdit Trees { get; init; }
        }

        /// <summary>
        /// Every indexed column's encoded value for one row, positionally aligned with the schema's
        /// index list; a zero-length key is a null column value, which is not indexed. One pass over
        /// the row for the whole set — encoding a column at a time deserialized the entire row per
        /// index, so a three-index table paid three full deserializes per put.
        /// </summary>
        public RowKey[]? EncodeIndexValues(ReadOnlySpan<byte> rowBytes)
        {
            if (Schema.Indexes.Count == 0)
                return null;
            var values = new RowKey[_indexColumns.Length];
            if (Schema.Codec is { } codec)
            {
                codec.EncodeColumnsFromBytes(rowBytes, _indexColumns, values);
                return values;
            }

            var row = RowSerializer.Deserialize(Schema, rowBytes.ToArray());
            for (var i = 0; i < _indexColumns.Length; i++)
            {
                var columnSchema = Schema.Column(_indexColumns[i]);
                var value = columnSchema.GetValue(row);
                values[i] = value is null ? default : SchemaKeyCodec.Encode(columnSchema, value);
            }

            return values;
        }

        public void Index(RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            var trees = Write().Trees;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue; // A null column value is unindexed, matching the in-memory store.
                trees.IndexAdd(i, value, key);
            }
        }

        public void Unindex(RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            var trees = Write().Trees;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue;
                trees.IndexRemove(i, value, key);
            }
        }

        private static long EntryOverhead(DirectoryEntry entry)
        {
            long overhead = 24;
            if (entry.IndexValues is { } values)
            {
                foreach (var value in values)
                    overhead += value.Length + 16;
            }

            return overhead;
        }
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, Exception?> PagedEngineMessage =
            LoggerMessage.Define<string>(
                LogLevel.Information,
                new EventId(1512, "PagedEngine"),
                "The paged hot store keeps paged rows in the {Engine} engine (HotStore:PagedEngine).");

        public static void PagedEngine(ILogger logger, string engine) => PagedEngineMessage(logger, engine, null);

        private static readonly Action<ILogger, string, long, long, Exception?> AutoResidencyDemotedMessage =
            LoggerMessage.Define<string, long, long>(
                LogLevel.Warning,
                new EventId(1505, "AutoResidencyDemoted"),
                "Table '{Table}' crossed Residency:AutoThresholdBytes ({Bytes} > {Threshold}) and is now paged. " +
                "Auto residency is threshold behaviour by explicit request; if this table must stay fast to scan, declare it Resident.");

        public static void AutoResidencyDemoted(ILogger logger, string table, long bytes, long threshold) =>
            AutoResidencyDemotedMessage(logger, table, bytes, threshold, null);

        private static readonly Action<ILogger, string, Residency, string, Exception?> ResidencyChangedMessage =
            LoggerMessage.Define<string, Residency, string>(
                LogLevel.Information,
                new EventId(1508, "ResidencyChanged"),
                "Table '{Table}' residency override applied ({Residency}); the table is now {Mode}.");

        public static void ResidencyChanged(ILogger logger, string table, Residency residency, string mode) =>
            ResidencyChangedMessage(logger, table, residency, mode, null);

        private static readonly Action<ILogger, string, string, string, Exception?> UnreadableRowSkippedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(1511, "UnreadableRowSkipped"),
                "Table '{Table}': row {Key} could not be read and was skipped by a scan ({Reason}). "
                + "The row is intact in the commit log — the hot store is a projection of it — so a restart "
                + "rebuilds it cleanly; this projection-only fault cannot survive one. A scan no longer fails, "
                + "and no longer takes the connection that issued it down, because one row is unreadable.");

        public static void UnreadableRowSkipped(ILogger logger, string table, string key, string reason) =>
            UnreadableRowSkippedMessage(logger, table, key, reason, null);

        private static readonly Action<ILogger, long, string, int, Exception?> LogSpaceReclaimedMessage =
            LoggerMessage.Define<long, string, int>(
                LogLevel.Debug,
                new EventId(1514, "PagedLogSpaceReclaimed"),
                "Compaction gave back {Bytes} bytes of the paged {Space} log ({Segments} segment(s) deleted).");

        public static void LogSpaceReclaimed(ILogger logger, string space, long bytes, int segments) =>
            LogSpaceReclaimedMessage(logger, bytes, space, segments, null);

        private static readonly Action<ILogger, string, string, Exception?> CompactionSkippedSegmentMessage =
            LoggerMessage.Define<string, string>(
                LogLevel.Warning,
                new EventId(1515, "PagedCompactionSkippedSegment"),
                "Compaction could not read a segment of the paged {Space} log and will leave it in place ({Reason}). "
                + "Its disk space is not reclaimed until a restart, which rebuilds the store from the commit log.");

        public static void CompactionSkippedSegment(ILogger logger, string space, string reason) =>
            CompactionSkippedSegmentMessage(logger, space, reason, null);

        private static readonly Action<ILogger, Exception?> CompactionStoppedMessage =
            LoggerMessage.Define(
                LogLevel.Error,
                new EventId(1516, "PagedCompactionStopped"),
                "Paged log compaction failed and has stopped for this store; its disk use now grows with write volume "
                + "until a restart. The store itself carries on.");

        public static void CompactionStopped(ILogger logger, Exception exception) =>
            CompactionStoppedMessage(logger, exception);

        private static readonly Action<ILogger, string, long, long, Exception?> DiskSpaceLowMessage =
            LoggerMessage.Define<string, long, long>(
                LogLevel.Warning,
                new EventId(1518, "PagedDiskSpaceLow"),
                "The hot store's disk ({Path}) has {Free} bytes free, under HotStore:Compaction:MinFreeDiskBytes ({Threshold}). "
                + "Compaction now runs as hard as it can. If the disk fills, a write to the paged store fails and the database "
                + "closes to writes (EventId 1517).");

        public static void DiskSpaceLow(ILogger logger, string path, long free, long threshold) =>
            DiskSpaceLowMessage(logger, path, free, threshold, null);

        private static readonly Action<ILogger, string, long, long, Exception?> DiskSpaceRecoveredMessage =
            LoggerMessage.Define<string, long, long>(
                LogLevel.Information,
                new EventId(1518, "PagedDiskSpaceLow"),
                "The hot store's disk ({Path}) has {Free} bytes free again, over HotStore:Compaction:MinFreeDiskBytes ({Threshold}).");

        public static void DiskSpaceRecovered(ILogger logger, string path, long free, long threshold) =>
            DiskSpaceRecoveredMessage(logger, path, free, threshold, null);

        private static readonly Action<ILogger, long, long, long, Exception?> LogOverCapMessage =
            LoggerMessage.Define<long, long, long>(
                LogLevel.Warning,
                new EventId(1519, "PagedLogOverCap"),
                "The paged logs hold {LogBytes} bytes, over HotStore:Compaction:MaxLogBytes ({Cap}), with {LiveBytes} bytes live. "
                + "Compaction now runs as hard as it can. If live bytes alone exceed the cap, it cannot get under it: raise the cap.");

        public static void LogOverCap(ILogger logger, long logBytes, long cap, long liveBytes) =>
            LogOverCapMessage(logger, logBytes, cap, liveBytes, null);

        private static readonly Action<ILogger, long, long, Exception?> LogUnderCapMessage =
            LoggerMessage.Define<long, long>(
                LogLevel.Information,
                new EventId(1519, "PagedLogOverCap"),
                "The paged logs are back under HotStore:Compaction:MaxLogBytes: {LogBytes} of {Cap} bytes.");

        public static void LogUnderCap(ILogger logger, long logBytes, long cap) =>
            LogUnderCapMessage(logger, logBytes, cap, null);
    }
}
