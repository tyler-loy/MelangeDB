using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using FASTER.core;
using MelangeDB.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// The FASTER-backed hot store: paged tables live in FASTER hybrid logs whose in-memory portion is
/// capped by <c>HotStore:MemoryBudgetBytes</c>, so the working set bounds memory instead of the
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

    private readonly IDevice _mainDevice;
    private readonly FasterKV<SpanByte, SpanByte> _main;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions> _mainSession;
    private readonly IDevice? _blobDevice;
    private readonly FasterKV<SpanByte, SpanByte>? _blob;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions>? _blobSession;
    private readonly long _bufferPoolCapacityBytes;

    /// <summary>Reused buffer for composed store and blob keys; see <see cref="StoreKey"/>.</summary>
    private byte[] _keyScratch = new byte[64];
    private bool _disposed;

    public FasterHotStore(HotStoreContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _registry = context.Schema;
        _logger = context.LoggerFactory.CreateLogger<FasterHotStore>();
        _autoThresholdBytes = context.Options.Residency.AutoThresholdBytes;

        var anyBlobColumns = context.Schema.Tables.Any(RowBlobSplitter.HasBytesColumns);
        var budget = Math.Max(context.Options.HotStore.MemoryBudgetBytes, 2L << 20);
        var totalBits = Math.Clamp(63 - BitOperations.LeadingZeroCount((ulong)budget), 21, 34);
        var mainMemoryBits = anyBlobColumns ? totalBits - 1 : totalBits;
        var blobMemoryBits = totalBits - 1;
        _bufferPoolCapacityBytes = (1L << mainMemoryBits) + (anyBlobColumns ? 1L << blobMemoryBits : 0);
        var hashBuckets = HashBuckets(context.Options.HotStore);

        // The store is rebuilt from snapshot + log replay on every start, so files left by a
        // previous run (clean or crashed) are stale projections; start from nothing.
        var path = context.Options.HotStore.Path;
        Directory.CreateDirectory(path);
        CleanStoreFiles(path);

        _mainDevice = Devices.CreateLogDevice(Path.Combine(path, "main.hlog"), deleteOnClose: true);
        _main = new FasterKV<SpanByte, SpanByte>(
            hashBuckets,
            new LogSettings
            {
                LogDevice = _mainDevice,
                MemorySizeBits = mainMemoryBits,
                PageSizeBits = Math.Min(mainMemoryBits - 1, 19),
                SegmentSizeBits = 26,
            });
        _mainSession = _main.For(new StoreFunctions()).NewSession<StoreFunctions>();

        if (anyBlobColumns)
        {
            _blobDevice = Devices.CreateLogDevice(Path.Combine(path, "blob.hlog"), deleteOnClose: true);
            _blob = new FasterKV<SpanByte, SpanByte>(
                hashBuckets,
                new LogSettings
                {
                    LogDevice = _blobDevice,
                    MemorySizeBits = blobMemoryBits,
                    PageSizeBits = Math.Min(blobMemoryBits - 1, 22),
                    SegmentSizeBits = 26,
                });
            _blobSession = _blob.For(new StoreFunctions()).NewSession<StoreFunctions>();
        }

        foreach (var table in context.Schema.Tables)
        {
            var declared = context.Residency.GetValueOrDefault(table.Id, table.Residency);
            _tables.Add(table.Id, new TableState(table, declared));
        }
    }

    public ulong AppliedLsn { get; private set; }

    public void LoadSnapshot(ulong lsn, IEnumerable<SnapshotRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        lock (_lock)
        {
            if (AppliedLsn != 0)
                throw new InvalidOperationException("A snapshot loads only into an empty store, before any record applies.");
            foreach (var row in rows)
            {
                if (_tables.TryGetValue(row.Table, out var table))
                    PutRow(table, row.Key, row.Row);
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
            foreach (var op in record.WriteSet)
            {
                if (!_tables.TryGetValue(op.Table, out var table))
                    continue; // A table this projection doesn't know; nothing to project.
                if (op.Kind == RowOpKind.Delete)
                    DeleteRow(table, op.Key);
                else
                    PutRow(table, op.Key, op.Row);
            }

            AppliedLsn = record.Lsn;
        }
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

        foreach (var key in ScanKeys(table))
        {
            byte[]? bytes;
            lock (_lock)
            {
                if (!_tables.TryGetValue(table, out var state))
                    yield break;
                bytes = ReadRow(state, key);
                if (bytes is not null)
                    state.AddRowsScanned(1);
            }

            if (bytes is not null)
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, bytes);
        }
    }

    // The index lives in the table's immutable version, so collecting a key list needs no lock —
    // only materializing the rows behind those keys does, and only for a paged table.

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
    {
        if (!_tables.TryGetValue(table, out var state))
            return [];
        var version = state.Current;
        return MaterializeRows(state, version, [.. version.Indexes[state.IndexPosition(column)].Equal(value)]);
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
    {
        if (!_tables.TryGetValue(table, out var state))
            return [];
        var version = state.Current;
        return MaterializeRows(state, version, [.. version.Indexes[state.IndexPosition(column)].Range(low, high)]);
    }

    public long Count(TableId table) =>
        _tables.TryGetValue(table, out var state) ? state.Current.RowCount : 0;

    public IEnumerable<RowKey> ScanKeys(TableId table)
    {
        RowKey[] keys;
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var state))
                return [];
            keys = state.SnapshotKeys();
        }

        return keys;
    }

    public HotStoreStatistics Statistics()
    {
        lock (_lock)
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

            return new HotStoreStatistics { Tables = tables, BufferPoolCapacityBytes = _bufferPoolCapacityBytes };
        }
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
            if (wantResident)
                Promote(state);
            else
                Demote(state);
            LogMessages.ResidencyChanged(_logger, tableName, residency, state.IsResident ? "resident" : "paged");
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;
            _disposed = true;
            _mainSession.Dispose();
            _blobSession?.Dispose();
            _main.Dispose();
            _blob?.Dispose();
            _mainDevice.Dispose();
            _blobDevice?.Dispose();
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

        public long Count(TableId table) => Pin(table) is { } pinned ? pinned.Version.RowCount : 0;

        public IEnumerable<RowKey> ScanKeys(TableId table) => Pin(table) is { } pinned ? pinned.Version.Keys : [];

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table)
        {
            if (Pin(table) is not { } pinned)
                return [];
            return pinned.Version.IsResident
                ? Resident(pinned)
                : Materialize(pinned, pinned.Version.Directory.Keys);
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
        {
            if (Pin(table) is not { } pinned)
                return [];
            var index = pinned.Version.Indexes[pinned.Owner.IndexPosition(column)];
            return Materialize(pinned, index.Equal(value));
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
        {
            if (Pin(table) is not { } pinned)
                return [];
            var index = pinned.Version.Indexes[pinned.Owner.IndexPosition(column)];
            return Materialize(pinned, index.Range(low, high));
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
            foreach (var key in keys)
            {
                ThrowIfUnusable(pinned);
                if (Read(pinned, key) is { } bytes)
                {
                    pinned.Owner.AddRowsScanned(1);
                    yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, bytes);
                }
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

        foreach (var key in keys)
        {
            byte[]? bytes;
            lock (_lock)
            {
                bytes = ReadRow(state, key);
            }

            if (bytes is not null)
            {
                state.AddRowsScanned(1);
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, bytes);
            }
        }
    }

    private byte[]? ReadRow(TableState table, in RowKey key) => ReadRow(table, table.Current, key);

    private byte[]? ReadRow(TableState table, TableVersion version, in RowKey key)
    {
        if (version.IsResident)
            return version.ResidentRows.TryGetValue(key, out var resident) ? resident : null;
        if (!version.Directory.TryGetValue(key, out var entry))
            return null;

        var rowKey = key;
        var main = ReadValue(_mainSession, StoreKey(table.Schema.Id, rowKey), table)
            ?? throw new InvalidDataException(
                $"Table '{table.Schema.Name}': key {rowKey} is in the directory but its main record is missing.");
        if (entry.BlobMask == 0)
            return main;
        return RowBlobSplitter.Join(table.Schema, main, entry.BlobMask, ordinal =>
            ReadValue(_blobSession!, BlobKey(table.Schema.Id, rowKey, ordinal), table)
            ?? throw new InvalidDataException(
                $"Table '{table.Schema.Name}': key {rowKey} is missing its out-of-line payload for bytes-column ordinal {ordinal}."));
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
                    DeleteValue(_blobSession!, BlobKey(table.Schema.Id, key, ordinal));
            }
        }

        UpsertValue(_mainSession, StoreKey(table.Schema.Id, key), main.Span);
        if (blobs is not null)
        {
            foreach (var (ordinal, payload) in blobs)
                UpsertValue(_blobSession!, BlobKey(table.Schema.Id, key, ordinal), payload.Span);
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
        DeleteValue(_mainSession, StoreKey(table.Schema.Id, key));
        var mask = entry.BlobMask;
        for (var ordinal = 0; mask != 0; ordinal++, mask >>= 1)
        {
            if ((mask & 1) != 0)
                DeleteValue(_blobSession!, BlobKey(table.Schema.Id, key, ordinal));
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
    /// Pins a paged table wholly into managed memory — one deliberate faulting pass over its rows.
    /// The hybrid-log records it leaves behind are unreachable (the directory is the authority)
    /// and vanish at the next start's rebuild, so they are not tombstoned here.
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

        table.BeginResident();
        foreach (var (key, bytes) in rows)
            table.PutResident(key, bytes);
    }

    private byte[]? ReadValue(
        ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions> session,
        ReadOnlySpan<byte> keyBytes,
        TableState faultAccounting)
    {
        byte[]? output = null;
        Status status;
        unsafe
        {
            fixed (byte* keyPointer = keyBytes)
            {
                var key = SpanByte.FromPointer(keyPointer, keyBytes.Length);
                byte[] result = [];
                status = session.Read(ref key, ref result);
                output = result;
            }
        }

        if (status.IsPending)
        {
            faultAccounting.PageFaults++;
            output = null;
            session.CompletePendingWithOutputs(out var outputs, wait: true);
            using (outputs)
            {
                while (outputs.Next())
                {
                    if (outputs.Current.Status.Found)
                        output = outputs.Current.Output;
                }
            }

            return output;
        }

        return status.Found ? output : null;
    }

    private static void UpsertValue(
        ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions> session,
        ReadOnlySpan<byte> keyBytes,
        ReadOnlySpan<byte> value)
    {
        unsafe
        {
            fixed (byte* keyPointer = keyBytes)
            fixed (byte* valuePointer = value)
            {
                var key = SpanByte.FromPointer(keyPointer, keyBytes.Length);
                var val = SpanByte.FromPointer(valuePointer, value.Length);
                var status = session.Upsert(ref key, ref val);
                if (status.IsPending)
                    session.CompletePending(wait: true);
            }
        }
    }

    private static void DeleteValue(
        ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions> session,
        ReadOnlySpan<byte> keyBytes)
    {
        unsafe
        {
            fixed (byte* keyPointer = keyBytes)
            {
                var key = SpanByte.FromPointer(keyPointer, keyBytes.Length);
                var status = session.Delete(ref key);
                if (status.IsPending)
                    session.CompletePending(wait: true);
            }
        }
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

    private static void CleanStoreFiles(string path)
    {
        foreach (var file in Directory.EnumerateFiles(path))
        {
            var name = Path.GetFileName(file);
            if (name.StartsWith("main.hlog", StringComparison.Ordinal) || name.StartsWith("blob.hlog", StringComparison.Ordinal))
                File.Delete(file);
        }
    }

    /// <summary>One row's pinned bookkeeping in a paged table: which bytes-columns are out of line, and the row's indexed values.</summary>
    private sealed record DirectoryEntry(uint BlobMask, RowKey[]? IndexValues);

    /// <summary>
    /// One immutable version of a table's managed-memory state: which tier it is in, a resident
    /// table's rows, a paged table's key directory, and the secondary indexes positionally aligned
    /// with the schema's index list. A write publishes a new version; a read view holds an older one.
    /// Row payloads and index-value arrays are shared across versions — they are replaced, never
    /// mutated — so an extra version costs container nodes, not a copy of the data.
    /// </summary>
    private sealed class TableVersion(
        bool isResident,
        ImmutableSortedDictionary<RowKey, byte[]> residentRows,
        ImmutableSortedDictionary<RowKey, DirectoryEntry> directory,
        ImmutableArray<SecondaryIndex> indexes)
    {
        public bool IsResident { get; } = isResident;

        public ImmutableSortedDictionary<RowKey, byte[]> ResidentRows { get; } = residentRows;

        public ImmutableSortedDictionary<RowKey, DirectoryEntry> Directory { get; } = directory;

        public ImmutableArray<SecondaryIndex> Indexes { get; } = indexes;

        public long RowCount => IsResident ? ResidentRows.Count : Directory.Count;

        public IEnumerable<RowKey> Keys => IsResident ? ResidentRows.Keys : Directory.Keys;

        public bool Contains(in RowKey key) => IsResident ? ResidentRows.ContainsKey(key) : Directory.ContainsKey(key);
    }

    private sealed class TableState
    {
        private readonly Dictionary<string, int> _indexPositions = new(StringComparer.Ordinal);

        /// <summary>Indexed column names in index order — the codec's one-pass encode reads this per put.</summary>
        private readonly string[] _indexColumns;
        private TableVersion _current;

        /// <summary>
        /// Recovery's builder state, non-null while the store is in bulk mode. Every mutator and
        /// every write-path read routes here when set, so replay mutates owned builder nodes in
        /// place instead of publishing a structurally shared version per row — versions built for
        /// a reader that cannot exist yet. <see cref="Current"/> is stale until
        /// <see cref="CompleteBulk"/> publishes.
        /// </summary>
        private BulkState? _bulk;

        public TableState(TableSchema schema, Residency declared)
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
            var indexes = ImmutableArray.CreateBuilder<SecondaryIndex>(schema.Indexes.Count);
            for (var i = 0; i < schema.Indexes.Count; i++)
            {
                _indexPositions[schema.Indexes[i].Column] = i;
                _indexColumns[i] = schema.Indexes[i].Column;
                indexes.Add(SecondaryIndex.Empty);
            }

            _current = new TableVersion(
                declared is Residency.Resident or Residency.Auto,
                ImmutableSortedDictionary<RowKey, byte[]>.Empty,
                ImmutableSortedDictionary<RowKey, DirectoryEntry>.Empty,
                indexes.MoveToImmutable());
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
        public bool IsResident => _bulk?.IsResident ?? Current.IsResident;

        public ImmutableSortedDictionary<RowKey, byte[]> ResidentRows => Current.ResidentRows;

        public ImmutableSortedDictionary<RowKey, DirectoryEntry> Directory => Current.Directory;

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

        public SecondaryIndex Index(string column) =>
            Current.Indexes[IndexPosition(column)];

        public RowKey[] SnapshotKeys() => [.. Current.Keys];

        /// <summary>A row's bytes if resident — the write path's read, so it consults the builders in bulk mode.</summary>
        public bool TryGetResident(in RowKey key, out byte[] bytes) =>
            _bulk is { } bulk
                ? bulk.Rows.TryGetValue(key, out bytes!)
                : Current.ResidentRows.TryGetValue(key, out bytes!);

        /// <summary>A paged row's directory entry — the write path's read, so it consults the builders in bulk mode.</summary>
        public bool TryGetDirectory(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry) =>
            _bulk is { } bulk
                ? bulk.Directory.TryGetValue(key, out entry)
                : Current.Directory.TryGetValue(key, out entry);

        /// <summary>The resident rows materialized for a tier migration, which is about to reset them.</summary>
        public KeyValuePair<RowKey, byte[]>[] SnapshotResidentRows() =>
            _bulk is { } bulk ? [.. bulk.Rows] : [.. Current.ResidentRows];

        public void PutResident(RowKey key, byte[] bytes)
        {
            if (_bulk is { } bulk)
            {
                if (bulk.Rows.TryGetValue(key, out var previousRow))
                {
                    Unindex(bulk.Indexes, key, EncodeIndexValues(previousRow));
                    ResidentDataBytes -= previousRow.Length;
                }
                else
                {
                    OverheadBytes += key.Length + 32;
                }

                ResidentDataBytes += bytes.Length;
                bulk.Rows[key] = bytes;
                Index(bulk.Indexes, key, EncodeIndexValues(bytes));
                return;
            }

            var current = Current;
            var rows = current.ResidentRows;
            var indexes = current.Indexes;
            if (rows.TryGetValue(key, out var previous))
            {
                indexes = Unindex(indexes, key, EncodeIndexValues(previous));
                ResidentDataBytes -= previous.Length;
            }
            else
            {
                OverheadBytes += key.Length + 32;
            }

            ResidentDataBytes += bytes.Length;
            Current = new TableVersion(
                current.IsResident,
                rows.SetItem(key, bytes),
                current.Directory,
                Index(indexes, key, EncodeIndexValues(bytes)));
        }

        public void RemoveResident(RowKey key)
        {
            if (_bulk is { } bulk)
            {
                if (!bulk.Rows.TryGetValue(key, out var previousRow))
                    return;
                ResidentDataBytes -= previousRow.Length;
                OverheadBytes -= key.Length + 32;
                bulk.Rows.Remove(key);
                Unindex(bulk.Indexes, key, EncodeIndexValues(previousRow));
                return;
            }

            var current = Current;
            if (!current.ResidentRows.TryGetValue(key, out var previous))
                return;
            ResidentDataBytes -= previous.Length;
            OverheadBytes -= key.Length + 32;
            Current = new TableVersion(
                current.IsResident,
                current.ResidentRows.Remove(key),
                current.Directory,
                Unindex(current.Indexes, key, EncodeIndexValues(previous)));
        }

        public void PutDirectory(RowKey key, DirectoryEntry entry, DirectoryEntry? previous)
        {
            if (previous is not null)
                OverheadBytes -= EntryOverhead(previous);
            else
                OverheadBytes += key.Length + 32;
            OverheadBytes += EntryOverhead(entry);
            if (_bulk is { } bulk)
            {
                bulk.Directory[key] = entry;
                return;
            }

            var current = Current;
            Current = new TableVersion(
                current.IsResident,
                current.ResidentRows,
                current.Directory.SetItem(key, entry),
                current.Indexes);
        }

        public void RemoveDirectory(RowKey key, DirectoryEntry entry)
        {
            OverheadBytes -= EntryOverhead(entry) + key.Length + 32;
            if (_bulk is { } bulk)
            {
                bulk.Directory.Remove(key);
                return;
            }

            var current = Current;
            Current = new TableVersion(
                current.IsResident,
                current.ResidentRows,
                current.Directory.Remove(key),
                current.Indexes);
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
            if (_bulk is { } bulk)
            {
                // An Auto demotion mid-replay: the migration restarts inside the builders, and
                // nothing publishes until recovery completes.
                bulk.IsResident = isResident;
                bulk.Rows.Clear();
                bulk.Directory.Clear();
                for (var i = 0; i < bulk.Indexes.Length; i++)
                    bulk.Indexes[i] = SecondaryIndex.Empty.ToBuilder();
                return;
            }

            var indexes = ImmutableArray.CreateBuilder<SecondaryIndex>(_indexPositions.Count);
            for (var i = 0; i < _indexPositions.Count; i++)
                indexes.Add(SecondaryIndex.Empty);
            Current = new TableVersion(
                isResident,
                ImmutableSortedDictionary<RowKey, byte[]>.Empty,
                ImmutableSortedDictionary<RowKey, DirectoryEntry>.Empty,
                indexes.MoveToImmutable());
        }

        /// <summary>Enters bulk mode: mutators write builders seeded from the current version.</summary>
        public void BeginBulk()
        {
            var current = Current;
            var indexes = new SecondaryIndex.Builder[current.Indexes.Length];
            for (var i = 0; i < indexes.Length; i++)
                indexes[i] = current.Indexes[i].ToBuilder();
            _bulk = new BulkState
            {
                IsResident = current.IsResident,
                Rows = current.ResidentRows.ToBuilder(),
                Directory = current.Directory.ToBuilder(),
                Indexes = indexes,
            };
        }

        /// <summary>Publishes one version from the builders and leaves bulk mode.</summary>
        public void CompleteBulk()
        {
            if (_bulk is not { } bulk)
                return;
            var indexes = ImmutableArray.CreateBuilder<SecondaryIndex>(bulk.Indexes.Length);
            foreach (var builder in bulk.Indexes)
                indexes.Add(builder.ToImmutable());
            _bulk = null;
            Current = new TableVersion(
                bulk.IsResident,
                bulk.Rows.ToImmutable(),
                bulk.Directory.ToImmutable(),
                indexes.MoveToImmutable());
        }

        private sealed class BulkState
        {
            public required bool IsResident { get; set; }

            public required ImmutableSortedDictionary<RowKey, byte[]>.Builder Rows { get; init; }

            public required ImmutableSortedDictionary<RowKey, DirectoryEntry>.Builder Directory { get; init; }

            public required SecondaryIndex.Builder[] Indexes { get; init; }
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
            if (_bulk is { } bulk)
            {
                Index(bulk.Indexes, key, values);
                return;
            }

            var current = Current;
            Current = new TableVersion(
                current.IsResident, current.ResidentRows, current.Directory, Index(current.Indexes, key, values));
        }

        public void Unindex(RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            if (_bulk is { } bulk)
            {
                Unindex(bulk.Indexes, key, values);
                return;
            }

            var current = Current;
            Current = new TableVersion(
                current.IsResident, current.ResidentRows, current.Directory, Unindex(current.Indexes, key, values));
        }

        private void Index(SecondaryIndex.Builder[] indexes, RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue; // A null column value is unindexed, matching the in-memory store.
                indexes[i].Add(value, key);
            }
        }

        private void Unindex(SecondaryIndex.Builder[] indexes, RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue;
                indexes[i].Remove(value, key);
            }
        }

        private ImmutableArray<SecondaryIndex> Index(
            ImmutableArray<SecondaryIndex> indexes,
            RowKey key,
            RowKey[]? values)
        {
            if (values is null)
                return indexes;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue; // A null column value is unindexed, matching the in-memory store.
                indexes = indexes.SetItem(i, indexes[i].Add(value, key));
            }

            return indexes;
        }

        private ImmutableArray<SecondaryIndex> Unindex(
            ImmutableArray<SecondaryIndex> indexes,
            RowKey key,
            RowKey[]? values)
        {
            if (values is null)
                return indexes;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue;
                indexes = indexes.SetItem(i, indexes[i].Remove(value, key));
            }

            return indexes;
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

    /// <summary>FASTER callbacks: reads copy the stored span out as a fresh array; writes copy the span in.</summary>
    private sealed class StoreFunctions : FunctionsBase<SpanByte, SpanByte, SpanByte, byte[], Empty>
    {
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            return true;
        }

        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            return true;
        }

        public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref byte[] output, ref UpsertInfo upsertInfo, WriteReason reason)
            => src.TryCopyTo(ref dst);

        public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref byte[] output, ref UpsertInfo upsertInfo)
            => src.TryCopyTo(ref dst);
    }

    private static class LogMessages
    {
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
    }
}
