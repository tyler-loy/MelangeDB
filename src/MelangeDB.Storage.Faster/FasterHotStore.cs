using System.Buffers.Binary;
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
/// </summary>
public sealed class FasterHotStore : IHotStore, IResidencyControl, IDisposable
{
    private readonly SchemaRegistry _registry;
    private readonly ILogger _logger;
    private readonly long _autoThresholdBytes;
    private readonly Lock _lock = new();
    private readonly Dictionary<TableId, TableState> _tables = [];

    private readonly IDevice _mainDevice;
    private readonly FasterKV<SpanByte, SpanByte> _main;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions> _mainSession;
    private readonly IDevice? _blobDevice;
    private readonly FasterKV<SpanByte, SpanByte>? _blob;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions>? _blobSession;
    private readonly long _bufferPoolCapacityBytes;
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

        // The store is rebuilt from snapshot + log replay on every start, so files left by a
        // previous run (clean or crashed) are stale projections; start from nothing.
        var path = context.Options.HotStore.Path;
        Directory.CreateDirectory(path);
        CleanStoreFiles(path);

        _mainDevice = Devices.CreateLogDevice(Path.Combine(path, "main.hlog"), deleteOnClose: true);
        _main = new FasterKV<SpanByte, SpanByte>(
            1L << 16,
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
                1L << 16,
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
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var state))
            {
                row = default;
                return false;
            }

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
                resident.RowsScanned++;
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
                    state.RowsScanned++;
            }

            if (bytes is not null)
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, bytes);
        }
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
    {
        RowKey[] keys;
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var state))
                yield break;
            var index = state.Index(column);
            keys = index.TryGetValue(value, out var set) ? [.. set] : [];
        }

        foreach (var pair in MaterializeRows(table, keys))
            yield return pair;
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
    {
        var keys = new List<RowKey>();
        lock (_lock)
        {
            if (!_tables.TryGetValue(table, out var state))
                yield break;
            foreach (var (value, set) in state.Index(column))
            {
                if (value.CompareTo(low) < 0)
                    continue;
                if (value.CompareTo(high) > 0)
                    break;
                keys.AddRange(set);
            }
        }

        foreach (var pair in MaterializeRows(table, keys))
            yield return pair;
    }

    public long Count(TableId table)
    {
        lock (_lock)
        {
            return _tables.TryGetValue(table, out var state) ? state.RowCount : 0;
        }
    }

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

    private IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> MaterializeRows(TableId table, IEnumerable<RowKey> keys)
    {
        foreach (var key in keys)
        {
            byte[]? bytes;
            lock (_lock)
            {
                bytes = _tables.TryGetValue(table, out var state) ? ReadRow(state, key) : null;
            }

            if (bytes is not null)
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, bytes);
        }
    }

    private byte[]? ReadRow(TableState table, in RowKey key)
    {
        if (table.IsResident)
            return table.ResidentRows.TryGetValue(key, out var resident) ? resident : null;
        if (!table.Directory.TryGetValue(key, out var entry))
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

        if (table.Directory.TryGetValue(key, out var previous))
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

        if (!table.Directory.TryGetValue(key, out var entry))
            return;
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
        var rows = table.ResidentRows;
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
        byte[] keyBytes,
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
        byte[] keyBytes,
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
        byte[] keyBytes)
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

    private static byte[] StoreKey(TableId table, in RowKey key)
    {
        var bytes = new byte[4 + key.Length];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, table.Value);
        key.Span.CopyTo(bytes.AsSpan(4));
        return bytes;
    }

    private static byte[] BlobKey(TableId table, in RowKey key, int bytesColumnOrdinal)
    {
        var bytes = new byte[4 + key.Length + 1];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, table.Value);
        key.Span.CopyTo(bytes.AsSpan(4));
        bytes[^1] = checked((byte)bytesColumnOrdinal);
        return bytes;
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

    private sealed class TableState
    {
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
            IsResident = declared is Residency.Resident or Residency.Auto;
            foreach (var index in schema.Indexes)
                Indexes.Add(index.Column, []);
        }

        public TableSchema Schema { get; }

        public Residency Declared { get; set; }

        /// <summary>Whether the table's rows are currently pinned in managed memory.</summary>
        public bool IsResident { get; private set; }

        public SortedDictionary<RowKey, byte[]> ResidentRows { get; private set; } = [];

        public SortedDictionary<RowKey, DirectoryEntry> Directory { get; private set; } = [];

        public Dictionary<string, SortedDictionary<RowKey, SortedSet<RowKey>>> Indexes { get; } = [];

        public long PageFaults { get; set; }

        public long RowsScanned { get; set; }

        /// <summary>Row data bytes held in managed memory — what the Auto threshold compares against.</summary>
        public long ResidentDataBytes { get; private set; }

        /// <summary>Bookkeeping bytes (keys, directory entries, index values) held in managed memory.</summary>
        public long OverheadBytes { get; private set; }

        public long ResidentBytes => ResidentDataBytes + OverheadBytes;

        public long RowCount => IsResident ? ResidentRows.Count : Directory.Count;

        public SortedDictionary<RowKey, SortedSet<RowKey>> Index(string column) =>
            Indexes.TryGetValue(column, out var index)
                ? index
                : throw new ArgumentException($"Table {Schema.Id} has no index on column '{column}'.", nameof(column));

        public RowKey[] SnapshotKeys() =>
            IsResident ? [.. ResidentRows.Keys] : [.. Directory.Keys];

        public void PutResident(RowKey key, byte[] bytes)
        {
            if (ResidentRows.TryGetValue(key, out var previous))
            {
                Unindex(key, EncodeIndexValues(previous));
                ResidentDataBytes -= previous.Length;
            }
            else
            {
                OverheadBytes += key.Length + 32;
            }

            ResidentRows[key] = bytes;
            ResidentDataBytes += bytes.Length;
            Index(key, EncodeIndexValues(bytes));
        }

        public void RemoveResident(RowKey key)
        {
            if (!ResidentRows.Remove(key, out var previous))
                return;
            ResidentDataBytes -= previous.Length;
            OverheadBytes -= key.Length + 32;
            Unindex(key, EncodeIndexValues(previous));
        }

        public void PutDirectory(RowKey key, DirectoryEntry entry, DirectoryEntry? previous)
        {
            if (previous is not null)
                OverheadBytes -= EntryOverhead(previous);
            else
                OverheadBytes += key.Length + 32;
            Directory[key] = entry;
            OverheadBytes += EntryOverhead(entry);
        }

        public void RemoveDirectory(RowKey key, DirectoryEntry entry)
        {
            Directory.Remove(key);
            OverheadBytes -= EntryOverhead(entry) + key.Length + 32;
        }

        /// <summary>Drops the resident dictionary and switches to paged bookkeeping; the caller re-puts the rows.</summary>
        public void BeginPaged()
        {
            ResidentRows = [];
            Directory = [];
            foreach (var index in Indexes.Values)
                index.Clear();
            ResidentDataBytes = 0;
            OverheadBytes = 0;
            IsResident = false;
        }

        /// <summary>Drops the paged bookkeeping and switches to resident storage; the caller re-puts the rows.</summary>
        public void BeginResident()
        {
            ResidentRows = [];
            Directory = [];
            foreach (var index in Indexes.Values)
                index.Clear();
            ResidentDataBytes = 0;
            OverheadBytes = 0;
            IsResident = true;
        }

        public RowKey[]? EncodeIndexValues(ReadOnlySpan<byte> rowBytes)
        {
            if (Schema.Indexes.Count == 0)
                return null;
            var values = new RowKey[Schema.Indexes.Count];
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var column = Schema.Indexes[i].Column;
                values[i] = EncodeColumn(column, rowBytes) ?? default;
            }

            return values;
        }

        public void Index(RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue; // A null column value is unindexed, matching the in-memory store.
                var index = Indexes[Schema.Indexes[i].Column];
                if (!index.TryGetValue(value, out var keys))
                    index[value] = keys = [];
                keys.Add(key);
            }
        }

        public void Unindex(RowKey key, RowKey[]? values)
        {
            if (values is null)
                return;
            for (var i = 0; i < Schema.Indexes.Count; i++)
            {
                var value = values[i];
                if (value.Length == 0)
                    continue;
                var index = Indexes[Schema.Indexes[i].Column];
                if (index.TryGetValue(value, out var keys))
                {
                    keys.Remove(key);
                    if (keys.Count == 0)
                        index.Remove(value);
                }
            }
        }

        private RowKey? EncodeColumn(string column, ReadOnlySpan<byte> rowBytes)
        {
            if (Schema.Codec is { } codec)
                return codec.EncodeColumnFromBytes(column, rowBytes);
            var row = RowSerializer.Deserialize(Schema, rowBytes.ToArray());
            var columnSchema = Schema.Column(column);
            var value = columnSchema.GetValue(row);
            return value is null ? null : KeyCodec.Encode(columnSchema, value);
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
