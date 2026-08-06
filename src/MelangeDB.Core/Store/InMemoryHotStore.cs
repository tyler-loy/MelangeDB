using System.Collections.Immutable;

namespace MelangeDB.Core;

/// <summary>
/// The dictionary-backed hot store: a legitimate projection of the commit log, not a stub, because
/// the log is the source of record. Rows are held in serialized form, so replaying the same log
/// into a fresh instance yields byte-identical state. Owns its secondary indexes — index
/// maintenance is store-owned, so a phase 07 engine swap never touches the applier pipeline.
/// <para>
/// Each table's rows and indexes are held in <b>persistent</b> (structurally shared) containers, so
/// <see cref="OpenReadView"/> is a reference capture rather than a copy: that is what lets a reader
/// hold a view pinned at one LSN while <see cref="Apply"/> keeps running. Measured at one million
/// 96-byte rows against the mutable containers this replaced: identical container memory, bulk build
/// 0.57×, point reads 0.99×, full scan 1.24×, and a put 0.39 µs against 0.22 µs — in exchange for a
/// pinned view costing nothing where cloning the table cost 28.6 ms.
/// </para>
/// </summary>
public sealed class InMemoryHotStore : IHotStore, IReadViewSource
{
    private readonly SchemaRegistry _registry;
    private readonly Dictionary<TableId, TableData> _tables = [];

    public InMemoryHotStore(SchemaRegistry registry)
        : this(registry, residency: null)
    {
    }

    /// <summary>
    /// Creates the store with resolved residency labels for its statistics. The labels are
    /// reporting only: this store holds every table in memory regardless — it does not page, which
    /// is exactly why it stays as the fast path for tests.
    /// </summary>
    public InMemoryHotStore(SchemaRegistry registry, IReadOnlyDictionary<TableId, Residency>? residency)
    {
        ArgumentNullException.ThrowIfNull(registry);
        _registry = registry;
        foreach (var table in registry.Tables)
        {
            var label = residency?.GetValueOrDefault(table.Id, table.Residency) ?? table.Residency;
            _tables.Add(table.Id, new TableData(table, label));
        }
    }

    public ulong AppliedLsn { get; private set; }

    public void LoadSnapshot(ulong lsn, IEnumerable<SnapshotRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (AppliedLsn != 0)
            throw new InvalidOperationException("A snapshot loads only into an empty store, before any record applies.");

        // Bulk path: no read view can exist yet, so the persistent containers are built through
        // their builders rather than one structurally shared version per row.
        var loaders = new Dictionary<TableId, TableData.BulkLoader>();
        foreach (var row in rows)
        {
            if (!_tables.TryGetValue(row.Table, out var table))
                continue;
            if (!loaders.TryGetValue(row.Table, out var loader))
                loaders[row.Table] = loader = table.BeginBulkLoad();
            loader.Put(row.Key, row.Row);
        }

        foreach (var loader in loaders.Values)
            loader.Commit();
        AppliedLsn = lsn;
    }

    public void Apply(CommitRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.Lsn <= AppliedLsn)
            return;

        if (record.WriteSet.Count == 1)
        {
            // The ordinary reducer commit. Grouping one op by table would cost more than it saves.
            var op = record.WriteSet[0];
            if (_tables.TryGetValue(op.Table, out var table))
                table.Apply([op]);
            AppliedLsn = record.Lsn;
            return;
        }

        // Several ops for one table become one version publish. Every intermediate version was
        // structurally shared but never observed — the whole record applies under the engine's
        // write lock, so no reader can land between two of its ops — and each one cost a path copy
        // of the row map plus one of every secondary index. Border batches and multi-row reducers
        // paid that per row.
        var byTable = new Dictionary<TableId, List<RowOp>>();
        foreach (var op in record.WriteSet)
        {
            if (!_tables.ContainsKey(op.Table))
                continue; // A table this projection doesn't know; nothing to project.
            if (!byTable.TryGetValue(op.Table, out var ops))
                byTable[op.Table] = ops = [];
            ops.Add(op);
        }

        foreach (var (id, ops) in byTable)
            _tables[id].Apply(ops);

        AppliedLsn = record.Lsn;
    }

    /// <summary>
    /// Captures every table's current version — see <see cref="IReadViewSource.OpenReadView"/>. The
    /// engine calls this holding the write lock, so the captured versions share one LSN; the cost is
    /// one dictionary of references, independent of how many rows the store holds.
    /// </summary>
    public IHotStoreReadView OpenReadView()
    {
        var captured = new Dictionary<TableId, PinnedTable>(_tables.Count);
        foreach (var (id, data) in _tables)
            captured.Add(id, new PinnedTable(data, data.Current));
        return new ReadView(AppliedLsn, captured);
    }

    public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row) =>
        _tables.TryGetValue(table, out var data)
            ? Reads.TryGetRow(data.Current, key, out row)
            : Reads.Missing(out row);

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table) =>
        _tables.TryGetValue(table, out var data) ? Reads.Scan(data, data.Current) : [];

    public long Count(TableId table) =>
        _tables.TryGetValue(table, out var data) ? data.Current.Rows.Count : 0;

    public IEnumerable<RowKey> ScanKeys(TableId table) =>
        _tables.TryGetValue(table, out var data) ? data.Current.Rows.Keys : [];

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value) =>
        _tables.TryGetValue(table, out var data) ? Reads.ScanIndex(data, data.Current, table, column, value) : [];

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high) =>
        _tables.TryGetValue(table, out var data) ? Reads.ScanIndexRange(data, data.Current, table, column, low, high) : [];

    public HotStoreStatistics Statistics()
    {
        var tables = new List<HotStoreTableStatistics>(_tables.Count);
        foreach (var table in _registry.Tables)
        {
            var data = _tables[table.Id];

            // Everything in this store is physically resident, so measured bytes are honest for
            // every table regardless of its declared residency label.
            tables.Add(new HotStoreTableStatistics(
                table.Id,
                table.Name,
                data.ResidencyLabel == Residency.Auto ? Residency.Resident : data.ResidencyLabel,
                data.Current.Rows.Count,
                data.ResidentBytes,
                PageFaults: 0,
                data.RowsScanned));
        }

        return new HotStoreStatistics { Tables = tables, BufferPoolCapacityBytes = 0 };
    }

    /// <summary>One table as a read view sees it: the owner (for schema and counters) and the pinned version.</summary>
    private readonly record struct PinnedTable(TableData Owner, TableVersion Version);

    /// <summary>
    /// A read view over versions captured at one LSN. Every read resolves against the captured
    /// version, so an <see cref="Apply"/> on the store — which only ever swaps a table's current
    /// version for a new one — is invisible here no matter how long the view is held.
    /// </summary>
    private sealed class ReadView(ulong lsn, Dictionary<TableId, PinnedTable> tables) : IHotStoreReadView
    {
        private bool _disposed;

        public ulong Lsn => lsn;

        public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row)
        {
            ThrowIfDisposed();
            return tables.TryGetValue(table, out var pinned)
                ? Reads.TryGetRow(pinned.Version, key, out row)
                : Reads.Missing(out row);
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table)
        {
            ThrowIfDisposed();
            return tables.TryGetValue(table, out var pinned) ? Reads.Scan(pinned.Owner, pinned.Version) : [];
        }

        public long Count(TableId table)
        {
            ThrowIfDisposed();
            return tables.TryGetValue(table, out var pinned) ? pinned.Version.Rows.Count : 0;
        }

        public IEnumerable<RowKey> ScanKeys(TableId table)
        {
            ThrowIfDisposed();
            return tables.TryGetValue(table, out var pinned) ? pinned.Version.Rows.Keys : [];
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
        {
            ThrowIfDisposed();
            return tables.TryGetValue(table, out var pinned)
                ? Reads.ScanIndex(pinned.Owner, pinned.Version, table, column, value)
                : [];
        }

        public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
        {
            ThrowIfDisposed();
            return tables.TryGetValue(table, out var pinned)
                ? Reads.ScanIndexRange(pinned.Owner, pinned.Version, table, column, low, high)
                : [];
        }

        /// <summary>
        /// Releases the captured versions so the garbage collector can reclaim whatever the pin was
        /// keeping alive. Reading a disposed view is a bug in the caller, not a stale read.
        /// </summary>
        public void Dispose()
        {
            _disposed = true;
            tables.Clear();
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(IHotStoreReadView), "This read view was disposed; its pinned LSN is no longer readable.");
        }
    }

    /// <summary>
    /// The read implementations, written once against a (table, version) pair so the live store and
    /// a pinned view cannot drift apart in behaviour — the difference between them is only which
    /// version they pass in.
    /// </summary>
    private static class Reads
    {
        public static bool Missing(out ReadOnlyMemory<byte> row)
        {
            row = default;
            return false;
        }

        public static bool TryGetRow(TableVersion version, in RowKey key, out ReadOnlyMemory<byte> row)
        {
            if (version.Rows.TryGetValue(key, out var bytes))
            {
                row = bytes;
                return true;
            }

            row = default;
            return false;
        }

        public static IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableData owner, TableVersion version)
        {
            // The scan counter is accumulated locally and published once: a per-row write to shared
            // state would be a data race against another thread's scan, and this is a statistic.
            long scanned = 0;
            try
            {
                foreach (var pair in version.Rows)
                {
                    scanned++;
                    yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(pair.Key, pair.Value);
                }
            }
            finally
            {
                owner.AddRowsScanned(scanned);
            }
        }

        public static IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(
            TableData owner, TableVersion version, TableId table, string column, RowKey value) =>
            Resolve(version, version.Index(owner.IndexPosition(table, column)).Equal(value));

        public static IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(
            TableData owner, TableVersion version, TableId table, string column, RowKey low, RowKey high) =>
            Resolve(version, version.Index(owner.IndexPosition(table, column)).Range(low, high));

        private static IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Resolve(
            TableVersion version, IEnumerable<RowKey> keys)
        {
            foreach (var key in keys)
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, version.Rows[key]);
        }
    }

    /// <summary>
    /// One immutable version of a table: its rows and its secondary indexes, positionally aligned
    /// with the schema's index list. A write publishes a new version; a read view holds an old one.
    /// Row payloads are shared across versions — they are never mutated in place, which is why a
    /// pinned view costs container nodes rather than a copy of the data.
    /// </summary>
    private sealed class TableVersion(
        ImmutableSortedDictionary<RowKey, byte[]> rows,
        ImmutableArray<SecondaryIndex> indexes)
    {
        public ImmutableSortedDictionary<RowKey, byte[]> Rows { get; } = rows;

        public ImmutableArray<SecondaryIndex> Indexes { get; } = indexes;

        public SecondaryIndex Index(int position) => Indexes[position];
    }

    private sealed class TableData
    {
        private readonly TableSchema _schema;
        private readonly Dictionary<string, int> _indexPositions = new(StringComparer.Ordinal);

        /// <summary>Indexed column names in index order — the codec's one-pass encode reads this per put.</summary>
        private readonly string[] _indexColumns;
        private long _rowsScanned;

        public TableData(TableSchema schema, Residency residencyLabel = Residency.Paged)
        {
            _schema = schema;
            ResidencyLabel = residencyLabel;
            _indexColumns = new string[schema.Indexes.Count];
            var indexes = ImmutableArray.CreateBuilder<SecondaryIndex>(schema.Indexes.Count);
            for (var i = 0; i < schema.Indexes.Count; i++)
            {
                _indexPositions[schema.Indexes[i].Column] = i;
                _indexColumns[i] = schema.Indexes[i].Column;
                indexes.Add(SecondaryIndex.Empty);
            }

            Current = new TableVersion(ImmutableSortedDictionary<RowKey, byte[]>.Empty, indexes.MoveToImmutable());
        }

        /// <summary>
        /// The table's current version. Written only under the engine's write lock and read without
        /// one, so the field is volatile: a reader must never see a torn or stale reference.
        /// </summary>
        public TableVersion Current
        {
            get => Volatile.Read(ref _current);
            private set => Volatile.Write(ref _current, value);
        }

        private TableVersion _current = null!;

        public Residency ResidencyLabel { get; }

        public long ResidentBytes { get; private set; }

        public long RowsScanned => Interlocked.Read(ref _rowsScanned);

        public void AddRowsScanned(long count)
        {
            if (count != 0)
                Interlocked.Add(ref _rowsScanned, count);
        }

        public int IndexPosition(TableId table, string column) =>
            _indexPositions.TryGetValue(column, out var position)
                ? position
                : throw new ArgumentException($"Table {table} has no index on column '{column}'.", nameof(column));

        /// <summary>
        /// Applies one record's ops for this table as a single version publish. The ops thread
        /// through local row and index references, so a later op in the same record sees the
        /// earlier ones — and only the result is published.
        /// </summary>
        public void Apply(List<RowOp> ops)
        {
            var current = Current;
            var rows = current.Rows;
            var indexes = current.Indexes;
            foreach (var op in ops)
            {
                if (op.Kind == RowOpKind.Delete)
                {
                    if (!rows.TryGetValue(op.Key, out var removed))
                        continue;
                    ResidentBytes -= op.Key.Length + removed.Length;
                    indexes = Unindex(indexes, op.Key, removed);
                    rows = rows.Remove(op.Key);
                    continue;
                }

                if (rows.TryGetValue(op.Key, out var previous))
                {
                    indexes = Unindex(indexes, op.Key, previous);
                    ResidentBytes -= previous.Length;
                }
                else
                {
                    ResidentBytes += op.Key.Length;
                }

                var bytes = op.Row.ToArray();
                ResidentBytes += bytes.Length;
                rows = rows.SetItem(op.Key, bytes);
                indexes = Index(indexes, op.Key, bytes);
            }

            Current = new TableVersion(rows, indexes);
        }

        /// <summary>
        /// Begins a bulk load through the persistent containers' builders — the recovery path, where
        /// no read view exists yet and publishing one version per row would be pure waste.
        /// </summary>
        public BulkLoader BeginBulkLoad() => new(this);

        private ImmutableArray<SecondaryIndex> Index(
            ImmutableArray<SecondaryIndex> indexes,
            RowKey key,
            byte[] bytes)
        {
            if (indexes.Length == 0)
                return indexes;
            var values = IndexedValues(bytes);
            for (var position = 0; position < values.Length; position++)
            {
                var value = values[position];
                if (value.Length == 0)
                    continue; // A null column value is unindexed.
                indexes = indexes.SetItem(position, indexes[position].Add(value, key));
            }

            return indexes;
        }

        private ImmutableArray<SecondaryIndex> Unindex(
            ImmutableArray<SecondaryIndex> indexes,
            RowKey key,
            byte[] bytes)
        {
            if (indexes.Length == 0)
                return indexes;
            var values = IndexedValues(bytes);
            for (var position = 0; position < values.Length; position++)
            {
                var value = values[position];
                if (value.Length == 0)
                    continue;
                indexes = indexes.SetItem(position, indexes[position].Remove(value, key));
            }

            return indexes;
        }

        /// <summary>
        /// Every indexed column's encoded value for one row, positionally aligned with the schema's
        /// index list; a zero-length key is a null column value, which is not indexed.
        /// <para>
        /// One pass over the row for the whole set. Asking for a column at a time deserialized the
        /// entire row per index, so a three-index table paid three full deserializes — and three
        /// re-allocations of that row's string and byte columns — on every put and every remove.
        /// </para>
        /// </summary>
        private RowKey[] IndexedValues(byte[] bytes)
        {
            var values = new RowKey[_indexColumns.Length];

            // The generated codec path: no reflection, no boxing. Falls back to the boxed column
            // accessors when the schema was built by reflection.
            if (_schema.Codec is { } codec)
            {
                codec.EncodeColumnsFromBytes(bytes, _indexColumns, values);
                return values;
            }

            var row = RowSerializer.Deserialize(_schema, bytes);
            for (var i = 0; i < _indexColumns.Length; i++)
            {
                var column = _schema.Column(_indexColumns[i]);
                var value = column.GetValue(row);
                values[i] = value is null ? default : SchemaKeyCodec.Encode(column, value);
            }

            return values;
        }

        /// <summary>Accumulates a bulk load into builders and publishes one version at the end.</summary>
        internal sealed class BulkLoader
        {
            private readonly TableData _table;
            private readonly ImmutableSortedDictionary<RowKey, byte[]>.Builder _rows;
            private readonly SecondaryIndex.Builder[] _indexes;

            public BulkLoader(TableData table)
            {
                _table = table;
                var current = table.Current;
                _rows = current.Rows.ToBuilder();
                _indexes = new SecondaryIndex.Builder[current.Indexes.Length];
                for (var i = 0; i < _indexes.Length; i++)
                    _indexes[i] = current.Indexes[i].ToBuilder();
            }

            public void Put(RowKey key, ReadOnlyMemory<byte> row)
            {
                var bytes = row.ToArray();
                if (!_rows.ContainsKey(key))
                    _table.ResidentBytes += key.Length;
                _rows[key] = bytes;
                _table.ResidentBytes += bytes.Length;
                var values = _table.IndexedValues(bytes);
                for (var position = 0; position < values.Length; position++)
                {
                    var value = values[position];
                    if (value.Length == 0)
                        continue;
                    _indexes[position].Add(value, key);
                }
            }

            public void Commit()
            {
                var indexes = ImmutableArray.CreateBuilder<SecondaryIndex>(_indexes.Length);
                foreach (var index in _indexes)
                    indexes.Add(index.ToImmutable());
                _table.Current = new TableVersion(_rows.ToImmutable(), indexes.MoveToImmutable());
            }
        }
    }
}
