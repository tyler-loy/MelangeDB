namespace MelangeDB.Core;

/// <summary>
/// The dictionary-backed hot store: a legitimate projection of the commit log, not a stub, because
/// the log is the source of record. Rows are held in serialized form, so replaying the same log
/// into a fresh instance yields byte-identical state. Owns its secondary indexes — index
/// maintenance is store-owned, so a phase 07 engine swap never touches the applier pipeline.
/// </summary>
public sealed class InMemoryHotStore : IHotStore
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
        foreach (var row in rows)
        {
            if (_tables.TryGetValue(row.Table, out var table))
                table.Put(row.Key, row.Row);
        }

        AppliedLsn = lsn;
    }

    public void Apply(CommitRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.Lsn <= AppliedLsn)
            return;
        foreach (var op in record.WriteSet)
        {
            if (!_tables.TryGetValue(op.Table, out var table))
                continue; // A table this projection doesn't know; nothing to project.
            if (op.Kind == RowOpKind.Delete)
                table.Remove(op.Key);
            else
                table.Put(op.Key, op.Row);
        }

        AppliedLsn = record.Lsn;
    }

    public bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row)
    {
        if (_tables.TryGetValue(table, out var data) && data.Rows.TryGetValue(key, out var bytes))
        {
            row = bytes;
            return true;
        }

        row = default;
        return false;
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table)
    {
        if (!_tables.TryGetValue(table, out var data))
            yield break;
        foreach (var pair in data.Rows)
        {
            data.RowsScanned++;
            yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(pair.Key, pair.Value);
        }
    }

    public long Count(TableId table) =>
        _tables.TryGetValue(table, out var data) ? data.Rows.Count : 0;

    public IEnumerable<RowKey> ScanKeys(TableId table)
    {
        if (!_tables.TryGetValue(table, out var data))
            yield break;
        foreach (var key in data.Rows.Keys)
            yield return key;
    }

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
                data.Rows.Count,
                data.ResidentBytes,
                PageFaults: 0,
                data.RowsScanned));
        }

        return new HotStoreStatistics { Tables = tables, BufferPoolCapacityBytes = 0 };
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value)
    {
        if (!_tables.TryGetValue(table, out var data))
            yield break;
        if (!data.Indexes.TryGetValue(column, out var index))
            throw new ArgumentException($"Table {table} has no index on column '{column}'.", nameof(column));
        if (!index.TryGetValue(value, out var keys))
            yield break;
        foreach (var key in keys)
            yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, data.Rows[key]);
    }

    public IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high)
    {
        if (!_tables.TryGetValue(table, out var data))
            yield break;
        if (!data.Indexes.TryGetValue(column, out var index))
            throw new ArgumentException($"Table {table} has no index on column '{column}'.", nameof(column));
        foreach (var (value, keys) in index)
        {
            if (value.CompareTo(low) < 0)
                continue;
            if (value.CompareTo(high) > 0)
                yield break;
            foreach (var key in keys)
                yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(key, data.Rows[key]);
        }
    }

    private sealed class TableData
    {
        private readonly TableSchema _schema;

        public TableData(TableSchema schema, Residency residencyLabel = Residency.Paged)
        {
            _schema = schema;
            ResidencyLabel = residencyLabel;
            foreach (var index in schema.Indexes)
                Indexes.Add(index.Column, []);
        }

        public SortedDictionary<RowKey, byte[]> Rows { get; } = [];

        public Dictionary<string, SortedDictionary<RowKey, SortedSet<RowKey>>> Indexes { get; } = [];

        public Residency ResidencyLabel { get; }

        public long ResidentBytes { get; private set; }

        public long RowsScanned { get; set; }

        public void Put(RowKey key, ReadOnlyMemory<byte> row)
        {
            if (Rows.TryGetValue(key, out var previous))
            {
                UnindexRow(key, previous);
                ResidentBytes -= previous.Length;
            }
            else
            {
                ResidentBytes += key.Length;
            }

            var bytes = row.ToArray();
            Rows[key] = bytes;
            ResidentBytes += bytes.Length;
            IndexRow(key, bytes);
        }

        public void Remove(RowKey key)
        {
            if (!Rows.Remove(key, out var previous))
                return;
            ResidentBytes -= key.Length + previous.Length;
            UnindexRow(key, previous);
        }

        private void IndexRow(RowKey key, byte[] bytes)
        {
            if (Indexes.Count == 0)
                return;
            foreach (var (column, entries) in IndexedValues(bytes))
            {
                if (!Indexes[column].TryGetValue(entries, out var keys))
                    Indexes[column][entries] = keys = [];
                keys.Add(key);
            }
        }

        private void UnindexRow(RowKey key, byte[] bytes)
        {
            if (Indexes.Count == 0)
                return;
            foreach (var (column, value) in IndexedValues(bytes))
            {
                if (Indexes[column].TryGetValue(value, out var keys))
                {
                    keys.Remove(key);
                    if (keys.Count == 0)
                        Indexes[column].Remove(value);
                }
            }
        }

        private IEnumerable<(string Column, RowKey Value)> IndexedValues(byte[] bytes)
        {
            // The generated codec path: no reflection, no boxing. Falls back to the boxed column
            // accessors when the schema was built by reflection.
            if (_schema.Codec is { } codec)
            {
                foreach (var index in _schema.Indexes)
                {
                    if (codec.EncodeColumnFromBytes(index.Column, bytes) is { } value)
                        yield return (index.Column, value);
                }

                yield break;
            }

            var row = RowSerializer.Deserialize(_schema, bytes);
            foreach (var index in _schema.Indexes)
            {
                var column = _schema.Column(index.Column);
                var value = column.GetValue(row);
                if (value is not null)
                    yield return (index.Column, SchemaKeyCodec.Encode(column, value));
            }
        }
    }
}
