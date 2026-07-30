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
    {
        ArgumentNullException.ThrowIfNull(registry);
        _registry = registry;
        foreach (var table in registry.Tables)
            _tables.Add(table.Id, new TableData(table));
    }

    public ulong AppliedLsn { get; private set; }

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
            yield return new KeyValuePair<RowKey, ReadOnlyMemory<byte>>(pair.Key, pair.Value);
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

        public TableData(TableSchema schema)
        {
            _schema = schema;
            foreach (var index in schema.Indexes)
                Indexes.Add(index.Column, []);
        }

        public SortedDictionary<RowKey, byte[]> Rows { get; } = [];

        public Dictionary<string, SortedDictionary<RowKey, SortedSet<RowKey>>> Indexes { get; } = [];

        public void Put(RowKey key, ReadOnlyMemory<byte> row)
        {
            if (Rows.TryGetValue(key, out var previous))
                UnindexRow(key, previous);
            var bytes = row.ToArray();
            Rows[key] = bytes;
            IndexRow(key, bytes);
        }

        public void Remove(RowKey key)
        {
            if (!Rows.Remove(key, out var previous))
                return;
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
                    yield return (index.Column, KeyCodec.Encode(column, value));
            }
        }
    }
}
