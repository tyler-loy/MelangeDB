namespace MelangeDB.Core;

/// <summary>
/// The overlay: a transaction's read/write view resolving the uncommitted write set before the
/// store, which is what makes read-your-writes work with no I/O in a reducer body.
/// </summary>
internal sealed class TransactionDb : IDbView
{
    private readonly SchemaRegistry _registry;
    private readonly IHotStore _store;
    private readonly WriteSet _writeSet;
    private readonly AutoIncStage _autoInc;

    public TransactionDb(SchemaRegistry registry, IHotStore store, WriteSet writeSet, AutoIncStage autoInc)
    {
        _registry = registry;
        _store = store;
        _writeSet = writeSet;
        _autoInc = autoInc;
    }

    public TRow Insert<TRow>(TRow row)
        where TRow : struct
    {
        var schema = _registry.Get(typeof(TRow));
        object boxed = row;
        foreach (var column in schema.AutoIncColumns)
        {
            var current = AutoIncSequencer.ToUInt64(column.GetValue(boxed));
            if (current is 0)
            {
                var id = _autoInc.Allocate(schema.Id);
                column.SetValue(boxed, column.Kind == ColumnKind.Int64 ? (long)id : id);
            }
            else if (current is { } explicitId)
            {
                _autoInc.ObserveExplicit(schema.Id, explicitId);
            }
        }

        var key = KeyCodec.Encode(schema.PrimaryKey, schema.PrimaryKey.GetValue(boxed));
        if (Exists(schema.Id, key))
            throw new InvalidOperationException($"Table '{schema.Name}': a row with primary key {key} already exists.");
        CheckUniqueConstraints(schema, key, boxed);
        var bytes = RowSerializer.Serialize(schema, boxed);
        _writeSet.Stage(new RowOp(RowOpKind.Insert, schema.Id, key, bytes));
        return (TRow)boxed;
    }

    public void Update<TRow>(TRow row)
        where TRow : struct
    {
        var schema = _registry.Get(typeof(TRow));
        object boxed = row;
        var key = KeyCodec.Encode(schema.PrimaryKey, schema.PrimaryKey.GetValue(boxed));
        if (!Exists(schema.Id, key))
            throw new InvalidOperationException($"Table '{schema.Name}': no row with primary key {key} to update.");
        CheckUniqueConstraints(schema, key, boxed);
        var bytes = RowSerializer.Serialize(schema, boxed);
        _writeSet.Stage(new RowOp(RowOpKind.Update, schema.Id, key, bytes));
    }

    public bool Delete<TRow>(object primaryKey)
        where TRow : struct
    {
        ArgumentNullException.ThrowIfNull(primaryKey);
        var schema = _registry.Get(typeof(TRow));
        var key = KeyCodec.Encode(schema.PrimaryKey, primaryKey);
        if (!Exists(schema.Id, key))
            return false;
        _writeSet.Stage(new RowOp(RowOpKind.Delete, schema.Id, key));
        return true;
    }

    public TRow? Find<TRow>(object primaryKey)
        where TRow : struct
    {
        ArgumentNullException.ThrowIfNull(primaryKey);
        var schema = _registry.Get(typeof(TRow));
        var key = KeyCodec.Encode(schema.PrimaryKey, primaryKey);
        if (_writeSet.TryGetPending(schema.Id, key, out var pending))
        {
            return pending.Kind == RowOpKind.Delete
                ? null
                : (TRow)RowSerializer.Deserialize(schema, pending.Row);
        }

        return _store.TryGetRow(schema.Id, key, out var stored)
            ? (TRow)RowSerializer.Deserialize(schema, stored)
            : null;
    }

    public IEnumerable<TRow> Scan<TRow>()
        where TRow : struct
    {
        var schema = _registry.Get(typeof(TRow));
        foreach (var (_, bytes) in ScanMerged(schema))
            yield return (TRow)RowSerializer.Deserialize(schema, bytes);
    }

    public IEnumerable<TRow> Filter<TRow>(string column, object value)
        where TRow : struct
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(value);
        var schema = _registry.Get(typeof(TRow));
        var columnSchema = schema.Column(column);
        if (columnSchema is { IsIndexed: false, IsUnique: false, IsPrimaryKey: false })
            throw new InvalidOperationException($"Table '{schema.Name}': column '{column}' is not indexed; declare [Index] or [Unique].");
        var encoded = KeyCodec.Encode(columnSchema, value);

        if (columnSchema.IsPrimaryKey)
        {
            var single = FindByEncodedKey(schema, encoded);
            if (single is not null)
                yield return (TRow)single;
            yield break;
        }

        foreach (var (key, bytes) in _store.ScanIndex(schema.Id, column, encoded))
        {
            if (_writeSet.TryGetPending(schema.Id, key, out _))
                continue; // The pending version is considered below.
            yield return (TRow)RowSerializer.Deserialize(schema, bytes);
        }

        foreach (var op in _writeSet.OpsFor(schema.Id))
        {
            if (op.Kind == RowOpKind.Delete)
                continue;
            var row = RowSerializer.Deserialize(schema, op.Row);
            if (KeyCodec.Encode(columnSchema, columnSchema.GetValue(row)) == encoded)
                yield return (TRow)row;
        }
    }

    private object? FindByEncodedKey(TableSchema schema, RowKey key)
    {
        if (_writeSet.TryGetPending(schema.Id, key, out var pending))
            return pending.Kind == RowOpKind.Delete ? null : RowSerializer.Deserialize(schema, pending.Row);
        return _store.TryGetRow(schema.Id, key, out var stored) ? RowSerializer.Deserialize(schema, stored) : null;
    }

    private bool Exists(TableId table, RowKey key)
    {
        if (_writeSet.TryGetPending(table, key, out var pending))
            return pending.Kind != RowOpKind.Delete;
        return _store.TryGetRow(table, key, out _);
    }

    private void CheckUniqueConstraints(TableSchema schema, RowKey selfKey, object row)
    {
        foreach (var index in schema.Indexes)
        {
            if (!index.Unique)
                continue;
            var column = schema.Column(index.Column);
            var value = column.GetValue(row);
            if (value is null)
                continue;
            var encoded = KeyCodec.Encode(column, value);

            foreach (var (key, _) in _store.ScanIndex(schema.Id, column.Name, encoded))
            {
                // A pending op on the conflicting row supersedes its stored version; the pending
                // scan below re-evaluates it.
                if (key != selfKey && !_writeSet.TryGetPending(schema.Id, key, out _))
                    throw new InvalidOperationException($"Table '{schema.Name}': unique constraint on '{column.Name}' violated.");
            }

            foreach (var op in _writeSet.OpsFor(schema.Id))
            {
                if (op.Kind == RowOpKind.Delete || op.Key == selfKey)
                    continue;
                var pendingRow = RowSerializer.Deserialize(schema, op.Row);
                var pendingValue = column.GetValue(pendingRow);
                if (pendingValue is not null && KeyCodec.Encode(column, pendingValue) == encoded)
                    throw new InvalidOperationException($"Table '{schema.Name}': unique constraint on '{column.Name}' violated.");
            }
        }
    }

    private IEnumerable<(RowKey Key, ReadOnlyMemory<byte> Row)> ScanMerged(TableSchema schema)
    {
        var pendingKeys = new SortedSet<RowKey>();
        foreach (var op in _writeSet.OpsFor(schema.Id))
            pendingKeys.Add(op.Key);

        var merged = new SortedDictionary<RowKey, ReadOnlyMemory<byte>>();
        foreach (var pair in _store.Scan(schema.Id))
        {
            if (!pendingKeys.Contains(pair.Key))
                merged[pair.Key] = pair.Value;
        }

        foreach (var key in pendingKeys)
        {
            if (_writeSet.TryGetPending(schema.Id, key, out var op) && op.Kind != RowOpKind.Delete)
                merged[key] = op.Row;
        }

        foreach (var pair in merged)
            yield return (pair.Key, pair.Value);
    }
}
