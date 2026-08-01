namespace MelangeDB.Core;

/// <summary>
/// The overlay: a transaction's read/write view resolving the uncommitted write set before the
/// store, which is what makes read-your-writes work with no I/O in a reducer body. When a table's
/// schema carries a generated <see cref="RowCodec{TRow}"/>, every serialize, deserialize, and key
/// encode dispatches through it — no reflection and no boxed rows on the invocation path; the
/// boxed <see cref="RowSerializer"/> path remains for reflection-built schemas.
/// </summary>
internal sealed class TransactionDb : IDbView
{
    private readonly SchemaRegistry _registry;
    private readonly IHotStore _store;
    private readonly WriteSet _writeSet;
    private readonly AutoIncStage _autoInc;
    private readonly TableAccessGuard? _guard;

    public TransactionDb(SchemaRegistry registry, IHotStore store, WriteSet writeSet, AutoIncStage autoInc, TableAccessGuard? guard = null)
    {
        _registry = registry;
        _store = store;
        _writeSet = writeSet;
        _autoInc = autoInc;
        _guard = guard;
    }

    /// <summary>Resolves a row type's schema, consulting the placement visibility guard when one is installed.</summary>
    private TableSchema Resolve<TRow>(TableAccess access)
        where TRow : struct
    {
        var schema = _registry.Get(typeof(TRow));
        _guard?.Invoke(schema, access);
        return schema;
    }

    public TRow Insert<TRow>(TRow row)
        where TRow : struct
    {
        var schema = Resolve<TRow>(TableAccess.Write);
        if (schema.Codec is RowCodec<TRow> codec)
        {
            codec.AssignAutoInc(ref row, _autoInc, schema.Id);
            var key = codec.EncodePrimaryKey(in row);
            if (Exists(schema.Id, key))
                throw new InvalidOperationException($"Table '{schema.Name}': a row with primary key {key} already exists.");
            CheckUniqueConstraints(schema, codec, key, in row);
            _writeSet.Stage(new RowOp(RowOpKind.Insert, schema.Id, key, codec.Serialize(in row)));
            return row;
        }

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

        var boxedKey = SchemaKeyCodec.Encode(schema.PrimaryKey, schema.PrimaryKey.GetValue(boxed));
        if (Exists(schema.Id, boxedKey))
            throw new InvalidOperationException($"Table '{schema.Name}': a row with primary key {boxedKey} already exists.");
        CheckUniqueConstraints(schema, boxedKey, boxed);
        _writeSet.Stage(new RowOp(RowOpKind.Insert, schema.Id, boxedKey, RowSerializer.Serialize(schema, boxed)));
        return (TRow)boxed;
    }

    public void Update<TRow>(TRow row)
        where TRow : struct
    {
        var schema = Resolve<TRow>(TableAccess.Write);
        if (schema.Codec is RowCodec<TRow> codec)
        {
            var key = codec.EncodePrimaryKey(in row);
            if (!Exists(schema.Id, key))
                throw new InvalidOperationException($"Table '{schema.Name}': no row with primary key {key} to update.");
            CheckUniqueConstraints(schema, codec, key, in row);
            _writeSet.Stage(new RowOp(RowOpKind.Update, schema.Id, key, codec.Serialize(in row)));
            return;
        }

        object boxed = row;
        var boxedKey = SchemaKeyCodec.Encode(schema.PrimaryKey, schema.PrimaryKey.GetValue(boxed));
        if (!Exists(schema.Id, boxedKey))
            throw new InvalidOperationException($"Table '{schema.Name}': no row with primary key {boxedKey} to update.");
        CheckUniqueConstraints(schema, boxedKey, boxed);
        _writeSet.Stage(new RowOp(RowOpKind.Update, schema.Id, boxedKey, RowSerializer.Serialize(schema, boxed)));
    }

    public bool Delete<TRow>(object primaryKey)
        where TRow : struct
    {
        ArgumentNullException.ThrowIfNull(primaryKey);
        var schema = Resolve<TRow>(TableAccess.Write);
        var key = SchemaKeyCodec.Encode(schema.PrimaryKey, primaryKey);
        if (!Exists(schema.Id, key))
            return false;
        _writeSet.Stage(new RowOp(RowOpKind.Delete, schema.Id, key));
        return true;
    }

    public TRow? Find<TRow>(object primaryKey)
        where TRow : struct
    {
        ArgumentNullException.ThrowIfNull(primaryKey);
        var schema = Resolve<TRow>(TableAccess.Read);
        var key = SchemaKeyCodec.Encode(schema.PrimaryKey, primaryKey);
        if (_writeSet.TryGetPending(schema.Id, key, out var pending))
        {
            return pending.Kind == RowOpKind.Delete
                ? null
                : Materialize<TRow>(schema, pending.Row);
        }

        return _store.TryGetRow(schema.Id, key, out var stored)
            ? Materialize<TRow>(schema, stored)
            : null;
    }

    public IEnumerable<TRow> Scan<TRow>()
        where TRow : struct
    {
        var schema = Resolve<TRow>(TableAccess.Read);
        foreach (var (_, bytes) in ScanMerged(schema))
            yield return Materialize<TRow>(schema, bytes);
    }

    public IEnumerable<TRow> Filter<TRow>(string column, object value)
        where TRow : struct
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(value);
        var schema = Resolve<TRow>(TableAccess.Read);
        var columnSchema = RequireIndexed(schema, column);
        var encoded = SchemaKeyCodec.Encode(columnSchema, value);

        if (columnSchema.IsPrimaryKey)
        {
            if (FindByEncodedKey<TRow>(schema, encoded) is { } single)
                yield return single;
            yield break;
        }

        foreach (var row in FilterCore<TRow>(schema, columnSchema, _store.ScanIndex(schema.Id, column, encoded), v => v == encoded))
            yield return row;
    }

    public IEnumerable<TRow> FilterRange<TRow>(string column, object low, object high)
        where TRow : struct
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(low);
        ArgumentNullException.ThrowIfNull(high);
        var schema = Resolve<TRow>(TableAccess.Read);
        var columnSchema = RequireIndexed(schema, column);
        var lowKey = SchemaKeyCodec.Encode(columnSchema, low);
        var highKey = SchemaKeyCodec.Encode(columnSchema, high);

        if (columnSchema.IsPrimaryKey)
        {
            foreach (var (key, bytes) in ScanMerged(schema))
            {
                if (key.CompareTo(lowKey) >= 0 && key.CompareTo(highKey) <= 0)
                    yield return Materialize<TRow>(schema, bytes);
            }

            yield break;
        }

        foreach (var row in FilterCore<TRow>(
            schema,
            columnSchema,
            _store.ScanIndexRange(schema.Id, column, lowKey, highKey),
            v => v.CompareTo(lowKey) >= 0 && v.CompareTo(highKey) <= 0))
        {
            yield return row;
        }
    }

    public bool Any<TRow>()
        where TRow : struct
        => Count<TRow>() > 0;

    /// <summary>
    /// The overlay-aware count: the store's O(1) row count adjusted by the pending ops, so an
    /// existence check materializes no row and pages nothing in. An Insert op is a row the store
    /// lacks; a Delete op removes one it has; an Update is neutral.
    /// </summary>
    public long Count<TRow>()
        where TRow : struct
    {
        var schema = Resolve<TRow>(TableAccess.Read);
        var count = _store.Count(schema.Id);
        foreach (var op in _writeSet.OpsFor(schema.Id))
        {
            if (op.Kind == RowOpKind.Insert)
                count++;
            else if (op.Kind == RowOpKind.Delete)
                count--;
        }

        return count;
    }

    /// <summary>
    /// The first row in primary-key order: the store's first key raced against the overlay's, so
    /// at most one row materializes. A pending delete of the store's first key falls back to the
    /// merged scan — rare, and still lazy.
    /// </summary>
    public TRow? First<TRow>()
        where TRow : struct
    {
        var schema = Resolve<TRow>(TableAccess.Read);
        var hasPending = false;
        foreach (var _ in _writeSet.OpsFor(schema.Id))
        {
            hasPending = true;
            break;
        }

        if (!hasPending)
        {
            // The common committed-state path: the store's scan is lazy, so exactly one row
            // materializes and nothing else pages in.
            foreach (var (_, bytes) in _store.Scan(schema.Id))
                return Materialize<TRow>(schema, bytes);
            return null;
        }

        foreach (var (_, bytes) in ScanMerged(schema))
            return Materialize<TRow>(schema, bytes);
        return null;
    }

    /// <summary>
    /// Stages a delete by pre-encoded key when the row still exists in the overlay — the
    /// scheduler's one-shot consumption, which must tolerate the reducer body having already
    /// deleted (or replaced) its own timer row.
    /// </summary>
    internal void DeleteExisting(TableSchema schema, RowKey key)
    {
        if (Exists(schema.Id, key))
            _writeSet.Stage(new RowOp(RowOpKind.Delete, schema.Id, key));
    }

    private static ColumnSchema RequireIndexed(TableSchema schema, string column)
    {
        var columnSchema = schema.Column(column);
        if (columnSchema is { IsIndexed: false, IsUnique: false, IsPrimaryKey: false })
            throw new InvalidOperationException($"Table '{schema.Name}': column '{column}' is not indexed; declare [Index] or [Unique].");
        return columnSchema;
    }

    /// <summary>Store index hits with pending rows overlaid, matched by encoded column value.</summary>
    private IEnumerable<TRow> FilterCore<TRow>(
        TableSchema schema,
        ColumnSchema column,
        IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> storeHits,
        Func<RowKey, bool> matches)
        where TRow : struct
    {
        foreach (var (key, bytes) in storeHits)
        {
            if (_writeSet.TryGetPending(schema.Id, key, out _))
                continue; // The pending version is considered below.
            yield return Materialize<TRow>(schema, bytes);
        }

        foreach (var op in _writeSet.OpsFor(schema.Id))
        {
            if (op.Kind == RowOpKind.Delete)
                continue;
            if (EncodePendingColumn(schema, column, op.Row) is { } pendingValue && matches(pendingValue))
                yield return Materialize<TRow>(schema, op.Row);
        }
    }

    private static TRow Materialize<TRow>(TableSchema schema, ReadOnlyMemory<byte> bytes)
        where TRow : struct
        => schema.Codec is RowCodec<TRow> codec
            ? codec.Deserialize(bytes.Span)
            : (TRow)RowSerializer.Deserialize(schema, bytes);

    private static RowKey? EncodePendingColumn(TableSchema schema, ColumnSchema column, ReadOnlyMemory<byte> rowBytes)
    {
        if (schema.Codec is { } codec)
            return codec.EncodeColumnFromBytes(column.Name, rowBytes.Span);
        var row = RowSerializer.Deserialize(schema, rowBytes);
        var value = column.GetValue(row);
        return value is null ? null : SchemaKeyCodec.Encode(column, value);
    }

    private TRow? FindByEncodedKey<TRow>(TableSchema schema, RowKey key)
        where TRow : struct
    {
        if (_writeSet.TryGetPending(schema.Id, key, out var pending))
            return pending.Kind == RowOpKind.Delete ? null : Materialize<TRow>(schema, pending.Row);
        return _store.TryGetRow(schema.Id, key, out var stored) ? Materialize<TRow>(schema, stored) : null;
    }

    private bool Exists(TableId table, RowKey key)
    {
        if (_writeSet.TryGetPending(table, key, out var pending))
            return pending.Kind != RowOpKind.Delete;
        return _store.TryGetRow(table, key, out _);
    }

    private void CheckUniqueConstraints<TRow>(TableSchema schema, RowCodec<TRow> codec, RowKey selfKey, in TRow row)
        where TRow : struct
    {
        foreach (var index in schema.Indexes)
        {
            if (!index.Unique)
                continue;
            if (codec.EncodeColumn(index.Column, in row) is not { } encoded)
                continue;
            CheckUniqueValue(schema, schema.Column(index.Column), selfKey, encoded);
        }
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
            CheckUniqueValue(schema, column, selfKey, SchemaKeyCodec.Encode(column, value));
        }
    }

    private void CheckUniqueValue(TableSchema schema, ColumnSchema column, RowKey selfKey, RowKey encoded)
    {
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
            if (EncodePendingColumn(schema, column, op.Row) == encoded)
                throw new InvalidOperationException($"Table '{schema.Name}': unique constraint on '{column.Name}' violated.");
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
