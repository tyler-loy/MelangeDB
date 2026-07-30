namespace MelangeDB;

/// <summary>The kind of a row operation in a write set.</summary>
public enum RowOpKind : byte
{
    Insert = 1,
    Update = 2,
    Delete = 3,
}

/// <summary>
/// One row-level operation in a write set, keyed by table and primary key. For
/// <see cref="RowOpKind.Delete"/> the row payload is empty; otherwise it carries the full
/// serialized row.
/// </summary>
public readonly struct RowOp
{
    public RowOp(RowOpKind kind, TableId table, RowKey key, ReadOnlyMemory<byte> row = default)
    {
        if (kind == RowOpKind.Delete && !row.IsEmpty)
            throw new ArgumentException("A delete op carries no row payload.", nameof(row));
        if (kind is RowOpKind.Insert or RowOpKind.Update && row.IsEmpty)
            throw new ArgumentException($"An {kind} op requires a row payload.", nameof(row));
        Kind = kind;
        Table = table;
        Key = key;
        Row = row;
    }

    public RowOpKind Kind { get; }

    public TableId Table { get; }

    public RowKey Key { get; }

    /// <summary>The serialized row; empty for deletes.</summary>
    public ReadOnlyMemory<byte> Row { get; }
}
