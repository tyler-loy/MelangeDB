namespace MelangeDB;

/// <summary>
/// The seam in front of the hot tier. A hot store is a projection of the commit log: it consumes
/// whole commit records through <see cref="Apply"/> and owns its secondary index maintenance, so a
/// storage engine swap never touches the applier pipeline. Rows are held and served in their
/// serialized form; the serialized bytes are the identity of a row's state.
/// </summary>
public interface IHotStore
{
    /// <summary>The LSN of the last record applied to this projection.</summary>
    ulong AppliedLsn { get; }

    /// <summary>
    /// Applies one commit record atomically. Records at or below <see cref="AppliedLsn"/> are
    /// ignored, making replay idempotent. Insert and update both put; delete removes.
    /// </summary>
    void Apply(CommitRecord record);

    /// <summary>Looks up a row's serialized bytes by primary key.</summary>
    bool TryGetRow(TableId table, in RowKey key, out ReadOnlyMemory<byte> row);

    /// <summary>Enumerates a table's rows in primary-key order.</summary>
    IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> Scan(TableId table);

    /// <summary>
    /// Enumerates rows whose indexed column's encoded value equals <paramref name="value"/>,
    /// in primary-key order.
    /// </summary>
    IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndex(TableId table, string column, RowKey value);

    /// <summary>
    /// Enumerates rows whose indexed column's encoded value falls within
    /// [<paramref name="low"/>, <paramref name="high"/>], both inclusive, in index-value order.
    /// </summary>
    IEnumerable<KeyValuePair<RowKey, ReadOnlyMemory<byte>>> ScanIndexRange(TableId table, string column, RowKey low, RowKey high);
}
