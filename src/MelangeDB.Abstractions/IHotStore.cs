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

    /// <summary>
    /// The number of rows in a table. O(1) for every store — this is what backs the existence
    /// APIs, so an existence check never pages a row in.
    /// </summary>
    long Count(TableId table);

    /// <summary>
    /// Enumerates a table's primary keys in order without materializing rows — a key walk touches
    /// the store's key directory only, never the buffer pool, so it faults nothing in.
    /// </summary>
    IEnumerable<RowKey> ScanKeys(TableId table);

    /// <summary>The store's current footprint and counters; see <see cref="HotStoreStatistics"/>.</summary>
    HotStoreStatistics Statistics();

    /// <summary>
    /// Bootstraps the projection from a snapshot at <paramref name="lsn"/>: streams every row in,
    /// then sets <see cref="AppliedLsn"/> to the snapshot LSN so replay resumes at the record
    /// after it. Called at most once, on an empty store, before any <see cref="Apply"/>. The row
    /// stream is consumed as it is enumerated, so a snapshot larger than memory loads without a
    /// materialized copy in between.
    /// </summary>
    void LoadSnapshot(ulong lsn, IEnumerable<SnapshotRow> rows);
}

/// <summary>One row of a snapshot being loaded: table, primary key, and serialized bytes.</summary>
public readonly record struct SnapshotRow(TableId Table, RowKey Key, ReadOnlyMemory<byte> Row);
