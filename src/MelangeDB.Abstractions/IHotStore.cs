namespace MelangeDB;

/// <summary>
/// The read surface of the hot tier, shared by the live store and by a pinned read view over it
/// (<see cref="IHotStoreReadView"/>). Rows are served in their serialized form; the serialized bytes
/// are the identity of a row's state.
/// </summary>
public interface IHotStoreReader
{
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
}

/// <summary>
/// The seam in front of the hot tier. A hot store is a projection of the commit log: it consumes
/// whole commit records through <see cref="Apply"/> and owns its secondary index maintenance, so a
/// storage engine swap never touches the applier pipeline.
/// <para>
/// <b>Thread safety:</b> reads through this interface are safe only while no <see cref="Apply"/>
/// runs — which is what the engine's write lock guarantees. Point reads and enumerations alike
/// (<see cref="IHotStoreReader.Scan"/> and friends are lazy) must run inside
/// <c>MelangeEngine.ReadConsistent</c>, or on a code path the engine already serializes (reducers,
/// policies, fan-out, appliers). What a raw scan racing an apply does is undefined and varies by
/// implementation — "collection was modified", a half-applied batch, or a silently stale answer that
/// looks perfectly plausible. Do not read the absence of an exception as permission. A store
/// implementing <see cref="IReadViewSource"/> offers the one supported way to read without that
/// constraint, and it says which LSN the answer belongs to.
/// </para>
/// </summary>
public interface IHotStore : IHotStoreReader
{
    /// <summary>The LSN of the last record applied to this projection.</summary>
    ulong AppliedLsn { get; }

    /// <summary>
    /// Applies one commit record atomically. Records at or below <see cref="AppliedLsn"/> are
    /// ignored, making replay idempotent. Insert and update both put; delete removes.
    /// </summary>
    void Apply(CommitRecord record);

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

/// <summary>
/// The optional capability of handing out a read view that an <see cref="IHotStore.Apply"/> cannot
/// disturb — what a snapshot-isolated reducer body reads through. Optional in the manner of
/// <c>IResidencyControl</c>: a store that cannot pin simply does not implement it, and the engine
/// refuses snapshot isolation at startup rather than discovering it mid-tick.
/// </summary>
public interface IReadViewSource
{
    /// <summary>
    /// Opens a read view pinned at the store's current <see cref="IHotStore.AppliedLsn"/>. Must be
    /// called on a path the engine serializes against <see cref="IHotStore.Apply"/> — the engine
    /// takes its write lock for exactly the duration of this call — so the view is pinned at one
    /// LSN across every table rather than straddling a commit. Cheap by contract: implementations
    /// capture references, they do not copy state.
    /// </summary>
    IHotStoreReadView OpenReadView();
}

/// <summary>
/// A read view of the hot tier pinned at one LSN. Reads through it are safe with no lock held and
/// with <see cref="IHotStore.Apply"/> running concurrently: it observes the state as of
/// <see cref="Lsn"/> and never a later one, however long it is held and however lazily its
/// enumerations are consumed.
/// <para>
/// Holding one costs the store whatever it must retain to keep that LSN readable, so dispose it as
/// soon as the body that opened it returns. It is not thread-safe for concurrent use by several
/// threads; one view belongs to one reader.
/// </para>
/// </summary>
public interface IHotStoreReadView : IHotStoreReader, IDisposable
{
    /// <summary>The LSN this view is pinned at — the store's applied LSN when it was opened.</summary>
    ulong Lsn { get; }
}

/// <summary>One row of a snapshot being loaded: table, primary key, and serialized bytes.</summary>
public readonly record struct SnapshotRow(TableId Table, RowKey Key, ReadOnlyMemory<byte> Row);
