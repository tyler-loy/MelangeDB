namespace MelangeDB;

/// <summary>
/// A hot store's self-reported footprint: per-table residency and counters, plus the paging
/// buffer-pool cap. The startup residency report and the <c>melange.store.*</c> signals both read
/// from here, so the memory budget is observable rather than theoretical.
/// </summary>
public sealed class HotStoreStatistics
{
    /// <summary>Per-table statistics, in schema registration order.</summary>
    public required IReadOnlyList<HotStoreTableStatistics> Tables { get; init; }

    /// <summary>
    /// The cap on the paging buffer pool in bytes, or zero for a store that does not page.
    /// Excludes resident tables, which are accounted per table — the store's total declared
    /// footprint is this plus the sum of <see cref="HotStoreTableStatistics.ResidentBytes"/>.
    /// </summary>
    public required long BufferPoolCapacityBytes { get; init; }

    /// <summary>
    /// The paged tier's logs, one per key-value space (<c>main</c> for row records, <c>blob</c> for
    /// out-of-line bytes columns); empty for a store that does not page. An append-only log keeps
    /// superseded and deleted records until space is reclaimed, so the gap between a log's
    /// <see cref="HotStoreLogStatistics.LogBytes"/> and its <see cref="HotStoreLogStatistics.LiveBytes"/>
    /// is what reclamation has yet to recover.
    /// </summary>
    public IReadOnlyList<HotStoreLogStatistics> Logs { get; init; } = [];

    /// <summary>
    /// Native memory held by the key directory and secondary indexes when they live off the
    /// managed heap (<c>HotStore:NativeDirectory</c>), including versions not yet collected; 0
    /// otherwise.
    /// </summary>
    public long NativeDirectoryBytes { get; init; }
}

/// <summary>
/// One paged key-value space's log: its footprint, its write mix, its flushing and its compaction.
/// Each value is cumulative or a current reading as its documentation says; a value the engine does
/// not track is -1. FASTER reports <see cref="LogBytes"/> only; MelangeLog reports all.
/// </summary>
public sealed record HotStoreLogStatistics
{
    /// <summary>The space: <c>main</c> for row records, <c>blob</c> for out-of-line bytes columns.</summary>
    public string Space { get; init; } = "";

    /// <summary>The log's extent, in memory and on disk.</summary>
    public long LogBytes { get; init; } = -1;

    /// <summary>The bytes of that log current records occupy.</summary>
    public long LiveBytes { get; init; } = -1;

    /// <summary>Cumulative log bytes compaction has given back.</summary>
    public long ReclaimedBytes { get; init; } = -1;

    /// <summary>Cumulative live bytes compaction copied forward to reclaim space: its write cost.</summary>
    public long RelocatedBytes { get; init; } = -1;

    /// <summary>Cumulative compaction steps, each one hold of the store lock.</summary>
    public long CompactionSteps { get; init; } = -1;

    /// <summary>Cumulative time compaction held the store lock, across <see cref="CompactionSteps"/>.</summary>
    public TimeSpan CompactionLockTime { get; init; } = TimeSpan.Zero;

    /// <summary>Cumulative writes that rewrote a record in memory, in place: no new log bytes.</summary>
    public long InPlaceWrites { get; init; } = -1;

    /// <summary>
    /// Cumulative writes that appended a new record, superseding the old one wherever it was. Against
    /// <see cref="InPlaceWrites"/>, the share of writes that make garbage for compaction.
    /// </summary>
    public long AppendedWrites { get; init; } = -1;

    /// <summary>The log bytes held in memory now (the newest part of the log), up to the space's budget.</summary>
    public long MemoryBytes { get; init; } = -1;

    /// <summary>Bytes in memory waiting for the background flush; writes stall when it reaches the read-only region's size.</summary>
    public long FlushLagBytes { get; init; } = -1;

    /// <summary>
    /// Cumulative writes that waited for the log's background flush to catch up: the disk falling
    /// behind the write rate.
    /// </summary>
    public long FlushStalls { get; init; } = -1;

    /// <summary>Segment files on disk now; each is preallocated and, in the default read mode, memory-mapped.</summary>
    public long SegmentFiles { get; init; } = -1;

    /// <summary>Keys in the space's hash index now.</summary>
    public long IndexEntries { get; init; } = -1;

    /// <summary>Slots in the space's hash index now; it grows by doubling past 75% load.</summary>
    public long IndexCapacity { get; init; } = -1;

    /// <summary>Cumulative index doublings.</summary>
    public long IndexResizes { get; init; } = -1;

    /// <summary>
    /// Cumulative records that failed their integrity check when read (a checksum or header mismatch):
    /// torn, misdirected or bit-rotted bytes, refused rather than returned.
    /// </summary>
    public long IntegrityFailures { get; init; } = -1;
}

/// <summary>One table's store-side statistics.</summary>
/// <param name="Table">The table's id.</param>
/// <param name="Name">The table's name.</param>
/// <param name="Residency">
/// The table's effective residency as the store is currently honoring it — an <c>Auto</c> table
/// reports <see cref="MelangeDB.Residency.Resident"/> or <see cref="MelangeDB.Residency.Paged"/>
/// depending on which side of its threshold it sits.
/// </param>
/// <param name="RowCount">Rows currently in the table.</param>
/// <param name="ResidentBytes">
/// Managed bytes this table pins in memory: full row data for a resident table; key-directory and
/// index bookkeeping only for a paged one, whose row data lives in the buffer pool.
/// </param>
/// <param name="PageFaults">Cumulative reads served from disk instead of memory.</param>
/// <param name="RowsScanned">Cumulative rows returned by full scans.</param>
public readonly record struct HotStoreTableStatistics(
    TableId Table,
    string Name,
    Residency Residency,
    long RowCount,
    long ResidentBytes,
    long PageFaults,
    long RowsScanned);
