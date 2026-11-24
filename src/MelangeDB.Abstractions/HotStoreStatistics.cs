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
}

/// <summary>One paged key-value space's log footprint.</summary>
/// <param name="Space">The space: <c>main</c> or <c>blob</c>.</param>
/// <param name="LogBytes">The log's extent, in memory and on disk.</param>
/// <param name="LiveBytes">The bytes of that log current records occupy, or -1 when the engine cannot tell.</param>
/// <param name="ReclaimedBytes">Cumulative log bytes compaction has given back.</param>
/// <param name="RelocatedBytes">Cumulative live bytes compaction copied forward to do it.</param>
/// <param name="FlushStalls">
/// Cumulative writes that waited for the log's background flush to catch up: the disk falling behind
/// the write rate. Zero for an engine that flushes on the writing thread.
/// </param>
public readonly record struct HotStoreLogStatistics(
    string Space, long LogBytes, long LiveBytes, long ReclaimedBytes = 0, long RelocatedBytes = 0, long FlushStalls = 0);

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
