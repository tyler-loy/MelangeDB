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
