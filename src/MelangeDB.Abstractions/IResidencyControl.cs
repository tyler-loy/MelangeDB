namespace MelangeDB;

/// <summary>
/// Optional runtime residency control on a hot store — what makes the per-table
/// <c>Residency:&lt;TableName&gt;</c> override <em>careful</em> rather than restart-only: an
/// operator hitting a slow scan can pin a table without a redeploy. Pinning faults the whole table
/// into memory; unpinning migrates it to the buffer pool. A store that does not page (the
/// in-memory store) simply doesn't implement this.
/// </summary>
public interface IResidencyControl
{
    /// <summary>Applies a new residency to a table, migrating its rows accordingly.</summary>
    void ApplyResidency(string tableName, Residency residency);
}
