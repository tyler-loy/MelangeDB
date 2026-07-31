namespace MelangeDB.Core;

/// <summary>Which path produced a write set — guards discriminate on it.</summary>
public enum CommitOrigin
{
    /// <summary>A reducer invocation.</summary>
    Reducer,

    /// <summary>The bulk ingestion path.</summary>
    Bulk,

    /// <summary>
    /// An internal apply — cluster replication, handoff import, or a saga marker. Placement and
    /// shard-span guards skip these: the write set was validated where it originated, and the
    /// applying node holds it precisely because its own placement rules say it may not produce it.
    /// </summary>
    Internal,
}

/// <summary>
/// Validates a transaction's collapsed write set at the commit point, under the engine's write
/// lock, <em>before</em> the log append — throwing aborts the transaction with zero trace. The
/// cluster layer installs its placement, shard-span, freeze, and fencing rules here; a single-node
/// deployment installs nothing and pays nothing.
/// </summary>
public interface ICommitGuard
{
    /// <summary>Validates one write set; throw to abort the transaction.</summary>
    void Validate(string reducerName, IReadOnlyList<RowOp> writeSet, CommitOrigin origin);
}

/// <summary>How a transaction is touching a table — the table-access guard discriminates on it.</summary>
public enum TableAccess
{
    Read,
    Write,
}

/// <summary>
/// Validates a table access inside a transaction or a policy read; throwing surfaces at the point
/// of access with the guard's message. The cluster layer installs its placement visibility rule
/// here — a Global table read on a shard node must fail with an explanation, not answer empty.
/// </summary>
public delegate void TableAccessGuard(TableSchema table, TableAccess access);
