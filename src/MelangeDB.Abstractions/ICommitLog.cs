namespace MelangeDB;

/// <summary>
/// The ordered, append-only, LSN-addressed record of committed transactions. The system of record:
/// every store is a projection of it. The single atomic append is the commit point.
/// </summary>
public interface ICommitLog : IDisposable
{
    /// <summary>The LSN of the most recent record, or 0 when the log is empty.</summary>
    ulong HeadLsn { get; }

    /// <summary>
    /// Appends one committed transaction, assigns the next LSN, and makes it durable per the
    /// configured fsync policy. Returns the record as written.
    /// </summary>
    CommitRecord Append(in CommitRequest request);

    /// <summary>Reads records in LSN order, starting at <paramref name="firstLsn"/> (inclusive).</summary>
    IEnumerable<CommitRecord> ReadFrom(ulong firstLsn);
}
