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
    /// The highest LSN removed by truncation, or 0 for an untruncated log. Records at or below it
    /// are gone; <see cref="ReadFrom"/> can serve nothing older than <c>BaseLsn + 1</c>.
    /// </summary>
    ulong BaseLsn { get; }

    /// <summary>
    /// The identifier of this log incarnation. An LSN is meaningful only within one log, so a
    /// resume cursor names the epoch it counts against; a recreated log mints a fresh epoch, which
    /// is what turns a stale cursor into an explicit failure instead of silent divergence.
    /// </summary>
    Guid EpochId { get; }

    /// <summary>
    /// The newest LSN the log's durability discipline promises will survive a crash. Anything that
    /// leaves the process — a subscription delta, a replica stream, a downstream projection's
    /// apply — must stay at or under it, because an LSN served beyond it could be untold by a
    /// crash. Defaults to <see cref="HeadLsn"/>, the honest answer for a log with no deferred
    /// durability; a log that buffers appends (group commit) overrides it with its fsynced
    /// watermark.
    /// </summary>
    ulong DurableLsn => HeadLsn;

    /// <summary>
    /// Appends one committed transaction, assigns the next LSN, and makes it durable per the
    /// configured fsync policy. Returns the record as written. A durability discipline that
    /// defers the flush (group commit) completes it after this returns; <see cref="DurableLsn"/>
    /// is the watermark of what has actually reached stable storage.
    /// </summary>
    CommitRecord Append(in CommitRequest request);

    /// <summary>Reads records in LSN order, starting at <paramref name="firstLsn"/> (inclusive).</summary>
    IEnumerable<CommitRecord> ReadFrom(ulong firstLsn);
}
