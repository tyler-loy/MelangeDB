namespace MelangeDB;

/// <summary>
/// Thrown when a transaction was appended to the log but could not be acknowledged: in a cluster,
/// the shard node's lease ended between the commit guard and the acknowledgement, so a new owner
/// may already have cut the log before it; or applying it to the hot store failed after the append,
/// so the database stopped before waiting for it to become durable. Either way the write may or
/// may not be part of history. Reported as <c>outcome_unknown</c>.
/// <para>
/// Deliberately not a <see cref="TransientRejectionException"/>: a transient refusal promises the
/// call was not applied and may be retried unchanged, and here that promise cannot be made. A
/// retry of a call that is not idempotent may apply it twice; the caller reads current state
/// before deciding.
/// </para>
/// </summary>
public sealed class CommitOutcomeUnknownException : InvalidOperationException
{
    public CommitOutcomeUnknownException(string message)
        : base(message)
    {
    }

    public CommitOutcomeUnknownException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
