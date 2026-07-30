namespace MelangeDB.Core;

/// <summary>
/// Observes every committed record synchronously, under the engine's write lock, after the append
/// and <em>before</em> the appliers advance any projection. At that moment the hot store still
/// holds the pre-image of every row the record touches — which is what lets the subscription
/// fan-out decide that an update moved a row out of a client's predicate. Implementations must be
/// fast and must not throw; a failure is logged and never poisons the committed transaction.
/// </summary>
public interface ICommitObserver
{
    /// <summary>Called once per committed record, in LSN order, under the write lock.</summary>
    void OnCommit(CommitRecord record);
}
