namespace MelangeDB;

/// <summary>
/// Thrown when a write is refused for a condition the system itself designed and expects to clear
/// — a row frozen mid-handoff, a write routed to a border copy just after the shard map flips, a
/// fenced node awaiting re-registration. Reported to the caller as <c>transient</c> with the
/// precise reason, never as a server fault: the client's contract is to retry on its next tick,
/// and the server logs nothing, because a seam walker crossing shards is the product working, not
/// failing. Derives from <see cref="InvalidOperationException"/> so callers that treated these
/// refusals as invalid operations still do.
/// </summary>
public class TransientRejectionException : InvalidOperationException
{
    public TransientRejectionException(string message)
        : base(message)
    {
    }

    public TransientRejectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
