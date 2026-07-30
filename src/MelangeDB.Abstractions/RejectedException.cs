namespace MelangeDB;

/// <summary>
/// Thrown by a reducer to reject the call: the transaction aborts with zero trace, and the
/// rejection is reported to the caller as expected behaviour rather than a server fault.
/// </summary>
public class RejectedException : Exception
{
    public RejectedException(string message)
        : base(message)
    {
    }

    public RejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
