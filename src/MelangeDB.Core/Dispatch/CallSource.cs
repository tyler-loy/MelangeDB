namespace MelangeDB.Core;

/// <summary>
/// Where a reducer call came from. In-process dispatch (the default) is the host's own code:
/// no rate limiting, no reducer policy, and lifecycle reducers are reachable. Client-originated
/// dispatch — the transports — is untrusted and gets all three checks, each applied before any
/// transaction opens.
/// </summary>
public readonly record struct CallSource(bool ClientOriginated, bool CallerIsGuest)
{
    /// <summary>The trusted in-process origin — the default.</summary>
    public static CallSource InProcess => default;

    /// <summary>An untrusted client origin, carrying whether the caller's token is guest-role.</summary>
    public static CallSource Client(bool callerIsGuest = false) => new(true, callerIsGuest);
}

/// <summary>
/// Thrown when a client call exceeds its identity's token bucket (<c>RateLimit:*</c>). Raised
/// before any transaction opens: nothing was appended, nothing happened.
/// </summary>
public sealed class RateLimitedException : Exception
{
    public RateLimitedException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// Thrown when a reducer's authorization policy denies a client call, or when
/// <c>Policies:DefaultReducerPosture</c> is <c>Deny</c> and the reducer declares no policy.
/// Raised before any transaction opens.
/// </summary>
public sealed class ReducerDeniedException : Exception
{
    public ReducerDeniedException(string message)
        : base(message)
    {
    }
}
