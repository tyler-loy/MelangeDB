using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>
/// Where a reducer call goes when the process that received it does not own the shard it runs on:
/// a cluster hub sends it to the shard's node. The MCP endpoint uses it, because an agent's call
/// reaches the hub, and the hub's own engine is not where a shard's reducers commit. Registered
/// by the cluster package on every clustered process; <see cref="RoutesByShard"/> is true only on
/// the hub, the bulk router's precedent.
/// </summary>
internal interface IShardCallRouter
{
    /// <summary>True on a cluster hub: a call must name the shard it runs on.</summary>
    bool RoutesByShard { get; }

    /// <summary>
    /// Runs <paramref name="reducer"/> on the node that owns <paramref name="shard"/> as a call
    /// from <paramref name="source"/> with <paramref name="claims"/>, so the node applies the same
    /// argument checks, rate limit and reducer policy a call made there would get. Failures are
    /// rethrown as the exceptions the node raised (see <see cref="ReducerCallErrors"/>). Throws
    /// <see cref="ArgumentException"/> for a shard that does not exist, without creating it.
    /// </summary>
    Task<ulong> CallAsync(
        ulong shard,
        string reducer,
        Identity caller,
        ReadOnlyMemory<byte> arguments,
        CallSource source,
        CallerClaims claims,
        CancellationToken cancellationToken);
}

/// <summary>
/// A reducer call's failure as a wire error code and back, so a call that crossed a process
/// boundary fails with the same exception it would have raised locally.
/// </summary>
internal static class ReducerCallErrors
{
    /// <summary>The error code for a call failure, or null for anything that is not one.</summary>
    public static string? CodeOf(Exception exception) => exception switch
    {
        ReducerArgumentException => MelangeErrorCodes.InvalidArguments,
        RejectedException => MelangeErrorCodes.Rejected,
        TransientRejectionException => MelangeErrorCodes.Transient,
        CommitOutcomeUnknownException => MelangeErrorCodes.OutcomeUnknown,
        RateLimitedException => MelangeErrorCodes.RateLimited,
        ReducerDeniedException => MelangeErrorCodes.Denied,
        UnknownReducerException => MelangeErrorCodes.UnknownReducer,
        _ => null,
    };

    /// <summary>The exception <see cref="CodeOf"/> mapped to <paramref name="code"/>, or null for an unknown code.</summary>
    public static Exception? FromCode(string? code, string message, string reducer) => code switch
    {
        MelangeErrorCodes.InvalidArguments => new ReducerArgumentException(message),
        MelangeErrorCodes.Rejected => new RejectedException(message),
        MelangeErrorCodes.Transient => new TransientRejectionException(message),
        MelangeErrorCodes.OutcomeUnknown => new CommitOutcomeUnknownException(message),
        MelangeErrorCodes.RateLimited => new RateLimitedException(message),
        MelangeErrorCodes.Denied => new ReducerDeniedException(message),
        MelangeErrorCodes.UnknownReducer => new UnknownReducerException(reducer),
        _ => null,
    };
}
