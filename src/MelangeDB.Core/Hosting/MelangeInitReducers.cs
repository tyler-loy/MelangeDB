using Microsoft.Extensions.Logging;

namespace MelangeDB.Core;

/// <summary>
/// Fires <see cref="ReducerKind.Init"/> reducers on an engine that has never committed anything —
/// the seam that gives a freshly created database the state it must already hold before it serves
/// anyone, timer rows above all.
/// <para>
/// It exists because of shards. A shard is created the first time a session resolves to it, and
/// its engine opens empty: a scheduled table is <c>Placement.Local</c>, so its timer rows live in
/// <em>that</em> engine, and application code has no handle on it to put them there. Without this
/// the first player to walk into a never-visited block gets a shard that serves reads and writes
/// correctly and simply never ticks — creatures inert, nothing growing, nothing decaying, no
/// error anywhere. Every engine that owns world state runs this at open: each per-shard engine,
/// the hub's, or the single engine of a deployment that is not clustered.
/// </para>
/// </summary>
public static partial class MelangeInitReducers
{
    /// <summary>
    /// Fires every <see cref="ReducerKind.Init"/> reducer whose execution site is one of
    /// <paramref name="sites"/>, if <paramref name="engine"/> is fresh. Returns the number that
    /// committed. Sites are passed together rather than in separate calls because the first
    /// commit ends the freshness a second call would test for.
    /// <para>
    /// "Fresh" is <c>HeadLsn == 0</c> — nothing was ever committed to this log — rather than "the
    /// directory did not exist". The difference is what makes the check correct against crashes
    /// and reassignment alike: a snapshot moves the log's base but never its head, so a recovered
    /// shard is never re-seeded, while a crash between creating a shard's directory and its first
    /// commit leaves a log that is still empty and does get seeded on the retry.
    /// </para>
    /// <para>
    /// Each fire is its own transaction and a thrower is logged, not rethrown: a seeding mistake
    /// must not cost the caller its shard (or its process). One consequence is worth stating —
    /// reducers that ran before the thrower stay committed, so the head has moved and only the
    /// failed ones are missing on the next start. Seeding idempotently is the application's half
    /// of that contract.
    /// </para>
    /// </summary>
    public static int Fire(
        MelangeEngine engine, MelangeReducerHost host, ILogger logger, string scope, params ReducerSite[] sites)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(sites);
        if (engine.Log.HeadLsn != 0)
            return 0;

        var reducers = host.Reducers
            .Where(descriptor => descriptor.Kind == ReducerKind.Init && Array.IndexOf(sites, descriptor.ExecutionSite) >= 0)
            .Select(static descriptor => descriptor.Name)
            .ToArray();
        if (reducers.Length == 0)
            return 0;

        var fired = 0;
        foreach (var reducer in reducers)
        {
            if (host.IsStopping)
                break;
            try
            {
                host.Call(reducer, InitCaller, ConnectionId.None, ReadOnlyMemory<byte>.Empty);
                fired++;
            }
            catch (Exception exception)
            {
                LogInitReducerFailed(logger, reducer, scope, exception);
            }
        }

        if (fired > 0)
            LogInitReducersFired(logger, fired, scope);
        return fired;
    }

    /// <summary>The identity an init fire runs as — what <c>ctx.Caller</c> is inside one.</summary>
    public static Identity InitCaller { get; } = Identity.Hash("melange/init");

    [LoggerMessage(EventId = 1105, EventName = "InitReducersFired", Level = LogLevel.Information,
        Message = "Seeded {Count} init reducer(s) into {Scope}, which had never committed anything. " +
            "They run once per fresh engine; a recovered one is never re-seeded.")]
    private static partial void LogInitReducersFired(ILogger logger, int count, string scope);

    [LoggerMessage(EventId = 1106, EventName = "InitReducerFailed", Level = LogLevel.Error,
        Message = "Init reducer '{Reducer}' threw while seeding {Scope} and committed nothing. Whatever it was to create " +
            "does not exist — if that was a scheduled table's timer rows, this engine will never tick. Init reducers that " +
            "already committed are not replayed on the next start, so fix the cause and seed the remainder by hand.")]
    private static partial void LogInitReducerFailed(ILogger logger, string reducer, string scope, Exception exception);
}
