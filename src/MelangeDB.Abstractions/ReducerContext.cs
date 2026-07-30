namespace MelangeDB;

/// <summary>
/// The ambient state a reducer is given so it can stay deterministic and replayable. Reaching for
/// <see cref="DateTime.Now"/> or <c>new Random()</c> inside a reducer instead is a bug.
/// </summary>
public sealed class ReducerContext
{
    public ReducerContext(Identity caller, ConnectionId connectionId, Timestamp timestamp, Random random, IDbView db)
    {
        Caller = caller;
        ConnectionId = connectionId;
        Timestamp = timestamp;
        Random = random;
        Db = db;
    }

    /// <summary>Who is acting. Stable across reconnects and restarts.</summary>
    public Identity Caller { get; }

    /// <summary>Which socket the call arrived on; <see cref="ConnectionId.None"/> for in-process work.</summary>
    public ConnectionId ConnectionId { get; }

    /// <summary>The transaction's timestamp. The only clock a reducer may read.</summary>
    public Timestamp Timestamp { get; }

    /// <summary>A random source seeded per commit. The only randomness a reducer may use.</summary>
    public Random Random { get; }

    /// <summary>The transactional view: write set overlaid on the store, read-your-writes included.</summary>
    public IDbView Db { get; }
}
