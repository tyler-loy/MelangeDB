namespace MelangeDB;

/// <summary>
/// The ambient state a reducer is given so it can stay deterministic and replayable. Reaching for
/// <see cref="DateTime.Now"/> or <c>new Random()</c> inside a reducer instead is a bug.
/// </summary>
public sealed class ReducerContext
{
    private readonly IEventCollector? _events;

    public ReducerContext(
        Identity caller,
        ConnectionId connectionId,
        Timestamp timestamp,
        Random random,
        IDbView db,
        IEventCollector? events = null)
    {
        Caller = caller;
        ConnectionId = connectionId;
        Timestamp = timestamp;
        Random = random;
        Db = db;
        _events = events;
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

    /// <summary>
    /// Publishes a domain event. <b>No I/O happens here</b> — the event is staged into the write
    /// set and lands in the commit record, so it is published exactly when this transaction
    /// commits and never when it aborts: the transactional outbox, with the commit log as the
    /// outbox. Handlers (<see cref="IEventHandler{TEvent}"/>) run outside the transaction, after
    /// the commit point, with at-least-once delivery.
    /// </summary>
    public void Publish<TEvent>(TEvent @event)
        where TEvent : notnull
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (_events is null)
        {
            throw new InvalidOperationException(
                "This context has no event collector; events can only be published from a reducer dispatched by the engine.");
        }

        _events.Publish(@event);
    }
}
