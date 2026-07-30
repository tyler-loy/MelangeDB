namespace MelangeDB;

/// <summary>
/// A domain-event handler, resolved from DI per delivery and invoked <em>outside</em> the emitting
/// transaction, after the commit point. Delivery is at-least-once: a handler that was down catches
/// up from its subscriber checkpoint, and a crash mid-delivery redelivers — so handlers must be
/// idempotent. A throwing handler is retried with backoff and then dead-lettered; it can never
/// wedge the applier pipeline or block later transactions.
/// </summary>
/// <typeparam name="TEvent">The event type this handler subscribes to.</typeparam>
public interface IEventHandler<in TEvent>
    where TEvent : notnull
{
    /// <summary>Handles one delivered event. Throwing triggers the retry-then-dead-letter policy.</summary>
    Task HandleAsync(TEvent @event, CancellationToken cancellationToken);
}
