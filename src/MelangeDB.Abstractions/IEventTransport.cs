using System.Diagnostics;

namespace MelangeDB;

/// <summary>
/// One committed event on its way from the commit point to subscribers: the LSN and timestamp of
/// the transaction that published it, the durable <see cref="EventRecord"/>, and — for in-process
/// delivery only — the emitting transaction's trace context, so the handler span can link back to
/// the reducer that published (linked, never parented; see docs/OBSERVABILITY.md).
/// </summary>
public sealed class EventEnvelope
{
    /// <summary>The LSN of the committing transaction.</summary>
    public required ulong Lsn { get; init; }

    /// <summary>The committing transaction's timestamp.</summary>
    public required Timestamp Timestamp { get; init; }

    /// <summary>The durable event: type name, publish depth, serialized payload.</summary>
    public required EventRecord Event { get; init; }

    /// <summary>
    /// The emitting transaction's trace context, when one was live at the commit point. In-memory
    /// only — a subscriber catching up from the log gets no link, which is honest: the emitting
    /// trace is long gone.
    /// </summary>
    public ActivityContext EmitterContext { get; init; }
}

/// <summary>The receiving end of an <see cref="IEventTransport"/>: a subscriber-side dispatcher.</summary>
public interface IEventInbox
{
    /// <summary>
    /// Receives one committed record's events. Called in LSN order for in-process delivery; must
    /// not block and must not throw.
    /// </summary>
    void Receive(IReadOnlyList<EventEnvelope> batch);
}

/// <summary>
/// The seam between the commit point and event delivery. The in-process implementation hands
/// envelopes straight to the local dispatcher; phase 09's distributed transport forwards them to
/// other nodes instead. Handler code never sees this interface, which is what lets the transport
/// change underneath it.
/// </summary>
public interface IEventTransport
{
    /// <summary>Attaches the dispatcher the transport delivers into. Called once, before any publish.</summary>
    void Connect(IEventInbox inbox);

    /// <summary>
    /// Carries one committed record's events toward every connected inbox. Called on the
    /// committing thread, after the commit point, under the engine's write lock — it must hand off
    /// and return, never block.
    /// </summary>
    void Publish(IReadOnlyList<EventEnvelope> batch);
}
