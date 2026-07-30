namespace MelangeDB.Core;

/// <summary>
/// The default <see cref="IEventTransport"/>: hands each committed batch straight to the local
/// dispatcher's inbox on the committing thread. The inbox only enqueues, so this never blocks the
/// commit path. Phase 09's distributed transport replaces this registration; handler code does
/// not change.
/// </summary>
public sealed class InProcessEventTransport : IEventTransport
{
    private IEventInbox? _inbox;

    public void Connect(IEventInbox inbox)
    {
        ArgumentNullException.ThrowIfNull(inbox);
        _inbox = inbox;
    }

    public void Publish(IReadOnlyList<EventEnvelope> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        _inbox?.Receive(batch);
    }
}
