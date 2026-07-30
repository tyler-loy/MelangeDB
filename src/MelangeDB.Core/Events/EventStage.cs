namespace MelangeDB.Core;

/// <summary>
/// The per-transaction event collector behind <c>ctx.Publish</c>: serializes each event
/// immediately — so an unserializable event aborts the transaction that published it, not a
/// delivery hours later — stamps the ambient publish depth, and enforces
/// <c>Events:MaxPublishDepth</c>. No I/O; the staged records ride the commit request into the log.
/// </summary>
internal sealed class EventStage(EventsOptions options) : IEventCollector
{
    private List<EventRecord>? _events;

    /// <summary>The staged events, or null when nothing was published.</summary>
    public IReadOnlyList<EventRecord>? Events => _events;

    public void Publish<TEvent>(TEvent @event)
        where TEvent : notnull
    {
        var depth = EventDispatchContext.CurrentDepth;
        if (depth >= options.MaxPublishDepth)
        {
            throw new InvalidOperationException(
                $"Publish depth limit reached: this reducer runs {depth} event→reducer hop(s) deep, and " +
                $"Events:MaxPublishDepth is {options.MaxPublishDepth}. An event whose handler calls a reducer that " +
                "publishes again is a cycle; the transaction is aborted to break it.");
        }

        var type = @event.GetType();
        var payload = EventCodec.Serialize(@event);
        _events ??= [];
        _events.Add(new EventRecord(type.FullName!, (byte)depth, payload));
    }
}
