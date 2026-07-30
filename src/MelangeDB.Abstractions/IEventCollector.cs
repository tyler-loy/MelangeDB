namespace MelangeDB;

/// <summary>
/// The sink behind <see cref="ReducerContext.Publish{TEvent}"/>: stages an event into the current
/// transaction's write set. No I/O happens here — the staged events become part of the commit
/// record, which is what makes publication transactional.
/// </summary>
public interface IEventCollector
{
    /// <summary>Stages one event for publication at the commit point.</summary>
    void Publish<TEvent>(TEvent @event)
        where TEvent : notnull;
}
