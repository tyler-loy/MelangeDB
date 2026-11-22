using System.Diagnostics.Metrics;

namespace MelangeDB.LoadTest;

/// <summary>
/// Sums <c>melange.subscription.rescope.duration</c> across every shard in the serve process: how
/// many re-scopes ran and how long they held a write lock in total. Set against the engines' busy
/// time it says how much of the lock re-scoping took from commits.
/// </summary>
internal sealed class RescopeListener : IDisposable
{
    private readonly MeterListener _listener = new();
    private long _count;
    private long _microseconds;

    public RescopeListener()
    {
        _listener.InstrumentPublished = static (instrument, listener) =>
        {
            if (instrument.Meter.Name == "MelangeDB" && instrument.Name == "melange.subscription.rescope.duration")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>((_, milliseconds, _, _) =>
        {
            Interlocked.Increment(ref _count);
            Interlocked.Add(ref _microseconds, (long)(milliseconds * 1000));
        });
        _listener.Start();
    }

    public long Count => Interlocked.Read(ref _count);

    public double Milliseconds => Interlocked.Read(ref _microseconds) / 1000.0;

    public void Dispose() => _listener.Dispose();
}
