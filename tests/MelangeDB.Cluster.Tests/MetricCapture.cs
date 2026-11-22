using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Collects measurements of named <c>MelangeDB</c> instruments while alive. The meter is
/// process-wide and tests run in parallel, so assertions filter by a tag value the test owns or
/// assert "at least", never an exact total.
/// </summary>
internal sealed class MetricCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly HashSet<string> _names;

    public MetricCapture(params string[] instruments)
    {
        _names = [.. instruments];
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "MelangeDB" && _names.Contains(instrument.Name))
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.Start();
    }

    public ConcurrentQueue<(string Instrument, double Value, Dictionary<string, object?> Tags)> Measurements { get; } = new();

    /// <summary>Polls observable instruments once, recording what they report now.</summary>
    public void Observe() => _listener.RecordObservableInstruments();

    public IEnumerable<(double Value, Dictionary<string, object?> Tags)> Of(string instrument) =>
        Measurements.Where(m => m.Instrument == instrument).Select(static m => (m.Value, m.Tags));

    private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var copy = new Dictionary<string, object?>();
        foreach (var tag in tags)
            copy[tag.Key] = tag.Value;
        Measurements.Enqueue((instrument.Name, value, copy));
    }

    public void Dispose() => _listener.Dispose();
}
