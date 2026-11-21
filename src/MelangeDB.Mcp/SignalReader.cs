using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;

namespace MelangeDB.Mcp;

/// <summary>
/// The <c>signals</c> tool's source: a <see cref="MeterListener"/> over MelangeDB's own meters
/// (every meter whose name starts with <c>MelangeDB</c>, the instruments OBSERVABILITY.md names),
/// keeping one value per instrument and tag set. Gauges and observable counters read their latest
/// value; counters sum, and histograms keep count, sum and max, since this listener started. It
/// publishes nothing new: it reads what the engine already emits for any metrics pipeline.
/// </summary>
internal sealed class SignalReader : IDisposable
{
    internal const int MaxSeries = 1000;

    private readonly MeterListener _listener = new();
    private readonly ConcurrentDictionary<(string Instrument, string Tags), Series> _series = new();

    public SignalReader()
    {
        _listener.InstrumentPublished = static (instrument, listener) =>
        {
            if (instrument.Meter.Name.StartsWith("MelangeDB", StringComparison.Ordinal))
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((i, v, t, _) => Record(i, v, t));
        _listener.SetMeasurementEventCallback<int>((i, v, t, _) => Record(i, v, t));
        _listener.SetMeasurementEventCallback<double>((i, v, t, _) => Record(i, v, t));
        _listener.SetMeasurementEventCallback<float>((i, v, t, _) => Record(i, v, t));
        _listener.Start();
    }

    internal sealed class Series(string instrument, string kind, string? unit, string tags)
    {
        public string Instrument { get; } = instrument;

        public string Kind { get; } = kind;

        public string? Unit { get; } = unit;

        public string Tags { get; } = tags;

        public double Value { get; set; }

        public long Count { get; set; }

        public double Max { get; set; } = double.MinValue;
    }

    /// <summary>
    /// The current reading, observable instruments polled now, filtered to instruments whose name
    /// starts with <paramref name="prefix"/> (all when empty), at most <paramref name="max"/> series,
    /// sorted by name. Returns the series and how many matched in all.
    /// </summary>
    public (IReadOnlyList<Series> Series, int Matched) Read(string? prefix, int max)
    {
        _listener.RecordObservableInstruments();
        var matching = _series.Values
            .Where(s => string.IsNullOrEmpty(prefix) || s.Instrument.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(static s => s.Instrument, StringComparer.Ordinal)
            .ThenBy(static s => s.Tags, StringComparer.Ordinal)
            .ToList();
        List<Series> copies;
        lock (_series)
        {
            copies = [.. matching.Take(Math.Clamp(max, 1, MaxSeries)).Select(static s => new Series(s.Instrument, s.Kind, s.Unit, s.Tags)
            {
                Value = s.Value,
                Count = s.Count,
                Max = s.Max,
            })];
        }

        return (copies, matching.Count);
    }

    private void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        where T : struct
    {
        var value = Convert.ToDouble(measurement, CultureInfo.InvariantCulture);
        var tagText = tags.Length == 0
            ? string.Empty
            : string.Join(",", tags.ToArray().OrderBy(static t => t.Key, StringComparer.Ordinal).Select(static t => $"{t.Key}={t.Value}"));
        var key = (instrument.Name, tagText);
        if (!_series.TryGetValue(key, out var series))
        {
            if (_series.Count >= MaxSeries)
                return; // Bounded: a cardinality explosion cannot grow this without limit.
            series = _series.GetOrAdd(key, _ => new Series(instrument.Name, KindOf(instrument), instrument.Unit, tagText));
        }

        lock (_series)
        {
            switch (series.Kind)
            {
                case "counter":
                case "updowncounter":
                    series.Value += value;
                    series.Count++;
                    break;
                case "histogram":
                    series.Value += value;
                    series.Count++;
                    series.Max = Math.Max(series.Max, value);
                    break;
                default:
                    // Gauges and observable counters report a current value or a running total.
                    series.Value = value;
                    series.Count++;
                    break;
            }
        }
    }

    private static string KindOf(Instrument instrument)
    {
        var name = instrument.GetType().Name;
        return name switch
        {
            _ when name.StartsWith("ObservableGauge", StringComparison.Ordinal) => "gauge",
            _ when name.StartsWith("ObservableCounter", StringComparison.Ordinal) => "observable-counter",
            _ when name.StartsWith("ObservableUpDownCounter", StringComparison.Ordinal) => "observable-updowncounter",
            _ when name.StartsWith("UpDownCounter", StringComparison.Ordinal) => "updowncounter",
            _ when name.StartsWith("Counter", StringComparison.Ordinal) => "counter",
            _ when name.StartsWith("Histogram", StringComparison.Ordinal) => "histogram",
            _ => "gauge",
        };
    }

    public void Dispose() => _listener.Dispose();
}
