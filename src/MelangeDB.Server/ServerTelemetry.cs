using System.Diagnostics;
using System.Diagnostics.Metrics;
using MelangeDB.Core;

namespace MelangeDB.Server;

/// <summary>
/// The transport's signals, on the same <c>MelangeDB</c> source and meter as the engine's. Table
/// names and rejection reasons are bounded and safe as dimensions; identities never appear here.
/// Delta spans are sampled — deltas are the highest-frequency operation in the system.
/// </summary>
internal sealed class ServerTelemetry : IDisposable
{
    private static readonly ActivitySource Source = new("MelangeDB");

    private readonly Func<TelemetryOptions> _options;
    private readonly Meter _meter;
    private readonly Counter<long> _deltaRows;
    private readonly Counter<long> _rejected;

    public ServerTelemetry(
        Func<TelemetryOptions> options,
        Func<int> activeConnections,
        Func<IEnumerable<KeyValuePair<string, int>>> activeSubscriptions)
    {
        _options = options;
        _meter = new Meter("MelangeDB");
        _deltaRows = _meter.CreateCounter<long>("melange.subscription.delta_rows", "{row}", "Row deltas emitted to subscribed clients.");
        _rejected = _meter.CreateCounter<long>("melange.subscription.rejected", "{sub}", "Subscriptions rejected before execution.");
        _meter.CreateObservableGauge("melange.connections.active", () => (long)activeConnections(), "{conn}", "Live websocket connections.");
        _meter.CreateObservableGauge(
            "melange.subscriptions.active",
            () => activeSubscriptions().Select(pair => new Measurement<long>(pair.Value, new KeyValuePair<string, object?>("table", pair.Key))),
            "{sub}",
            "Live subscriptions per table.");
    }

    /// <summary>The expensive half of a subscription: the initial-set span.</summary>
    public Activity? StartInitialSet(string table)
    {
        var activity = Source.StartActivity("melange.subscription.initial");
        activity?.SetTag("melange.table", table);
        return activity;
    }

    public static void CompleteInitialSet(Activity? activity, long rows, long bytes)
    {
        activity?.SetTag("melange.rows", rows);
        activity?.SetTag("melange.bytes", bytes);
    }

    /// <summary>Emits a sampled <c>melange.subscription.delta</c> span per fan-out.</summary>
    public void SampleDeltaSpan(string table, int subscribers)
    {
        if (Random.Shared.NextDouble() >= _options().DeltaSpanSampleRatio)
            return;
        using var activity = Source.StartActivity("melange.subscription.delta");
        activity?.SetTag("melange.table", table);
        activity?.SetTag("melange.subscribers", subscribers);
    }

    public void RecordDeltaRows(string table, int rows) =>
        _deltaRows.Add(rows, new KeyValuePair<string, object?>("table", table));

    public void RecordRejected(string reason) =>
        _rejected.Add(1, new KeyValuePair<string, object?>("reason", reason));

    public void Dispose() => _meter.Dispose();
}
