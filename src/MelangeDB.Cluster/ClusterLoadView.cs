using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace MelangeDB.Cluster;

/// <summary>
/// One shard's most recent load sample as the hub holds it: which node reported it, the busy
/// fraction of the shard engine's write lock over the reporting interval (the saturation signal
/// the published hotspot ceilings are ceilings on), the shard log's head, the shard's resident
/// footprint, and its border-band row count. <see cref="At"/> is when the sample arrived — a
/// stale timestamp means the owner has gone quiet, and consumers judge that themselves rather
/// than the view guessing for them.
/// </summary>
public sealed record ShardLoad(
    ShardKey Shard,
    string NodeName,
    double Utilization,
    ulong HeadLsn,
    long ResidentBytes,
    int BorrowedRows,
    DateTimeOffset At);

/// <summary>
/// The hub's aggregation of heartbeat-carried per-shard load — the feed the rebalance loop reads
/// and the operator's answer to "which island is hot right now". Latest-sample per shard for the
/// snapshot, plus a bounded per-shard utilization history so sustained-window questions ("hot for
/// the whole window, not one spike") are answerable without a second collection pass. Exported as
/// observable gauges on the <c>MelangeDB</c> meter, tagged by shard and node.
/// </summary>
public sealed class ClusterLoadView : IDisposable
{
    /// <summary>
    /// Bounded per-shard history depth. At the default 1 s heartbeat this is 10 minutes — far past
    /// any sane rebalance window — and it exists so a misconfigured window cannot grow memory.
    /// </summary>
    private const int MaxHistorySamples = 600;

    private readonly ConcurrentDictionary<ShardKey, ShardLoad> _latest = [];
    private readonly ConcurrentDictionary<ShardKey, ConcurrentQueue<(DateTimeOffset At, double Utilization)>> _history = [];
    private readonly Meter _meter = new("MelangeDB");

    public ClusterLoadView()
    {
        _meter.CreateObservableGauge(
            "melange.cluster.shard.utilization",
            ObserveUtilization,
            unit: "{ratio}",
            description: "Busy fraction of one shard engine's write lock over its last heartbeat interval, tagged by shard and node.");
        _meter.CreateObservableGauge(
            "melange.cluster.shard.resident_bytes",
            ObserveResidentBytes,
            unit: "By",
            description: "One shard's resident-table footprint as its owner last sampled it, tagged by shard and node.");
    }

    /// <summary>Records one node's heartbeat-carried samples.</summary>
    internal void Record(string nodeName, IReadOnlyList<ShardLoadDto> loads, DateTimeOffset now)
    {
        foreach (var load in loads)
        {
            var shard = new ShardKey(load.Shard);
            _latest[shard] = new ShardLoad(
                shard, nodeName, load.Utilization, load.HeadLsn, load.ResidentBytes, load.BorrowedRows, now);
            var history = _history.GetOrAdd(shard, static _ => new ConcurrentQueue<(DateTimeOffset, double)>());
            history.Enqueue((now, load.Utilization));
            while (history.Count > MaxHistorySamples)
                history.TryDequeue(out _);
        }
    }

    /// <summary>Every shard's most recent sample, in shard order.</summary>
    public IReadOnlyList<ShardLoad> Snapshot() =>
        [.. _latest.Values.OrderBy(static load => load.Shard)];

    /// <summary>
    /// The shard's mean utilization over the trailing window, or null when the history does not
    /// cover the window — a loop acting on partial coverage would mistake "just started sampling"
    /// for "sustained", which is exactly the spike-versus-sustained confusion the window exists to
    /// prevent.
    /// </summary>
    public double? SustainedUtilization(ShardKey shard, TimeSpan window, DateTimeOffset now)
    {
        if (!_history.TryGetValue(shard, out var history))
            return null;
        var floor = now - window;
        var sum = 0d;
        var count = 0;
        var covered = false;
        foreach (var (at, utilization) in history)
        {
            if (at <= floor)
            {
                covered = true; // At least one sample predates the window: the window is fully observed.
                continue;
            }

            sum += utilization;
            count++;
        }

        return covered && count > 0 ? sum / count : null;
    }

    private IEnumerable<Measurement<double>> ObserveUtilization()
    {
        foreach (var load in _latest.Values)
        {
            yield return new Measurement<double>(
                load.Utilization,
                new KeyValuePair<string, object?>("shard", load.Shard.Value),
                new KeyValuePair<string, object?>("node", load.NodeName));
        }
    }

    private IEnumerable<Measurement<long>> ObserveResidentBytes()
    {
        foreach (var load in _latest.Values)
        {
            yield return new Measurement<long>(
                load.ResidentBytes,
                new KeyValuePair<string, object?>("shard", load.Shard.Value),
                new KeyValuePair<string, object?>("node", load.NodeName));
        }
    }

    public void Dispose() => _meter.Dispose();
}
