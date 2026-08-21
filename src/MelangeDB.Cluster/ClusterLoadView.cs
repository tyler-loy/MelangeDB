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
/// <para>
/// <see cref="AuthoritativeRows"/> counts rows in <c>Partitioned</c> tables minus the shard's
/// border-band copies — what would be lost permanently if the shard were removed, as opposed to
/// what comes back on its own. <c>Local</c> tables (timer rows above all) and <c>Replicated</c>
/// rows are excluded for that reason, so a shard holding nothing but its own timer row reads as
/// zero. It is a sampled reading and deliberately advisory: anything that <em>acts</em> on
/// emptiness must re-check under the shard's own lock rather than trust a gauge.
/// </para>
/// <para>
/// A drain-quiesced shard reports no sample at all while it is closed; the view tracks that
/// separately, so nothing here is a placeholder for a measurement that could not be taken.
///
/// </para>
/// </summary>
public sealed record ShardLoad(
    ShardKey Shard,
    string NodeName,
    double Utilization,
    ulong HeadLsn,
    long ResidentBytes,
    int BorrowedRows,
    long AuthoritativeRows,
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

    /// <summary>
    /// Shards their owner last reported as drain-quiesced, and when. Kept apart from
    /// <see cref="_latest"/> deliberately: a drain marker is not a measurement — the shard's
    /// engine is closed — so parking one in the sample map would publish a head LSN and a
    /// footprint of zero for a shard that simply is not measurable, and would oblige every
    /// reader of a "latest sample" to know that some samples are not samples.
    /// </summary>
    private readonly ConcurrentDictionary<ShardKey, DateTimeOffset> _draining = [];
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
        _meter.CreateObservableGauge(
            "melange.cluster.shard.authoritative_rows",
            ObserveAuthoritativeRows,
            unit: "{row}",
            description: "Rows one shard owns — rows in Partitioned tables minus border-band copies, so Local timer rows and Replicated copies are excluded — as its owner last sampled it, tagged by shard and node. Advisory: a reader acting on emptiness must re-check under the shard's lock.");
        _meter.CreateObservableGauge(
            "melange.cluster.shard.borrowed_rows",
            ObserveBorrowedRows,
            unit: "{row}",
            description: "Border-band rows one shard holds copies of and a neighbour owns, as its owner last sampled it, tagged by shard and node.");
    }

    /// <summary>Records one node's heartbeat-carried samples.</summary>
    internal void Record(string nodeName, IReadOnlyList<ShardLoadDto> loads, DateTimeOffset now)
    {
        foreach (var load in loads)
        {
            var shard = new ShardKey(load.Shard);

            // A drain marker carries no readings, so it updates neither the sample map nor
            // the utilization series: its zero in the series would drag a quiesced shard's
            // sustained load down and colour the next rebalance decision about it.
            if (load.Draining)
            {
                _draining[shard] = now;
                continue;
            }

            _draining.TryRemove(shard, out _);
            _latest[shard] = new ShardLoad(
                shard, nodeName, load.Utilization, load.HeadLsn, load.ResidentBytes, load.BorrowedRows,
                load.AuthoritativeRows, now);
            var history = _history.GetOrAdd(shard, static _ => new ConcurrentQueue<(DateTimeOffset, double)>());
            history.Enqueue((now, load.Utilization));
            while (history.Count > MaxHistorySamples)
                history.TryDequeue(out _);
        }
    }

    /// <summary>
    /// Shards whose most recent sample says they hold nothing of their own: no authoritative rows,
    /// not drain-quiesced, and reported inside <paramref name="freshness"/> so a node that has gone
    /// quiet cannot make its shards look empty by falling silent.
    /// <para>
    /// <b>This is a narrowing, not a verdict.</b> It answers "which shards are worth asking about",
    /// and deliberately does not answer "which shards may be removed". Two conditions are missing
    /// on purpose:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    /// <b>Nothing pinning the log</b> — a backup streaming, a lagging subscriber, an unsettled
    /// handoff marker. Truncation floors are evaluated only inside a truncation decision, under
    /// the engine write lock, because one of them writes a file when evaluated and all of them
    /// race a scrape; a sampled reading of them would be wrong by construction. That check belongs
    /// where the write lock is already held.
    /// </description></item>
    /// <item><description>
    /// <b>Unoccupied</b> — the gateway holds session state, not this view.
    /// </description></item>
    /// </list>
    /// <para>
    /// So a caller that acts on this list must re-check both, on the owning node, under the lock.
    /// The list exists so that check runs against a handful of shards instead of every shard the
    /// cluster has ever created.
    /// </para>
    /// </summary>
    public IReadOnlyList<ShardLoad> ShardsHoldingNothing(DateTimeOffset now, TimeSpan freshness) =>
        [.. _latest.Values
            .Where(load => load.AuthoritativeRows == 0
                && !_draining.ContainsKey(load.Shard)
                && now - load.At <= freshness)
            .OrderBy(static load => load.Shard)];

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

    private IEnumerable<Measurement<long>> ObserveAuthoritativeRows()
    {
        foreach (var load in _latest.Values)
        {
            yield return new Measurement<long>(
                load.AuthoritativeRows,
                new KeyValuePair<string, object?>("shard", load.Shard.Value),
                new KeyValuePair<string, object?>("node", load.NodeName));
        }
    }

    private IEnumerable<Measurement<long>> ObserveBorrowedRows()
    {
        foreach (var load in _latest.Values)
        {
            yield return new Measurement<long>(
                load.BorrowedRows,
                new KeyValuePair<string, object?>("shard", load.Shard.Value),
                new KeyValuePair<string, object?>("node", load.NodeName));
        }
    }

    public void Dispose() => _meter.Dispose();
}
