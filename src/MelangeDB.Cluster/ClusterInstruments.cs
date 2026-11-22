using System.Diagnostics.Metrics;

namespace MelangeDB.Cluster;

/// <summary>
/// Instruments recorded from static placement code (the span check runs in guards that hold no
/// runtime reference), on the same <c>MelangeDB</c> meter as everything else. Process-wide, like
/// the activity source: every engine in the process reports through these.
/// </summary>
internal static class ClusterInstruments
{
    private static readonly Meter Meter = new("MelangeDB");

    private static readonly Counter<long> SpanViolations = Meter.CreateCounter<long>(
        "melange.shard.span_violations",
        unit: "{tx}",
        description: "Transactions the shard-span check refused because their rows resolve to more than one shard, by reducer. Counted only where Cluster:ShardSpanCheck runs.");

    private static readonly Counter<long> BandOverruns = Meter.CreateCounter<long>(
        "melange.shard.band_overruns",
        unit: "{tx}",
        description: "Writes refused as transient because they carried a row their shard owned past its border band before the entity's handoff landed, by table. A steady rate means handoffs are not keeping up with Cluster:BorderBandChunks.");

    public static void SpanViolation(string reducer) =>
        SpanViolations.Add(1, new KeyValuePair<string, object?>("reducer", reducer));

    public static void BandOverrun(string table) =>
        BandOverruns.Add(1, new KeyValuePair<string, object?>("table", table));
}
