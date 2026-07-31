namespace MelangeDB.LoadTest;

/// <summary>
/// What the serve side publishes at <c>/loadtest/stats</c> and the driver polls: the hub's
/// handoff counters, the server process's memory and GC state, and an echo of the world geometry
/// so a mismatched driver fails loudly instead of mysteriously. All server-side numbers describe
/// the one serve process (hub and shard nodes together) — that is what the endpoint can honestly
/// measure, and the doc says so.
/// </summary>
public sealed record ServerStats
{
    public required DateTimeOffset TimestampUtc { get; init; }

    // World geometry echo — the driver validates its flags against these.
    public required int WorldBlocksX { get; init; }

    public required int WorldBlocksY { get; init; }

    public required int BlockChunksX { get; init; }

    public required int BlockChunksY { get; init; }

    // Hub handoff counters (ClusterMetrics).
    public required long HandoffsStarted { get; init; }

    public required long HandoffsCompleted { get; init; }

    public required long HandoffsAborted { get; init; }

    public required long HandoffsUnresolved { get; init; }

    public required long HandoffsRateLimited { get; init; }

    public required long HandoffsInFlight { get; init; }

    // Serve-process metrics: hub + shard nodes share this process.
    public required long WorkingSetBytes { get; init; }

    public required long GcHeapBytes { get; init; }

    public required int Gen0Collections { get; init; }

    public required int Gen1Collections { get; init; }

    public required int Gen2Collections { get; init; }
}
