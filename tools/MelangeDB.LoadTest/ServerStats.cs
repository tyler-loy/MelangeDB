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

    /// <summary>Whether the serve process runs Server GC; heap count, and so resident size, follows the core count.</summary>
    public bool ServerGc { get; init; }

    public int ProcessorCount { get; init; }

    /// <summary>Total time the process has spent paused for garbage collection.</summary>
    public double GcPauseMs { get; init; }

    /// <summary>Processor seconds so far by kind of thread (<see cref="ThreadCpu"/>); empty off Windows.</summary>
    public Dictionary<string, double> ThreadCpuSeconds { get; init; } = [];

    /// <summary>The serve process's total processor seconds so far.</summary>
    public double ProcessCpuSeconds { get; init; }

    // The write-lock split (serve --lock-split), summed over every traced Move; zero when off.
    public long LockSplitTransactions { get; init; }

    public double LockedMsSum { get; init; }

    public double PostCommitMsSum { get; init; }

    // Phase 31's wall attribution: re-scopes (melange.subscription.rescope.duration) summed over
    // every shard, and each shard's cumulative write-lock busy time, keyed by shard value.
    public long RescopeCount { get; init; }

    public double RescopeMsSum { get; init; }

    public Dictionary<string, double> WriteLockBusyMsByShard { get; init; } = [];

    // Handoff timing (ClusterMetrics' StepDurations): the hub's saga steps and gateway holds, and the
    // shard nodes' boundary-monitor crossing waits, summed over nodes. Keyed by step name.
    public Dictionary<string, StepStat> HandoffSteps { get; init; } = [];

    public long BoundarySignalsDropped { get; init; }

    public long BoundaryCrossingsDropped { get; init; }

    public long GatewaySwapsPreopened { get; init; }

    /// <summary>Writes refused as band overruns (an entity past the band before its handoff), over the shards' current owners.</summary>
    public long BandOverruns { get; init; }

    public long GatewaySwapsConnected { get; init; }

    // The serve process's thread pool, sampled every 100 ms over the whole run: queued work items
    // (a request handler waits behind these) and thread count.
    public long ThreadPoolSamples { get; init; }

    public double ThreadPoolQueueMean { get; init; }

    public long ThreadPoolQueueMax { get; init; }

    public long ThreadPoolQueueP99 { get; init; }

    public int ThreadPoolThreadsMax { get; init; }

    // Liveness, whole-run (phase 30's timing soak): heartbeat gaps as the hub received them, the
    // shard nodes' lease-renewal gaps (all nodes together), and the false deaths they caused. The
    // "over" maps are keyed by threshold in ms (ClusterMetrics' LivenessGaps thresholds).
    public long HeartbeatGapCount { get; init; }

    public double HeartbeatGapMaxMs { get; init; }

    public Dictionary<int, long> HeartbeatGapsOverMs { get; init; } = [];

    public long LeaseRenewalGapCount { get; init; }

    public double LeaseRenewalGapMaxMs { get; init; }

    public Dictionary<int, long> LeaseRenewalGapsOverMs { get; init; } = [];

    public long NodesDeclaredDead { get; init; }

    // Replica lag (phase 30), sampled every 100 ms in the serve process: each replica's applied
    // LSN against its owner's durable LSN, both read directly, so the hub's relay delay is in it.

    /// <summary>Samples taken (one per replica per tick).</summary>
    public long ReplicaLagSamples { get; init; }

    public long ReplicaLagP50Records { get; init; }

    public long ReplicaLagP99Records { get; init; }

    public long ReplicaLagMaxRecords { get; init; }

    /// <summary>
    /// How long the oldest record a replica had not applied had been durable on its owner, taken
    /// in samples where it was behind (100 ms resolution): what a promotion at that instant would
    /// have had to replay, in time rather than records.
    /// </summary>
    public double ReplicaStalenessMaxMs { get; init; }

    public double ReplicaStalenessP99Ms { get; init; }

    public long LeaseLapses { get; init; }
}

/// <summary>One <see cref="MelangeDB.Cluster.StepDurations"/> as served: cumulative, so a window is a difference.</summary>
public sealed record StepStat
{
    public long Count { get; init; }

    public double TotalMs { get; init; }

    public double MaxMs { get; init; }

    /// <summary>Records over each threshold (StepDurations.Thresholds), keyed by threshold.</summary>
    public Dictionary<int, long> Over { get; init; } = [];

    public static StepStat Of(MelangeDB.Cluster.StepDurations steps) => new()
    {
        Count = steps.Count,
        TotalMs = steps.TotalMs,
        MaxMs = steps.MaxMs,
        Over = MelangeDB.Cluster.StepDurations.Thresholds.ToDictionary(static t => t, steps.Over),
    };

    public static StepStat Sum(IEnumerable<StepStat> stats)
    {
        var list = stats.ToList();
        return new StepStat
        {
            Count = list.Sum(static s => s.Count),
            TotalMs = list.Sum(static s => s.TotalMs),
            MaxMs = list.Count == 0 ? 0 : list.Max(static s => s.MaxMs),
            Over = MelangeDB.Cluster.StepDurations.Thresholds.ToDictionary(static t => t, t => list.Sum(s => s.Over.GetValueOrDefault(t))),
        };
    }
}
