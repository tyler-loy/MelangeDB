namespace MelangeDB.Core;

/// <summary>
/// One truncation floor's answer at the moment truncation was decided: the mechanism that still
/// needs old records and the highest LSN it permits compaction to remove. Names identify
/// mechanisms, not instances — a small static set by construction, which is what keeps
/// <c>melange.log.truncation_floor</c>'s tag out of the cardinality trap.
/// </summary>
/// <param name="Name">The mechanism's name, one of <see cref="TruncationFloorNames"/> for the built-in holders.</param>
/// <param name="Lsn">The highest LSN this holder permits removing; below the head by however far it is behind.</param>
public readonly record struct TruncationFloor(string Name, ulong Lsn);

/// <summary>
/// The floors as they stood when truncation was last decided — the answer to "why is the log not
/// truncating", which until this existed required a debugger.
/// <para>
/// It is a cached reading, not a live query, and deliberately so: floor providers run under the
/// engine's write lock, read engine state without further locking, and may have side effects (the
/// cluster's borrowed-sidecar refresh is registered as a floor precisely to run at that moment).
/// Evaluating them from a metrics scrape would race the pin list and rewrite the sidecar per
/// scrape. So the floors refresh at truncation cadence, and the gauge pairs them with the *live*
/// head — which is what makes the pinned distance grow while a stuck floor stands still.
/// </para>
/// </summary>
/// <param name="SnapshotLsn">The snapshot LSN truncation was decided behind.</param>
/// <param name="HeadLsn">The log head at evaluation.</param>
/// <param name="BaseLsn">The log base after the decision — the oldest LSN still on disk, minus one.</param>
/// <param name="EffectiveFloor">The minimum across every floor: what compaction was allowed to remove.</param>
/// <param name="Governing">The floor holding the effective floor down — the name an operator wants.</param>
/// <param name="Floors">Every floor consulted, in evaluation order.</param>
public sealed record TruncationFloorReport(
    ulong SnapshotLsn,
    ulong HeadLsn,
    ulong BaseLsn,
    ulong EffectiveFloor,
    TruncationFloor Governing,
    IReadOnlyList<TruncationFloor> Floors)
{
    /// <summary>
    /// Records between the effective floor and the head at evaluation — the headline number, and
    /// the one that grows without bound when a holder stops checkpointing. The live gauge
    /// recomputes it against the current head; this is its value at the last decision.
    /// </summary>
    public long PinnedRecords => HeadLsn > EffectiveFloor ? (long)(HeadLsn - EffectiveFloor) : 0;
}

/// <summary>
/// The names of the truncation floors MelangeDB registers itself. Constants because they are a
/// metric dimension and a health-check description: they are contract, not prose.
/// </summary>
public static class TruncationFloorNames
{
    /// <summary>The snapshot itself — truncation never passes the LSN it was taken at. The healthy governing floor.</summary>
    public const string Snapshot = "snapshot";

    /// <summary>The Resume retention window: a reconnecting client's gap must stay servable from the log.</summary>
    public const string ResumeWindow = "resume-window";

    /// <summary>The slowest live event subscriber's checkpoint, from the bus.</summary>
    public const string EventBus = "event-bus";

    /// <summary>An online backup's truncation pin, held for the length of the archive stream.</summary>
    public const string BackupPin = "backup-pin";

    /// <summary>A shard handoff's pending freeze marker, held until the origin releases or aborts.</summary>
    public const string ShardFreeze = "shard-freeze";

    /// <summary>A shard handoff's unsettled import marker, held until the origin is known settled.</summary>
    public const string ShardImport = "shard-import";

    /// <summary>The borrowed-sidecar refresh point — a floor that never pins, registered to run when truncation is decided.</summary>
    public const string ShardSidecar = "shard-sidecar";

    /// <summary>The cluster event forwarder's cursor: records not yet forwarded to the hub.</summary>
    public const string ClusterEvents = "cluster-events";

    /// <summary>
    /// A floor registered through the unnamed overload — a third-party holder that never named
    /// itself. Diagnostic in its own right: it says the log is pinned by something outside
    /// MelangeDB.
    /// </summary>
    public const string Unnamed = "unnamed";
}
