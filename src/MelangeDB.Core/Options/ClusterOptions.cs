namespace MelangeDB.Core;

/// <summary>The role a node plays in a cluster (<c>Cluster:Role</c>).</summary>
public enum ClusterRole
{
    /// <summary>
    /// Not clustered. The default — a single-node deployment ignores placement entirely and
    /// behaves exactly as it did before clustering existed.
    /// </summary>
    None,

    /// <summary>
    /// The hub: identity, <c>Global</c> and <c>Replicated</c> tables, the Postgres tier, shard
    /// assignment, and the gateway. Exactly one per cluster.
    /// </summary>
    Hub,

    /// <summary>
    /// A shard node: <c>Partitioned</c> tables for its assigned shards, and the scheduled
    /// reducers whose timer rows live in those shards.
    /// </summary>
    Shard,
}

/// <summary>When the shard-span debug check runs (<c>Cluster:ShardSpanCheck</c>).</summary>
public enum ShardSpanCheckMode
{
    /// <summary>
    /// Enabled when the entry assembly is a Debug build (JIT tracking enabled) — the check exists
    /// to turn a placement mistake into a test failure, not to tax the production hot path. The
    /// default.
    /// </summary>
    DebugOnly,

    /// <summary>Always enabled, Release builds included.</summary>
    Always,

    /// <summary>Never enabled.</summary>
    Off,
}

/// <summary>
/// Clustering options (<c>MelangeDb:Cluster:*</c>). Everything here is restart-only: a node's
/// role, name, and addresses are its identity in the cluster, and changing them live would be a
/// different node. <c>Role = None</c> (the default) disables all of it.
/// </summary>
public sealed class ClusterOptions
{
    /// <summary>See <see cref="ClusterRole"/>. The default, <c>None</c>, means not clustered.</summary>
    public ClusterRole Role { get; set; } = ClusterRole.None;

    /// <summary>
    /// This node's stable name — the membership store's key for assignments and fencing. Required
    /// for shard nodes; the hub defaults to <c>hub</c>.
    /// </summary>
    public string NodeName { get; set; } = "";

    /// <summary>
    /// The cluster secret: the HMAC key behind node-link mutual authentication and hub-minted
    /// identity assertions. Required whenever <see cref="Role"/> is not <c>None</c>; every node
    /// presents the same secret. Treat it like a database password — anyone holding it can join
    /// the cluster and impersonate any player (see docs/THREAT-MODEL.md).
    /// </summary>
    public string Secret { get; set; } = "";

    /// <summary>
    /// Hub only: the TCP port the node-link listener binds. 0 binds an ephemeral port (useful for
    /// tests; the bound port is logged and exposed to the process).
    /// </summary>
    public int NodeListenPort { get; set; }

    /// <summary>
    /// Hub only: the interface address the node-link listener binds. The default,
    /// <c>127.0.0.1</c>, only admits same-machine nodes — safe by construction. A multi-machine
    /// cluster sets <c>0.0.0.0</c> or a specific internal interface; every connection still has
    /// to prove the cluster secret, but widening the bind should be paired with network-level
    /// controls (see docs/THREAT-MODEL.md).
    /// </summary>
    public string NodeListenAddress { get; set; } = "127.0.0.1";

    /// <summary>Shard only: the hub's node-link address, as <c>host:port</c>.</summary>
    public string HubAddress { get; set; } = "";

    /// <summary>
    /// Shard only: the base HTTP address where this node's shard websocket endpoints are
    /// reachable by the gateway (e.g. <c>http://10.0.0.5:5001</c>). Internal infrastructure —
    /// never exposed to clients.
    /// </summary>
    public string PublicAddress { get; set; } = "";

    /// <summary>
    /// Lifetime of a hub-minted internal identity assertion. Assertions are re-minted on client
    /// re-auth; the TTL only bounds how long a captured assertion stays redeemable.
    /// </summary>
    public int AssertionTtlSeconds { get; set; } = 300;

    /// <summary>How often a shard node heartbeats the hub over its node link.</summary>
    public int HeartbeatIntervalMs { get; set; } = 1_000;

    /// <summary>
    /// Silence after which the hub suspects a shard node dead and reassigns its shards — and,
    /// symmetrically, after which a node that cannot reach the hub considers its own shard leases
    /// expired and fences itself. The self-fencing side is what makes a wrongly-suspected-dead
    /// node stop writing players it no longer owns.
    /// </summary>
    public int FailureTimeoutMs { get; set; } = 10_000;

    /// <summary>See <see cref="ShardSpanCheckMode"/>.</summary>
    public ShardSpanCheckMode ShardSpanCheck { get; set; } = ShardSpanCheckMode.DebugOnly;

    /// <summary>
    /// Shard only: the directory per-shard engines root their commit logs and hot stores under;
    /// shard <c>k</c> lives in <c>{ShardDataPath}/shard-k</c>. On reassignment the new owner
    /// opens the same directory, so it must be storage reachable from every shard node (phase 09
    /// assumes shared or re-attachable volumes; log shipping is a later phase).
    /// </summary>
    public string ShardDataPath { get; set; } = "./data/shards";

    /// <summary>
    /// Spatial strategy only: how deep (in chunks) each shard's read-only border band reaches into
    /// its neighbours. Deeper is smoother and costs bandwidth plus memory on every node. The
    /// default is derived, not guessed: the band must cover
    /// <c>HandoffMarginChunks</c> plus the distance an entity travels during one handoff window
    /// (crossing detection + saga), which for the reference workload is
    /// 1 + ceil(8 m/s x ~1 s / 64 m per chunk) = 2 (docs/CLUSTERING.md shows the derivation).
    /// Must exceed <see cref="HandoffMarginChunks"/>; values below 1 are clamped to 1.
    /// </summary>
    public int BorderBandChunks { get; set; } = 2;

    /// <summary>
    /// Spatial strategy only: the hysteresis margin, in chunks. A handoff triggers only once an
    /// entity is strictly more than this many chunks past a block boundary, so pacing across the
    /// line never triggers one per step — after a transfer the entity must travel back through
    /// the full margin before the reverse transfer can fire. 0 disables the margin (entities
    /// transfer on first crossing, which is what creatures use regardless).
    /// </summary>
    public int HandoffMarginChunks { get; set; } = 1;

    /// <summary>
    /// The rate limit on automatic (boundary-triggered) handoffs: the hub will not start a new
    /// transfer for an entity within this many milliseconds of its previous one. The second half
    /// of hysteresis — even an entity oscillating deeper than the margin triggers a bounded
    /// number of transfers per unit time, never one per step.
    /// </summary>
    public int HandoffMinIntervalMs { get; set; } = 2_000;

    /// <summary>
    /// How long the gateway queues a drained shard's reducer calls before answering queued
    /// callers with a retryable error. Deliberately far above the handoff queue's patience:
    /// recovering a shard is slower than importing one player, and this cap exists to bound a
    /// <em>wedged</em> drain, not a normal one.
    /// </summary>
    public int DrainQueueTimeoutMs { get; set; } = 60_000;
}
