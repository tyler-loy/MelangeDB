namespace MelangeDB;

/// <summary>Which storage engine holds a table.</summary>
public enum StorageTier
{
    /// <summary>The in-process log-structured store holding world state. The default.</summary>
    Hot,

    /// <summary>Opt-in Postgres storage. Eventually consistent with the log by design.</summary>
    Relational,
}

/// <summary>Whether a table must stay wholly in memory.</summary>
public enum Residency
{
    /// <summary>The table may spill to disk; memory is bounded by working set. The default.</summary>
    Paged,

    /// <summary>The table is pinned wholly in memory. Opt-in, a declared and bounded commitment.</summary>
    Resident,

    /// <summary>Resident while under a configured size threshold, paged beyond it.</summary>
    Auto,
}

/// <summary>Which node in a cluster holds a table. Ignored entirely by single-node deployments.</summary>
public enum Placement
{
    /// <summary>Rows split across shard nodes by shard key. One writer per shard, many readers.</summary>
    Partitioned,

    /// <summary>A full copy on every node, written only by the hub.</summary>
    Replicated,

    /// <summary>The table lives on the hub only.</summary>
    Global,

    /// <summary>The table lives on one node and never leaves it.</summary>
    Local,
}

/// <summary>
/// Declares a <c>partial struct</c> as a MelangeDB table. <see cref="Tier"/>, <see cref="Placement"/>,
/// and <see cref="Residency"/> are three independent axes; conflating any two is a design error.
/// </summary>
[AttributeUsage(AttributeTargets.Struct)]
public sealed class TableAttribute : Attribute
{
    /// <summary>The table name; defaults to the struct name.</summary>
    public string? Name { get; set; }

    /// <summary>
    /// Whether this table may sync to clients at all, subject to policies. Private by default:
    /// a private table is server-internal and no subscription may name it.
    /// </summary>
    public bool Public { get; set; }

    public StorageTier Tier { get; set; } = StorageTier.Hot;

    public Residency Residency { get; set; } = Residency.Paged;

    public Placement Placement { get; set; } = Placement.Partitioned;

    /// <summary>
    /// The column the shard strategy derives this table's shard key from. Never the primary key
    /// (compile error MELANGE0018): handoff re-homes a row by rewriting this column while the
    /// row's stored key stays fixed, so the shard id must be its own column.
    /// </summary>
    public string? ShardBy { get; set; }

    /// <summary>
    /// Names the reducer this table's timer rows fire. Declaring it makes rows of this table
    /// scheduling data, not client data: the table must carry exactly one <see cref="ScheduleAt"/>
    /// column, is implicitly private, and is always <see cref="Placement.Local"/> — declaring any
    /// other <see cref="Placement"/> alongside <c>Scheduled</c> is compile error MELANGE0022.
    /// <para>
    /// <c>Local</c> <em>is</em> the per-shard partitioning docs/CLUSTERING.md describes, because a
    /// shard node runs one engine per shard it owns and timers are rows in that engine's own log:
    /// node-local on a per-shard engine means shard-local, so one declared timer table becomes one
    /// independent set of timers per shard, firing on whichever node owns it. Seed those rows from
    /// a <see cref="ReducerKind.Init"/> reducer — a shard created on first visit starts empty, and
    /// nothing else can reach into its engine to give it timers.
    /// </para>
    /// The named reducer's signature is <c>void Name(ReducerContext ctx, ThisTable timer)</c>, and
    /// it is not client-callable.
    /// </summary>
    public string? Scheduled { get; set; }
}
