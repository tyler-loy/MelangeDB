namespace MelangeDB.Cluster;

/// <summary>One shard node as the membership store knows it.</summary>
public sealed record NodeRecord(string NodeName, string PublicAddress, DateTimeOffset LastSeen, bool Alive);

/// <summary>
/// One shard's current ownership: the owning node, the fencing token minted for that ownership
/// term, and the shard's originator id. The fencing token increases on every reassignment — a
/// message carrying an older token is from a previous owner and is rejected. The originator is
/// stable for the shard's lifetime: the AutoInc sequence continues from the shard's own log when
/// ownership moves, so the same prefix keeps minting unique ids.
/// </summary>
public sealed record ShardAssignment(ShardKey Shard, string? NodeName, long FencingToken, ushort Originator);

/// <summary>
/// The cluster's ownership registry: which nodes exist, which node owns each shard under which
/// fencing token, and each shard's originator id. Owned and written exclusively by the hub —
/// shard nodes learn assignments over their node links, never by reading the store. Postgres-backed
/// in production (the hub already has Postgres); in-memory for tests and single-process clusters.
/// Implementations must be safe for concurrent use.
/// </summary>
public interface IMembershipStore
{
    /// <summary>Registers (or re-registers) a shard node, marking it alive.</summary>
    NodeRecord RegisterNode(string nodeName, string publicAddress, DateTimeOffset now);

    /// <summary>Records a heartbeat; returns false for a node that was never registered.</summary>
    bool Heartbeat(string nodeName, DateTimeOffset now);

    /// <summary>All registered nodes.</summary>
    IReadOnlyList<NodeRecord> Nodes();

    /// <summary>
    /// Returns the shard's assignment, creating it if the shard is new: a fresh originator id is
    /// minted (the hub reserves originator 0) and the shard is assigned to the live node owning
    /// the fewest shards. A shard created while no node is alive gets a null owner and is
    /// assigned on the next registration or heartbeat sweep.
    /// </summary>
    ShardAssignment EnsureShard(ShardKey shard, DateTimeOffset now);

    /// <summary>The shard's assignment, or null for a shard that was never created.</summary>
    ShardAssignment? GetAssignment(ShardKey shard);

    /// <summary>Every shard currently assigned to <paramref name="nodeName"/>.</summary>
    IReadOnlyList<ShardAssignment> AssignmentsFor(string nodeName);

    /// <summary>Every shard the store knows, assigned or not.</summary>
    IReadOnlyList<ShardAssignment> AllAssignments();

    /// <summary>
    /// Marks a node dead and reassigns each of its shards to a surviving live node, bumping every
    /// moved shard's fencing token — the old owner's token is now stale everywhere. Shards with
    /// no surviving candidate become unowned (null node) with a bumped token, and are assigned on
    /// the next registration. Returns the assignments that changed.
    /// </summary>
    IReadOnlyList<ShardAssignment> MarkDead(string nodeName, DateTimeOffset now);

    /// <summary>
    /// Assigns every unowned shard to live nodes (fewest-shards-first), bumping fencing tokens.
    /// Called when a node registers. Returns the assignments that changed.
    /// </summary>
    IReadOnlyList<ShardAssignment> AssignUnowned(DateTimeOffset now);
}
