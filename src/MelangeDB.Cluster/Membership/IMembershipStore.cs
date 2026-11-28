namespace MelangeDB.Cluster;

/// <summary>One shard node as the membership store knows it.</summary>
public sealed record NodeRecord(string NodeName, string PublicAddress, DateTimeOffset LastSeen, bool Alive);

/// <summary>
/// One shard's current ownership: the owning node, the fencing token minted for that ownership
/// term, and the shard's originator id. The fencing token increases on every reassignment — a
/// message carrying an older token is from a previous owner and is rejected. The originator is
/// stable for the shard's lifetime: the AutoInc sequence continues from the shard's own log when
/// ownership moves, so the same prefix keeps minting unique ids. <see cref="ReplicaNode"/> is the
/// node keeping a warm replica (phase 30), never the owner; changing it does not change the term.
/// </summary>
public sealed record ShardAssignment(ShardKey Shard, string? NodeName, long FencingToken, ushort Originator, string? ReplicaNode = null);

/// <summary>
/// The cluster's ownership registry: which nodes exist, which node owns each shard under which
/// fencing token, and each shard's originator id. Owned and written exclusively by the hub —
/// shard nodes learn assignments over their node links, never by reading the store. Postgres-backed
/// in production (the hub already has Postgres). Otherwise the hub keeps it in a file beside its
/// own data (<see cref="FileMembershipStore"/>, the default), so a restart forgets neither the terms
/// nor the originators it minted; the in-memory store is for tests.
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
    /// moved shard's fencing token — the old owner's token is now stale everywhere. A shard whose
    /// replica is on a live node goes to that node, which promotes its warm projection; the rest go
    /// to the least-loaded live node. Replicas on the dead node are cleared. Shards with
    /// no surviving candidate become unowned (null node) with a bumped token, and are assigned on
    /// the next registration. Returns the assignments that changed.
    /// </summary>
    IReadOnlyList<ShardAssignment> MarkDead(string nodeName, DateTimeOffset now);

    /// <summary>
    /// Assigns every unowned shard to live nodes (fewest-shards-first), bumping fencing tokens.
    /// Called when a node registers. Returns the assignments that changed.
    /// </summary>
    IReadOnlyList<ShardAssignment> AssignUnowned(DateTimeOffset now);

    /// <summary>
    /// Moves one shard to a named live node, bumping its fencing token — the planned drain's
    /// membership step. The caller (the hub's drain) guarantees the previous owner has already
    /// quiesced the shard; this method only records the new term. Throws for an unknown shard or
    /// a node that is not registered and alive — a drain must never assign to a corpse.
    /// </summary>
    ShardAssignment Reassign(ShardKey shard, string toNode, DateTimeOffset now);

    /// <summary>
    /// Names the node that keeps <paramref name="shard"/>'s warm replica, or none. Placement is the
    /// hub's decision; the store only records it, and clears it itself where a change of owner or
    /// a death would otherwise leave it naming the owner or a dead node. Does not bump the fencing
    /// token: a replica owns nothing. Throws for an unknown shard, a replica on the owner, or a
    /// node that is not registered and alive.
    /// </summary>
    ShardAssignment SetReplica(ShardKey shard, string? replicaNode);

    /// <summary>
    /// Raises <paramref name="shard"/>'s fencing token to <c><paramref name="logTerm"/> + 1</c>,
    /// keeping its owner and replica, when <paramref name="nodeName"/> still owns it under
    /// <paramref name="heldToken"/>. The owner reports this when the shard's log refused to open
    /// because it is already at <paramref name="logTerm"/>: a term this store minted and no longer
    /// remembers, such as after an in-memory store's hub restarted or a store was restored from
    /// an older backup (#314). The new term is still strictly higher than any that wrote the log,
    /// so the fence's guarantee holds.
    /// <para>
    /// Returns the raised assignment, or null when the report is out of date: the shard is unknown,
    /// has moved, or its token is no longer <paramref name="heldToken"/> or is already past
    /// <paramref name="logTerm"/>.
    /// </para>
    /// </summary>
    ShardAssignment? RaiseTerm(ShardKey shard, string nodeName, long heldToken, long logTerm);

    /// <summary>
    /// Forgets a shard entirely — the counterpart <see cref="EnsureShard"/> never had. Returns
    /// false for a shard the store does not know, so a repeated reap is not an error.
    /// <para>
    /// The caller guarantees the shard is already closed and its data removed; this only drops the
    /// ownership record. <b>The originator is not released</b>: ids minted under it outlive the
    /// shard, so allocation is a high-water mark and a removed shard's prefix retires with it.
    /// A shard key that is visited again afterwards is simply new — <see cref="EnsureShard"/>
    /// mints it a fresh originator and a fresh term.
    /// </para>
    /// </summary>
    bool RemoveShard(ShardKey shard);
}
