using System.Security.Cryptography;
using System.Text;
using MelangeDB.Core;

namespace MelangeDB.Cluster;

/// <summary>One serialized row op on a node link. Keys and rows travel as the store's own bytes.</summary>
internal sealed record WireOp(byte Kind, uint Table, byte[] Key, byte[]? Row)
{
    public static WireOp From(in RowOp op) =>
        new((byte)op.Kind, op.Table.Value, op.Key.ToArray(), op.Kind == RowOpKind.Delete ? null : op.Row.ToArray());

    public RowOp ToRowOp() =>
        new((RowOpKind)Kind, new TableId(Table), new RowKey(Key), Row ?? default(ReadOnlyMemory<byte>));
}

/// <summary>One serialized domain event on a node link.</summary>
internal sealed record WireEvent(string EventType, byte Depth, byte[] Payload)
{
    public static WireEvent From(in EventRecord record) =>
        new(record.EventType, record.Depth, record.Payload.ToArray());

    public EventRecord ToRecord() => new(EventType, Depth, Payload);
}

internal sealed record ShardAssignmentDto(ulong Shard, string? NodeName, long FencingToken, ushort Originator)
{
    public static ShardAssignmentDto From(ShardAssignment a) =>
        new(a.Shard.Value, a.NodeName, a.FencingToken, a.Originator);

    public ShardAssignment ToAssignment() => new(new ShardKey(Shard), NodeName, FencingToken, Originator);
}

internal sealed record AuthRequest(string NodeName, string PublicAddress, string NodeNonce, string Proof);

internal sealed record AuthReply(string Proof, ShardAssignmentDto[] Assignments, int FailureTimeoutMs);

internal sealed record HeartbeatReply(ShardAssignmentDto[] Assignments);

/// <summary>
/// One owned shard's load sample, riding the heartbeat — no new clock, no new message. The
/// utilization is the busy fraction of the shard engine's write lock since the previous
/// heartbeat (see <c>MelangeEngine.WriteLockBusyTicks</c>): the resource the published hotspot
/// ceilings are ceilings on, already in [0, 1], no per-hardware calibration needed.
/// </summary>
internal sealed record ShardLoadDto(ulong Shard, double Utilization, ulong HeadLsn, long ResidentBytes, int BorrowedRows);

/// <summary>The heartbeat's body: every owned shard's current load sample.</summary>
internal sealed record HeartbeatRequest(ShardLoadDto[] Loads);

/// <summary>
/// The hub asking a shard's owner to quiesce it for a planned drain: take a fresh snapshot (so
/// the destination's recovery tail is short), close the shard's engine, and stop serving it. The
/// fencing token proves the request is from the current term; the reply's head LSN is where the
/// destination's recovery will land. The owner marks the shard as draining so its own heartbeat
/// cannot reopen it while the hub is between quiesce and reassign — an entry that outlives
/// 2 x Cluster:FailureTimeoutMs expires and the shard reopens, which is the self-healing bound
/// for a hub that died mid-drain.
/// </summary>
internal sealed record ShardDrain(ulong Shard, long FencingToken);

internal sealed record ShardDrainReply(ulong HeadLsn);

/// <summary>The hub abandoning a drain after quiesce: the owner clears the draining mark and reopens.</summary>
internal sealed record ShardDrainAbort(ulong Shard);

/// <summary>
/// The hub pushing a node's full assignment list outside the heartbeat clock — sent to a drain's
/// destination so it opens the moved shard now rather than up to one heartbeat later. The reply
/// waits for the open (recovery included), which is how the hub knows the shard is serving before
/// it swaps the gateways. Always the full list, because assignment application is a diff against
/// it: a partial list would close every shard it omitted.
/// </summary>
internal sealed record AssignmentsApply(ShardAssignmentDto[] Assignments);

internal sealed record ReplicaSubscribe(ulong FromLsn);

internal sealed record ReplicaBatch(ulong[] Lsns, WireOp[][] Records);

internal sealed record ReplicaTableSnapshot(uint Table, WireOp[] Rows);

/// <summary>
/// A full-state replication reset: the hub's entire Replicated table set at one LSN, sent when a
/// node's cursor fell below the hub log's truncation base — the gap's records are gone, so the
/// stream cannot serve it and only a bootstrap can. The node applies it as upserts <em>plus
/// deletions of local rows absent from the snapshot</em>, because a pure upsert bootstrap would
/// resurrect rows the hub deleted during the gap.
/// </summary>
internal sealed record ReplicaReset(ulong Lsn, ReplicaTableSnapshot[] Tables);

internal sealed record EventsForward(ulong UpToLsn, ulong ShardValue, WireEvent[] Events, long[] TimestampsMicros, ulong[] Lsns);

/// <summary>
/// An observer shard's request to receive an owner shard's border slice, carrying the observer's
/// durable cursor (epoch-qualified — a cursor against another log incarnation means reset) and
/// its current band depth, so the owner can detect a widened band and answer with a full reset.
/// Sent observer node → hub, routed hub → owner node as <c>border-subscribe-owner</c>.
/// </summary>
internal sealed record BorderSubscribe(
    ulong OwnerShard, ulong ObserverShard, string Epoch, ulong FromLsn, int BandChunks, bool ForceReset);

/// <summary>False when the owner shard does not exist yet (empty world region) — benign, retry later.</summary>
internal sealed record BorderSubscribeReply(bool Exists);

/// <summary>
/// One batch of an owner shard's border-relevant row ops for one observer, in LSN order up to
/// <see cref="UpToLsn"/>. Sent owner node → hub as <c>border-batch</c>, routed hub → observer node
/// as <c>border-apply</c>; the ack chain is the flow control, and the observer persists its cursor
/// before acking, so delivery is at-least-once and re-application reconciles.
/// </summary>
internal sealed record BorderBatch(ulong OwnerShard, ulong ObserverShard, string Epoch, ulong UpToLsn, WireOp[] Ops);

/// <summary>
/// A full border-band reset: every row of the owner's slice for this observer, captured at one
/// LSN. Sent when the observer's cursor cannot be served from the owner's log — truncated past, a
/// different epoch, or a widened band — because silently resuming past a gap is the bug class the
/// replica stream's bootstrap already kills; the observer applies upserts <em>plus deletion of
/// rows previously borrowed from this owner that the snapshot lacks</em>.
/// </summary>
internal sealed record BorderReset(ulong OwnerShard, ulong ObserverShard, string Epoch, ulong Lsn, ReplicaTableSnapshot[] Tables);

/// <summary>The log-arguments payload of a <c>melange/border</c> record: which shard owns the copied rows.</summary>
internal sealed record BorderMarker(ulong Owner);

internal sealed record HandoffFreeze(string HandoffId, ulong FromShard, ulong ToShard, long FencingToken, string PlayerHex);

internal sealed record HandoffFrozenRows(WireOp[] Rows);

internal sealed record HandoffImport(
    string HandoffId, ulong FromShard, ulong ToShard, long FencingToken, string PlayerHex, WireOp[] Rows);

internal sealed record HandoffRelease(string HandoffId, ulong FromShard, long FencingToken);

/// <summary>
/// A shard node telling the hub an anchored entity crossed its boundary past the margin — the
/// origin-decides trigger of a seamless handoff. A notification, not a request: the hub owns the
/// decision (in-flight dedupe, rate limit) and the origin keeps serving the entity meanwhile.
/// </summary>
internal sealed record HandoffRequest(string PlayerHex, ulong FromShard, ulong ToShard, long FencingToken);

/// <summary>An anchored entity entered the border band: the gateway pre-opens destination sessions on it.</summary>
internal sealed record HandoffApproach(string PlayerHex, ulong FromShard, ulong[] ToShards);

/// <summary>
/// A node's reconciler resolved a stranded handoff (the coordinator died or lost its link
/// mid-saga): released means the destination owns the entity now, so the hub must run its
/// transfer listeners and gateway notifications late — better late than a stale session map.
/// </summary>
internal sealed record HandoffResolved(string HandoffId, string PlayerHex, ulong FromShard, ulong ToShard, bool Released);

/// <summary>
/// A hub-initiated reducer execution on the shard owning <see cref="Shard"/> — the primitive a
/// cross-shard saga's steps are made of. Arguments travel pre-encoded (the hub validates and
/// encodes with the same registry the node decodes with); the fencing token makes a step against
/// a stale owner fail loudly instead of executing on the wrong term.
/// </summary>
internal sealed record ShardExecute(ulong Shard, long FencingToken, string Reducer, string CallerHex, string ArgsB64);

internal sealed record ShardExecuteReply(ulong Lsn);

internal sealed record HandoffQuery(string HandoffId, ulong ToShard);

internal sealed record HandoffQueryReply(bool Imported);

/// <summary>The destination's reconciler asking whether the origin's freeze is still unresolved.</summary>
internal sealed record HandoffFreezeQuery(string HandoffId, ulong FromShard);

internal sealed record HandoffFreezeQueryReply(bool Pending);

/// <summary>
/// The mutual-authentication proofs both ends of a node link exchange at connect: each side
/// proves possession of the cluster secret over the other side's nonce, so neither a rogue
/// process dialing the hub port nor something impersonating the hub gets past the handshake.
/// </summary>
internal static class LinkProof
{
    public static string NewNonce() => Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));

    public static string Compute(string secret, string nonce, string party) =>
        Convert.ToBase64String(HMACSHA256.HashData(
            Encoding.UTF8.GetBytes(secret),
            Encoding.UTF8.GetBytes(nonce + "|" + party)));

    public static bool Verify(string secret, string nonce, string party, string proof)
    {
        var expected = Compute(secret, nonce, party);
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(expected),
            Encoding.UTF8.GetBytes(proof));
    }
}
