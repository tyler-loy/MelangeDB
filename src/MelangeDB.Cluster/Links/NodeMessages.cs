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

/// <summary>
/// A shard this node keeps a warm replica of (phase 30): the originator its sequences continue
/// under, and the ceiling, the owner's last reported durable LSN (0 until the owner has reported).
/// </summary>
internal sealed record ReplicaDto(ulong Shard, ushort Originator, ulong Ceiling);

internal sealed record HeartbeatReply(ShardAssignmentDto[] Assignments, ReplicaDto[]? Replicas = null);

/// <summary>
/// One owned shard's load sample, riding the heartbeat — no new clock, no new message. The
/// utilization is the busy fraction of the shard engine's write lock since the previous
/// heartbeat (see <c>MelangeEngine.WriteLockBusyTicks</c>): the resource the published hotspot
/// ceilings are ceilings on, already in [0, 1], no per-hardware calibration needed.
/// </summary>
internal sealed record ShardLoadDto(
    ulong Shard,
    double Utilization,
    ulong HeadLsn,
    long ResidentBytes,
    int BorrowedRows,
    long AuthoritativeRows,
    bool Draining = false,
    ulong DurableLsn = 0);

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
/// The hub asking a shard's owner for the first half of a removal: check that it holds nothing of
/// its own and nothing is pinning its log, then seal it against further writes and close it. The
/// emptiness check and the seal happen in one hold of the owner's <em>engine write lock</em>,
/// because they cannot be separated — a shard found empty and then closed can take a row in
/// between, from a reducer call that resolved it before the reap started. The directory survives
/// this message; it is deleted by <see cref="ShardReapDelete"/>, after the membership row is gone.
/// </summary>
internal sealed record ShardReap(ulong Shard, long FencingToken);

/// <summary>
/// The hub telling the owner the membership row is retired and the sealed shard's directory may
/// go. Carries no fencing token: the shard has no term any more, which is the point. Sent only
/// after <see cref="ShardReap"/> succeeded, so that an interruption anywhere leaves a shard that
/// is still openable rather than a retired originator that a stale assignment can re-mint under.
/// </summary>
internal sealed record ShardReapDelete(ulong Shard);

/// <summary>
/// What the owner did. <see cref="Refusal"/> is null when the shard was removed, and otherwise
/// names the condition that was not met — a refusal is the expected answer, not an error, because
/// the hub decides from a sampled view and the owner is the one holding the truth.
/// </summary>
internal sealed record ShardReapReply(bool Reaped, string? Refusal);

/// <summary>
/// The hub pushing a node's full assignment list outside the heartbeat clock — sent to a drain's
/// destination so it opens the moved shard now rather than up to one heartbeat later. The reply
/// waits for the open (recovery included), which is how the hub knows the shard is serving before
/// it swaps the gateways. Always the full list, because assignment application is a diff against
/// it: a partial list would close every shard it omitted.
/// </summary>
internal sealed record AssignmentsApply(ShardAssignmentDto[] Assignments);

internal sealed record ReplicaSubscribe(ulong FromLsn);

/// <summary>
/// One slice of the hub's Replicated stream: the records carrying a Replicated op, in LSN order.
/// <see cref="PrevLsn"/> is the last Replicated record this stream delivered before the slice (or
/// where the subscription started), and it is what makes the slice safe to apply: a shard whose
/// replica cursor — the last Replicated LSN it applied — is behind it would be missing records, and
/// applying the slice would advance its cursor over the hole for good. Zero (an older hub) means
/// unknown, and is applied as before.
/// </summary>
internal sealed record ReplicaBatch(ulong[] Lsns, WireOp[][] Records, ulong PrevLsn = 0);

/// <summary>
/// A node pulling one slice of the Replicated stream for a shard it is opening, before the shard
/// is published (#283). The pushed stream only reaches a shard once it is in service, so without
/// this a newly opened shard served its first calls with empty or partly applied Replicated
/// tables. <see cref="FromLsn"/> is the shard's own replica cursor.
/// </summary>
internal sealed record ReplicaRead(ulong FromLsn);

/// <summary>
/// The hub's answer to <see cref="ReplicaRead"/>. <see cref="HeadLsn"/> is the hub's head when the
/// request arrived, already durable: the node pulls until <see cref="ScannedTo"/> reaches the head
/// its first request was answered with. <see cref="Reset"/> replaces the records when the cursor
/// fell below the hub log's truncation base.
/// </summary>
internal sealed record ReplicaReadReply(
    ulong HeadLsn,
    ulong ScannedTo,
    ulong[] Lsns,
    WireOp[][] Records,
    ReplicaReset? Reset = null);

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

/// <summary>
/// One part of a <see cref="BorderReset"/> too large for one node-link frame (a million-row shard's
/// band runs to hundreds of MiB). Sent owner node → hub as <c>border-reset-part</c> and routed hub →
/// observer node as <c>border-reset-part-apply</c>, in order, each acked before the next. The
/// observer stages the parts and applies the reset, deletions and cursor included, only on the
/// <see cref="Final"/> one: a partial reset would delete every borrowed row a later part carries.
/// A part out of sequence discards the staging and fails, and the owner starts the reset over
/// under a new <see cref="ResetId"/>. A separate type rather than fields on
/// <see cref="BorderReset"/>, so a node that predates parts refuses them rather than applying
/// each one as a whole reset.
/// </summary>
internal sealed record BorderResetPart(string ResetId, int Part, bool Final, BorderReset Reset);

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
/// <summary>
/// One reducer call on a shard's node. <see cref="ClientOriginated"/>, <see cref="CallerIsGuest"/>
/// and <see cref="Claims"/> carry a client call's source and token claims, so the node rate
/// limits it and applies the reducer's policy as it would to a call made there; absent (a
/// server-side saga step, or a hub from before they existed) the call is in-process and trusted.
/// </summary>
internal sealed record ShardExecute(
    ulong Shard,
    long FencingToken,
    string Reducer,
    string CallerHex,
    string ArgsB64,
    bool ClientOriginated = false,
    bool CallerIsGuest = false,
    IReadOnlyList<KeyValuePair<string, string>>? Claims = null);

internal sealed record ShardExecuteReply(ulong Lsn);

/// <summary>
/// One bulk row on its way to the shard that owns it. <see cref="Row"/> is the <em>routing
/// preimage</em>: the caller's values, coerced and serialized by the hub with the same
/// <c>RowSerializer</c> the engine uses, with unsupplied columns at their defaults. It is the
/// bytes the hub handed the strategy to decide the destination, so the receiver re-deciding from
/// the same bytes is comparing like with like.
/// <para>
/// <see cref="Supplied"/> names the columns the caller actually gave, because the difference
/// matters: an <c>[AutoInc]</c> column the caller omitted must be <em>allocated by the owning
/// shard</em> — originator-prefixed, so shards never coordinate — and a preimage cannot say that
/// on its own, every unsupplied column reading as zero. The receiver reconstitutes the caller's
/// dictionary from the two and stages it through the ordinary bulk path.
/// </para>
/// </summary>
internal sealed record BulkRowDto(string Table, byte[] Row, string[] Supplied);

/// <summary>
/// The hub asking a shard's owner to take one group of a fanned-out bulk batch. Single-shard by
/// construction — but the receiver re-resolves every row anyway, because "by construction" is the
/// hub's construction and this is the only place that can check it. The fencing token makes a
/// group aimed at a stale owner fail loudly instead of landing on the wrong term.
/// </summary>
internal sealed record ShardBulk(ulong Shard, long FencingToken, string CallerHex, BulkRowDto[] Rows);

internal sealed record ShardBulkReply(ulong Lsn, int Rows);

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
