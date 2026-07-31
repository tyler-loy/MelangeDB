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

internal sealed record HandoffFreeze(string HandoffId, ulong FromShard, ulong ToShard, long FencingToken, string PlayerHex);

internal sealed record HandoffFrozenRows(WireOp[] Rows);

internal sealed record HandoffImport(
    string HandoffId, ulong FromShard, ulong ToShard, long FencingToken, string PlayerHex, WireOp[] Rows);

internal sealed record HandoffRelease(string HandoffId, ulong FromShard, long FencingToken);

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
