using System.Text.Json;
using MelangeDB.Core;

namespace MelangeDB.Cluster;

/// <summary>
/// The membership store a hub keeps in one file beside its own data, the default when no
/// Postgres store is registered. It behaves exactly like <see cref="InMemoryMembershipStore"/>,
/// and every change of ownership is written durably before anyone is told about it. A hub that
/// restarts on its kept data therefore remembers each shard's fencing token and originator. If it
/// re-minted them, as an in-memory store does, a shard whose log moved past term 1 would be
/// refused by every node (#314), and a shard handed another shard's originator would mint ids
/// that one already used.
/// <para>
/// Nodes are kept as well, so a node remembered from before the restart that never comes back is
/// declared dead by the hub's failure detector, counting from the restart, and its shards move on,
/// as with the Postgres store. A heartbeat is not written: only a revival changes the file.
/// </para>
/// </summary>
public sealed class FileMembershipStore : IMembershipStore
{
    /// <summary>The file's name inside the hub's commit log directory.</summary>
    public const string FileName = "melange.membership.json";

    private const int FormatVersion = 1;

    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    private readonly InMemoryMembershipStore _inner;

    /// <summary>Opens the store at <paramref name="path"/>, starting empty if the file does not exist.</summary>
    public FileMembershipStore(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        Path = System.IO.Path.GetFullPath(path);
        _inner = new InMemoryMembershipStore(Load(Path), Save);
    }

    /// <summary>The file this store persists to.</summary>
    public string Path { get; }

    public NodeRecord RegisterNode(string nodeName, string publicAddress, DateTimeOffset now) => _inner.RegisterNode(nodeName, publicAddress, now);

    public bool Heartbeat(string nodeName, DateTimeOffset now) => _inner.Heartbeat(nodeName, now);

    public IReadOnlyList<NodeRecord> Nodes() => _inner.Nodes();

    public ShardAssignment EnsureShard(ShardKey shard, DateTimeOffset now) => _inner.EnsureShard(shard, now);

    public ShardAssignment? GetAssignment(ShardKey shard) => _inner.GetAssignment(shard);

    public IReadOnlyList<ShardAssignment> AssignmentsFor(string nodeName) => _inner.AssignmentsFor(nodeName);

    public IReadOnlyList<ShardAssignment> AllAssignments() => _inner.AllAssignments();

    public IReadOnlyList<ShardAssignment> MarkDead(string nodeName, DateTimeOffset now) => _inner.MarkDead(nodeName, now);

    public IReadOnlyList<ShardAssignment> AssignUnowned(DateTimeOffset now) => _inner.AssignUnowned(now);

    public ShardAssignment Reassign(ShardKey shard, string toNode, DateTimeOffset now) => _inner.Reassign(shard, toNode, now);

    public ShardAssignment SetReplica(ShardKey shard, string? replicaNode) => _inner.SetReplica(shard, replicaNode);

    public ShardAssignment? RaiseTerm(ShardKey shard, string nodeName, long heldToken, long logTerm) =>
        _inner.RaiseTerm(shard, nodeName, heldToken, logTerm);

    public bool RemoveShard(ShardKey shard) => _inner.RemoveShard(shard);

    private static MembershipState? Load(string path)
    {
        if (!File.Exists(path))
            return null;
        var document = JsonSerializer.Deserialize<Document>(File.ReadAllBytes(path), SerializerOptions)
            ?? throw new InvalidDataException($"'{path}' is empty.");
        if (document.Version != FormatVersion)
            throw new InvalidDataException($"'{path}' has membership format version {document.Version}; this build reads version {FormatVersion}.");
        return new MembershipState(
            [.. document.Nodes.Select(static n => new NodeRecord(n.Name, n.Address, n.LastSeen, n.Alive))],
            [.. document.Shards.Select(static s => new ShardAssignment(new ShardKey(s.Shard), s.Node, s.Token, s.Originator, s.Replica))],
            document.NextOriginator);
    }

    private void Save(MembershipState state)
    {
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(Path)!);
        var document = new Document(
            FormatVersion,
            state.NextOriginator,
            [.. state.Nodes.OrderBy(static n => n.NodeName, StringComparer.Ordinal).Select(static n => new NodeEntry(n.NodeName, n.PublicAddress, n.LastSeen, n.Alive))],
            [.. state.Shards.OrderBy(static s => s.Shard).Select(static s => new ShardEntry(s.Shard.Value, s.NodeName, s.FencingToken, s.Originator, s.ReplicaNode))]);
        DurableFile.WriteAllBytes(Path, JsonSerializer.SerializeToUtf8Bytes(document, SerializerOptions));
    }

    private sealed record Document(int Version, ushort NextOriginator, NodeEntry[] Nodes, ShardEntry[] Shards);

    private sealed record NodeEntry(string Name, string Address, DateTimeOffset LastSeen, bool Alive);

    private sealed record ShardEntry(ulong Shard, string? Node, long Token, ushort Originator, string? Replica);
}
