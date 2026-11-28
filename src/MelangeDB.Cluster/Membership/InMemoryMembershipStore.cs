namespace MelangeDB.Cluster;

/// <summary>
/// The in-memory membership store: correct, hub-local, and gone with the hub process. The right
/// choice for tests. A hub that restarts on kept shard data needs the store to remember the terms
/// and originators it minted: <see cref="FileMembershipStore"/> (the default) or the
/// Postgres-backed store.
/// </summary>
public sealed class InMemoryMembershipStore : IMembershipStore
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, NodeRecord> _nodes = new(StringComparer.Ordinal);
    private readonly Dictionary<ShardKey, ShardAssignment> _shards = [];
    private readonly Action<MembershipState>? _persist;
    private ushort _nextOriginator = 1; // 0 is the hub's.

    public InMemoryMembershipStore()
    {
    }

    /// <summary>
    /// A store that starts from <paramref name="state"/> and hands every change to
    /// <paramref name="persist"/> before it takes effect: a change whose persist throws is undone,
    /// so nothing is handed out that the persisted state does not hold.
    /// </summary>
    internal InMemoryMembershipStore(MembershipState? state, Action<MembershipState> persist)
    {
        _persist = persist;
        if (state is null)
            return;
        foreach (var node in state.Nodes)
            _nodes[node.NodeName] = node;
        foreach (var shard in state.Shards)
            _shards[shard.Shard] = shard;
        _nextOriginator = state.NextOriginator;
    }

    public NodeRecord RegisterNode(string nodeName, string publicAddress, DateTimeOffset now)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeName);
        lock (_lock)
        {
            var record = new NodeRecord(nodeName, publicAddress, now, Alive: true);
            var changed = !_nodes.TryGetValue(nodeName, out var previous)
                || previous.PublicAddress != publicAddress
                || !previous.Alive;
            Mutate(changed, () => _nodes[nodeName] = record);
            return record;
        }
    }

    public bool Heartbeat(string nodeName, DateTimeOffset now)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(nodeName, out var record))
                return false;

            // Only a revival is persisted: LastSeen moves every beat and nothing reads it back.
            Mutate(!record.Alive, () => _nodes[nodeName] = record with { LastSeen = now, Alive = true });
            return true;
        }
    }

    public IReadOnlyList<NodeRecord> Nodes()
    {
        lock (_lock)
        {
            return [.. _nodes.Values.OrderBy(static n => n.NodeName, StringComparer.Ordinal)];
        }
    }

    public ShardAssignment EnsureShard(ShardKey shard, DateTimeOffset now)
    {
        lock (_lock)
        {
            if (_shards.TryGetValue(shard, out var existing))
                return existing;
            if (_nextOriginator == 0)
                throw new InvalidOperationException("The 16-bit originator space is exhausted.");
            var assignment = new ShardAssignment(shard, LeastLoadedNode(), FencingToken: 1, _nextOriginator);
            Mutate(true, () =>
            {
                _shards[shard] = assignment;
                _nextOriginator++;
            });
            return assignment;
        }
    }

    public ShardAssignment? GetAssignment(ShardKey shard)
    {
        lock (_lock)
        {
            return _shards.GetValueOrDefault(shard);
        }
    }

    public IReadOnlyList<ShardAssignment> AssignmentsFor(string nodeName)
    {
        lock (_lock)
        {
            return [.. _shards.Values.Where(a => a.NodeName == nodeName).OrderBy(static a => a.Shard)];
        }
    }

    public IReadOnlyList<ShardAssignment> AllAssignments()
    {
        lock (_lock)
        {
            return [.. _shards.Values.OrderBy(static a => a.Shard)];
        }
    }

    public IReadOnlyList<ShardAssignment> MarkDead(string nodeName, DateTimeOffset now)
    {
        lock (_lock)
        {
            var changed = new List<ShardAssignment>();
            Mutate(true, () =>
            {
                if (_nodes.TryGetValue(nodeName, out var record))
                    _nodes[nodeName] = record with { Alive = false, LastSeen = now };

                // A dead node keeps no replicas; the hub places new ones.
                foreach (var replicated in _shards.Values.Where(a => a.ReplicaNode == nodeName).ToList())
                    _shards[replicated.Shard] = replicated with { ReplicaNode = null };

                // A shard with a live replica goes to the replica's node, which promotes its warm
                // projection; the rest take the cold path on the least-loaded node.
                foreach (var assignment in _shards.Values.Where(a => a.NodeName == nodeName).ToList())
                {
                    var replica = assignment.ReplicaNode is { } candidate && _nodes.TryGetValue(candidate, out var node) && node.Alive
                        ? candidate
                        : null;
                    var next = WithOwner(assignment, replica ?? LeastLoadedNode(except: nodeName));
                    _shards[assignment.Shard] = next;
                    changed.Add(next);
                }
            });

            return changed;
        }
    }

    public IReadOnlyList<ShardAssignment> AssignUnowned(DateTimeOffset now)
    {
        lock (_lock)
        {
            var unowned = _shards.Values.Where(static a => a.NodeName is null).ToList();
            var changed = new List<ShardAssignment>();
            Mutate(unowned.Count > 0 && LeastLoadedNode() is not null, () =>
            {
                foreach (var assignment in unowned)
                {
                    if (LeastLoadedNode() is not { } owner)
                        break;
                    var next = WithOwner(assignment, owner);
                    _shards[assignment.Shard] = next;
                    changed.Add(next);
                }
            });

            return changed;
        }
    }

    public ShardAssignment Reassign(ShardKey shard, string toNode, DateTimeOffset now)
    {
        ArgumentException.ThrowIfNullOrEmpty(toNode);
        lock (_lock)
        {
            if (!_shards.TryGetValue(shard, out var assignment))
                throw new InvalidOperationException($"{shard} was never created; nothing to reassign.");
            if (!_nodes.TryGetValue(toNode, out var node) || !node.Alive)
                throw new InvalidOperationException($"Node '{toNode}' is not registered and alive; a drain must never assign to a corpse.");
            var next = WithOwner(assignment, toNode);
            Mutate(true, () => _shards[shard] = next);
            return next;
        }
    }

    public ShardAssignment SetReplica(ShardKey shard, string? replicaNode)
    {
        lock (_lock)
        {
            if (!_shards.TryGetValue(shard, out var assignment))
                throw new InvalidOperationException($"{shard} was never created; it has no replica to place.");
            if (replicaNode is not null)
            {
                if (replicaNode == assignment.NodeName)
                    throw new InvalidOperationException($"'{replicaNode}' owns {shard}; a shard's replica must be on another node.");
                if (!_nodes.TryGetValue(replicaNode, out var node) || !node.Alive)
                    throw new InvalidOperationException($"Node '{replicaNode}' is not registered and alive; it cannot keep a replica.");
            }

            var next = assignment with { ReplicaNode = replicaNode };
            Mutate(next != assignment, () => _shards[shard] = next);
            return next;
        }
    }

    public ShardAssignment? RaiseTerm(ShardKey shard, string nodeName, long heldToken, long logTerm)
    {
        lock (_lock)
        {
            if (!_shards.TryGetValue(shard, out var assignment)
                || assignment.NodeName != nodeName
                || assignment.FencingToken != heldToken
                || assignment.FencingToken >= logTerm)
            {
                return null;
            }

            var next = assignment with { FencingToken = logTerm + 1 };
            Mutate(true, () => _shards[shard] = next);
            return next;
        }
    }

    /// <summary>A change of owner: a new term, and no replica on the new owner's own node.</summary>
    private static ShardAssignment WithOwner(ShardAssignment assignment, string? owner) => assignment with
    {
        NodeName = owner,
        FencingToken = assignment.FencingToken + 1,
        ReplicaNode = assignment.ReplicaNode == owner ? null : assignment.ReplicaNode,
    };

    private string? LeastLoadedNode(string? except = null)
    {
        var candidates = _nodes.Values
            .Where(n => n.Alive && n.NodeName != except)
            .OrderBy(n => _shards.Values.Count(a => a.NodeName == n.NodeName))
            .ThenBy(static n => n.NodeName, StringComparer.Ordinal)
            .ToList();
        return candidates.Count > 0 ? candidates[0].NodeName : null;
    }

    public bool RemoveShard(ShardKey shard)
    {
        lock (_lock)
        {
            // _nextOriginator is untouched on purpose: it is a high-water mark, not a free list.
            var removed = false;
            Mutate(_shards.ContainsKey(shard), () => removed = _shards.Remove(shard));
            return removed;
        }
    }

    /// <summary>
    /// Applies a change under <see cref="_lock"/>. With a persist hook and <paramref name="changes"/>
    /// set, the new state is persisted before the change is returned to anyone, and a failed
    /// persist rolls it back and throws.
    /// </summary>
    private void Mutate(bool changes, Action change)
    {
        if (_persist is null || !changes)
        {
            change();
            return;
        }

        var nodes = new Dictionary<string, NodeRecord>(_nodes, StringComparer.Ordinal);
        var shards = new Dictionary<ShardKey, ShardAssignment>(_shards);
        var nextOriginator = _nextOriginator;
        change();
        try
        {
            _persist(new MembershipState([.. _nodes.Values], [.. _shards.Values], _nextOriginator));
        }
        catch
        {
            _nodes.Clear();
            foreach (var (name, node) in nodes)
                _nodes[name] = node;
            _shards.Clear();
            foreach (var (key, assignment) in shards)
                _shards[key] = assignment;
            _nextOriginator = nextOriginator;
            throw;
        }
    }
}

/// <summary>Everything a membership store must not forget, as <see cref="FileMembershipStore"/> persists it.</summary>
internal sealed record MembershipState(NodeRecord[] Nodes, ShardAssignment[] Shards, ushort NextOriginator);
