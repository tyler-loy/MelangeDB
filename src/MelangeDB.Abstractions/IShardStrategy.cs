namespace MelangeDB;

/// <summary>
/// A row seen by the shard strategy: the serialized bytes plus schema-aware column access. The
/// engine constructs these; a strategy only reads. <see cref="Column"/> returns the boxed column
/// value (enums as their declared enum type), or throws for an unknown column name.
/// </summary>
public readonly struct RowRef
{
    private readonly Func<string, object?> _column;

    public RowRef(ReadOnlyMemory<byte> bytes, Func<string, object?> column)
    {
        ArgumentNullException.ThrowIfNull(column);
        Bytes = bytes;
        _column = column;
    }

    /// <summary>The row's serialized form — the same bytes the write set and the log carry.</summary>
    public ReadOnlyMemory<byte> Bytes { get; }

    /// <summary>Reads one column's boxed value by name.</summary>
    public object? Column(string name) => _column(name);
}

/// <summary>
/// The session facts a shard strategy may route on: who the session is, and a read-only view of
/// the hub's committed state — the strategy answers "which shard is this player in?" from hub
/// tables (a session-to-instance table, a party table), never from shard-local state, because the
/// gateway asks before any shard attachment exists.
/// </summary>
public sealed class SessionContext
{
    public SessionContext(Identity identity, bool isGuest, IDbView hubDb)
    {
        ArgumentNullException.ThrowIfNull(hubDb);
        Identity = identity;
        IsGuest = isGuest;
        HubDb = hubDb;
    }

    /// <summary>The session's identity.</summary>
    public Identity Identity { get; }

    /// <summary>Whether the identity authenticated with the IdP's guest role.</summary>
    public bool IsGuest { get; }

    /// <summary>Read-only committed state of the hub — Global and Replicated tables.</summary>
    public IDbView HubDb { get; }
}

/// <summary>
/// The seam where the developer defines what a shard <em>means</em>. MelangeDB supplies the
/// mechanism — one writer per shard, one commit log per shard, handoff, interest — and this
/// interface supplies the meaning: how rows and sessions map onto shards, and which foreign
/// shards a shard must observe. The one contract a strategy's tables must uphold: <b>rows mutated
/// in the same transaction must resolve to the same shard</b> (the shard-span debug check fails
/// loudly when they do not; see docs/CLUSTERING.md).
/// </summary>
public interface IShardStrategy
{
    /// <summary>Which shard owns this row?</summary>
    ShardKey ShardForRow(TableId table, in RowRef row);

    /// <summary>Which shard is this session currently attached to?</summary>
    ShardKey ShardForSession(SessionContext session);

    /// <summary>
    /// Which foreign shards must this shard hold read-only slices of? Empty for instancing —
    /// instances are causally disjoint by definition. Spatial strategies (phase 10) return
    /// neighbouring shards here.
    /// </summary>
    IReadOnlyList<ShardKey> InterestOf(ShardKey shard);
}
