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
/// How handoff re-homes a transferred row on the destination shard. Instancing rewrites the
/// explicit shard-id column; a spatial strategy's rows already carry their location, so the
/// content <em>is</em> the shard and rewriting anything would corrupt it.
/// </summary>
public enum RowRehoming
{
    /// <summary>
    /// The import rewrites the row's <c>ShardBy</c> column to the destination shard's key — the
    /// instancing shape, where the shard id is an explicit column with no other meaning.
    /// </summary>
    RewriteShardBy,

    /// <summary>
    /// The row's content (a chunk id, a position) already resolves it to a shard, so the import
    /// leaves the bytes untouched and instead <em>asserts</em> that
    /// <see cref="IShardStrategy.ShardForRow"/> answers the destination — a transferred row that
    /// still resolves elsewhere is a protocol error, and failing loudly beats silently re-homing
    /// a row whose position contradicts its owner.
    /// </summary>
    ByContent,
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
    /// instances are causally disjoint by definition. Spatial strategies return the neighbouring
    /// blocks here, and <see cref="InterestedInRow"/> narrows each to its border band.
    /// </summary>
    IReadOnlyList<ShardKey> InterestOf(ShardKey shard);

    /// <summary>
    /// May <paramref name="shard"/>'s writer commit this row? The default is the strict contract —
    /// only the row's own shard — which is exactly right for instancing. A spatial strategy widens
    /// it at the seam: an entity the origin still owns may stand a border band's depth inside a
    /// neighbouring block while its handoff is pending, and refusing the write there would freeze
    /// the world at every boundary line.
    /// </summary>
    bool MayCommit(ShardKey shard, TableId table, in RowRef row) => ShardForRow(table, row) == shard;

    /// <summary>How handoff re-homes this table's rows; see <see cref="RowRehoming"/>.</summary>
    RowRehoming RehomingOf(TableId table) => RowRehoming.RewriteShardBy;

    /// <summary>
    /// Whether <paramref name="observer"/>'s read-only slice of <paramref name="owner"/> includes
    /// this row. The default — everything — suits strategies whose interest is all-or-nothing; a
    /// spatial strategy narrows it to the border band, which is the whole point of a band: the
    /// observer holds the edge it can see across, not the neighbour's world.
    /// </summary>
    bool InterestedInRow(ShardKey owner, ShardKey observer, TableId table, in RowRef row) => true;
}
