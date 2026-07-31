using MelangeDB.Core;

namespace MelangeDB.Cluster;

/// <summary>
/// The instancing strategy MelangeDB ships (docs/CLUSTERING.md, strategy B): the shard key is an
/// explicit instance-id column — each Partitioned table names it via <c>ShardBy</c> — instances
/// are causally disjoint, and <see cref="InterestOf"/> is empty by definition. What instance a
/// session is in is the application's knowledge (a hub table, typically), supplied as the
/// session locator.
/// </summary>
public sealed class InstancingShardStrategy : IShardStrategy
{
    private readonly Dictionary<TableId, string> _shardByColumn = [];
    private readonly Func<SessionContext, ShardKey> _sessionLocator;

    public InstancingShardStrategy(SchemaRegistry schema, Func<SessionContext, ShardKey> sessionLocator)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(sessionLocator);
        _sessionLocator = sessionLocator;
        foreach (var table in schema.Tables)
        {
            if (table.Placement != Placement.Partitioned)
                continue;
            if (table.ShardBy is null)
            {
                throw new InvalidOperationException(
                    $"Table '{table.Name}' is Placement.Partitioned but declares no ShardBy column; the instancing " +
                    "strategy derives the shard key from an explicit instance-id column — declare " +
                    "[Table(ShardBy = nameof(...))] naming an integer column.");
            }

            var column = table.Column(table.ShardBy);
            if (column.Kind is not (ColumnKind.UInt8 or ColumnKind.UInt16 or ColumnKind.UInt32 or ColumnKind.UInt64
                or ColumnKind.Int8 or ColumnKind.Int16 or ColumnKind.Int32 or ColumnKind.Int64))
            {
                throw new InvalidOperationException(
                    $"Table '{table.Name}': ShardBy column '{column.Name}' is {column.Kind}; the instancing strategy " +
                    "requires an integer instance-id column.");
            }

            _shardByColumn[table.Id] = table.ShardBy;
        }
    }

    public ShardKey ShardForRow(TableId table, in RowRef row)
    {
        if (!_shardByColumn.TryGetValue(table, out var column))
            throw new ArgumentException($"Table {table} is not a Partitioned table this strategy knows.", nameof(table));
        var value = row.Column(column)
            ?? throw new InvalidOperationException($"ShardBy column '{column}' read null.");
        return new ShardKey(Convert.ToUInt64(value, System.Globalization.CultureInfo.InvariantCulture));
    }

    public ShardKey ShardForSession(SessionContext session) => _sessionLocator(session);

    /// <summary>Instances are causally disjoint: no instance ever observes another.</summary>
    public IReadOnlyList<ShardKey> InterestOf(ShardKey shard) => [];
}

/// <summary>
/// Collects the rows that travel with a player during handoff. MelangeDB supplies the transfer
/// mechanism; which rows <em>are</em> the player's is the application's knowledge, exactly like
/// the shard strategy itself.
/// </summary>
public interface IHandoffCollector
{
    /// <summary>Adds one row (of a Partitioned table) to the transfer set.</summary>
    void Add<TRow>(TRow row)
        where TRow : struct;
}

/// <summary>
/// Selects the rows that follow a player between shards — the "player-owned tables share the
/// player's shard key" convention made concrete. Called on the origin shard's engine, after the
/// player is frozen, with a read-only view of that shard.
/// </summary>
public interface IHandoffSet
{
    /// <summary>Adds every row that must move with <paramref name="player"/> out of <paramref name="shard"/>.</summary>
    void Collect(Identity player, ShardKey shard, IDbView shardDb, IHandoffCollector rows);
}
