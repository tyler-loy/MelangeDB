using MelangeDB.Core;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>A chunk's coordinates in the world grid. Negative coordinates are valid.</summary>
public readonly record struct ChunkPosition(int Cx, int Cy);

/// <summary>
/// The world's shape, as the developer declares it — chunk encoding and block size are the game's
/// knowledge, not MelangeDB's. Band depth and hysteresis are tunables and live in configuration
/// (<c>Cluster:BorderBandChunks</c>, <c>Cluster:HandoffMarginChunks</c>), not here.
/// </summary>
public sealed record SpatialGeometry
{
    /// <summary>Width of one shard block, in chunks.</summary>
    public required int BlockWidthChunks { get; init; }

    /// <summary>Height of one shard block, in chunks.</summary>
    public required int BlockHeightChunks { get; init; }

    /// <summary>
    /// Decodes a chunk-id column value into chunk coordinates. The developer owns the encoding —
    /// <c>cx * worldWidth + cy</c>, bit packing, whatever the game already uses — and this is the
    /// one place MelangeDB learns it.
    /// </summary>
    public required Func<ulong, ChunkPosition> DecodeChunk { get; init; }
}

/// <summary>
/// A strategy that can judge a row's position against its shard's boundary — what the boundary
/// monitor consumes to detect approaches and crossings. Spatial strategies implement it;
/// instancing has no boundaries and does not.
/// </summary>
public interface IBoundaryStrategy
{
    /// <summary>Assesses one row of <paramref name="owner"/> against the owner's boundary.</summary>
    BoundaryAssessment Assess(ShardKey owner, TableId table, in RowRef row);
}

/// <summary>
/// One row's relation to its owning shard's boundary. <see cref="CrossedInto"/> is non-null when
/// the row's position resolves to a foreign shard (with how deep, in chunks, it has penetrated);
/// <see cref="Approaching"/> lists neighbouring shards whose boundary lies within the border band
/// — the signal to pre-open sessions and expect a transfer.
/// </summary>
public readonly record struct BoundaryAssessment(
    ShardKey? CrossedInto,
    int CrossedDepthChunks,
    IReadOnlyList<ShardKey> Approaching)
{
    public static readonly BoundaryAssessment Interior = new(null, 0, []);
}

/// <summary>
/// The spatial strategy (docs/CLUSTERING.md, strategy A): a contiguous world of chunks, a shard is
/// a rectangular block of them, and the shard key is the block's coordinates packed into a
/// <see cref="ShardKey"/> — two full 32-bit halves, so a world can grow without ever migrating key
/// encodings. Each Partitioned table names its chunk-id column via <c>ShardBy</c>; the column must
/// be at least 32 bits wide, because the reference workload's <c>cx * 157 + cy</c> in a
/// <c>ushort</c> tops out at 65,535 and a 20 km world overflows it — the exact migration trap the
/// width requirement exists to close. Interest is the eight neighbouring blocks, narrowed per row
/// to the border band; ownership at the seam is widened by the band depth so an entity mid-handoff
/// can stand just across the line without freezing the boundary.
/// </summary>
public sealed class SpatialShardStrategy : IShardStrategy, IBoundaryStrategy
{
    private readonly Dictionary<TableId, string> _chunkColumn = [];
    private readonly SpatialGeometry _geometry;
    private readonly Func<SessionContext, ShardKey> _sessionLocator;
    private readonly Func<ClusterOptions> _cluster;

    public SpatialShardStrategy(
        SchemaRegistry schema,
        SpatialGeometry geometry,
        Func<SessionContext, ShardKey> sessionLocator,
        IOptionsMonitor<MelangeDbOptions> options)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(geometry);
        ArgumentNullException.ThrowIfNull(sessionLocator);
        ArgumentNullException.ThrowIfNull(options);
        if (geometry.BlockWidthChunks < 1 || geometry.BlockHeightChunks < 1)
        {
            throw new ArgumentException(
                $"SpatialGeometry blocks must be at least 1x1 chunks; got {geometry.BlockWidthChunks}x{geometry.BlockHeightChunks}.",
                nameof(geometry));
        }

        _geometry = geometry;
        _sessionLocator = sessionLocator;
        _cluster = () => options.CurrentValue.Cluster;
        ValidateTunables(options.CurrentValue.Cluster, geometry);

        foreach (var table in schema.Tables)
        {
            if (table.Placement != Placement.Partitioned)
                continue;
            if (table.ShardBy is null)
            {
                throw new InvalidOperationException(
                    $"Table '{table.Name}' is Placement.Partitioned but declares no ShardBy column; the spatial " +
                    "strategy derives the shard from a chunk-id column — declare [Table(ShardBy = nameof(...))] " +
                    "naming the column that carries the row's chunk id.");
            }

            var column = table.Column(table.ShardBy);
            if (column.Kind is not (ColumnKind.UInt32 or ColumnKind.UInt64 or ColumnKind.Int32 or ColumnKind.Int64))
            {
                throw new InvalidOperationException(
                    $"Table '{table.Name}': ShardBy column '{column.Name}' is {column.Kind}, which is narrower than " +
                    "32 bits. A chunk-id key that fits today's world overflows when the world grows (cx * 157 + cy " +
                    "in a ushort tops out at 65,535 — a 20 km world overflows it), and widening the column later " +
                    "means migrating every row. Declare the chunk id as uint or ulong from the start.");
            }

            _chunkColumn[table.Id] = table.ShardBy;
        }
    }

    /// <summary>
    /// Validates the spatial tunables loudly at construction; live reads clamp defensively
    /// instead, because a reload must degrade a running node's behavior, never crash it.
    /// </summary>
    private static void ValidateTunables(ClusterOptions cluster, SpatialGeometry geometry)
    {
        if (cluster.BorderBandChunks < 1)
        {
            throw new InvalidOperationException(
                $"Cluster:BorderBandChunks is {cluster.BorderBandChunks}; the band must be at least one chunk deep — " +
                "a zero-depth band means no read-only overlap and no seamless handoff.");
        }

        if (cluster.HandoffMarginChunks < 0)
            throw new InvalidOperationException($"Cluster:HandoffMarginChunks is {cluster.HandoffMarginChunks}; it cannot be negative.");
        if (cluster.HandoffMarginChunks >= cluster.BorderBandChunks)
        {
            throw new InvalidOperationException(
                $"Cluster:HandoffMarginChunks ({cluster.HandoffMarginChunks}) must be smaller than " +
                $"Cluster:BorderBandChunks ({cluster.BorderBandChunks}): an entity is still writable on its origin " +
                "while it stands up to the margin past the boundary, so the band — which is also the ownership slack " +
                "at the seam — must reach beyond the margin or the entity would be unwritable before its handoff " +
                "could trigger.");
        }

        if (cluster.BorderBandChunks > Math.Min(geometry.BlockWidthChunks, geometry.BlockHeightChunks))
        {
            throw new InvalidOperationException(
                $"Cluster:BorderBandChunks ({cluster.BorderBandChunks}) exceeds the block size " +
                $"({geometry.BlockWidthChunks}x{geometry.BlockHeightChunks} chunks); a band deeper than a block would " +
                "need interest in neighbours-of-neighbours, which the eight-neighbour interest model does not " +
                "provide. Deepen the blocks or shallow the band.");
        }

        if (cluster.HandoffMinIntervalMs < 0)
            throw new InvalidOperationException($"Cluster:HandoffMinIntervalMs is {cluster.HandoffMinIntervalMs}; it cannot be negative.");
    }

    /// <summary>The band depth, clamped for live reads; see <see cref="ValidateTunables"/>.</summary>
    internal int BandChunks => Math.Max(1, _cluster().BorderBandChunks);

    /// <summary>The hysteresis margin, clamped to stay below the band for live reads.</summary>
    internal int MarginChunks => Math.Clamp(_cluster().HandoffMarginChunks, 0, BandChunks - 1);

    /// <summary>Packs block coordinates into a shard key: two full 32-bit halves, negatives included.</summary>
    public static ShardKey ShardOfBlock(int bx, int by) => new(((ulong)(uint)bx << 32) | (uint)by);

    /// <summary>Unpacks a shard key into block coordinates.</summary>
    public static (int Bx, int By) BlockOf(ShardKey shard) => ((int)(uint)(shard.Value >> 32), (int)(uint)shard.Value);

    /// <summary>The shard owning the block that contains this chunk.</summary>
    public ShardKey ShardOfChunk(ChunkPosition chunk) =>
        ShardOfBlock(FloorDiv(chunk.Cx, _geometry.BlockWidthChunks), FloorDiv(chunk.Cy, _geometry.BlockHeightChunks));

    public ShardKey ShardForRow(TableId table, in RowRef row) => ShardOfChunk(ChunkOf(table, row));

    public ShardKey ShardForSession(SessionContext session) => _sessionLocator(session);

    /// <summary>The eight neighbouring blocks — the shards this shard holds border bands of.</summary>
    public IReadOnlyList<ShardKey> InterestOf(ShardKey shard)
    {
        var (bx, by) = BlockOf(shard);
        var neighbours = new ShardKey[8];
        var i = 0;
        for (var dx = -1; dx <= 1; dx++)
        {
            for (var dy = -1; dy <= 1; dy++)
            {
                if (dx != 0 || dy != 0)
                    neighbours[i++] = ShardOfBlock(bx + dx, by + dy);
            }
        }

        return neighbours;
    }

    /// <summary>
    /// The strict contract, widened at the seam: the owner may also commit a row standing up to
    /// the band depth inside a neighbouring block — the entity it still owns whose handoff has not
    /// completed yet. Beyond the band the write fails loudly, because an entity that deep into
    /// foreign territory means handoffs are not keeping up and the band was sized too shallow.
    /// </summary>
    public bool MayCommit(ShardKey shard, TableId table, in RowRef row)
    {
        var chunk = ChunkOf(table, row);
        if (ShardOfChunk(chunk) == shard)
            return true;
        return DistanceToBlock(chunk, shard) <= BandChunks;
    }

    /// <summary>Spatial rows carry their location; the content is the shard. See <see cref="RowRehoming.ByContent"/>.</summary>
    public RowRehoming RehomingOf(TableId table) => RowRehoming.ByContent;

    /// <summary>The observer's slice of a neighbour: rows within the band depth of the observer's own block.</summary>
    public bool InterestedInRow(ShardKey owner, ShardKey observer, TableId table, in RowRef row)
    {
        var chunk = ChunkOf(table, row);
        return ShardOfChunk(chunk) == owner && DistanceToBlock(chunk, observer) <= BandChunks;
    }

    public BoundaryAssessment Assess(ShardKey owner, TableId table, in RowRef row)
    {
        var chunk = ChunkOf(table, row);
        var home = ShardOfChunk(chunk);
        if (home != owner)
            return new BoundaryAssessment(home, DistanceToBlock(chunk, owner), []);

        List<ShardKey>? approaching = null;
        var band = BandChunks;
        foreach (var neighbour in InterestOf(owner))
        {
            if (DistanceToBlock(chunk, neighbour) <= band)
                (approaching ??= []).Add(neighbour);
        }

        return approaching is null ? BoundaryAssessment.Interior : new BoundaryAssessment(null, 0, approaching);
    }

    private ChunkPosition ChunkOf(TableId table, in RowRef row)
    {
        if (!_chunkColumn.TryGetValue(table, out var column))
            throw new ArgumentException($"Table {table} is not a Partitioned table this strategy knows.", nameof(table));
        var value = row.Column(column)
            ?? throw new InvalidOperationException($"Chunk-id column '{column}' read null.");
        var raw = Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
        if (raw < 0)
            throw new InvalidOperationException($"Chunk-id column '{column}' read {raw}; chunk ids cannot be negative.");
        return _geometry.DecodeChunk((ulong)raw);
    }

    /// <summary>Chebyshev distance from a chunk to a block's rectangle; 0 when the chunk is inside.</summary>
    internal int DistanceToBlock(ChunkPosition chunk, ShardKey block)
    {
        var (bx, by) = BlockOf(block);
        var dx = DistanceToRange(chunk.Cx, bx * _geometry.BlockWidthChunks, _geometry.BlockWidthChunks);
        var dy = DistanceToRange(chunk.Cy, by * _geometry.BlockHeightChunks, _geometry.BlockHeightChunks);
        return Math.Max(dx, dy);
    }

    private static int DistanceToRange(int value, int low, int length)
    {
        var high = low + length - 1;
        if (value < low)
            return low - value;
        return value > high ? value - high : 0;
    }

    private static int FloorDiv(int value, int divisor)
    {
        var quotient = value / divisor;
        return value < 0 && value % divisor != 0 ? quotient - 1 : quotient;
    }
}
