using MelangeDB.Core;
using Microsoft.Extensions.Options;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The spatial strategy's pure geometry: key packing, block math, band membership, seam
/// ownership, and the validation that closes the narrow-key migration trap.
/// </summary>
public class SpatialStrategyTests
{
    private static readonly SpatialGeometry Geometry = new()
    {
        BlockWidthChunks = 4,
        BlockHeightChunks = 4,
        DecodeChunk = Chunks.At,
    };

    private static SpatialShardStrategy Strategy(int band = 2, int margin = 1) =>
        new(
            SchemaRegistry.FromTypes(typeof(Terrain), typeof(PlayerPos), typeof(Pack), typeof(Critter)),
            Geometry,
            static _ => SpatialShardStrategy.ShardOfBlock(0, 0),
            new StaticOptionsMonitor(new MelangeDbOptions
            {
                Cluster = { BorderBandChunks = band, HandoffMarginChunks = margin },
            }));

    private static RowRef Row(SpatialShardStrategy _, uint chunkId)
    {
        var schema = SchemaRegistry.FromTypes(typeof(Terrain));
        var table = schema.Get(typeof(Terrain));
        var bytes = RowSerializer.Serialize(table, new Terrain { Id = chunkId, ChunkId = chunkId, Biome = "dune" });
        return table.ToRowRef(bytes);
    }

    private static TableId TerrainId => SchemaRegistry.FromTypes(typeof(Terrain)).Get(typeof(Terrain)).Id;

    [Fact]
    public void Shard_keys_pack_two_full_32_bit_block_coordinates_so_world_growth_never_migrates_keys()
    {
        Assert.Equal((0, 0), SpatialShardStrategy.BlockOf(SpatialShardStrategy.ShardOfBlock(0, 0)));
        Assert.Equal((3, 7), SpatialShardStrategy.BlockOf(SpatialShardStrategy.ShardOfBlock(3, 7)));
        Assert.Equal((-1, -2), SpatialShardStrategy.BlockOf(SpatialShardStrategy.ShardOfBlock(-1, -2)));
        Assert.Equal(
            (int.MaxValue, int.MinValue),
            SpatialShardStrategy.BlockOf(SpatialShardStrategy.ShardOfBlock(int.MaxValue, int.MinValue)));
        Assert.NotEqual(SpatialShardStrategy.ShardOfBlock(1, 0), SpatialShardStrategy.ShardOfBlock(0, 1));
    }

    [Fact]
    public void A_chunk_resolves_to_the_block_containing_it_with_floored_division_for_negatives()
    {
        var strategy = Strategy();
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(0, 0), strategy.ShardOfChunk(new ChunkPosition(0, 0)));
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(0, 0), strategy.ShardOfChunk(new ChunkPosition(3, 3)));
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(1, 0), strategy.ShardOfChunk(new ChunkPosition(4, 2)));
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(2, 1), strategy.ShardOfChunk(new ChunkPosition(11, 7)));
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(-1, 0), strategy.ShardOfChunk(new ChunkPosition(-1, 0)));
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(-1, -1), strategy.ShardOfChunk(new ChunkPosition(-4, -4)));
    }

    [Fact]
    public void Interest_is_exactly_the_eight_neighbouring_blocks()
    {
        var strategy = Strategy();
        var interest = strategy.InterestOf(SpatialShardStrategy.ShardOfBlock(2, 2));
        Assert.Equal(8, interest.Count);
        Assert.Equal(8, interest.Distinct().Count());
        Assert.DoesNotContain(SpatialShardStrategy.ShardOfBlock(2, 2), interest);
        for (var dx = -1; dx <= 1; dx++)
        {
            for (var dy = -1; dy <= 1; dy++)
            {
                if (dx != 0 || dy != 0)
                    Assert.Contains(SpatialShardStrategy.ShardOfBlock(2 + dx, 2 + dy), interest);
            }
        }
    }

    [Fact]
    public void The_border_slice_of_a_neighbour_is_the_rows_within_band_depth_of_the_observer()
    {
        var strategy = Strategy(band: 2);
        var owner = SpatialShardStrategy.ShardOfBlock(1, 0); // Chunks x 4..7.
        var observer = SpatialShardStrategy.ShardOfBlock(0, 0); // Chunks x 0..3.

        Assert.True(strategy.InterestedInRow(owner, observer, TerrainId, Row(strategy, Chunks.Id(4, 2))));
        Assert.True(strategy.InterestedInRow(owner, observer, TerrainId, Row(strategy, Chunks.Id(5, 2))));
        Assert.False(strategy.InterestedInRow(owner, observer, TerrainId, Row(strategy, Chunks.Id(6, 2))));

        // A row that does not resolve to the claimed owner is never part of the owner's slice.
        Assert.False(strategy.InterestedInRow(owner, observer, TerrainId, Row(strategy, Chunks.Id(2, 2))));
    }

    [Fact]
    public void The_owner_may_commit_a_row_up_to_the_band_depth_across_the_line_and_no_further()
    {
        var strategy = Strategy(band: 2);
        var home = SpatialShardStrategy.ShardOfBlock(0, 0);

        Assert.True(strategy.MayCommit(home, TerrainId, Row(strategy, Chunks.Id(2, 2))));
        Assert.True(strategy.MayCommit(home, TerrainId, Row(strategy, Chunks.Id(4, 2)))); // Depth 1: mid-handoff seam.
        Assert.True(strategy.MayCommit(home, TerrainId, Row(strategy, Chunks.Id(5, 2)))); // Depth 2: still in band.
        Assert.False(strategy.MayCommit(home, TerrainId, Row(strategy, Chunks.Id(6, 2)))); // Depth 3: overdue handoff, loud.
    }

    [Fact]
    public void Assess_reports_approaches_within_the_band_and_crossings_with_their_depth()
    {
        // Band 1 in a 4x4 block leaves genuinely interior chunks; band 2 would not, since every
        // chunk of a 4-wide block is within two of some edge — the geometry, not a bug.
        var strategy = Strategy(band: 1, margin: 0);
        var home = SpatialShardStrategy.ShardOfBlock(1, 1); // Chunks 4..7 x 4..7.

        var interior = strategy.Assess(home, TerrainId, Row(strategy, Chunks.Id(6, 6)));
        Assert.Null(interior.CrossedInto);
        Assert.Empty(interior.Approaching);

        var nearEdge = strategy.Assess(home, TerrainId, Row(strategy, Chunks.Id(7, 6)));
        Assert.Null(nearEdge.CrossedInto);
        Assert.Contains(SpatialShardStrategy.ShardOfBlock(2, 1), nearEdge.Approaching);
        Assert.DoesNotContain(SpatialShardStrategy.ShardOfBlock(0, 1), nearEdge.Approaching);

        var corner = strategy.Assess(home, TerrainId, Row(strategy, Chunks.Id(7, 7)));
        Assert.Equal(3, corner.Approaching.Count); // East, south, and the corner block.

        var crossed = strategy.Assess(home, TerrainId, Row(strategy, Chunks.Id(9, 6)));
        Assert.Equal(SpatialShardStrategy.ShardOfBlock(2, 1), crossed.CrossedInto);
        Assert.Equal(2, crossed.CrossedDepthChunks);
    }

    [Fact]
    public void Spatial_tables_re_home_by_content_and_instancing_tables_by_rewrite()
    {
        Assert.Equal(RowRehoming.ByContent, Strategy().RehomingOf(TerrainId));
        IShardStrategy instancing = new InstancingShardStrategy(
            SchemaRegistry.FromTypes(typeof(Mob)), static _ => new ShardKey(1));
        Assert.Equal(RowRehoming.RewriteShardBy, instancing.RehomingOf(TerrainId));
    }

    [Fact]
    public void A_chunk_id_column_narrower_than_32_bits_is_refused_naming_the_overflow_trap()
    {
        var failure = Assert.Throws<InvalidOperationException>(() => new SpatialShardStrategy(
            NarrowChunkSchema(),
            Geometry,
            static _ => default,
            new StaticOptionsMonitor(new MelangeDbOptions())));
        Assert.Contains("narrower than 32 bits", failure.Message);
        Assert.Contains("65,535", failure.Message);
    }

    /// <summary>A hand-built schema with a ushort chunk column — the exact reference-workload trap.</summary>
    private static SchemaRegistry NarrowChunkSchema() =>
        new([
            new TableSchema(
                typeof(NarrowChunkRow),
                "NarrowChunkRow",
                [
                    new ColumnSchema
                    {
                        Name = "Id",
                        ClrType = typeof(ulong),
                        Kind = ColumnKind.UInt64,
                        IsPrimaryKey = true,
                        GetValue = static row => ((NarrowChunkRow)row).Id,
                        SetValue = static (_, _) => { },
                    },
                    new ColumnSchema
                    {
                        Name = "ChunkId",
                        ClrType = typeof(ushort),
                        Kind = ColumnKind.UInt16,
                        GetValue = static row => ((NarrowChunkRow)row).ChunkId,
                        SetValue = static (_, _) => { },
                    },
                ],
                placement: Placement.Partitioned,
                shardBy: "ChunkId"),
        ]);

    [Fact]
    public void Tunables_are_validated_loudly_at_construction()
    {
        var margin = Assert.Throws<InvalidOperationException>(() => Strategy(band: 2, margin: 2));
        Assert.Contains("HandoffMarginChunks", margin.Message);
        Assert.Contains("smaller than", margin.Message);

        var band = Assert.Throws<InvalidOperationException>(() => Strategy(band: 0, margin: 0));
        Assert.Contains("BorderBandChunks", band.Message);

        var deep = Assert.Throws<InvalidOperationException>(() => Strategy(band: 5, margin: 1));
        Assert.Contains("exceeds the block size", deep.Message);
    }

    internal struct NarrowChunkRow
    {
        public ulong Id { get; init; }

        public ushort ChunkId { get; init; }
    }

    /// <summary>A fixed options monitor for pure strategy tests.</summary>
    internal sealed class StaticOptionsMonitor(MelangeDbOptions value) : IOptionsMonitor<MelangeDbOptions>
    {
        public MelangeDbOptions CurrentValue { get; } = value;

        public MelangeDbOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<MelangeDbOptions, string?> listener) => null;
    }
}
