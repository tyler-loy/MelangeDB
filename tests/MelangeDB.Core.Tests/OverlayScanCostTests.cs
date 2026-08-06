using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// What the overlay's read paths actually touch. Correctness of the merged view is covered
/// elsewhere; these pin its <em>cost</em>, because both defects they guard were invisible to a
/// result-shape assertion — the answers were right, the work to reach them was not.
/// <para>
/// Chunk ids are seeded even, so an odd id is always a key the store does not hold and a pending
/// insert can be placed at any point in key order.
/// </para>
/// </summary>
public class OverlayScanCostTests
{
    private const int TableSize = 500;

    [Fact]
    public void A_primary_key_range_reads_only_the_rows_inside_the_window()
    {
        using var harness = Seeded();

        var before = RowsScanned(harness);
        harness.Invoke("Window", ctx =>
        {
            var window = ctx.Db.FilterRange<TerrainChunk>(nameof(TerrainChunk.ChunkId), 960L, 978L).ToList();
            Assert.Equal(10, window.Count);
            Assert.Equal(960L, window[0].ChunkId);
        });

        // The keys are ordered, so the 480 rows below the window were never candidates. Reading
        // them to throw them away is nearly free on this store and pages in most of the table on a
        // paged one — which is the failure walking the key directory exists to avoid.
        Assert.Equal(0, RowsScanned(harness) - before);
    }

    [Fact]
    public void A_primary_key_range_still_sees_the_transaction_s_own_writes()
    {
        // The cheap path must not become a stale path: a window read after a write in the same
        // transaction resolves the overlay, including a pending insert that lands mid-range, a
        // pending delete of a stored row, and a pending update of another.
        using var harness = Seeded();

        harness.Invoke("WindowOverPending", ctx =>
        {
            ctx.Db.Insert(new TerrainChunk { ChunkId = 967, Data = [9], Kind = ChunkKind.Ore });
            ctx.Db.Delete<TerrainChunk>(968L);
            ctx.Db.Update(new TerrainChunk { ChunkId = 970, Data = [7], Kind = ChunkKind.Rock });

            var window = ctx.Db.FilterRange<TerrainChunk>(nameof(TerrainChunk.ChunkId), 964L, 972L).ToList();
            Assert.Equal([964L, 966L, 967L, 970L, 972L], window.Select(c => c.ChunkId));
            Assert.Equal(ChunkKind.Ore, window.Single(c => c.ChunkId == 967).Kind);
            Assert.Equal(ChunkKind.Rock, window.Single(c => c.ChunkId == 970).Kind);
        });
    }

    [Fact]
    public void Pending_writes_outside_the_window_stay_outside_it()
    {
        using var harness = Seeded();

        harness.Invoke("WindowExcludesPending", ctx =>
        {
            ctx.Db.Insert(new TerrainChunk { ChunkId = 1801, Data = [9], Kind = ChunkKind.Ore });
            ctx.Db.Insert(new TerrainChunk { ChunkId = 201, Data = [9], Kind = ChunkKind.Ore });

            var window = ctx.Db.FilterRange<TerrainChunk>(nameof(TerrainChunk.ChunkId), 960L, 964L).ToList();
            Assert.Equal([960L, 962L, 964L], window.Select(c => c.ChunkId));
        });
    }

    [Fact]
    public void Taking_the_first_row_after_a_write_does_not_read_the_whole_table()
    {
        using var harness = Seeded();

        var before = RowsScanned(harness);
        harness.Invoke("FirstAfterWrite", ctx =>
        {
            // A staged op used to switch the merged scan from lazy to a full materialize of the
            // table into a SortedDictionary, so inserting one row and then asking for one row read
            // five hundred.
            ctx.Db.Insert(new TerrainChunk { ChunkId = 20_001, Data = [1], Kind = ChunkKind.Empty });
            Assert.Equal(0L, ctx.Db.First<TerrainChunk>()!.Value.ChunkId);
        });

        var scanned = RowsScanned(harness) - before;
        Assert.True(scanned < TableSize, $"the merged scan read {scanned} rows of a {TableSize}-row table to return its first");
    }

    [Fact]
    public void A_pending_insert_that_sorts_first_wins_the_first_row()
    {
        using var harness = Seeded();

        harness.Invoke("FirstIsPending", ctx =>
        {
            ctx.Db.Insert(new TerrainChunk { ChunkId = -1, Data = [1], Kind = ChunkKind.Ore });
            Assert.Equal(-1L, ctx.Db.First<TerrainChunk>()!.Value.ChunkId);
        });
    }

    [Fact]
    public void A_pending_delete_of_the_first_row_moves_first_to_the_next_one()
    {
        using var harness = Seeded();

        harness.Invoke("FirstIsDeleted", ctx =>
        {
            ctx.Db.Delete<TerrainChunk>(0L);
            Assert.Equal(2L, ctx.Db.First<TerrainChunk>()!.Value.ChunkId);
        });
    }

    [Fact]
    public void A_full_scan_interleaves_stored_and_pending_rows_in_key_order()
    {
        using var harness = Seeded();

        harness.Invoke("ScanOrder", ctx =>
        {
            ctx.Db.Insert(new TerrainChunk { ChunkId = -5, Data = [1], Kind = ChunkKind.Ore });
            ctx.Db.Insert(new TerrainChunk { ChunkId = 501, Data = [1], Kind = ChunkKind.Ore });
            ctx.Db.Insert(new TerrainChunk { ChunkId = 20_001, Data = [1], Kind = ChunkKind.Ore });
            ctx.Db.Delete<TerrainChunk>(6L);

            var ids = ctx.Db.Scan<TerrainChunk>().Select(c => c.ChunkId).ToList();
            Assert.Equal(ids.OrderBy(id => id), ids);
            Assert.Equal(TableSize + 2, ids.Count);
            Assert.Equal(-5L, ids[0]);
            Assert.Equal(20_001L, ids[^1]);
            Assert.DoesNotContain(6L, ids);
            Assert.Single(ids, id => id == 501L);
        });
    }

    private static EngineHarness Seeded()
    {
        var harness = new EngineHarness(tables: [typeof(TerrainChunk)]);
        harness.Invoke("Seed", ctx =>
        {
            for (long id = 0; id < TableSize; id++)
                ctx.Db.Insert(new TerrainChunk { ChunkId = id * 2, Data = [1], Kind = ChunkKind.Empty });
        });

        return harness;
    }

    private static long RowsScanned(EngineHarness harness) =>
        harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == nameof(TerrainChunk)).RowsScanned;
}
