using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Paged scans read their rows a block at a time, with each block's FASTER reads in flight
/// together, rather than one synchronous disk read per row (#212). The block boundary is new
/// surface: a row at the edge of a block, the short last block, a read completed from disk rather
/// than memory, a row overwritten after a view pinned it. These run with far more rows than the
/// harness's 8 MiB store budget holds, so most reads go to disk and complete as pending reads.
/// </summary>
public class BatchedScanTests
{
    // Not a multiple of the block size, so the scan ends on a short block.
    private const int Rows = 30_007;

    private static StoreHarness Seeded()
    {
        var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        var rows = new List<BulkRow>(Rows);
        for (var i = 0; i < Rows; i++)
        {
            rows.Add(new BulkRow("TerrainBlob", new Dictionary<string, object?>
            {
                ["ChunkId"] = (long)i,
                ["Region"] = i % 16,
                ["Data"] = StoreContractTests.MakeBlob(i, 256), // At the inline threshold: out of line.
            }));
        }

        harness.Engine.BulkInsert(StoreHarness.Caller, rows);
        return harness;
    }

    private static TerrainBlob Decode(StoreHarness harness, ReadOnlyMemory<byte> row) =>
        (TerrainBlob)RowSerializer.Deserialize(harness.Engine.Schema.Get(typeof(TerrainBlob)), row);

    [Fact]
    public void A_paged_scan_returns_every_row_once_in_key_order_with_its_payload()
    {
        using var harness = Seeded();
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        var faultsBefore = harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "TerrainBlob").PageFaults;

        var expected = 0L;
        foreach (var (_, bytes) in harness.Engine.HotStore.Scan(table))
        {
            var row = Decode(harness, bytes);
            Assert.Equal(expected, row.ChunkId);
            Assert.Equal((int)(expected % 16), row.Region);
            Assert.Equal(StoreContractTests.MakeBlob((int)expected, 256), row.Data);
            expected++;
        }

        Assert.Equal(Rows, expected);
        var faultsAfter = harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "TerrainBlob").PageFaults;
        Assert.True(faultsAfter > faultsBefore, "the scan must have gone to disk, or the pending-read path went untested");
    }

    [Fact]
    public void An_index_scan_across_blocks_returns_exactly_the_matching_rows()
    {
        using var harness = Seeded();
        var schema = harness.Engine.Schema.Get(typeof(TerrainBlob));
        var region = SchemaKeyCodec.Encode(schema.Column("Region"), 5);

        var chunks = harness.Engine.HotStore.ScanIndex(schema.Id, "Region", region)
            .Select(pair => Decode(harness, pair.Value))
            .Select(row =>
            {
                Assert.Equal(5, row.Region);
                Assert.Equal(StoreContractTests.MakeBlob((int)row.ChunkId, 256), row.Data);
                return row.ChunkId;
            })
            .Order()
            .ToList();

        Assert.Equal(Enumerable.Range(0, Rows).Where(static i => i % 16 == 5).Select(static i => (long)i), chunks);
    }

    [Fact]
    public void A_view_scan_across_blocks_reads_every_row_at_its_pinned_value()
    {
        using var harness = Seeded();
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        using var view = ((IReadViewSource)harness.Engine.HotStore).OpenReadView();

        // After the pin: overwrite rows on both sides of several block edges, delete one, add one.
        // The view must see none of it.
        harness.Invoke("rewrite", ctx =>
        {
            foreach (var chunk in new long[] { 0, 127, 128, 255, 256, 20_000, Rows - 1 })
            {
                var row = ctx.Db.Find<TerrainBlob>(chunk)!.Value;
                ctx.Db.Update(row with { Data = StoreContractTests.MakeBlob(9_999_999, 300) });
            }

            Assert.True(ctx.Db.Delete<TerrainBlob>(129L));
            ctx.Db.Insert(new TerrainBlob { ChunkId = Rows + 10, Region = 0, Data = StoreContractTests.MakeBlob(1, 256) });
        });

        var expected = 0L;
        foreach (var (_, bytes) in view.Scan(table))
        {
            var row = Decode(harness, bytes);
            Assert.Equal(expected, row.ChunkId);
            Assert.Equal(StoreContractTests.MakeBlob((int)expected, 256), row.Data);
            expected++;
        }

        Assert.Equal(Rows, expected);
    }
}
