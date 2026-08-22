using System.Linq;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Issue #137, the write that PRODUCED the unreadable row — not the forced-corruption path
/// UnreadableBlobRowTests uses. The reference workload's row went bad through the ordinary public
/// API: a paged blob written large (v2, 339 bytes) and then UPDATED smaller (v3, 315 bytes) for the
/// same key. On the live store the later read returned 339 against a main record declaring 315 —
/// a shrinking overwrite that did not truncate the out-of-line payload.
///
/// These two tests answer the two operational questions that decision hung on:
///   1. Does a shrink-in-place through Insert-then-Update reproduce the disagreement live?
///   2. Does a restart (log wiped deleteOnClose, replay re-splits each committed row) heal it?
/// </summary>
public class BlobShrinkRepro137Tests
{
    // 39 and 42 records of the reference workload's 8-byte record + 3-byte header: the exact bytes.
    private const int V2 = 3 + 42 * 8; // 339
    private const int V3 = 3 + 39 * 8; // 315

    [Fact]
    public void A_paged_blob_updated_smaller_reads_back_at_the_new_size()
    {
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);

        harness.Invoke("v2", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 606596, Region = 1, Data = StoreContractTests.MakeBlob(2, V2) }));
        harness.Invoke("v3", ctx =>
        {
            var blob = ctx.Db.Find<TerrainBlob>(606596L)!.Value;
            ctx.Db.Update(blob with { Data = StoreContractTests.MakeBlob(3, V3) });
        });

        // The row must be readable, present in a scan, and exactly the v3 bytes.
        var scanned = harness.Engine.HotStore.Scan(
            harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob)).Id).ToList();
        Assert.Single(scanned);
        var row = harness.Engine.CommittedView.Find<TerrainBlob>(606596L);
        Assert.NotNull(row);
        Assert.Equal(V3, row!.Value.Data.Length);
        Assert.Equal(StoreContractTests.MakeBlob(3, V3), row.Value.Data);
    }

    [Fact]
    public void The_shrink_survives_a_restart()
    {
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        harness.Invoke("v2", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 606596, Region = 1, Data = StoreContractTests.MakeBlob(2, V2) }));
        harness.Invoke("v3", ctx =>
        {
            var blob = ctx.Db.Find<TerrainBlob>(606596L)!.Value;
            ctx.Db.Update(blob with { Data = StoreContractTests.MakeBlob(3, V3) });
        });

        harness.Restart();

        var row = harness.Engine.CommittedView.Find<TerrainBlob>(606596L);
        Assert.NotNull(row);
        Assert.Equal(StoreContractTests.MakeBlob(3, V3), row!.Value.Data);
        Assert.Single(harness.Engine.HotStore.Scan(
            harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob)).Id));
    }
}
