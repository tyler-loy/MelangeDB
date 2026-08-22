using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Issue #137: one paged row whose out-of-line payload disagreed with its main record's declared
/// length threw <see cref="InvalidDataException"/> out of a subscription's initial-set scan,
/// escaped the connection handler, and killed every client whose first subscription scanned that
/// row — a whole-server outage from a single unreadable row.
/// <para>
/// The disagreement is a state the public API cannot produce: a write splits the row and upserts
/// its main record and its blob together, and recovery rebuilds both from one commit-log record,
/// so main and blob are always the same version. It is forced here through
/// <see cref="FasterHotStore.CorruptBlobPayloadForTest"/>, which writes a wrong-length payload the
/// way the store itself would — the only way to exercise resilience to a state that otherwise
/// cannot arise. What the fix guarantees: a scan skips the row and keeps going, logging it; a
/// point read of that key still tells the truth, because a caller asking for one row is owed it.
/// </para>
/// </summary>
public class UnreadableBlobRowTests
{
    [Fact]
    public void A_scan_survives_a_row_whose_blob_disagrees_with_its_main_record()
    {
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        harness.Invoke("seed", ctx =>
        {
            for (var id = 0; id < 8; id++)
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = 1, Data = StoreContractTests.MakeBlob(id, 300) });
        });

        var store = (FasterHotStore)harness.Engine.HotStore;
        var table = harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob));
        var victim = SchemaKeyCodec.Encode(table.PrimaryKey, 4L);

        // The reported shape exactly: the blob grew past what the main record declares (339 vs the
        // 300 the row was written with). Ordinal 0 — the single bytes column.
        store.CorruptBlobPayloadForTest(table.Id, victim, ordinal: 0, StoreContractTests.MakeBlob(999, 339));

        // Before the fix this threw out of Scan and would take the subscribing connection with it.
        var scanned = harness.Engine.HotStore.Scan(table.Id).Select(p => p.Key).ToList();

        // Every intact row comes back; only the corrupt one is missing.
        Assert.Equal(7, scanned.Count);
        Assert.DoesNotContain(victim, scanned);
        for (var id = 0; id < 8; id++)
        {
            if (id != 4)
                Assert.Contains(SchemaKeyCodec.Encode(table.PrimaryKey, (long)id), scanned);
        }

        // An index range scan (the subscription range path) is resilient the same way.
        var byRegion = harness.Engine.HotStore
            .ScanIndexRange(table.Id, nameof(TerrainBlob.Region), SchemaKeyCodec.Encode(table.Column(nameof(TerrainBlob.Region)), 0), SchemaKeyCodec.Encode(table.Column(nameof(TerrainBlob.Region)), 9))
            .Count();
        Assert.Equal(7, byRegion);
    }

    [Fact]
    public void A_point_read_of_the_corrupt_row_still_surfaces_the_fault_with_its_key()
    {
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        harness.Invoke("seed", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 42, Region = 1, Data = StoreContractTests.MakeBlob(42, 300) }));

        var store = (FasterHotStore)harness.Engine.HotStore;
        var table = harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob));
        var key = SchemaKeyCodec.Encode(table.PrimaryKey, 42L);
        store.CorruptBlobPayloadForTest(table.Id, key, ordinal: 0, StoreContractTests.MakeBlob(1, 339));

        // A scan hides it; a point read must not — and the message must carry the key, so an
        // operator can repair the one row without bisecting the table (ask #3).
        var fault = Assert.Throws<InvalidDataException>(() =>
            harness.Engine.HotStore.TryGetRow(table.Id, key, out _));
        Assert.Contains(key.ToString(), fault.Message);
        Assert.Contains("339", fault.Message);
        Assert.Contains("300", fault.Message);
    }

    [Fact]
    public void A_restart_rebuilds_the_row_from_the_log_and_the_corruption_does_not_persist()
    {
        // This is also the answer to the issue's own hypothesis (its ask #2): that a blob write
        // outlived its commit and survived a restart. It cannot. The store opens its FASTER logs
        // deleteOnClose and wipes them at startup; recovery is snapshot + commit-log replay, and
        // replay splits each committed row and upserts its main and blob together. So a restart
        // cannot carry a main/blob disagreement forward — it rebuilds both from one log record —
        // which makes "rebuild from the log" (a restart, or rewriting the row once the point-read
        // path can reach it) the repair, and means the observed state was live, not persistent.
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        harness.Invoke("seed", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 7, Region = 1, Data = StoreContractTests.MakeBlob(7, 300) }));

        var table = harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob));
        var key = SchemaKeyCodec.Encode(table.PrimaryKey, 7L);
        ((FasterHotStore)harness.Engine.HotStore).CorruptBlobPayloadForTest(table.Id, key, ordinal: 0, StoreContractTests.MakeBlob(2, 339));
        Assert.Empty(harness.Engine.HotStore.Scan(table.Id));

        harness.Restart();

        var row = Assert.Single(harness.Engine.HotStore.Scan(table.Id));
        Assert.Equal(key, row.Key);
        var rebuilt = harness.Engine.CommittedView.Find<TerrainBlob>(7L);
        Assert.NotNull(rebuilt);
        Assert.Equal(StoreContractTests.MakeBlob(7, 300), rebuilt!.Value.Data);
    }
}
