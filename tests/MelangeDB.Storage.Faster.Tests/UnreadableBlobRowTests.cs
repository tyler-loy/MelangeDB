using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Issue #137: one paged row whose out-of-line payload disagreed with its main record's declared
/// length threw <see cref="InvalidDataException"/> out of a subscription's initial-set scan,
/// escaped the connection handler, and killed every client whose first subscription scanned that
/// row — a whole-server outage from a single unreadable row.
/// <para>
/// The disagreement had one real cause — a shrinking overwrite of an out-of-line blob that did not
/// truncate the stored payload (see <see cref="BlobShrinkRepro137Tests"/>, which guards the write
/// path). With that fixed, the store no longer produces the disagreement through any path, so it is
/// forced here through <see cref="FasterHotStore.CorruptBlobPayloadForTest"/>, which writes a
/// wrong-length payload the way it would appear on disk — the only way left to exercise resilience
/// to a row that is unreadable for any reason (a future codec bug, a bad sector). What this half of
/// the fix guarantees regardless of cause: a scan skips the row and keeps going, logging it; a
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
        // A corruption confined to the live projection — as this seam produces, writing a bad
        // payload into the blob store without a matching commit-log record — heals on restart,
        // because recovery rebuilds the store from the log alone (deleteOnClose wipes the FASTER
        // files at startup; replay re-splits each committed row through the now-correct write path).
        // This is NOT the same as claiming the disagreement can never survive a restart: the write
        // path itself could poison the committed history's replay, which is exactly what issue #137
        // was — a shrinking overwrite reproduced deterministically on replay. That is fixed at its
        // source, and BlobShrinkRepro137Tests.The_shrink_survives_a_restart is the guard for it;
        // this test covers the narrower promise that a projection-only fault is not durable.
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

    [Fact]
    public void ContainsKey_is_true_for_a_corrupt_row_without_reading_it()
    {
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        harness.Invoke("seed", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 5, Region = 1, Data = StoreContractTests.MakeBlob(5, 300) }));

        var store = (FasterHotStore)harness.Engine.HotStore;
        var table = harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob));
        var key = SchemaKeyCodec.Encode(table.PrimaryKey, 5L);
        store.CorruptBlobPayloadForTest(table.Id, key, ordinal: 0, StoreContractTests.MakeBlob(9, 339));

        // The row is present and must be reportable as present, without decoding it — a point read
        // throws on the corrupt payload, but existence is a directory question.
        Assert.True(store.ContainsKey(table.Id, key));
        Assert.Throws<InvalidDataException>(() => store.TryGetRow(table.Id, key, out _));
        Assert.False(store.ContainsKey(table.Id, SchemaKeyCodec.Encode(table.PrimaryKey, 999L)));
    }

    [Fact]
    public void A_corrupt_row_can_be_deleted_and_rewritten_live()
    {
        // With no subscription on the table (so the commit fan-out reads no pre-image), a reducer's
        // existence check no longer decodes the row, and the store's delete works off the directory
        // — so a poisoned row is removable and re-insertable while the store is live, rather than
        // stuck until a restart. (The subscribed case, where the fan-out reads the pre-image, is
        // covered at the SubscriptionEngine layer.)
        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        harness.Invoke("seed", ctx =>
        {
            ctx.Db.Insert(new TerrainBlob { ChunkId = 1, Region = 1, Data = StoreContractTests.MakeBlob(1, 300) });
            ctx.Db.Insert(new TerrainBlob { ChunkId = 2, Region = 1, Data = StoreContractTests.MakeBlob(2, 300) });
        });

        var store = (FasterHotStore)harness.Engine.HotStore;
        var table = harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TerrainBlob));
        store.CorruptBlobPayloadForTest(table.Id, SchemaKeyCodec.Encode(table.PrimaryKey, 1L), ordinal: 0, StoreContractTests.MakeBlob(9, 339));

        // Delete the poisoned row live.
        harness.Invoke("purge", ctx => Assert.True(ctx.Db.Delete<TerrainBlob>(1L)));
        Assert.Null(harness.Engine.CommittedView.Find<TerrainBlob>(1L));

        // And a fresh insert of the same key writes a clean row that reads back.
        harness.Invoke("reinsert", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 1, Region = 2, Data = StoreContractTests.MakeBlob(11, 300) }));
        Assert.Equal(StoreContractTests.MakeBlob(11, 300), harness.Engine.CommittedView.Find<TerrainBlob>(1L)!.Value.Data);
        Assert.Equal(2, harness.Engine.HotStore.Scan(table.Id).Count());
    }
}
