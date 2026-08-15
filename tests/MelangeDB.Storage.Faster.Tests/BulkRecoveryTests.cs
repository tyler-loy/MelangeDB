using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Recovery's bulk mode (#51): the engine takes the whole replay through <see cref="IBulkRecovery"/>
/// when the store offers it, so the FASTER store's managed state builds through builders and
/// publishes one version per table rather than one structurally shared version per replayed op.
/// These tests pin the equivalence: a store rebuilt through bulk mode answers exactly like the
/// store that produced the log — rows, secondary indexes, counts, and residency included.
/// </summary>
public class BulkRecoveryTests
{
    [Fact]
    public void The_faster_store_offers_bulk_recovery()
    {
        using var faster = new StoreHarness(StoreKind.Faster);
        Assert.IsAssignableFrom<IBulkRecovery>(faster.Engine.HotStore);
    }

    [Fact]
    public void Replay_through_bulk_mode_rebuilds_rows_indexes_and_counts_identically()
    {
        using var harness = new StoreHarness(StoreKind.Faster);

        // A mixed history over both tiers: inserts, updates that move an indexed value (unindex +
        // reindex through the builders), and deletes (remove + unindex through the builders).
        // Creature is resident with an indexed column; TerrainBlob is paged with an indexed column
        // and an out-of-line payload, so the directory and blob mask replay too.
        for (var i = 0; i < 120; i++)
        {
            var id = i;
            harness.Invoke("mixed", ctx =>
            {
                ctx.Db.Insert(new Creature { ChunkId = id % 10, Name = $"c{id}", X = id });
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = id % 8, Data = StoreContractTests.MakeBlob(id, 600) });
                if (id % 3 == 0 && id > 0)
                {
                    var moved = ctx.Db.Find<TerrainBlob>((long)(id - 1))!.Value;
                    ctx.Db.Update(moved with { Region = 99, Data = StoreContractTests.MakeBlob(id + 9000, 800) });
                }

                if (id % 7 == 0 && id > 10)
                    ctx.Db.Delete<TerrainBlob>((long)(id - 10));
            });
        }

        var rows = harness.Dump();
        var indexed = IndexProbe(harness);

        harness.Restart();

        Assert.Equal(rows, harness.Dump());
        Assert.Equal(indexed, IndexProbe(harness));
    }

    [Fact]
    public void Auto_demotion_crossing_its_threshold_mid_replay_lands_paged_and_intact()
    {
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.AutoThresholdBytes = 64 * 1024);

        for (var i = 0; i <= 80; i++)
        {
            var id = i;
            harness.Invoke("grow", ctx => ctx.Db.Insert(new AutoSized { Id = id, Payload = StoreContractTests.MakeBlob(id, 1000) }));
        }

        Assert.Equal(
            Residency.Paged,
            harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "AutoSized").Residency);

        // The replay re-crosses the Auto threshold mid-stream, so the demotion — a tier reset plus
        // a re-put of every row — runs entirely inside the builders and publishes once at the end.
        harness.Restart();

        Assert.Equal(
            Residency.Paged,
            harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "AutoSized").Residency);
        harness.Invoke("verify", ctx =>
        {
            Assert.Equal(81, ctx.Db.Count<AutoSized>());
            for (var i = 0; i <= 80; i++)
                Assert.Equal(StoreContractTests.MakeBlob(i, 1000), ctx.Db.Find<AutoSized>((long)i)!.Value.Payload);
        });
    }

    /// <summary>Every secondary-index answer that could diverge if the builders mis-rebuilt an index.</summary>
    private static List<string> IndexProbe(StoreHarness harness)
    {
        var probe = new List<string>();
        harness.Invoke("probe", ctx =>
        {
            foreach (var creature in ctx.Db.FilterRange<Creature>("ChunkId", 2, 5))
                probe.Add($"creature|{creature.Id}|{creature.ChunkId}");
            foreach (var blob in ctx.Db.FilterRange<TerrainBlob>("Region", 0, 7))
                probe.Add($"blob|{blob.ChunkId}|{blob.Region}");
            foreach (var moved in ctx.Db.FilterRange<TerrainBlob>("Region", 99, 99))
                probe.Add($"moved|{moved.ChunkId}");
        });
        return probe;
    }
}
