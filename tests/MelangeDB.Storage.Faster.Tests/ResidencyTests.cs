using System.Diagnostics.Metrics;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Residency behaviour: resolution (config over attribute over default), Auto's threshold
/// demotion, the careful runtime override, the startup report's accuracy against real process
/// memory, and the melange.store.* signals. Residency is a declared, computable budget — these
/// tests are what keep it an observed one.
/// </summary>
public class ResidencyTests
{
    [Fact]
    public void Attribute_residency_reaches_the_store()
    {
        using var harness = new StoreHarness(StoreKind.Faster);
        var statistics = harness.Engine.HotStore.Statistics();
        Assert.Equal(Residency.Resident, statistics.Tables.Single(t => t.Name == "ItemDefinition").Residency);
        Assert.Equal(Residency.Paged, statistics.Tables.Single(t => t.Name == "TerrainBlob").Residency);
        Assert.Equal(Residency.Resident, statistics.Tables.Single(t => t.Name == "AutoSized").Residency); // Auto starts resident.
    }

    [Fact]
    public void Config_override_wins_over_the_attribute()
    {
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.PerTable["ItemDefinition"] = Residency.Paged);
        var statistics = harness.Engine.HotStore.Statistics();
        Assert.Equal(Residency.Paged, statistics.Tables.Single(t => t.Name == "ItemDefinition").Residency);
    }

    [Fact]
    public void Residency_default_applies_to_unannotated_tables()
    {
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.Default = Residency.Resident);
        var statistics = harness.Engine.HotStore.Statistics();

        // Creature declares nothing, so it takes the configured default; TerrainBlob declares
        // nothing either — Paged *is* the unspecified value, which is exactly the documented
        // semantic: pin it back down with the per-table override.
        Assert.Equal(Residency.Resident, statistics.Tables.Single(t => t.Name == "Creature").Residency);
    }

    [Fact]
    public void Auto_table_demotes_to_paged_past_the_threshold_and_stays_correct()
    {
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.AutoThresholdBytes = 64 * 1024);

        harness.Invoke("small", ctx => ctx.Db.Insert(new AutoSized { Id = 0, Payload = StoreContractTests.MakeBlob(0, 1000) }));
        Assert.Equal(
            Residency.Resident,
            harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "AutoSized").Residency);

        for (var i = 1; i <= 80; i++)
        {
            var id = i;
            harness.Invoke("grow", ctx => ctx.Db.Insert(new AutoSized { Id = id, Payload = StoreContractTests.MakeBlob(id, 1000) }));
        }

        Assert.Equal(
            Residency.Paged,
            harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "AutoSized").Residency);

        // Demotion migrated every row intact.
        harness.Invoke("verify", ctx =>
        {
            Assert.Equal(81, ctx.Db.Count<AutoSized>());
            for (var i = 0; i <= 80; i++)
                Assert.Equal(StoreContractTests.MakeBlob(i, 1000), ctx.Db.Find<AutoSized>((long)i)!.Value.Payload);
        });
    }

    [Fact]
    public void Runtime_override_promotes_and_demotes_with_identical_results()
    {
        using var harness = new StoreHarness(StoreKind.Faster);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 30; i++)
                ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = i % 3, Data = StoreContractTests.MakeBlob(i, 2000) });
        });

        var before = harness.Dump();
        var control = Assert.IsAssignableFrom<IResidencyControl>(harness.Engine.HotStore);

        control.ApplyResidency("TerrainBlob", Residency.Resident);
        Assert.Equal(
            Residency.Resident,
            harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "TerrainBlob").Residency);
        Assert.Equal(before, harness.Dump());

        control.ApplyResidency("TerrainBlob", Residency.Paged);
        Assert.Equal(
            Residency.Paged,
            harness.Engine.HotStore.Statistics().Tables.Single(t => t.Name == "TerrainBlob").Residency);
        Assert.Equal(before, harness.Dump());

        // And the indexes moved with the rows both ways.
        harness.Invoke("filter", ctx => Assert.Equal(10, ctx.Db.Filter<TerrainBlob>("Region", 1).Count()));
    }

    /// <summary>
    /// The report-accuracy done-criterion. Stated tolerance: with a resident dataset large enough
    /// to dominate noise (~50 MiB), the report's measured resident bytes must agree with the real
    /// GC heap growth within a factor of [0.6, 1.8] — the slack covers allocator rounding,
    /// SortedDictionary node overhead the estimate prices at a flat per-row constant, and test-run
    /// heap noise. A budget that doesn't predict reality is worse than none; this is the test
    /// that keeps the report honest.
    /// </summary>
    [Fact]
    [Trait("Category", "Slow")]
    public void Residency_report_total_matches_measured_process_memory()
    {
        const int rowCount = 500;
        const int blobSize = 100 * 1024;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        var baseline = GC.GetTotalMemory(forceFullCollection: true);

        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.PerTable["TerrainBlob"] = Residency.Resident,
            tables: [typeof(TerrainBlob)]);

        // The load runs in its own frame: a Debug-build JIT roots discarded locals (the returned
        // CommitRecords and their write sets) until the frame pops, which would measure the test
        // rather than the store.
        LoadResidentBlobs(harness, rowCount, blobSize);

        var statistics = harness.Engine.HotStore.Statistics();
        var reported = statistics.Tables.Single(t => t.Name == "TerrainBlob").ResidentBytes;
        GC.Collect();
        GC.WaitForPendingFinalizers();
        var measured = GC.GetTotalMemory(forceFullCollection: true) - baseline;

        Assert.True(reported > (long)rowCount * blobSize, "the report must count the blob data it pins");
        var ratio = (double)measured / reported;
        Assert.InRange(ratio, 0.6, 1.8);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void LoadResidentBlobs(StoreHarness harness, int rowCount, int blobSize)
    {
        for (var start = 0; start < rowCount; start += 100)
        {
            var rows = new List<BulkRow>(100);
            for (var i = start; i < start + 100; i++)
            {
                rows.Add(new BulkRow("TerrainBlob", new Dictionary<string, object?>
                {
                    ["ChunkId"] = (long)i,
                    ["Region"] = i % 4,
                    ["Data"] = StoreContractTests.MakeBlob(i, blobSize),
                }));
            }

            harness.Engine.BulkInsert(StoreHarness.Caller, rows);
        }
    }

    [Fact]
    public void Store_metrics_report_per_table_measurements()
    {
        using var harness = new StoreHarness(StoreKind.Faster);
        harness.Invoke("seed", ctx =>
        {
            ctx.Db.Insert(new ItemDefinition { Id = 1, Name = "sword", Value = 5 });
            ctx.Db.Insert(new TerrainBlob { ChunkId = 1, Region = 0, Data = StoreContractTests.MakeBlob(1, 500) });
        });
        harness.Invoke("scan", ctx => Assert.Single(ctx.Db.Scan<ItemDefinition>()));

        var gauges = new Dictionary<(string Instrument, string Table), long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "MelangeDB" && instrument.Name.StartsWith("melange.store.", StringComparison.Ordinal))
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "table")
                    gauges[(instrument.Name, (string)tag.Value!)] = value;
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();

        Assert.True(gauges[("melange.store.resident_bytes", "ItemDefinition")] > 0);
        Assert.True(gauges[("melange.store.scan_rows", "ItemDefinition")] >= 1);
        Assert.True(gauges.ContainsKey(("melange.store.page_faults", "TerrainBlob")));
    }
}
