using System.Diagnostics;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// The two numbers the phase's done-criteria demand recorded: bulk loading the reference
/// workload's ~24.6k blob rows dramatically faster than per-row transactions, and a resident
/// table's full scan within a small factor of the in-memory store. Thresholds are deliberately
/// loose (10x and 5x) so the tests assert the property without flaking on machine speed; the
/// measured numbers land in the test output.
/// </summary>
[Trait("Category", "Slow")]
public class PerformanceTests(Xunit.ITestOutputHelper output)
{
    [Fact]
    public void Bulk_loading_reference_blob_workload_is_dramatically_faster_than_per_row()
    {
        // The reference workload: one RLE-compressed terrain blob per chunk across ~24.6k chunks.
        const int bulkRows = 24_600;
        const int perRowSample = 300;
        const int blobSize = 1024;

        using var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);

        var stopwatch = Stopwatch.StartNew();
        const int batchSize = 4100;
        for (var start = 0; start < bulkRows; start += batchSize)
        {
            var rows = new List<BulkRow>(batchSize);
            for (var i = start; i < start + batchSize; i++)
            {
                rows.Add(new BulkRow("TerrainBlob", new Dictionary<string, object?>
                {
                    ["ChunkId"] = (long)i,
                    ["Region"] = i % 64,
                    ["Data"] = StoreContractTests.MakeBlob(i, blobSize),
                }));
            }

            harness.Engine.BulkInsert(StoreHarness.Caller, rows);
        }

        var bulkElapsed = stopwatch.Elapsed;
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        Assert.Equal(bulkRows, harness.Engine.HotStore.Count(table));

        // Per-row transactions under the same (default, durable) fsync policy: a sample large
        // enough to average, extrapolated to a per-row rate.
        stopwatch.Restart();
        for (var i = 0; i < perRowSample; i++)
        {
            var id = 1_000_000 + i;
            harness.Invoke("insert-one", ctx =>
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = 0, Data = StoreContractTests.MakeBlob(id, blobSize) }));
        }

        var perRowElapsed = stopwatch.Elapsed;

        var bulkMicrosPerRow = bulkElapsed.TotalMicroseconds / bulkRows;
        var perRowMicros = perRowElapsed.TotalMicroseconds / perRowSample;
        var speedup = perRowMicros / bulkMicrosPerRow;
        output.WriteLine($"bulk: {bulkRows} rows in {bulkElapsed.TotalMilliseconds:F0}ms ({bulkMicrosPerRow:F1}us/row)");
        output.WriteLine($"per-row: {perRowSample} rows in {perRowElapsed.TotalMilliseconds:F0}ms ({perRowMicros:F1}us/row)");
        output.WriteLine($"speedup: {speedup:F0}x");
        Assert.True(speedup >= 10, $"bulk must be dramatically faster than per-row; measured {speedup:F1}x");
    }

    [Fact]
    public void Resident_full_scan_performs_within_a_small_factor_of_the_in_memory_store()
    {
        const int rowCount = 50_000;
        using var inMemory = new StoreHarness(StoreKind.InMemory, tables: [typeof(ItemDefinition)]);
        using var faster = new StoreHarness(StoreKind.Faster, tables: [typeof(ItemDefinition)]);

        foreach (var harness in new[] { inMemory, faster })
        {
            var rows = new List<BulkRow>(rowCount);
            for (var i = 0; i < rowCount; i++)
            {
                rows.Add(new BulkRow("ItemDefinition", new Dictionary<string, object?>
                {
                    ["Id"] = i,
                    ["Name"] = $"item-{i}",
                    ["Value"] = i * 3,
                }));
            }

            harness.Engine.BulkInsert(StoreHarness.Caller, rows);
        }

        var inMemoryMs = TimeScan(inMemory);
        var fasterMs = TimeScan(faster);
        var factor = fasterMs / Math.Max(inMemoryMs, 0.001);
        output.WriteLine($"in-memory scan of {rowCount} rows: {inMemoryMs:F2}ms");
        output.WriteLine($"FASTER resident scan of {rowCount} rows: {fasterMs:F2}ms");
        output.WriteLine($"factor: {factor:F2}x");
        Assert.True(factor < 5, $"a resident scan must stay within a small factor of in-memory; measured {factor:F2}x");
    }

    private static double TimeScan(StoreHarness harness)
    {
        var table = harness.Engine.Schema.Get(typeof(ItemDefinition)).Id;
        var store = harness.Engine.HotStore;
        var best = double.MaxValue;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var stopwatch = Stopwatch.StartNew();
            long total = 0;
            foreach (var pair in store.Scan(table))
                total += pair.Value.Length;
            stopwatch.Stop();
            Assert.True(total > 0);
            best = Math.Min(best, stopwatch.Elapsed.TotalMilliseconds);
        }

        return best;
    }
}
