using System.Diagnostics;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The measurement behind two settled phase-04 decisions: per-row policy evaluation cost and
/// column-mask cost on the delta path. Shipped as a test so the number regenerates on any machine;
/// the recorded figures live in docs/plan-phase-04.md. The assertions are sanity ceilings only —
/// two orders of magnitude above the measurement — so the suite never flakes on a slow CI box.
/// </summary>
public class PolicyCostMeasurementTests(Xunit.ITestOutputHelper output)
{
    [Fact]
    public async Task Measure_per_row_policy_and_column_mask_evaluation()
    {
        await using var host = await TransportTestHost.StartAsync(services: services =>
        {
            services.AddSingleton<IRowPolicy<InventoryItem>, InventoryVisibility>();
            services.AddSingleton<IRowPolicy<InventoryItem>, AdminSeesAllInventory>();
            services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>();
        });
        host.Call("GiveItem", TestTokens.IdentityOf("bob"), 0, "measured-item");
        host.Call("Spawn", "Measured", HideoutHidesPosition.HideoutRoom);

        var policies = new PolicySet(host.Services, host.Engine.Schema);
        var context = new PolicyContext(TestTokens.IdentityOf("alice"), false, host.Engine.CommittedView);

        // Row path: two policies unioned, the second doing a Find into a private table per row —
        // the worst realistic shape, since the cheap policy misses first (bob's row, alice asking).
        var inventory = host.Engine.Schema.Get(typeof(InventoryItem));
        var inventoryRow = host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(inventory.Id).Single().Value);
        var evaluator = policies.For(inventory.Id)!;
        const int iterations = 100_000;
        Assert.False(evaluator.IsRowVisible(inventoryRow.Span, context));
        var rowWatch = Stopwatch.StartNew();
        for (var i = 0; i < iterations; i++)
            evaluator.IsRowVisible(inventoryRow.Span, context);
        rowWatch.Stop();

        // Column path: one mask evaluated and intersected per row.
        var player = host.Engine.Schema.Get(typeof(PlayerState));
        var playerRow = host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(player.Id).Single().Value);
        var columnEvaluator = policies.For(player.Id)!;
        var allColumns = player.Columns.Select(c => c.Name).ToArray();
        var columnWatch = Stopwatch.StartNew();
        for (var i = 0; i < iterations; i++)
        {
            var visible = new HashSet<string>(allColumns, StringComparer.Ordinal);
            columnEvaluator.IntersectColumns(playerRow.Span, context, visible);
        }

        columnWatch.Stop();

        var rowNanos = rowWatch.Elapsed.TotalNanoseconds / iterations;
        var columnNanos = columnWatch.Elapsed.TotalNanoseconds / iterations;
        output.WriteLine($"Row policy (union of 2, one private-table Find): {rowNanos:F0} ns/row ({iterations / rowWatch.Elapsed.TotalSeconds:N0} rows/s)");
        output.WriteLine($"Column mask (1 policy, intersect): {columnNanos:F0} ns/row ({iterations / columnWatch.Elapsed.TotalSeconds:N0} rows/s)");

        // Sanity ceilings, not the measurement: far above any healthy machine's numbers.
        Assert.True(rowNanos < 100_000, $"row policy evaluation took {rowNanos:F0} ns/row");
        Assert.True(columnNanos < 100_000, $"column mask evaluation took {columnNanos:F0} ns/row");
    }
}
