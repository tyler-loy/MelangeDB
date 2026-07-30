using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// MELANGE0017: a full scan over a table that is not Resident fires; the same scan over a
/// Resident table is silent; and the existence APIs never fire, because they are the sanctioned
/// answer to the existence-check shape of the problem.
/// </summary>
public class TableScanAnalyzerTests
{
    private const string Prologue = """
        using MelangeDB;

        namespace Game;

        """;

    [Fact]
    public async Task Iter_over_paged_table_fires()
    {
        var diagnostics = await GeneratorTestHost.RunScanAnalyzerAsync(Prologue + """
            [Table]
            public partial struct Creature
            {
                [PrimaryKey] public ulong Id;
                public string Name;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void CountAll(ReducerContext ctx)
                {
                    foreach (var creature in ctx.Db.Creature.Iter())
                        _ = creature;
                }
            }
            """);
        var diagnostic = Assert.Single(diagnostics, d => d.Id == "MELANGE0017");
        Assert.Contains("Creature", diagnostic.GetMessage());
    }

    [Fact]
    public async Task Iter_over_resident_table_is_silent()
    {
        var diagnostics = await GeneratorTestHost.RunScanAnalyzerAsync(Prologue + """
            [Table(Residency = Residency.Resident)]
            public partial struct ItemDefinition
            {
                [PrimaryKey] public int Id;
                public string Name;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void CountAll(ReducerContext ctx)
                {
                    foreach (var item in ctx.Db.ItemDefinition.Iter())
                        _ = item;
                }
            }
            """);
        Assert.DoesNotContain(diagnostics, d => d.Id == "MELANGE0017");
    }

    [Fact]
    public async Task Iter_over_auto_table_fires()
    {
        // Auto is resident only until a size threshold: a scan that is fast until a cliff arrives
        // under production load is exactly what the diagnostic exists to surface.
        var diagnostics = await GeneratorTestHost.RunScanAnalyzerAsync(Prologue + """
            [Table(Residency = Residency.Auto)]
            public partial struct Cache
            {
                [PrimaryKey] public int Id;
                public string Payload;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void CountAll(ReducerContext ctx)
                {
                    foreach (var entry in ctx.Db.Cache.Iter())
                        _ = entry;
                }
            }
            """);
        Assert.Contains(diagnostics, d => d.Id == "MELANGE0017");
    }

    [Fact]
    public async Task Existence_apis_over_paged_table_are_silent()
    {
        var diagnostics = await GeneratorTestHost.RunScanAnalyzerAsync(Prologue + """
            [Table]
            public partial struct Creature
            {
                [PrimaryKey] public ulong Id;
                public string Name;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void Checks(ReducerContext ctx)
                {
                    if (!ctx.Db.Creature.Any())
                        return;
                    _ = ctx.Db.Creature.Count;
                    _ = ctx.Db.Creature.First();
                }
            }
            """);
        Assert.DoesNotContain(diagnostics, d => d.Id == "MELANGE0017");
    }

    [Fact]
    public async Task Scan_outside_a_reducer_still_fires()
    {
        // The analyzer is the porting tool: an admin or init helper scanning a paged table is
        // still on the list it produces.
        var diagnostics = await GeneratorTestHost.RunScanAnalyzerAsync(Prologue + """
            [Table]
            public partial struct Creature
            {
                [PrimaryKey] public ulong Id;
                public string Name;
            }

            public static class Admin
            {
                public static int CountAll(IDbView db)
                {
                    var count = 0;
                    foreach (var creature in db.Creature.Iter())
                        count++;
                    return count;
                }
            }
            """);
        Assert.Contains(diagnostics, d => d.Id == "MELANGE0017");
    }
}
