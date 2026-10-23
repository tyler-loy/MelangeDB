using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// The archive tier at compile time (road-to-0.3 phase 25): MELANGE0024–0026 refuse what an
/// archive table cannot honour, and the generated accessor offers append and delete-by-key only —
/// the absent members are the documentation that a reducer cannot read rows living in Postgres.
/// </summary>
public class ArchiveTierTests
{
    private static string Table(string attribute, string extraColumn = "") => $$"""
        using MelangeDB;

        [Table({{attribute}})]
        public partial struct LedgerArchive
        {
            [PrimaryKey][AutoInc] public long Id;
            public long Delta;
            {{extraColumn}}
        }
        """;

    [Fact]
    public void A_private_archive_table_compiles_clean()
    {
        var result = GeneratorTestHost.RunGenerator(Table("Tier = StorageTier.Archive, Placement = Placement.Global"));
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Melange0024_fires_for_a_public_archive_table()
    {
        var result = GeneratorTestHost.RunGenerator(Table("Tier = StorageTier.Archive, Public = true"));
        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0024");
        Assert.Contains("LedgerArchive", diagnostic.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void Melange0025_fires_for_unique_on_an_archive_table()
    {
        var result = GeneratorTestHost.RunGenerator(Table("Tier = StorageTier.Archive", "[Unique] public string Reference;"));
        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0025");
        Assert.Contains("'Reference'", diagnostic.GetMessage(), StringComparison.Ordinal);

        // One finding, not two: the archive rule subsumes the Partitioned one for the same column.
        Assert.DoesNotContain(result.MelangeDiagnostics, d => d.Id == "MELANGE0003");
    }

    [Theory]
    [InlineData("Tier = StorageTier.Archive, Residency = Residency.Resident", "Residency.Resident")]
    [InlineData("Tier = StorageTier.Archive, Placement = Placement.Replicated", "Placement.Replicated")]
    public void Melange0026_fires_for_declarations_only_a_hot_table_can_honour(string attribute, string declared)
    {
        var result = GeneratorTestHost.RunGenerator(Table(attribute));
        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0026");
        Assert.Contains(declared, diagnostic.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void Melange0026_fires_for_a_scheduled_archive_table()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Tier = StorageTier.Archive, Scheduled = nameof(Reducers.Tick))]
            public partial struct ArchivedTick
            {
                [PrimaryKey][AutoInc] public ulong Id;
                public ScheduleAt ScheduledAt;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void Tick(ReducerContext ctx, ArchivedTick timer)
                {
                }
            }
            """);
        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0026");
        Assert.Contains("Scheduled", diagnostic.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void The_accessor_appends_and_deletes_by_key()
    {
        var result = GeneratorTestHost.RunGenerator(Table("Tier = StorageTier.Archive") + """

            public sealed class Reducers
            {
                [Reducer]
                public void Record(ReducerContext ctx, long delta)
                {
                    var row = ctx.Db.LedgerArchive.Insert(new LedgerArchive { Delta = delta });
                    ctx.Db.LedgerArchive.Id.Delete(row.Id);
                }
            }
            """);
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Theory]
    [InlineData("ctx.Db.LedgerArchive.Id.Find(1L)")]
    [InlineData("ctx.Db.LedgerArchive.Iter()")]
    [InlineData("ctx.Db.LedgerArchive.Count")]
    [InlineData("ctx.Db.LedgerArchive.Update(default)")]
    public void Nothing_that_reads_or_updates_is_generated(string expression)
    {
        var result = GeneratorTestHost.RunGenerator(Table("Tier = StorageTier.Archive") + $$"""

            public sealed class Reducers
            {
                [Reducer]
                public void Peek(ReducerContext ctx)
                {
                    _ = {{expression}};
                }
            }
            """);
        Assert.Contains(result.Errors, d => d.Id is "CS1061" or "CS0117");
    }
}
