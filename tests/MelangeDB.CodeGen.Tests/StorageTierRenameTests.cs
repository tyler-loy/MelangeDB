using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// The 0.3 rename of <c>StorageTier.Relational</c> to <c>HotWithRelationalBackplane</c>: the old
/// name must fail to compile with a message naming the replacement, and the shared value must still
/// generate the backplane tier — an assembly compiled against 0.2 keeps meaning the same thing.
/// </summary>
public class StorageTierRenameTests
{
    [Fact]
    public void The_old_name_is_a_compile_error_naming_the_replacement()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Tier = StorageTier.Relational)]
            public partial struct Registration
            {
                [PrimaryKey][AutoInc] public long Id;
            }
            """);

        var error = Assert.Single(result.Errors, d => d.Id == "CS0619");
        Assert.Contains("HotWithRelationalBackplane", error.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void The_backplane_tier_generates_under_its_new_name()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Tier = StorageTier.HotWithRelationalBackplane)]
            public partial struct Registration
            {
                [PrimaryKey][AutoInc] public long Id;
            }
            """);

        Assert.Empty(result.Errors);
        var (_, table) = Assert.Single(result.GeneratedSources, s => s.HintName.EndsWith("Registration.Table.g.cs", StringComparison.Ordinal));
        Assert.Contains("tier: global::MelangeDB.StorageTier.HotWithRelationalBackplane,", table, StringComparison.Ordinal);
    }
}
