using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// MELANGE0018: ShardBy must not name the primary-key column. Handoff re-homes a row by rewriting
/// its ShardBy column while the stored row key stays fixed, so a primary-key shard column would
/// silently diverge from its key on the first transfer.
/// </summary>
public class ShardByDiagnosticTests
{
    [Fact]
    public void Melange0018_fires_when_shard_by_names_the_primary_key()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Placement = Placement.Partitioned, ShardBy = nameof(InstanceId))]
            public partial struct Dungeon
            {
                [PrimaryKey] public uint InstanceId;
                public int Level;
            }
            """);
        var diagnostic = Assert.Single(result.MelangeDiagnostics, static d => d.Id == "MELANGE0018");
        Assert.Contains("its own column", diagnostic.GetMessage());
    }

    [Fact]
    public void Melange0018_is_silent_when_shard_by_names_a_non_key_column()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Placement = Placement.Partitioned, ShardBy = nameof(InstanceId))]
            public partial struct Creature
            {
                [PrimaryKey] public long Id;
                [Index] public uint InstanceId;
            }
            """);
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }
}
