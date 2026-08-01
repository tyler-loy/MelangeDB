using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// Snapshot tests over the generator's emitted text — the phase's top-listed risk is generator
/// debuggability, and a reviewed snapshot is the cheapest regression net. The expected files live
/// in Snapshots/; on drift the test writes a *.actual next to the build output for diffing.
/// </summary>
public class GeneratedOutputTests
{
    private const string SnapshotSource = """
        using MelangeDB;

        namespace Snapshot;

        public enum Species
        {
            Wolf,
            Bear,
        }

        [Table(Public = true, ShardBy = nameof(ChunkId))]
        public partial struct Creature
        {
            [PrimaryKey][AutoInc] public ulong Id;
            [Index] public ushort ChunkId;
            public Species Kind;
            public float X;
            public string Name;
            public byte[] Genome;
            [ServerOnly] public ulong NextThinkAt;
        }

        public sealed class AdminOnly : IReducerPolicy
        {
            public bool MayCall(string reducer, PolicyContext ctx) => false;
        }

        public sealed class CreatureReducers
        {
            [Reducer]
            public void Spawn(ReducerContext ctx, ushort chunkId, string name, int[] stats)
            {
            }

            [Reducer(Policy = typeof(AdminOnly))]
            public void Cull(ReducerContext ctx)
            {
            }
        }
        """;

    [Fact]
    public void Table_codec_and_accessors_match_snapshot() =>
        AssertSnapshot("Snapshot_Creature.Table.g.cs", "Creature.Table.expected.cs");

    [Fact]
    public void Model_and_dispatcher_match_snapshot() =>
        AssertSnapshot("MelangeModel.g.cs", "MelangeModel.expected.cs");

    [Fact]
    public void Schema_manifest_matches_snapshot() =>
        AssertSnapshot("MelangeSchemaManifest.g.cs", "MelangeSchemaManifest.expected.cs");

    [Fact]
    public void Schema_manifest_is_deterministic_across_runs()
    {
        var first = GeneratorTestHost.RunGenerator(SnapshotSource);
        var second = GeneratorTestHost.RunGenerator(SnapshotSource);
        var (_, firstManifest) = Assert.Single(first.GeneratedSources, s => s.HintName == "MelangeSchemaManifest.g.cs");
        var (_, secondManifest) = Assert.Single(second.GeneratedSources, s => s.HintName == "MelangeSchemaManifest.g.cs");
        Assert.Equal(firstManifest, secondManifest);
    }

    [Fact]
    public void Schema_manifest_carries_only_the_client_visible_surface()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            public enum Rarity { Common, Epic }
            public enum HiddenReason { Banned }

            [Table(Public = true)]
            public partial struct Item
            {
                [PrimaryKey] public ulong Id;
                public Rarity Tier;
                [ServerOnly] public HiddenReason Reason;
            }

            [Table]
            public partial struct Audit
            {
                [PrimaryKey] public long Id;
            }

            [Table(Scheduled = nameof(Reducers.Tick))]
            public partial struct TickTimer
            {
                [PrimaryKey][AutoInc] public ulong Id;
                public ScheduleAt At;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void Grant(ReducerContext ctx, Rarity tier) { }

                [Reducer]
                public void Tick(ReducerContext ctx, TickTimer timer) { }

                [Reducer(ReducerKind.ClientConnected)]
                public void Connected(ReducerContext ctx) { }
            }
            """);
        Assert.Empty(result.Errors);
        var (_, manifest) = Assert.Single(result.GeneratedSources, s => s.HintName == "MelangeSchemaManifest.g.cs");

        // Public table and its client-visible surface ship; everything server-side stays home:
        // the private table, the [ServerOnly] column and the enum only it references, the timer
        // table, the scheduled reducer, and the lifecycle reducer.
        Assert.Contains("\"\"Item\"\"", manifest);
        Assert.Contains("\"\"Rarity\"\"", manifest);
        Assert.Contains("\"\"Grant\"\"", manifest);
        Assert.DoesNotContain("Audit", manifest);
        Assert.DoesNotContain("Reason", manifest);
        Assert.DoesNotContain("HiddenReason", manifest);
        Assert.DoesNotContain("TickTimer", manifest);
        Assert.DoesNotContain("\"\"Tick\"\"", manifest);
        Assert.DoesNotContain("Connected", manifest);
    }

    [Fact]
    public void Modules_with_no_client_surface_emit_no_manifest()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Audit
            {
                [PrimaryKey] public long Id;
            }
            """);
        Assert.Empty(result.Errors);
        Assert.DoesNotContain(result.GeneratedSources, s => s.HintName == "MelangeSchemaManifest.g.cs");
    }

    [Fact]
    public void Generated_output_compiles_without_errors()
    {
        var result = GeneratorTestHost.RunGenerator(SnapshotSource);
        Assert.Empty(result.Errors);
        Assert.Empty(result.MelangeDiagnostics);
    }

    [Fact]
    public void Every_column_kind_round_trips_through_the_emitter()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Kitchen
            {
                [PrimaryKey] public long Id;
                public bool A;
                public sbyte B;
                public byte C;
                public short D;
                public ushort E;
                public int F;
                public uint G;
                public ulong H;
                public float I;
                public double J;
                public string K;
                public byte[] L;
                public Identity M;
                public Timestamp N;
            }
            """);
        Assert.Empty(result.Errors);
        Assert.Empty(result.MelangeDiagnostics);
    }

    [Fact]
    public void Property_columns_generate_and_compile()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Settings
            {
                [PrimaryKey] public string Key { get; set; }
                public string Value { get; set; }
            }
            """);
        Assert.Empty(result.Errors);
        Assert.Empty(result.MelangeDiagnostics);
    }

    private static void AssertSnapshot(string hintName, string expectedFile)
    {
        var result = GeneratorTestHost.RunGenerator(SnapshotSource);
        var (_, actual) = Assert.Single(result.GeneratedSources, s => s.HintName == hintName);
        actual = actual.Replace("\r\n", "\n");

        var expectedPath = Path.Combine(AppContext.BaseDirectory, "Snapshots", expectedFile);
        Directory.CreateDirectory(Path.GetDirectoryName(expectedPath)!);
        if (!File.Exists(expectedPath))
        {
            File.WriteAllText(expectedPath + ".actual", actual);
            Assert.Fail($"Missing snapshot {expectedFile}; review and check in the .actual file written next to it.");
        }

        var expected = File.ReadAllText(expectedPath).Replace("\r\n", "\n");
        if (expected != actual)
            File.WriteAllText(expectedPath + ".actual", actual);
        Assert.Equal(expected, actual);
    }
}
