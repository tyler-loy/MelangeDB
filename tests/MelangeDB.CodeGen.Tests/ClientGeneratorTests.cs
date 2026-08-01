using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// The client generator, fed by the server generator's own manifest — the writer and the reader
/// held together in one loop, so the tests can never drift from the real contract. Emitted
/// shapes are snapshot-tested in the house style, and the whole tree must compile against the
/// real MelangeDB.Client assembly, not just render as text.
/// </summary>
public class ClientGeneratorTests
{
    /// <summary>The same declarations the server-side snapshots use — one schema, two trees.</summary>
    private const string ServerSource = """
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

    private static string Manifest => GeneratorTestHost.ExportManifest(ServerSource);

    [Fact]
    public void Table_bindings_match_snapshot() =>
        GeneratorTestHost.AssertSnapshot(
            GeneratorTestHost.RunClientGenerator(Manifest),
            "Creature.Client.g.cs",
            "Creature.Client.expected.cs");

    [Fact]
    public void Client_model_matches_snapshot() =>
        GeneratorTestHost.AssertSnapshot(
            GeneratorTestHost.RunClientGenerator(Manifest),
            "MelangeClientModel.g.cs",
            "MelangeClientModel.expected.cs");

    [Fact]
    public void Generated_bindings_compile_against_the_real_client_library()
    {
        var result = GeneratorTestHost.RunClientGenerator(Manifest);
        Assert.Empty(result.GeneratorDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Bindings_carry_the_manifest_schema_hash()
    {
        var manifest = Manifest;
        var result = GeneratorTestHost.RunClientGenerator(manifest);
        var (_, model) = Assert.Single(result.GeneratedSources, s => s.HintName == "MelangeClientModel.g.cs");

        // The hash in the bindings is the manifest's own — the drift detector the connection
        // wrapper surfaces.
        var marker = "\"schemaHash\": \"";
        var start = manifest.IndexOf(marker, StringComparison.Ordinal) + marker.Length;
        var hash = manifest[start..manifest.IndexOf('"', start)];
        Assert.Contains($"public const string Hash = \"{hash}\";", model);
    }

    [Fact]
    public void No_manifest_means_no_output_and_no_noise()
    {
        var result = GeneratorTestHost.RunClientGenerator([], source: "public class Nothing { }");
        Assert.Empty(result.GeneratorDiagnostics);
        Assert.Empty(result.GeneratedSources);
    }

    [Fact]
    public void Melange0020_fires_on_a_malformed_manifest()
    {
        var result = GeneratorTestHost.RunClientGenerator("{ \"format\": 1, \"tables\": ");
        Assert.Contains(result.GeneratorDiagnostics, d => d.Id == "MELANGE0020");
        Assert.Empty(result.GeneratedSources);
    }

    [Fact]
    public void Melange0020_fires_on_an_unknown_format_version()
    {
        var result = GeneratorTestHost.RunClientGenerator("""{ "format": 99, "schemaHash": "x", "module": "m", "enums": [], "tables": [], "reducers": [] }""");
        var diagnostic = Assert.Single(result.GeneratorDiagnostics, d => d.Id == "MELANGE0020");
        Assert.Contains("format 99", diagnostic.GetMessage());
    }

    [Fact]
    public void Melange0021_fires_when_two_manifests_land_in_one_project()
    {
        var manifest = Manifest;
        var result = GeneratorTestHost.RunClientGenerator(
        [
            (Path.Combine("game", "melange-schema.json"), manifest),
            (Path.Combine("admin", "melange-schema.json"), manifest),
        ]);
        Assert.Contains(result.GeneratorDiagnostics, d => d.Id == "MELANGE0021");
        Assert.Empty(result.GeneratedSources);
    }

    [Fact]
    public void A_second_consumer_of_the_same_manifest_compiles_its_own_tree()
    {
        // Deliverable 5 of issue #20: several projects generate from one schema. The sample
        // client is the first consumer; this compilation — different assembly, different user
        // code — is the second. Same manifest, its own full tree, using the bindings from source.
        var manifest = Manifest;
        var result = GeneratorTestHost.RunClientGenerator(manifest, source: """
            using MelangeDB.Types;

            public static class SecondConsumer
            {
                public static async System.Threading.Tasks.Task RunAsync(global::MelangeDB.Client.MelangeClient client)
                {
                    var conn = new MelangeConnection(client);
                    conn.Db.Creature.OnInsert += c => System.Console.WriteLine($"{c.Name} ({c.Kind}) at {c.X}");
                    var sub = await conn.Db.Creature.ChunkId.SubscribeRangeAsync(0, 10);
                    await conn.Db.Creature.ChunkId.RescopeRangeAsync(sub, 5, 15);
                    Creature? found = conn.Db.Creature.Id.Find(1UL);
                    var nearby = conn.Db.Creature.ChunkId.Filter(5, 15);
                    var lsn = await conn.Reducers.SpawnAsync(7, "wolf", new[] { 1, 2, 3 });
                    System.Console.WriteLine($"{conn.SchemaHash} {found?.Id} {nearby.Count} {lsn} {sub.Cache.Count}");
                }
            }
            """);
        Assert.Empty(result.GeneratorDiagnostics);
        Assert.Empty(result.Errors);
    }
}
