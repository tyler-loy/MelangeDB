using MelangeDB.Cli;
using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// <c>melange generate --lang cs</c> writes exactly what the Roslyn generator emits: same files,
/// same names, same bytes. The two front doors share one parser and one emitter, so a difference
/// here means the wiring drifted — a file one adds and the other forgets, a renamed hint, an
/// encoding or line-ending change on the way to disk.
/// </summary>
public class GenerateCommandTests
{
    public static TheoryData<string> Manifests => ["conformance.json", "transport.json", "snapshot"];

    [Theory]
    [MemberData(nameof(Manifests))]
    public void Generate_cs_writes_the_generators_files_byte_for_byte(string manifest)
    {
        var json = manifest == "snapshot"
            ? GeneratorTestHost.ExportManifest(SnapshotSource)
            : File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Manifests", manifest));

        var analyzer = GeneratorTestHost.RunClientGenerator(json);
        Assert.Empty(analyzer.Errors);

        var directory = Directory.CreateTempSubdirectory("melange-generate-").FullName;
        try
        {
            ClientGenerate.WriteTo(ClientGenerate.Generate(json, "cs"), directory);
            var written = Directory.GetFiles(directory).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToList();
            Assert.Equal(analyzer.GeneratedSources.Select(static s => s.HintName).Order(StringComparer.Ordinal), written);
            foreach (var (hintName, source) in analyzer.GeneratedSources)
                Assert.Equal(System.Text.Encoding.UTF8.GetBytes(source), File.ReadAllBytes(Path.Combine(directory, hintName)));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>
    /// The TypeScript bindings the repo commits — the conformance adapter's, and a fixture the
    /// TypeScript package type-checks — are what `generate --lang ts` writes today.
    /// </summary>
    [Theory]
    [InlineData("conformance.json", "conformance.ts")]
    [InlineData("transport.json", "transport.ts")]
    public void The_committed_typescript_bindings_are_current(string manifest, string bindings)
    {
        var json = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Manifests", manifest));
        var generated = Assert.Single(ClientGenerate.Generate(json, "ts"));
        Assert.Equal("melange-bindings.ts", generated.Name);
        var committed = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Bindings", bindings)).ReplaceLineEndings("\n");
        Assert.True(
            generated.Content == committed,
            $"The committed TypeScript bindings for {manifest} are stale. Regenerate with `melange generate --lang ts <manifest> -o <dir>` (see clients/typescript/README.md).");
    }

    [Fact]
    public void Rewriting_unchanged_files_touches_nothing()
    {
        var json = GeneratorTestHost.ExportManifest(SnapshotSource);
        var directory = Directory.CreateTempSubdirectory("melange-generate-").FullName;
        try
        {
            var files = ClientGenerate.Generate(json, "cs");
            Assert.Equal(files.Count, ClientGenerate.WriteTo(files, directory).Count);
            Assert.Empty(ClientGenerate.WriteTo(files, directory));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void A_malformed_manifest_is_refused_not_half_generated() =>
        Assert.Throws<ManifestException>(() => ClientGenerate.Generate("""{ "format": 99 }""", "cs"));

    private const string SnapshotSource = """
        using MelangeDB;

        namespace Snapshot;

        public enum Species { Wolf, Bear }

        [Table(Public = true)]
        public partial struct Creature
        {
            [PrimaryKey][AutoInc] public ulong Id;
            [Index] public ushort ChunkId;
            public Species Kind;
            public string Name;
        }

        public sealed record Census(int Count, string? Oldest);

        public sealed class CreatureReducers
        {
            [Reducer]
            public void Spawn(ReducerContext ctx, ushort chunkId, string name, int[] stats)
            {
            }
        }

        public sealed class CreatureProducers
        {
            [Producer]
            public Census Count(ProducerContext ctx, ushort chunkId = 0) => new(0, null);
        }
        """;
}
