using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// Producers in the schema manifest (road-to-0.3 phase 26): format 2 carries them and their result
/// types, the client generator turns that into records, stubs, and decoders that compile, and the
/// parser refuses a manifest whose references do not resolve. A module without producers still
/// writes format 1, so its hash does not move.
/// </summary>
public class ProducerManifestTests
{
    private const string Server = """
        using System.Collections.Generic;
        using System.Threading.Tasks;
        using MelangeDB;

        namespace Game;

        public enum Rarity : byte { Common = 1, Rare = 2 }

        public enum Huge : ulong { Big = 18446744073709551615 }

        public struct Point
        {
            public int X;
            public int Y { get; set; }
        }

        public sealed record Item(string Name, Rarity Rarity, Huge Size, Point Where, Point? Aim, int? Stack, byte[]? Icon, List<string> Tags, int[][] Grid, List<int?> Gaps);

        public sealed class Node
        {
            public string Name { get; set; } = "";
            public List<Node> Children { get; set; } = [];
        }

        [Table(Public = true)]
        public partial struct Player
        {
            [PrimaryKey] public Identity Id;
            public string Name;
        }

        public sealed class GameProducers
        {
            [Producer]
            public Task<List<Item>> Items(ProducerContext ctx, int page = 1, string? after = null, Rarity rarity = Rarity.Rare, double scale = 1.5, bool all = true) =>
                Task.FromResult(new List<Item>());

            [Producer] public Node Tree(ProducerContext ctx) => new();

            [Producer] public Timestamp Now(ProducerContext ctx) => ctx.Timestamp;

            [Producer] public long? Maybe(ProducerContext ctx, Identity who) => null;
        }
        """;

    [Fact]
    public void A_module_with_producers_writes_format_2_with_its_producers_and_types()
    {
        var manifest = GeneratorTestHost.ExportManifest(Server);

        Assert.Contains("\"format\": 2", manifest, StringComparison.Ordinal);
        Assert.Contains("\"returns\": \"Item[]\"", manifest, StringComparison.Ordinal);
        Assert.Contains("\"type\": \"Int32?[]\"", manifest, StringComparison.Ordinal);
        Assert.Contains("\"type\": \"Int32[][]\"", manifest, StringComparison.Ordinal);
        Assert.Contains("\"type\": \"Point?\"", manifest, StringComparison.Ordinal);
        Assert.Contains("\"default\": \"1\"", manifest, StringComparison.Ordinal);
        Assert.Contains("\"name\": \"Rarity\"", manifest, StringComparison.Ordinal);
    }

    [Fact]
    public void A_module_without_producers_still_writes_format_1()
    {
        var manifest = GeneratorTestHost.ExportManifest("""
            using MelangeDB;

            [Table(Public = true)]
            public partial struct Player
            {
                [PrimaryKey] public Identity Id;
            }
            """);

        Assert.Contains("\"format\": 1", manifest, StringComparison.Ordinal);
        Assert.DoesNotContain("producers", manifest, StringComparison.Ordinal);
    }

    [Fact]
    public void The_client_generator_turns_producers_into_records_stubs_and_decoders_that_compile()
    {
        var client = GeneratorTestHost.RunClientGenerator(GeneratorTestHost.ExportManifest(Server), """
            using System.Collections.Generic;
            using System.Threading.Tasks;
            using MelangeDB.Types;

            public static class Usage
            {
                public static async Task<int> Use(MelangeConnection conn)
                {
                    List<Item> items = await conn.Producers.ItemsAsync();
                    List<Item> more = await conn.Producers.ItemsAsync(2, "after", Rarity.Common, 2.0, false);
                    Node tree = await conn.Producers.TreeAsync();
                    long? maybe = await conn.Producers.MaybeAsync(default);
                    MelangeDB.Timestamp now = await conn.Producers.NowAsync();
                    Point where = items[0].Where;
                    return items.Count + more.Count + tree.Children.Count + (maybe is null ? 0 : 1) + where.X + items[0].Gaps.Count + items[0].Grid[0].Count;
                }
            }
            """);

        Assert.Empty(client.Errors);
        Assert.Empty(client.MelangeDiagnostics);
        var model = Assert.Single(client.GeneratedSources, s => s.HintName == "MelangeClientModel.g.cs").Source;
        Assert.Contains("public sealed partial record Item", model, StringComparison.Ordinal);
        Assert.Contains("public partial record struct Point", model, StringComparison.Ordinal);
        Assert.Contains("int page = 1, string after = default, Rarity rarity = (Rarity)(2), double scale = 1.5D, bool all = true", model, StringComparison.Ordinal);
    }

    [Fact]
    public void A_result_type_named_like_a_public_table_or_another_result_type_is_refused()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            namespace A { public sealed record Line(string Text); }
            namespace B { public sealed record Line(int Number); }

            [Table(Public = true)]
            public partial struct Player { [PrimaryKey] public Identity Id; }

            public sealed record Player2(string Name);

            public sealed class P
            {
                [Producer] public A.Line First(ProducerContext ctx) => new("a");
                [Producer] public B.Line Second(ProducerContext ctx) => new(1);
            }

            namespace C
            {
                public sealed record Player(string Name);
                public sealed class Q { [Producer] public Player Me(ProducerContext ctx) => new("me"); }
            }
            """);

        var messages = result.MelangeDiagnostics.Where(d => d.Id == "MELANGE0031").Select(d => d.GetMessage()).ToList();
        Assert.Contains(messages, m => m.Contains("'Line'", StringComparison.Ordinal));
        Assert.Contains(messages, m => m.Contains("'Player'", StringComparison.Ordinal) && m.Contains("public table row type", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("\"returns\": \"Mystery[]\"", "Mystery")]
    [InlineData("\"format\": 3", "formats 1-2")]
    public void The_parser_refuses_a_manifest_it_cannot_bind(string replacement, string expected)
    {
        var manifest = GeneratorTestHost.ExportManifest(Server);
        manifest = replacement.StartsWith("\"format\"", StringComparison.Ordinal)
            ? manifest.Replace("\"format\": 2", replacement, StringComparison.Ordinal)
            : manifest.Replace("\"returns\": \"Item[]\"", replacement, StringComparison.Ordinal);

        var client = GeneratorTestHost.RunClientGenerator(manifest);

        var diagnostic = Assert.Single(client.GeneratorDiagnostics, d => d.Id == "MELANGE0020");
        Assert.Contains(expected, diagnostic.GetMessage(), StringComparison.Ordinal);
    }
}
