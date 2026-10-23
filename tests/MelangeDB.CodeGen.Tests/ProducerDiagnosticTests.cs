using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// <c>[Producer]</c> at compile time (road-to-0.3 phase 26): MELANGE0028 refuses a signature the
/// dispatcher cannot call, MELANGE0029 a result the wire cannot carry — naming the member path that
/// failed — and MELANGE0030 a name another callable already has.
/// </summary>
public class ProducerDiagnosticTests
{
    private static string Producers(string members, string extra = "") => $$"""
        using System.Collections.Generic;
        using System.Threading.Tasks;
        using MelangeDB;

        public sealed record Line(string Key, long Delta, Timestamp At, int? Rank, List<string> Tags);

        [Table]
        public partial struct Ledger
        {
            [PrimaryKey] public string Key;
            public long Delta;
        }

        {{extra}}

        public sealed class LedgerProducers
        {
            {{members}}
        }
        """;

    [Fact]
    public void A_well_formed_producer_compiles_clean_and_registers()
    {
        var result = GeneratorTestHost.RunGenerator(Producers("""
            [Producer]
            public async Task<List<Line>> History(ProducerContext ctx, int page = 1, string? after = null)
            {
                await Task.Yield();
                return [];
            }

            [Producer] public long Balance(ProducerContext ctx) => 0;

            [Producer] public ValueTask<Line?> Latest(ProducerContext ctx, string[] keys) => new((Line?)null);
            """));

        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
        var model = Assert.Single(result.GeneratedSources, s => s.HintName == "MelangeModel.g.cs").Source;
        Assert.Contains("\"History\"", model, StringComparison.Ordinal);
        Assert.Contains("reader.ExpectCount(0, 2);", model, StringComparison.Ordinal);
        Assert.Contains("MelangeProducerCodecs", model, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("[Producer] public long Missing() => 0;", "must take ProducerContext as its first parameter")]
    [InlineData("[Producer] public void Nothing(ProducerContext ctx) { }", "a producer returns a value")]
    [InlineData("[Producer] public Task Untyped(ProducerContext ctx) => Task.CompletedTask;", "a producer returns a value")]
    [InlineData("[Producer] public static long Static(ProducerContext ctx) => 0;", "is static")]
    [InlineData("[Producer] public long Gap(ProducerContext ctx, int a = 1, int b) => 0;", "no default after one with a default")]
    [InlineData("[Producer] public long Ref(ProducerContext ctx, ref int a) => 0;", "by reference")]
    [InlineData("[Producer] public long Bad(ProducerContext ctx, object o) => 0;", "which a client call cannot carry")]
    public void Melange0028_names_what_is_wrong_with_the_signature(string member, string reason)
    {
        var result = GeneratorTestHost.RunGenerator(Producers(member));

        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0028");
        Assert.Contains(reason, diagnostic.GetMessage(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("public Ledger Row(ProducerContext ctx) => default;", "its result", "[Table] struct")]
    [InlineData("public List<Ledger> Rows(ProducerContext ctx) => [];", "its result[]", "[Table] struct")]
    [InlineData("public Dictionary<string, long> Totals(ProducerContext ctx) => [];", "its result", "framework type")]
    [InlineData("public Wrapper Wrapped(ProducerContext ctx) => new();", "its result.Inner", "framework type")]
    [InlineData("public ScheduleAt When(ProducerContext ctx) => default;", "its result", "only a table column carries")]
    public void Melange0029_names_the_member_path_that_cannot_be_encoded(string member, string path, string reason)
    {
        var result = GeneratorTestHost.RunGenerator(Producers(
            "[Producer] " + member,
            "public sealed class Wrapper { public object Inner { get; set; } = new(); }"));

        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0029");
        var message = diagnostic.GetMessage();
        Assert.Contains(path, message, StringComparison.Ordinal);
        Assert.Contains(reason, message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_recursive_result_type_is_encodable()
    {
        var result = GeneratorTestHost.RunGenerator(Producers(
            "[Producer] public Node Tree(ProducerContext ctx) => new();",
            "public sealed class Node { public string Name { get; set; } = \"\"; public List<Node> Children { get; set; } = []; }"));

        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Melange0030_fires_for_a_producer_named_like_a_reducer_or_another_producer()
    {
        var result = GeneratorTestHost.RunGenerator(Producers(
            """
            [Producer] public long Credit(ProducerContext ctx) => 0;
            [Producer(Name = "Twice")] public long A(ProducerContext ctx) => 0;
            [Producer(Name = "Twice")] public long B(ProducerContext ctx) => 0;
            """,
            "public sealed class LedgerReducers { [Reducer] public void Credit(ReducerContext ctx) { } }"));

        var diagnostics = result.MelangeDiagnostics.Where(d => d.Id == "MELANGE0030").Select(d => d.GetMessage()).ToList();
        Assert.Equal(3, diagnostics.Count);
        Assert.Contains(diagnostics, m => m.Contains("'Credit' shares its name with a reducer", StringComparison.Ordinal));
        Assert.Equal(2, diagnostics.Count(m => m.Contains("'Twice' shares its name with another producer", StringComparison.Ordinal)));
        Assert.Empty(result.Errors);
    }
}
