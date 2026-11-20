using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// Reducer parameters with defaults (road-to-0.4 phase 28): a trailing parameter that declares a
/// default may be left off a call, so a client whose bindings predate it keeps calling — the rule
/// producers already had. Every reducer's arity check now names its parameters, so a call that is
/// short a required one is told which.
/// </summary>
public class ReducerDefaultsTests
{
    private static string Reducers(string members) => $$"""
        using MelangeDB;

        [Table]
        public partial struct Player
        {
            [PrimaryKey] public long Id;
            public int Room;
        }

        public sealed class PlayerReducers
        {
            {{members}}
        }
        """;

    [Fact]
    public void Trailing_defaults_make_those_parameters_optional_and_the_arity_check_names_every_parameter()
    {
        var result = GeneratorTestHost.RunGenerator(Reducers("""
            [Reducer]
            public void Join(ReducerContext ctx, long id, int room = 42, string note = "hi") =>
                ctx.Db.Player.Insert(new Player { Id = id, Room = room });

            [Reducer]
            public void Leave(ReducerContext ctx, long id) { }
            """));

        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
        var model = Assert.Single(result.GeneratedSources, s => s.HintName == "MelangeModel.g.cs").Source;
        Assert.Contains("reader.ExpectCount(1, 3, \"id, room, note\");", model, StringComparison.Ordinal);
        Assert.Contains("reader.ExpectCount(1, 1, \"id\");", model, StringComparison.Ordinal);
        Assert.Contains("if (reader.Count > 1)", model, StringComparison.Ordinal);
        Assert.Contains("if (reader.Count > 2)", model, StringComparison.Ordinal);
    }

    [Fact]
    public void A_required_parameter_after_a_defaulted_one_is_refused()
    {
        // C# itself refuses this order too (CS1737); the generator says why it matters on the wire.
        var result = GeneratorTestHost.RunGenerator(Reducers("""
            [Reducer]
            public void Gap(ReducerContext ctx, int room = 1, long id) { }
            """));

        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0009");
        Assert.Contains("no default after one with a default", diagnostic.GetMessage(), StringComparison.Ordinal);
    }
}
