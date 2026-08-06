using System.Text.RegularExpressions;
using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// The compile-time execution-site resolution behind gateway routing: reducers touching only
/// Global and Replicated tables are hub-executed; anything else — a Partitioned touch, an opaque
/// body, an unknown table — resolves to the shard, where a misplaced Global read fails loudly at
/// runtime instead of silently answering empty.
/// </summary>
public class ExecutionSiteTests
{
    private const string Tables = """
        using MelangeDB;

        [Table(Placement = Placement.Global)]
        public partial struct Account
        {
            [PrimaryKey] public long Id;
            public string Email;
        }

        [Table(Placement = Placement.Replicated)]
        public partial struct ItemDef
        {
            [PrimaryKey] public long Id;
            public string Name;
        }

        [Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(InstanceId))]
        public partial struct Creature
        {
            [PrimaryKey] public long Id;
            [Index] public uint InstanceId;
        }

        """;

    private static string SiteOf(string generated, string reducerName)
    {
        // The site argument is no longer last in the descriptor — `isolation:` follows it — so the
        // terminator is a comma or a paren depending on emission order, not always a paren.
        var match = Regex.Match(
            generated,
            $"\"{reducerName}\".*?site: global::MelangeDB\\.ReducerSite\\.(\\w+)[,)]",
            RegexOptions.Singleline);
        Assert.True(match.Success, $"No emitted site found for reducer '{reducerName}'.");
        return match.Groups[1].Value;
    }

    private static string Generate(string reducers)
    {
        var result = GeneratorTestHost.RunGenerator(Tables + reducers);
        Assert.Empty(result.Errors);
        var (_, source) = Assert.Single(result.GeneratedSources, static s => s.HintName == "MelangeModel.g.cs");
        return source;
    }

    [Fact]
    public void A_reducer_touching_only_global_and_replicated_tables_is_hub_executed()
    {
        var generated = Generate("""
            public sealed class R
            {
                [Reducer]
                public void Register(ReducerContext ctx, string email)
                {
                    foreach (var def in ctx.Db.ItemDef.Iter()) { }
                    ctx.Db.Account.Insert(new Account { Id = 1, Email = email });
                }
            }
            """);
        Assert.Equal("Hub", SiteOf(generated, "Register"));
    }

    [Fact]
    public void A_reducer_touching_a_partitioned_table_is_shard_executed()
    {
        var generated = Generate("""
            public sealed class R
            {
                [Reducer]
                public void Gather(ReducerContext ctx, long creatureId)
                {
                    var creature = ctx.Db.Creature.Id.Find(creatureId);
                    var def = ctx.Db.ItemDef.Id.Find(1L);
                }
            }
            """);
        Assert.Equal("Shard", SiteOf(generated, "Gather"));
    }

    [Fact]
    public void Generic_IDbView_calls_count_as_touches_of_their_type_argument()
    {
        var generated = Generate("""
            public sealed class R
            {
                [Reducer]
                public void Audit(ReducerContext ctx)
                {
                    foreach (var account in ctx.Db.Scan<Account>()) { }
                }
            }
            """);
        Assert.Equal("Hub", SiteOf(generated, "Audit"));
    }

    [Fact]
    public void A_body_passing_ctx_to_a_helper_is_opaque_and_shard_executed()
    {
        var generated = Generate("""
            public sealed class R
            {
                [Reducer]
                public void Delegating(ReducerContext ctx)
                {
                    Helper(ctx);
                }

                private static void Helper(ReducerContext ctx) =>
                    ctx.Db.Account.Insert(new Account { Id = 2, Email = "x" });
            }
            """);
        Assert.Equal("Shard", SiteOf(generated, "Delegating"));
    }

    [Fact]
    public void An_explicit_site_declaration_wins_over_the_analysis()
    {
        var generated = Generate("""
            public sealed class R
            {
                [Reducer(Site = ReducerSite.Hub)]
                public void Escalated(ReducerContext ctx)
                {
                    Helper(ctx);
                }

                private static void Helper(ReducerContext ctx) =>
                    ctx.Db.Account.Insert(new Account { Id = 3, Email = "y" });
            }
            """);
        Assert.Equal("Hub", SiteOf(generated, "Escalated"));
    }

    [Fact]
    public void Lifecycle_reducers_are_hub_executed()
    {
        var generated = Generate("""
            public sealed class R
            {
                [Reducer(ReducerKind.ClientConnected)]
                public void OnConnected(ReducerContext ctx)
                {
                    var creature = ctx.Db.Creature.Id.Find(1L);
                }
            }
            """);
        Assert.Equal("Hub", SiteOf(generated, "OnConnected"));
    }
}
