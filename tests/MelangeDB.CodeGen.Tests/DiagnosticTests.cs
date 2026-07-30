using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// Every MELANGE diagnostic has a test proving it fires and a test proving valid code compiles
/// clean. Ids are stable public API.
/// </summary>
public class DiagnosticTests
{
    [Fact]
    public void Melange0001_fires_on_table_without_primary_key()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct NoKey
            {
                public int Value;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0001");
    }

    [Fact]
    public void Melange0001_fires_on_table_with_two_primary_keys()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct TwoKeys
            {
                [PrimaryKey] public long A;
                [PrimaryKey] public long B;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0001");
    }

    [Fact]
    public void Melange0002_fires_on_autoinc_non_integer_column()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct BadCounter
            {
                [PrimaryKey] public long Id;
                [AutoInc] public string Serial;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0002");
    }

    [Fact]
    public void Melange0002_fires_on_autoinc_int_column()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct NarrowCounter
            {
                [PrimaryKey] public long Id;
                [AutoInc] public int Count;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0002");
    }

    [Fact]
    public void Melange0003_fires_on_unique_column_of_partitioned_table()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Account
            {
                [PrimaryKey] public long Id;
                [Unique] public string Email;
            }
            """);
        var diagnostic = Assert.Single(result.MelangeDiagnostics, d => d.Id == "MELANGE0003");
        Assert.Contains("CLUSTERING.md", diagnostic.GetMessage());
    }

    [Fact]
    public void Melange0003_is_silent_on_global_placement()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Placement = Placement.Global)]
            public partial struct Account
            {
                [PrimaryKey] public long Id;
                [Unique] public string Email;
            }
            """);
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Melange0004_fires_on_unserializable_reducer_parameter()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public void Misconfigured(ReducerContext ctx, System.Collections.Generic.List<int> values)
                {
                }
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0004");
    }

    [Fact]
    public async Task Melange0005_fires_on_datetime_now_in_reducer_body()
    {
        var diagnostics = await GeneratorTestHost.RunAnalyzerAsync("""
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public void Tick(ReducerContext ctx)
                {
                    var now = System.DateTime.Now;
                    var utc = System.DateTimeOffset.UtcNow;
                }
            }
            """);
        Assert.Equal(2, diagnostics.Count(d => d.Id == "MELANGE0005"));
    }

    [Fact]
    public async Task Melange0006_fires_on_new_random_in_reducer_body()
    {
        var diagnostics = await GeneratorTestHost.RunAnalyzerAsync("""
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public void Roll(ReducerContext ctx)
                {
                    var roll = new System.Random().Next(6);
                }
            }
            """);
        Assert.Contains(diagnostics, d => d.Id == "MELANGE0006");
    }

    [Fact]
    public void Melange0007_fires_on_serveronly_column_of_private_table()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Creature
            {
                [PrimaryKey] public long Id;
                [ServerOnly] public ulong NextThinkAt;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0007");
    }

    [Fact]
    public void Melange0007_is_silent_on_public_table()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Public = true)]
            public partial struct Creature
            {
                [PrimaryKey] public long Id;
                [ServerOnly] public ulong NextThinkAt;
            }
            """);
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Melange0008_fires_on_async_reducer()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using System.Threading.Tasks;
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public async Task Slow(ReducerContext ctx)
                {
                    await Task.Yield();
                }
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0008");
    }

    [Fact]
    public void Melange0009_fires_on_reducer_without_context_parameter()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public void NoContext(int x)
                {
                }
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0009");
    }

    [Fact]
    public void Melange0009_fires_on_static_reducer()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public static void Detached(ReducerContext ctx)
                {
                }
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0009");
    }

    [Fact]
    public async Task Melange0010_fires_on_known_io_types_in_reducer_body()
    {
        var diagnostics = await GeneratorTestHost.RunAnalyzerAsync("""
            using MelangeDB;

            public sealed class Reducers
            {
                [Reducer]
                public void Leaky(ReducerContext ctx)
                {
                    System.IO.File.ReadAllText("save.json");
                    System.Threading.Thread.Sleep(10);
                    System.Threading.Tasks.Task.Delay(10).Wait();
                }
            }
            """);
        Assert.True(diagnostics.Count(d => d.Id == "MELANGE0010") >= 3);
    }

    [Fact]
    public void Melange0011_fires_on_unsupported_column_type()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Event
            {
                [PrimaryKey] public long Id;
                public System.DateTime When;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0011");
    }

    [Fact]
    public void Melange0012_fires_on_float_primary_key()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table]
            public partial struct Position
            {
                [PrimaryKey] public float X;
            }
            """);
        Assert.Contains(result.MelangeDiagnostics, d => d.Id == "MELANGE0012");
    }

    [Fact]
    public void Melange0013_fires_on_two_tables_with_the_same_struct_name()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            namespace World
            {
                [Table]
                public partial struct Player
                {
                    [PrimaryKey] public long Id;
                }
            }

            namespace Lobby
            {
                [Table]
                public partial struct Player
                {
                    [PrimaryKey] public long Id;
                }
            }
            """);
        Assert.Equal(2, result.MelangeDiagnostics.Count(d => d.Id == "MELANGE0013"));
    }

    [Fact]
    public void Melange0013_fires_on_two_structs_declaring_the_same_table_name()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            [Table(Name = "player")]
            public partial struct PlayerRow
            {
                [PrimaryKey] public long Id;
            }

            [Table(Name = "player")]
            public partial struct PlayerRecord
            {
                [PrimaryKey] public long Id;
            }
            """);
        Assert.Equal(2, result.MelangeDiagnostics.Count(d => d.Id == "MELANGE0013"));
    }

    [Fact]
    public void Melange0013_is_silent_when_names_are_distinct()
    {
        var result = GeneratorTestHost.RunGenerator("""
            using MelangeDB;

            namespace World
            {
                [Table]
                public partial struct Player
                {
                    [PrimaryKey] public long Id;
                }
            }

            namespace Lobby
            {
                [Table]
                public partial struct LobbyPlayer
                {
                    [PrimaryKey] public long Id;
                }
            }
            """);
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task Valid_tables_and_reducers_compile_clean_with_zero_melange_diagnostics()
    {
        var source = """
            using MelangeDB;

            [Table(Public = true)]
            public partial struct Player
            {
                [PrimaryKey] public Identity Id;
                [Index] public int RoomId;
                public float X;
                public string Name;
                [ServerOnly] public ulong LastSeenTick;
            }

            [Table(Placement = Placement.Global, Tier = StorageTier.Relational)]
            public partial struct Registration
            {
                [PrimaryKey][AutoInc] public long Id;
                [Unique] public string Email;
                public Timestamp CreatedAt;
            }

            public sealed class Reducers
            {
                [Reducer]
                public void Move(ReducerContext ctx, int roomId, float x, string note, byte[] payload, int[] path, Placement kind)
                {
                    ctx.Db.Player.RoomId.Filter(roomId);
                    var when = ctx.Timestamp;
                    var jitter = ctx.Random.Next(3);
                }

                [Reducer(ReducerKind.ClientConnected)]
                public void OnConnected(ReducerContext ctx)
                {
                }
            }
            """;
        var result = GeneratorTestHost.RunGenerator(source);
        Assert.Empty(result.MelangeDiagnostics);
        Assert.Empty(result.Errors);

        var analyzerDiagnostics = await GeneratorTestHost.RunAnalyzerAsync(source);
        Assert.DoesNotContain(analyzerDiagnostics, d => d.Id.StartsWith("MELANGE", StringComparison.Ordinal));
    }
}
