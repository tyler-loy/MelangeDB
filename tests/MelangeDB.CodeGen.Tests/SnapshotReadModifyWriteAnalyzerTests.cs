using Xunit;

namespace MelangeDB.CodeGen.Tests;

/// <summary>
/// MELANGE0023: a row read with Find and written back with Update inside a reducer declared
/// <c>Isolation.Snapshot</c> fires; the same body serialized is silent; and rows the body did not
/// Find — recomputed rows, and rows from a sweep's own iteration — are silent, because those are
/// the shapes the isolation level exists for.
/// </summary>
public class SnapshotReadModifyWriteAnalyzerTests
{
    private const string Prologue = """
        using MelangeDB;

        namespace Game;

        [Table]
        public partial struct Player
        {
            [PrimaryKey] public ulong Id;
            [Unique] public string Name;
            [Index] public int RoomId;
            public float X;
            public float Y;
        }

        """;

    [Fact]
    public async Task Find_then_update_in_snapshot_reducer_fires()
    {
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void Move(ReducerContext ctx, ulong id, float x, float y)
                {
                    var player = ctx.Db.Player.Id.Find(id) ?? throw new RejectedException("not joined");
                    ctx.Db.Player.Update(player with { X = x, Y = y });
                }
            }
            """);
        var diagnostic = Assert.Single(diagnostics, d => d.Id == "MELANGE0023");
        Assert.Contains("Move", diagnostic.GetMessage());
        Assert.Contains("Player", diagnostic.GetMessage());
    }

    [Fact]
    public async Task Same_body_without_snapshot_isolation_is_silent()
    {
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer]
                public void Move(ReducerContext ctx, ulong id, float x, float y)
                {
                    var player = ctx.Db.Player.Id.Find(id) ?? throw new RejectedException("not joined");
                    ctx.Db.Player.Update(player with { X = x, Y = y });
                }

                [Reducer(Isolation = Isolation.Serialized)]
                public void MoveExplicit(ReducerContext ctx, ulong id, float x, float y)
                {
                    var player = ctx.Db.Player.Id.Find(id) ?? throw new RejectedException("not joined");
                    ctx.Db.Player.Update(player with { X = x, Y = y });
                }
            }
            """);
        Assert.DoesNotContain(diagnostics, d => d.Id == "MELANGE0023");
    }

    [Fact]
    public async Task Update_through_nullable_value_fires()
    {
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void Move(ReducerContext ctx, ulong id, float x)
                {
                    var found = ctx.Db.Player.Id.Find(id);
                    if (found is null)
                        return;
                    ctx.Db.Player.Update(found.Value with { X = x });
                }
            }
            """);
        Assert.Single(diagnostics, d => d.Id == "MELANGE0023");
    }

    [Fact]
    public async Task Update_of_pattern_bound_find_result_fires()
    {
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void Move(ReducerContext ctx, ulong id, float x)
                {
                    if (ctx.Db.Player.Id.Find(id) is { } player)
                        ctx.Db.Player.Update(player with { X = x });
                }
            }
            """);
        Assert.Single(diagnostics, d => d.Id == "MELANGE0023");
    }

    [Fact]
    public async Task Find_on_unique_column_then_update_fires()
    {
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void Rename(ReducerContext ctx, string name)
                {
                    var player = ctx.Db.Player.Name.Find(name) ?? throw new RejectedException("unknown");
                    ctx.Db.Player.Update(player with { Name = name + "!" });
                }
            }
            """);
        Assert.Single(diagnostics, d => d.Id == "MELANGE0023");
    }

    [Fact]
    public async Task Update_of_recomputed_row_is_silent()
    {
        // The eligible shape: the row's every column comes from the reducer's own inputs, so a
        // concurrent commit to it is overwritten by a defensible answer, not a stale copy.
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void Respawn(ReducerContext ctx, ulong id, int roomId)
                {
                    var player = new Player { Id = id, Name = "p" + id, RoomId = roomId, X = 0f, Y = 0f };
                    ctx.Db.Player.Update(player);
                }
            }
            """);
        Assert.DoesNotContain(diagnostics, d => d.Id == "MELANGE0023");
    }

    [Fact]
    public async Task Update_of_swept_rows_is_silent()
    {
        // Deliberate: rows from the body's own iteration are not tracked, because updating rows
        // mid-sweep is what the legitimate recompute sweeps do every tick. Only the single-row
        // Find-then-Update shape is detectable without drowning the real customers in warnings.
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void SweepRoom(ReducerContext ctx, int roomId)
                {
                    foreach (var player in ctx.Db.Player.RoomId.Filter(roomId))
                        ctx.Db.Player.Update(player with { X = 0f, Y = 0f });
                }
            }
            """);
        Assert.DoesNotContain(diagnostics, d => d.Id == "MELANGE0023");
    }

    [Fact]
    public async Task Find_then_delete_is_silent()
    {
        // A delete carries no stale columns to write back; if the row changed meanwhile, the
        // delete still expresses the body's decision, and reconcile drops it if already gone.
        var diagnostics = await GeneratorTestHost.RunSnapshotRmwAnalyzerAsync(Prologue + """
            public sealed class Reducers
            {
                [Reducer(Isolation = Isolation.Snapshot)]
                public void Kick(ReducerContext ctx, ulong id)
                {
                    var player = ctx.Db.Player.Id.Find(id);
                    if (player is not null)
                        ctx.Db.Player.Id.Delete(player.Value.Id);
                }
            }
            """);
        Assert.DoesNotContain(diagnostics, d => d.Id == "MELANGE0023");
    }
}
