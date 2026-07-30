using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>The generated typed accessors: Find by key, Filter by index, range Filter, unique Find.</summary>
public class TypedAccessorTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void Find_insert_update_and_delete_through_typed_accessors()
    {
        var id = Identity.Hash("p");
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Player.Insert(new Player { Id = id, RoomId = 1, Name = "P" });
            var found = ctx.Db.Player.Id.Find(id);
            Assert.NotNull(found);
            ctx.Db.Player.Update(found.Value with { RoomId = 2 });
        });
        _harness.Invoke("Verify", ctx =>
        {
            Assert.Equal(2, ctx.Db.Player.Id.Find(id)!.Value.RoomId);
            Assert.Single(ctx.Db.Player.Iter());
            Assert.True(ctx.Db.Player.Id.Delete(id));
        });
        _harness.Invoke("Gone", ctx => Assert.Null(ctx.Db.Player.Id.Find(id)));
    }

    [Fact]
    public void Filter_by_index_and_unique_find()
    {
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Player.Insert(new Player { Id = Identity.Hash("a"), RoomId = 1, Name = "A" });
            ctx.Db.Player.Insert(new Player { Id = Identity.Hash("b"), RoomId = 1, Name = "B" });
            ctx.Db.Player.Insert(new Player { Id = Identity.Hash("c"), RoomId = 2, Name = "C" });
            ctx.Db.Registration.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp });
        });
        _harness.Invoke("Verify", ctx =>
        {
            Assert.Equal(2, ctx.Db.Player.RoomId.Filter(1).Count());
            var registration = ctx.Db.Registration.Email.Find("a@example.com");
            Assert.NotNull(registration);
            Assert.Null(ctx.Db.Registration.Email.Find("nobody@example.com"));
        });
    }

    [Fact]
    public void Range_filter_spans_store_and_overlay()
    {
        _harness.Invoke("Seed", ctx =>
        {
            for (var room = 0; room < 10; room++)
                ctx.Db.Player.Insert(new Player { Id = Identity.Hash($"p{room}"), RoomId = room, Name = $"P{room}" });
        });
        _harness.Invoke("Range", ctx =>
        {
            Assert.Equal([3, 4, 5, 6], ctx.Db.Player.RoomId.Filter(3, 6).Select(p => p.RoomId).Order().ToList());

            // A pending move into the range and a pending delete out of it both resolve first.
            var p9 = ctx.Db.Player.Id.Find(Identity.Hash("p9"))!.Value;
            ctx.Db.Player.Update(p9 with { RoomId = 4 });
            ctx.Db.Player.Id.Delete(Identity.Hash("p3"));
            Assert.Equal([4, 4, 5, 6], ctx.Db.Player.RoomId.Filter(3, 6).Select(p => p.RoomId).Order().ToList());
        });

        // Negative-to-positive ranges exercise the sign-flipped key encoding.
        _harness.Invoke("Signs", ctx =>
        {
            ctx.Db.Player.Insert(new Player { Id = Identity.Hash("neg"), RoomId = -5, Name = "N" });
            Assert.Equal([-5, 0, 1], ctx.Db.Player.RoomId.Filter(-6, 1).Select(p => p.RoomId).Order().ToList());
        });
    }

    [Fact]
    public void Range_filter_on_primary_key_scans_by_key_order()
    {
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.TerrainChunk.Insert(new TerrainChunk { ChunkId = -10, Kind = ChunkKind.Rock, Data = [1] });
            ctx.Db.TerrainChunk.Insert(new TerrainChunk { ChunkId = 0, Kind = ChunkKind.Empty, Data = [2] });
            ctx.Db.TerrainChunk.Insert(new TerrainChunk { ChunkId = 15, Kind = ChunkKind.Ore, Data = [3] });
        });
        _harness.Invoke("Verify", ctx =>
        {
            var ids = ctx.Db.FilterRange<TerrainChunk>(nameof(TerrainChunk.ChunkId), -10L, 0L).Select(c => c.ChunkId).ToList();
            Assert.Equal([-10, 0], ids);
        });
    }
}
