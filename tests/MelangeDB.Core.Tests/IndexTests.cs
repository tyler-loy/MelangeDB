using Xunit;

namespace MelangeDB.Core.Tests;

public class IndexTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void Filter_by_indexed_column_sees_store_and_overlay_consistently()
    {
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("a"), RoomId = 1, Name = "A" });
            ctx.Db.Insert(new Player { Id = Identity.Hash("b"), RoomId = 1, Name = "B" });
            ctx.Db.Insert(new Player { Id = Identity.Hash("c"), RoomId = 2, Name = "C" });
        });

        _harness.Invoke("Overlay", ctx =>
        {
            // Committed state.
            Assert.Equal(2, ctx.Db.Filter<Player>(nameof(Player.RoomId), 1).Count());

            // A pending insert, a pending move out of the room, and a pending delete all resolve
            // through the overlay before the store's index.
            ctx.Db.Insert(new Player { Id = Identity.Hash("d"), RoomId = 1, Name = "D" });
            var b = ctx.Db.Find<Player>(Identity.Hash("b"))!.Value;
            ctx.Db.Update(b with { RoomId = 2 });
            ctx.Db.Delete<Player>(Identity.Hash("a"));

            var room1 = ctx.Db.Filter<Player>(nameof(Player.RoomId), 1).Select(p => p.Name).ToList();
            Assert.Equal(["D"], room1);
            var room2 = ctx.Db.Filter<Player>(nameof(Player.RoomId), 2).Select(p => p.Name).Order().ToList();
            Assert.Equal(["B", "C"], room2);
        });
    }

    [Fact]
    public void Indexes_survive_restart()
    {
        var owner = Identity.Hash("owner");
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new InventoryItem { Owner = owner, ItemName = "pick", Quantity = 1 });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("other"), ItemName = "axe", Quantity = 1 });
        });
        _harness.Restart();
        _harness.Invoke("Verify", ctx =>
        {
            var mine = Assert.Single(ctx.Db.Filter<InventoryItem>(nameof(InventoryItem.Owner), owner));
            Assert.Equal("pick", mine.ItemName);
        });
    }

    [Fact]
    public void Deleting_a_row_removes_it_from_its_indexes()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("a"), RoomId = 5, Name = "A" }));
        _harness.Invoke("Delete", ctx => Assert.True(ctx.Db.Delete<Player>(Identity.Hash("a"))));
        _harness.Invoke("Verify", ctx => Assert.Empty(ctx.Db.Filter<Player>(nameof(Player.RoomId), 5)));
    }

    [Fact]
    public void Unique_constraint_is_enforced_against_store_and_overlay()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp }));

        // Against the store.
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("DupStored", ctx =>
            ctx.Db.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp })));

        // Against a pending write in the same transaction.
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("DupPending", ctx =>
        {
            ctx.Db.Insert(new Registration { Email = "b@example.com", CreatedAt = ctx.Timestamp });
            ctx.Db.Insert(new Registration { Email = "b@example.com", CreatedAt = ctx.Timestamp });
        }));

        // Freed within the same transaction: the old holder changed its value first.
        _harness.Invoke("Reuse", ctx =>
        {
            var existing = Assert.Single(ctx.Db.Filter<Registration>(nameof(Registration.Email), "a@example.com"));
            ctx.Db.Update(existing with { Email = "moved@example.com" });
            ctx.Db.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp });
        });
    }

    [Fact]
    public void Filter_on_unindexed_column_throws()
    {
        var exception = Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Bad", ctx =>
            ctx.Db.Filter<Player>(nameof(Player.Name), "A").ToList()));
        Assert.Contains("not indexed", exception.Message);
    }

    [Fact]
    public void Filter_by_primary_key_column_works()
    {
        var id = Identity.Hash("a");
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = id, RoomId = 1, Name = "A" }));
        _harness.Invoke("Verify", ctx =>
        {
            var row = Assert.Single(ctx.Db.Filter<Player>(nameof(Player.Id), id));
            Assert.Equal("A", row.Name);
        });
    }
}
