using Xunit;

namespace MelangeDB.Core.Tests;

public class TransactionTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void Reducer_inserts_updates_deletes_and_reads_its_own_writes()
    {
        var alice = Identity.Hash("alice");
        var bob = Identity.Hash("bob");

        _harness.Invoke("Setup", ctx =>
        {
            ctx.Db.Insert(new Player { Id = alice, RoomId = 1, X = 1f, Y = 2f, Name = "Alice" });
            ctx.Db.Insert(new Player { Id = bob, RoomId = 1, X = 3f, Y = 4f, Name = "Bob" });

            // Read-your-writes: the uncommitted insert is visible through the overlay.
            var pending = ctx.Db.Find<Player>(alice);
            Assert.NotNull(pending);
            Assert.Equal("Alice", pending.Value.Name);

            var moved = pending.Value with { X = 10f };
            ctx.Db.Update(moved);
            Assert.Equal(10f, ctx.Db.Find<Player>(alice)!.Value.X);

            Assert.True(ctx.Db.Delete<Player>(bob));
            Assert.Null(ctx.Db.Find<Player>(bob));
            Assert.Single(ctx.Db.Scan<Player>());
        });

        // And after commit, through the store.
        _harness.Invoke("Verify", ctx =>
        {
            var players = ctx.Db.Scan<Player>().ToList();
            var alicePlayer = Assert.Single(players);
            Assert.Equal(10f, alicePlayer.X);
            Assert.Null(ctx.Db.Find<Player>(bob));
        });
    }

    [Fact]
    public void Throwing_reducer_leaves_zero_trace()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));
        var before = _harness.Dump();
        var headBefore = _harness.Engine.Log.HeadLsn;

        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Failing", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("q"), RoomId = 2, Name = "Q" });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("q"), ItemName = "sword", Quantity = 1 });
            throw new InvalidOperationException("boom");
        }));

        Assert.Equal(before, _harness.Dump());
        Assert.Equal(headBefore, _harness.Engine.Log.HeadLsn);

        // No consumed AutoInc value: the id the aborted insert briefly held is handed out again.
        _harness.Invoke("Next", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("r"), ItemName = "axe", Quantity = 1 });
            Assert.Equal(1UL, item.Id);
        });
    }

    [Fact]
    public void Rejected_reducer_aborts_and_rethrows()
    {
        var before = _harness.Dump();
        var exception = Assert.Throws<RejectedException>(() => _harness.Invoke("Reject", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" });
            throw new RejectedException("PvP is off");
        }));
        Assert.Equal("PvP is off", exception.Message);
        Assert.Equal(before, _harness.Dump());
    }

    [Fact]
    public void Two_tables_mutated_in_one_reducer_commit_atomically()
    {
        var owner = Identity.Hash("owner");
        _harness.Invoke("Both", ctx =>
        {
            ctx.Db.Insert(new Player { Id = owner, RoomId = 1, Name = "Owner" });
            ctx.Db.Insert(new InventoryItem { Owner = owner, ItemName = "pick", Quantity = 1 });
        });
        Assert.Equal(1UL, _harness.Engine.Log.HeadLsn);
        _harness.Invoke("Verify", ctx =>
        {
            Assert.NotNull(ctx.Db.Find<Player>(owner));
            Assert.Single(ctx.Db.Scan<InventoryItem>());
        });

        // And neither lands when the reducer throws after touching both.
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("BothFail", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("other"), RoomId = 2, Name = "Other" });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("other"), ItemName = "torch", Quantity = 9 });
            throw new InvalidOperationException("late failure");
        }));
        _harness.Invoke("Verify2", ctx =>
        {
            Assert.Null(ctx.Db.Find<Player>(Identity.Hash("other")));
            Assert.Single(ctx.Db.Scan<InventoryItem>());
        });
    }

    [Fact]
    public void Nested_reducer_calls_are_forbidden()
    {
        var exception = Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Outer", _ =>
            _harness.Invoke("Inner", _ => { })));
        Assert.Contains("Nested reducer calls are forbidden", exception.Message);
    }

    [Fact]
    public void Duplicate_insert_throws_and_update_of_missing_row_throws()
    {
        var id = Identity.Hash("p");
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = id, RoomId = 1, Name = "P" }));
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Dup", ctx =>
            ctx.Db.Insert(new Player { Id = id, RoomId = 2, Name = "Q" })));
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Missing", ctx =>
            ctx.Db.Update(new Player { Id = Identity.Hash("nobody"), RoomId = 1, Name = "X" })));
    }

    [Fact]
    public void Write_set_collapses_to_one_op_per_row()
    {
        var id = Identity.Hash("p");
        _harness.Invoke("Churn", ctx =>
        {
            var player = ctx.Db.Insert(new Player { Id = id, RoomId = 1, X = 0f, Name = "P" });
            ctx.Db.Update(player with { X = 1f });
            ctx.Db.Update(player with { X = 2f });
        });

        var record = _harness.Engine.Log.ReadFrom(1).Single();
        var op = Assert.Single(record.WriteSet);
        Assert.Equal(RowOpKind.Insert, op.Kind);
        _harness.Invoke("Verify", ctx => Assert.Equal(2f, ctx.Db.Find<Player>(id)!.Value.X));
    }

    [Fact]
    public void Insert_then_delete_nets_to_nothing_and_appends_no_record()
    {
        var head = _harness.Engine.Log.HeadLsn;
        _harness.Invoke("Churn", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "ghost", Quantity = 1 });
            Assert.True(ctx.Db.Delete<InventoryItem>(item.Id));
        });
        Assert.Equal(head, _harness.Engine.Log.HeadLsn);
        _harness.Invoke("Verify", ctx => Assert.Empty(ctx.Db.Scan<InventoryItem>()));
    }

    [Fact]
    public void Delete_then_reinsert_collapses_to_update()
    {
        var id = Identity.Hash("p");
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = id, RoomId = 1, Name = "Old" }));
        _harness.Invoke("Replace", ctx =>
        {
            Assert.True(ctx.Db.Delete<Player>(id));
            ctx.Db.Insert(new Player { Id = id, RoomId = 2, Name = "New" });
        });

        var record = _harness.Engine.Log.ReadFrom(2).Single();
        var op = Assert.Single(record.WriteSet);
        Assert.Equal(RowOpKind.Update, op.Kind);
        _harness.Invoke("Verify", ctx => Assert.Equal("New", ctx.Db.Find<Player>(id)!.Value.Name));
    }

    [Fact]
    public void Read_only_reducer_appends_nothing()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("p"), RoomId = 1, Name = "P" }));
        var head = _harness.Engine.Log.HeadLsn;
        _harness.Invoke("ReadOnly", ctx => Assert.Single(ctx.Db.Scan<Player>()));
        Assert.Equal(head, _harness.Engine.Log.HeadLsn);
    }

    [Fact]
    public void Context_supplies_caller_timestamp_and_seeded_random()
    {
        _harness.Invoke("Context", ctx =>
        {
            Assert.Equal(EngineHarness.Caller, ctx.Caller);
            Assert.True(ctx.Timestamp.UnixTimeMicroseconds > 0);
            Assert.NotNull(ctx.Random);
            Assert.Equal(ConnectionId.None, ctx.ConnectionId);
        });
    }

    [Fact]
    public void Enum_blob_and_float_columns_round_trip()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new TerrainChunk
        {
            ChunkId = -42,
            Data = [1, 2, 3, 255],
            Kind = ChunkKind.Ore,
        }));
        _harness.Invoke("Verify", ctx =>
        {
            var chunk = ctx.Db.Find<TerrainChunk>(-42L);
            Assert.NotNull(chunk);
            Assert.Equal(new byte[] { 1, 2, 3, 255 }, chunk.Value.Data);
            Assert.Equal(ChunkKind.Ore, chunk.Value.Kind);
        });
    }
}
