using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The pending side of a reducer's index reads, and the one thing about it no result assertion
/// checks.
/// <para>
/// A reducer that inserts N rows into a table with a <c>[Unique]</c> column, or filters an indexed
/// column once per row it stages, used to decode every pending row of that table on every check —
/// the results were right and the cost was N²/2 decodes under the write lock. The write set now
/// carries an index overlay maintained from the typed row the reducer handed in, so the pending
/// side is a lookup. The cost claim is pinned by counting decodes through the table's codec, the
/// rest by the overlay's correctness: what a stage, a re-stage, a delete and a delete-then-insert
/// leave visible.
/// </para>
/// </summary>
public class PendingIndexOverlayTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-overlay-").FullName;
    private readonly SchemaRegistry _schema = CountingCodec<Player>.Registry(typeof(Player), typeof(InventoryItem), typeof(Registration));
    private readonly MelangeEngine _engine;

    public PendingIndexOverlayTests()
    {
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(_root, "hot") },
            CommitLog = { Path = Path.Combine(_root, "log"), FsyncPolicy = FsyncPolicy.OsBuffered },
            Snapshots = { Enabled = false },
        };
        _engine = new MelangeEngine(options, _schema, NullLoggerFactory.Instance);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A store handle Windows has not released yet; the temp reaper gets it.
        }
    }

    [Fact]
    public void Staging_rows_with_a_unique_column_decodes_none_of_the_pending_ones()
    {
        var counter = CountingCodec<Player>.CounterFor(_schema, typeof(Registration));

        // Counted inside the reducer: once it commits, the store decodes each row once to maintain
        // the unique index, which is the store's cost and not the question here.
        const int rows = 400;
        long duringReducer = -1;
        Invoke("Register", ctx =>
        {
            counter.Reset();
            for (var i = 0; i < rows; i++)
                ctx.Db.Insert(new Registration { Id = i + 1, Email = $"u{i}@example.com", CreatedAt = ctx.Timestamp });
            duringReducer = counter.Deserializations;
        });

        // Each insert checks its email against the pending rows. That used to decode every one of
        // them: 400 inserts, ~80,000 decodes. The overlay answers from the values the reducer
        // staged, so the whole reducer decodes nothing.
        Assert.Equal(0, duringReducer);
        Assert.Equal(rows, _engine.CommittedView.Scan<Registration>().Count());
    }

    [Fact]
    public void Unique_violations_are_still_caught_against_pending_and_stored_rows()
    {
        Invoke("Seed", ctx => ctx.Db.Insert(new Registration { Id = 1, Email = "a@example.com", CreatedAt = ctx.Timestamp }));

        // Against a stored row.
        var stored = Assert.Throws<InvalidOperationException>(() =>
            Invoke("Dup", ctx => ctx.Db.Insert(new Registration { Id = 2, Email = "a@example.com", CreatedAt = ctx.Timestamp })));
        Assert.Contains("unique constraint", stored.Message);

        // Against a pending row in the same transaction.
        var pending = Assert.Throws<InvalidOperationException>(() =>
            Invoke("DupPending", ctx =>
            {
                ctx.Db.Insert(new Registration { Id = 3, Email = "b@example.com", CreatedAt = ctx.Timestamp });
                ctx.Db.Insert(new Registration { Id = 4, Email = "b@example.com", CreatedAt = ctx.Timestamp });
            }));
        Assert.Contains("unique constraint", pending.Message);

        // A pending update that moves the value frees it; a pending delete of the holder frees it;
        // and a row may keep its own value across an update.
        Invoke("Move", ctx =>
        {
            ctx.Db.Update(new Registration { Id = 1, Email = "moved@example.com", CreatedAt = ctx.Timestamp });
            ctx.Db.Insert(new Registration { Id = 5, Email = "a@example.com", CreatedAt = ctx.Timestamp });
            ctx.Db.Update(new Registration { Id = 5, Email = "a@example.com", CreatedAt = ctx.Timestamp });
        });
        Invoke("DeleteThenReuse", ctx =>
        {
            Assert.True(ctx.Db.Delete<Registration>(5L));
            ctx.Db.Insert(new Registration { Id = 6, Email = "a@example.com", CreatedAt = ctx.Timestamp });
        });

        var emails = _engine.CommittedView.Scan<Registration>().Select(r => (r.Id, r.Email)).OrderBy(r => r.Id).ToList();
        Assert.Equal([(1L, "moved@example.com"), (6L, "a@example.com")], emails);

        // An insert that was deleted inside the transaction leaves its value free again.
        Invoke("InsertThenDelete", ctx =>
        {
            ctx.Db.Insert(new Registration { Id = 7, Email = "c@example.com", CreatedAt = ctx.Timestamp });
            Assert.True(ctx.Db.Delete<Registration>(7L));
            ctx.Db.Insert(new Registration { Id = 8, Email = "c@example.com", CreatedAt = ctx.Timestamp });
        });
        Assert.Equal("c@example.com", _engine.CommittedView.Find<Registration>(8L)!.Value.Email);
    }

    [Fact]
    public void Filter_answers_from_the_overlay_and_decodes_only_what_it_returns()
    {
        var owner = Identity.Hash("owner");
        var other = Identity.Hash("other");
        var counter = CountingCodec<Player>.CounterFor(_schema, typeof(InventoryItem));

        Invoke("Stock", ctx =>
        {
            for (var i = 0; i < 200; i++)
                ctx.Db.Insert(new InventoryItem { Id = (ulong)(i + 1), Owner = i % 10 == 0 ? owner : other, ItemName = $"i{i}", Quantity = i });

            counter.Reset();
            var mine = ctx.Db.Filter<InventoryItem>(nameof(InventoryItem.Owner), owner).ToList();
            Assert.Equal(20, mine.Count);
            Assert.All(mine, item => Assert.Equal(owner, item.Owner));

            // Twenty rows came back, so twenty decodes: the ones materialized for the caller. The
            // pending scan this replaced decoded all two hundred to find them.
            Assert.Equal(20, counter.Deserializations);
        });
    }

    [Fact]
    public void The_overlay_follows_updates_deletes_and_reinserts_across_stored_and_pending_rows()
    {
        var a = Identity.Hash("a");
        var b = Identity.Hash("b");
        Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new InventoryItem { Id = 1, Owner = a, ItemName = "stored-a", Quantity = 1 });
            ctx.Db.Insert(new InventoryItem { Id = 2, Owner = b, ItemName = "stored-b", Quantity = 1 });
        });

        Invoke("Mutate", ctx =>
        {
            // A pending update of a stored row: the store's hit is superseded by the pending version.
            ctx.Db.Update(new InventoryItem { Id = 1, Owner = b, ItemName = "moved-to-b", Quantity = 2 });
            // A pending insert, then moved.
            ctx.Db.Insert(new InventoryItem { Id = 3, Owner = a, ItemName = "new-a", Quantity = 3 });
            ctx.Db.Update(new InventoryItem { Id = 3, Owner = b, ItemName = "new-then-b", Quantity = 3 });
            // A stored row deleted, then re-inserted under the other owner.
            Assert.True(ctx.Db.Delete<InventoryItem>(2UL));
            ctx.Db.Insert(new InventoryItem { Id = 2, Owner = a, ItemName = "reborn-a", Quantity = 4 });
            // A pending insert deleted again: gone from both owners.
            ctx.Db.Insert(new InventoryItem { Id = 4, Owner = a, ItemName = "ghost", Quantity = 0 });
            Assert.True(ctx.Db.Delete<InventoryItem>(4UL));

            Assert.Equal(["reborn-a"], Names(ctx.Db.Filter<InventoryItem>(nameof(InventoryItem.Owner), a)));
            Assert.Equal(["moved-to-b", "new-then-b"], Names(ctx.Db.Filter<InventoryItem>(nameof(InventoryItem.Owner), b)));
        });

        // And the same through FilterRange on an indexed integer column, against pending rows only.
        Invoke("Rooms", ctx =>
        {
            for (var i = 0; i < 30; i++)
                ctx.Db.Insert(new Player { Id = Identity.Hash($"p{i}"), RoomId = i, Name = $"p{i}" });
            ctx.Db.Update(new Player { Id = Identity.Hash("p5"), RoomId = 50, Name = "p5" });
            Assert.True(ctx.Db.Delete<Player>(Identity.Hash("p6")));

            var rooms = ctx.Db.FilterRange<Player>(nameof(Player.RoomId), 4, 8).Select(p => p.RoomId).OrderBy(r => r).ToList();
            Assert.Equal([4, 7, 8], rooms);
            Assert.Equal([50], ctx.Db.FilterRange<Player>(nameof(Player.RoomId), 49, 51).Select(p => p.RoomId).ToList());
        });
    }

    private static List<string> Names(IEnumerable<InventoryItem> items) =>
        items.Select(i => i.ItemName).OrderBy(n => n, StringComparer.Ordinal).ToList();

    private void Invoke(string name, Action<ReducerContext> body) =>
        _engine.Invoke(name, EngineHarness.Caller, body);
}
