using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The pinned read view: what a snapshot-isolated reducer body will read through. The property under
/// test throughout is that a view observes the LSN it was opened at and never a later one — however
/// long it is held, however lazily its enumerations are consumed, and whatever the store is doing
/// meanwhile. Reading the live store across an <c>Apply</c> is the thing this exists to replace;
/// it throws "collection was modified" at best.
/// </summary>
public class HotStoreReadViewTests : IDisposable
{
    private static readonly TableId Players = TableId.FromName("Player");

    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    private IReadViewSource Source => (IReadViewSource)_harness.Engine.HotStore;

    [Fact]
    public void A_view_does_not_see_a_row_inserted_after_it_opened()
    {
        Join("first", room: 7);
        using var view = Source.OpenReadView();

        Join("second", room: 7);

        Assert.Equal(1, view.Count(Players));
        Assert.Equal(2, _harness.Engine.HotStore.Count(Players));
        Assert.False(view.TryGetRow(Players, Key("second"), out _));
        Assert.True(_harness.Engine.HotStore.TryGetRow(Players, Key("second"), out _));
    }

    [Fact]
    public void A_view_still_sees_a_row_deleted_after_it_opened()
    {
        Join("doomed", room: 7);
        using var view = Source.OpenReadView();

        _harness.Invoke("Leave", ctx => Assert.True(ctx.Db.Delete<Player>(Identity.Hash("doomed"))));

        Assert.True(view.TryGetRow(Players, Key("doomed"), out _));
        Assert.False(_harness.Engine.HotStore.TryGetRow(Players, Key("doomed"), out _));
        Assert.Equal(1, view.Count(Players));
        Assert.Equal(0, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void A_lazy_scan_held_across_an_apply_completes_with_the_row_set_it_started_on()
    {
        for (var i = 0; i < 20; i++)
            Join($"p{i:D2}", room: 7);

        using var view = Source.OpenReadView();
        using var scan = view.Scan(Players).GetEnumerator();

        // One row consumed, nineteen still to come — the window in which the old store would have
        // invalidated the enumerator.
        Assert.True(scan.MoveNext());

        for (var i = 20; i < 40; i++)
            Join($"p{i:D2}", room: 7);
        _harness.Invoke("Purge", ctx => Assert.True(ctx.Db.Delete<Player>(Identity.Hash("p05"))));

        var seen = 1;
        while (scan.MoveNext())
            seen++;

        Assert.Equal(20, seen);
        Assert.Equal(39, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void An_index_scan_resolves_against_the_pinned_version_not_the_live_one()
    {
        Join("mover", room: 7);
        using var view = Source.OpenReadView();

        _harness.Invoke("Move", ctx =>
        {
            var player = ctx.Db.Find<Player>(Identity.Hash("mover"))!.Value;
            ctx.Db.Update(player with { RoomId = 8 });
        });

        Assert.Single(view.ScanIndex(Players, "RoomId", Room(7)));
        Assert.Empty(view.ScanIndex(Players, "RoomId", Room(8)));
        Assert.Empty(_harness.Engine.HotStore.ScanIndex(Players, "RoomId", Room(7)));
        Assert.Single(_harness.Engine.HotStore.ScanIndex(Players, "RoomId", Room(8)));
    }

    [Fact]
    public void An_index_range_scan_is_pinned_too()
    {
        Join("a", room: 3);
        Join("b", room: 5);
        using var view = Source.OpenReadView();

        Join("c", room: 4);

        Assert.Equal(2, view.ScanIndexRange(Players, "RoomId", Room(0), Room(9)).Count());
        Assert.Equal(3, _harness.Engine.HotStore.ScanIndexRange(Players, "RoomId", Room(0), Room(9)).Count());
    }

    [Fact]
    public void A_key_walk_is_pinned_and_stays_in_primary_key_order()
    {
        Join("a", room: 1);
        Join("b", room: 1);
        using var view = Source.OpenReadView();

        Join("c", room: 1);

        var keys = view.ScanKeys(Players).ToList();
        Assert.Equal(2, keys.Count);
        Assert.Equal(keys.OrderBy(k => k).ToList(), keys);
        Assert.Equal(3, _harness.Engine.HotStore.ScanKeys(Players).Count());
    }

    [Fact]
    public void The_view_reports_the_lsn_it_was_pinned_at()
    {
        Join("first", room: 7);
        using var view = Source.OpenReadView();
        var pinned = _harness.Engine.HotStore.AppliedLsn;

        Join("second", room: 7);

        Assert.Equal(pinned, view.Lsn);
        Assert.NotEqual(pinned, _harness.Engine.HotStore.AppliedLsn);
    }

    [Fact]
    public void A_disposed_view_refuses_reads_rather_than_serving_them_from_a_released_pin()
    {
        Join("first", room: 7);
        var view = Source.OpenReadView();
        view.Dispose();

        Assert.Throws<ObjectDisposedException>(() => view.Count(Players));
        Assert.Throws<ObjectDisposedException>(() => view.Scan(Players));
        Assert.Throws<ObjectDisposedException>(() => view.ScanKeys(Players));
        Assert.Throws<ObjectDisposedException>(() => view.ScanIndex(Players, "RoomId", Room(7)));
        Assert.Throws<ObjectDisposedException>(() => view.TryGetRow(Players, Key("first"), out _));
    }

    [Fact]
    public void Two_views_opened_at_different_lsns_disagree_and_both_are_right()
    {
        Join("first", room: 7);
        using var early = Source.OpenReadView();
        Join("second", room: 7);
        using var late = Source.OpenReadView();
        Join("third", room: 7);

        Assert.Equal(1, early.Count(Players));
        Assert.Equal(2, late.Count(Players));
        Assert.Equal(3, _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void A_scan_racing_a_writer_on_another_thread_completes_with_the_pinned_row_set()
    {
        for (var i = 0; i < 200; i++)
            Join($"p{i:D3}", room: 7);

        using var view = Source.OpenReadView();
        using var writing = new CancellationTokenSource();
        using var firstCommit = new ManualResetEventSlim();
        var written = 0;
        var writer = new Thread(() =>
        {
            var i = 1000;
            while (!writing.IsCancellationRequested)
            {
                Join($"p{i++:D4}", room: 9);
                Interlocked.Increment(ref written);
                firstCommit.Set();
            }
        })
        {
            IsBackground = true,
        };

        writer.Start();
        try
        {
            // Wait for the writer to be genuinely running before scanning. Two hundred rows take
            // microseconds and a thread takes longer to start, so without this the scan finishes
            // first and the test passes having raced nothing at all.
            Assert.True(
                firstCommit.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken),
                "the writer thread never committed");

            // Interleaved deliberately: the scan yields lazily while commits land underneath it,
            // which against the live store is precisely the "collection was modified" case.
            var seen = 0;
            foreach (var _ in view.Scan(Players))
            {
                seen++;
                if (seen % 20 == 0)
                    Thread.Yield();
            }

            Assert.Equal(200, seen);
            Assert.Equal(200, view.Count(Players));
        }
        finally
        {
            writing.Cancel();
            Assert.True(writer.Join(TimeSpan.FromSeconds(10)), "the writer thread did not stop");
        }

        Assert.Equal(200 + Volatile.Read(ref written), _harness.Engine.HotStore.Count(Players));
    }

    [Fact]
    public void Recovery_rebuilds_a_store_whose_views_still_pin_correctly()
    {
        Join("first", room: 7);
        Join("second", room: 8);
        _harness.Restart();

        using var view = ((IReadViewSource)_harness.Engine.HotStore).OpenReadView();
        Join("third", room: 9);

        Assert.Equal(2, view.Count(Players));
        Assert.Single(view.ScanIndex(Players, "RoomId", Room(8)));
        Assert.Empty(view.ScanIndex(Players, "RoomId", Room(9)));
        Assert.Equal(3, _harness.Engine.HotStore.Count(Players));
    }

    private void Join(string name, int room) =>
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player
        {
            Id = Identity.Hash(name),
            RoomId = room,
            Name = name,
        }));

    private static RowKey Key(string name)
    {
        var schema = SchemaFor();
        return SchemaKeyCodec.Encode(schema.PrimaryKey, Identity.Hash(name));
    }

    private static RowKey Room(int room)
    {
        var schema = SchemaFor();
        return SchemaKeyCodec.Encode(schema.Column("RoomId"), room);
    }

    private static TableSchema SchemaFor()
    {
        var registry = SchemaRegistry.FromTypes(typeof(Player));
        Assert.True(registry.TryGetByName("Player", out var schema));
        return schema;
    }
}
