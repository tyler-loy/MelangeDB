using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <see cref="MelangeEngine.OpenReadView"/>: committed state pinned at one LSN, readable off the
/// write lock — the scope a producer's call reads in, and what a policy evaluated before its
/// transaction now reads.
/// </summary>
public class PinnedReadViewTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void A_view_answers_for_the_lsn_it_pinned_and_not_a_later_one()
    {
        Join("first");
        using var view = _harness.Engine.OpenReadView();
        var pinned = view.Lsn;

        Join("second");

        Assert.Equal(1, view.Count<Player>());
        Assert.Null(view.Find<Player>(Identity.Hash("second")));
        Assert.Equal(pinned, view.Lsn);
        using var later = _harness.Engine.OpenReadView();
        Assert.Equal(2, later.Count<Player>());
        Assert.True(later.Lsn > pinned);
    }

    [Fact]
    public void A_view_cannot_write()
    {
        using var view = _harness.Engine.OpenReadView();

        Assert.Throws<InvalidOperationException>(() => view.Insert(new Player { Id = Identity.Hash("x"), Name = "x" }));
        Assert.Throws<InvalidOperationException>(() => view.Delete<Player>(Identity.Hash("x")));
    }

    [Fact]
    public void A_lazy_query_read_after_the_view_is_released_throws_rather_than_reading_a_released_pin()
    {
        Join("first");
        var view = _harness.Engine.OpenReadView();
        var lazy = view.Filter<Player>(nameof(Player.RoomId), 1);
        view.Dispose();

        var failure = Assert.Throws<ObjectDisposedException>(() => lazy.ToList());
        Assert.Contains("materialize", failure.Message);
    }

    [Fact]
    public void Concurrent_reads_of_one_view_from_many_threads_all_see_the_pinned_state()
    {
        for (var i = 0; i < 50; i++)
            Join($"player-{i}");
        using var view = _harness.Engine.OpenReadView();

        var counts = Enumerable.Range(0, 16)
            .AsParallel()
            .WithDegreeOfParallelism(8)
            .Select(_ => view.Scan<Player>().Count() + view.Filter<Player>(nameof(Player.RoomId), 1).Count())
            .ToArray();

        Assert.All(counts, count => Assert.Equal(100, count));
    }

    [Fact]
    public async Task A_view_never_sees_a_multi_table_commit_half_applied()
    {
        // Each commit writes one row to each of two tables. A view opened while the store applies
        // that commit, unlocked, can capture the first table after it and the second before it.
        using var stop = new CancellationTokenSource();
        var owner = Identity.Hash("owner");
        var writer = Task.Run(WriteUntilStopped, TestContext.Current.CancellationToken);

        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (DateTime.UtcNow < deadline)
            {
                using var view = _harness.Engine.OpenReadView();
                Assert.Equal(view.Count<Player>(), view.Count<InventoryItem>());
            }
        }
        finally
        {
            await stop.CancelAsync();
            await writer.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        }

        void WriteUntilStopped()
        {
            var n = 0;
            while (!stop.IsCancellationRequested)
            {
                var name = $"p{n++}";
                _harness.Invoke("Pair", ctx =>
                {
                    ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, Name = name });
                    ctx.Db.Insert(new InventoryItem { Owner = owner, ItemName = name, Quantity = 1 });
                });
            }
        }
    }

    [Fact]
    public void A_store_without_pinned_reads_refuses_a_view_and_ReadCommitted_falls_back_to_the_lock()
    {
        using var degraded = new EngineHarness(storeProvider: ReadViewlessStore.Provider);
        degraded.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("a"), RoomId = 1, Name = "a" }));

        Assert.False(degraded.Engine.SupportsReadViews);
        var failure = Assert.Throws<NotSupportedException>(() => degraded.Engine.OpenReadView());
        Assert.Contains("IReadViewSource", failure.Message);
        Assert.Equal(1, degraded.Engine.ReadCommitted(db => db.Count<Player>()));
    }

    [Fact]
    public void ReadCommitted_reads_through_a_pin_when_the_store_offers_one()
    {
        Join("first");

        Assert.True(_harness.Engine.SupportsReadViews);
        Assert.Equal(1, _harness.Engine.ReadCommitted(db => db.Count<Player>()));
    }

    private void Join(string name) =>
        _harness.Invoke("Join", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, Name = name }));
}
