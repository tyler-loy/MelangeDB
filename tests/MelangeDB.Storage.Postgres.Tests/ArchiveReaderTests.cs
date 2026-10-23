using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// <see cref="IArchiveReader"/> (road-to-0.3 phase 25): keyset-paginated reads of an archive table
/// outside a reducer, filtered on the key or an indexed column, with the unapplied log tail
/// overlaid so a just-archived row is never briefly invisible — or marked stale when the tail is
/// too long to overlay. <c>Stat</c> plays the archive table: an AutoInc key, an indexed Metric,
/// and a Timestamp to round-trip.
/// </summary>
[Collection(PostgresCollection.Name)]
public class ArchiveReaderTests(PostgresContainerFixture postgres)
{
    private static SchemaRegistry WithArchive() => TierHarness.RegistryWithTier("Stat", StorageTier.Archive);

    private static PostgresArchiveReader Reader(TierHarness harness, int maxOverlay = PostgresArchiveReader.MaxOverlayRecords) =>
        new(harness.Engine, harness.Connections, new StaticOptionsMonitor(harness.Options), maxOverlay);

    private static long Record(TierHarness harness, string metric, long value)
    {
        long id = 0;
        harness.Invoke("RecordStat", ctx => id = ctx.Db.Stat.Insert(new Stat { Metric = metric, Value = value, At = ctx.Timestamp }).Id);
        return id;
    }

    [Fact]
    public async Task Pages_newest_first_by_an_indexed_column_until_the_cursor_runs_out()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var gold = new List<long>();
        for (var i = 0; i < 5; i++)
        {
            gold.Add(Record(harness, "gold", i));
            Record(harness, "xp", i);
        }

        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        var reader = Reader(harness);
        var query = new ArchiveQuery { Column = "Metric", Equal = "gold", Descending = true, Limit = 2 };

        var seen = new List<long>();
        object? cursor = null;
        do
        {
            var page = await reader.ReadAsync<Stat>(query with { After = cursor }, TestContext.Current.CancellationToken);
            Assert.False(page.Stale);
            Assert.All(page.Rows, row => Assert.Equal("gold", row.Metric));
            seen.AddRange(page.Rows.Select(static row => row.Id));
            cursor = page.Next;
        }
        while (cursor is not null);

        Assert.Equal(gold.AsEnumerable().Reverse(), seen);
    }

    [Fact]
    public async Task Rows_round_trip_with_their_own_types()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var committed = default(Stat);
        harness.Invoke("RecordStat", ctx => committed = ctx.Db.Stat.Insert(new Stat { Metric = "m", Value = 42, At = ctx.Timestamp }));
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        var page = await Reader(harness).ReadAsync<Stat>(new ArchiveQuery(), TestContext.Current.CancellationToken);
        Assert.Equal(committed, Assert.Single(page.Rows));
    }

    [Fact]
    public async Task The_unapplied_tail_is_overlaid_and_a_long_one_is_marked_stale()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var first = Record(harness, "gold", 1);
        var second = Record(harness, "gold", 2);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // Postgres stops applying; the world keeps archiving and deleting.
        await harness.StopTierAsync();
        var third = Record(harness, "gold", 3);
        harness.Invoke("Erase", ctx => ctx.Db.Stat.Id.Delete(first));

        var query = new ArchiveQuery { Column = "Metric", Equal = "gold" };
        var overlaid = await Reader(harness).ReadAsync<Stat>(query, TestContext.Current.CancellationToken);
        Assert.False(overlaid.Stale);
        Assert.Equal([second, third], overlaid.Rows.Select(static row => row.Id));

        var stale = await Reader(harness, maxOverlay: 1).ReadAsync<Stat>(query, TestContext.Current.CancellationToken);
        Assert.True(stale.Stale);
        Assert.Equal([first, second], stale.Rows.Select(static row => row.Id));
    }

    [Fact]
    public async Task AsOfLsn_leaves_out_rows_committed_after_it_in_Postgres_and_in_the_overlay()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var first = Record(harness, "gold", 1);
        var second = Record(harness, "gold", 2);
        var asOf = harness.Engine.Log.HeadLsn;
        var applied = Record(harness, "gold", 3);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        await harness.StopTierAsync();
        var overlaid = Record(harness, "gold", 4);

        var reader = Reader(harness);
        var query = new ArchiveQuery { Column = "Metric", Equal = "gold" };
        var then = await reader.ReadAsync<Stat>(query with { AsOfLsn = asOf }, TestContext.Current.CancellationToken);
        var now = await reader.ReadAsync<Stat>(query, TestContext.Current.CancellationToken);

        Assert.Equal([first, second], then.Rows.Select(static row => row.Id));
        Assert.Equal([first, second, applied, overlaid], now.Rows.Select(static row => row.Id));
    }

    [Fact]
    public async Task A_hot_and_archive_read_at_one_lsn_counts_a_row_moved_in_between_exactly_once()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        harness.Invoke("Count", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = 7, Count = 70 }));
        using var view = harness.Engine.OpenReadView();

        // The sweep moves the row from hot to archive after the view pinned and before the archive read.
        harness.Invoke("Sweep", ctx =>
        {
            ctx.Db.HotCounter.Id.Delete(7L);
            ctx.Db.Stat.Insert(new Stat { Metric = "counter-7", Value = 70, At = ctx.Timestamp });
        });
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        var query = new ArchiveQuery { Column = "Metric", Equal = "counter-7" };
        var reader = Reader(harness);
        var hot = view.Find<HotCounter>(7L) is null ? 0 : 1;
        var asOf = await reader.ReadAsync<Stat>(query with { AsOfLsn = view.Lsn }, TestContext.Current.CancellationToken);
        var latest = await reader.ReadAsync<Stat>(query, TestContext.Current.CancellationToken);

        Assert.Equal(1, hot + asOf.Rows.Count);
        Assert.Equal(2, hot + latest.Rows.Count); // what an unpinned archive read would have double counted
    }

    [Fact]
    public async Task AsOfLsn_past_the_log_head_is_refused()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();

        var query = new ArchiveQuery { AsOfLsn = harness.Engine.Log.HeadLsn + 10 };
        await Assert.ThrowsAsync<ArgumentException>(() => Reader(harness).ReadAsync<Stat>(query, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Only_archive_tables_and_indexed_columns_are_readable()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var reader = Reader(harness);
        await Assert.ThrowsAsync<ArgumentException>(() => reader.ReadAsync<HotCounter>(new ArchiveQuery(), TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(() => reader.ReadAsync<Stat>(new ArchiveQuery { Column = "Value", Equal = 1L }, TestContext.Current.CancellationToken));
    }
}
