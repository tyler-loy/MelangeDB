using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The archive tier against Postgres (road-to-0.3 phase 25): archive rows reach Postgres through
/// the applier and nowhere else; a move from a backplane table is one atomic write set; a newly
/// declared archive table is registered, never copied; and a projection that would have to be
/// rebuilt over a truncated log refuses rather than start an archive with a hole (EventId 1611).
/// <c>HotCounter</c> plays the archive table, declared on that tier by the harness.
/// </summary>
[Collection(PostgresCollection.Name)]
public class ArchivePostgresTests
{
    private readonly PostgresContainerFixture _postgres;

    public ArchivePostgresTests(PostgresContainerFixture postgres) => _postgres = postgres;

    private static SchemaRegistry WithArchive() => TierHarness.RegistryWithTier("HotCounter", StorageTier.Archive);

    private static long HotRows<T>(TierHarness harness) =>
        harness.Engine.HotStore.Count(harness.Engine.Schema.Get(typeof(T)).Id);

    private static async Task<long> PostgresRowsAsync(TierHarness harness, string table) =>
        (long)(await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"{table}\""))!;

    [Fact]
    public async Task Archive_rows_reach_postgres_and_nowhere_else()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        for (var i = 1; i <= 4; i++)
            harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = i, Count = i * 10 }));
        harness.Invoke("Erase", ctx => ctx.Db.HotCounter.Id.Delete(4L));
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        Assert.Equal(0, HotRows<HotCounter>(harness));
        Assert.Equal(3L, await PostgresRowsAsync(harness, "HotCounter"));
        Assert.Equal(60L, await harness.ScalarAsync($"SELECT sum(\"Count\")::bigint FROM \"{harness.Schema}\".\"HotCounter\""));
    }

    [Fact]
    public async Task Moving_a_row_from_the_backplane_into_the_archive_lands_both_halves_together()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var stat = default(Stat);
        harness.Invoke("RecordStat", ctx => stat = ctx.Db.Stat.Insert(new Stat { Metric = "gold", Value = 250, At = ctx.Timestamp }));

        var moved = harness.Invoke("ArchiveOld", ctx =>
        {
            ctx.Db.Stat.Id.Delete(stat.Id);
            ctx.Db.HotCounter.Insert(new HotCounter { Id = stat.Id, Count = stat.Value });
        });
        await harness.WaitAppliedAsync(moved);

        Assert.Equal(0L, await PostgresRowsAsync(harness, "Stat"));
        Assert.Equal(1L, await PostgresRowsAsync(harness, "HotCounter"));
        Assert.Equal(0, HotRows<Stat>(harness));
        Assert.Equal(0, HotRows<HotCounter>(harness));
    }

    [Fact]
    public async Task A_newly_declared_archive_table_is_registered_and_never_copied()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(
            _postgres.ConnectionString,
            PostgresContainerFixture.NewSchema(),
            registry: () => TierHarness.RegistryWithout("HotCounter"));
        await harness.StartTierAsync();
        var lsn = harness.Invoke("RecordStat", ctx => ctx.Db.Stat.Insert(new Stat { Metric = "m", Value = 1, At = ctx.Timestamp }));
        await harness.WaitAppliedAsync(lsn);

        harness.RegistryFactory = WithArchive;
        await harness.RestartAsync();
        harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = 1, Count = 1 }));
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        Assert.False(harness.Logs.Has(1609));
        Assert.Equal(1L, await PostgresRowsAsync(harness, "HotCounter"));
        Assert.Equal(1L, await harness.ScalarAsync(
            $"SELECT count(*) FROM \"{harness.Schema}\".\"__melange_projected\" WHERE \"table_name\" = 'HotCounter'"));
    }

    [Fact]
    public async Task A_projection_rebuilt_over_a_truncated_log_refuses_rather_than_start_an_archive_with_a_hole()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        for (var i = 1; i <= 3; i++)
            harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = i, Count = i }));
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // The applier is caught up, so the snapshot may truncate everything behind it: the
        // archive's rows now exist only in Postgres.
        harness.Options.Resume.RetentionWindowSeconds = 0;
        harness.Engine.TakeSnapshot();
        Assert.True(harness.Engine.Log.BaseLsn > 0);

        // Postgres is lost: the schema, checkpoint and all. A mirror could rebuild from the hot
        // store; the archive cannot, and the applier must say so instead of anchoring.
        await harness.StopTierAsync();
        await harness.ExecuteAsync($"DROP SCHEMA \"{harness.Schema}\" CASCADE");
        await harness.RestartAsync();
        await TierHarness.WaitUntilAsync(() => harness.Tier.IsStalled, "the applier to stall");

        var refusal = Assert.Single(harness.Logs.Events, e => e.EventId == 1611);
        Assert.Contains("HotCounter", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("point-in-time recovery", refusal.Message, StringComparison.Ordinal);
        Assert.Null(await harness.StoredCheckpointAsync());
    }
}
