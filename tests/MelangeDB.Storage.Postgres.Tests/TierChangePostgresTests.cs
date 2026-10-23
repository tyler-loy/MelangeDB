using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// A table moving onto or off the relational backplane (road-to-0.3 phase 25). The applier skips
/// a hot table's ops, so by the time a table is switched to <c>HotWithRelationalBackplane</c> its
/// checkpoint is past that table's whole history — the copy has to come from the hot store, and
/// the <c>__melange_projected</c> registry is how the applier knows which tables need one.
/// <c>HotCounter</c> is the table that moves: hot in the generated schema, staged onto the
/// backplane by booting the same table under another tier.
/// </summary>
[Collection(PostgresCollection.Name)]
public class TierChangePostgresTests
{
    private readonly PostgresContainerFixture _postgres;

    public TierChangePostgresTests(PostgresContainerFixture postgres) => _postgres = postgres;

    private static Func<SchemaRegistry> CounterOn(StorageTier tier) =>
        () => TierHarness.RegistryWithTier("HotCounter", tier);

    private static void Count(TierHarness harness, long id, long count) =>
        harness.Invoke("Count", ctx =>
        {
            if (ctx.Db.HotCounter.Id.Find(id) is { } existing)
                ctx.Db.HotCounter.Update(existing with { Count = count });
            else
                ctx.Db.HotCounter.Insert(new HotCounter { Id = id, Count = count });
        });

    private static async Task<long> PostgresCountersAsync(TierHarness harness) =>
        (long)(await harness.ScalarAsync(
            $"SELECT coalesce(sum(\"Id\" * 1000 + \"Count\"), 0)::bigint FROM \"{harness.Schema}\".\"HotCounter\""))!;

    private static long HotCounters(TierHarness harness)
    {
        var table = harness.Engine.Schema.Get(typeof(HotCounter));
        return harness.Engine.HotStore.Scan(table.Id)
            .Select(pair => (HotCounter)RowSerializer.Deserialize(table, pair.Value))
            .Sum(row => (row.Id * 1000) + row.Count);
    }

    private static async Task<string[]> RegistryAsync(TierHarness harness)
    {
        var names = await harness.ScalarAsync(
            $"SELECT string_agg(\"table_name\", ',' ORDER BY \"table_name\") FROM \"{harness.Schema}\".\"__melange_projected\"");
        return names is string joined ? joined.Split(',') : [];
    }

    [Fact]
    public async Task A_fresh_anchor_registers_every_backplane_table()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();
        var lsn = harness.Invoke("RecordStat", ctx => ctx.Db.Stat.Insert(new Stat { Metric = "m", Value = 1, At = ctx.Timestamp }));
        await harness.WaitAppliedAsync(lsn);

        Assert.Equal(["Account", "Stat"], await RegistryAsync(harness));
        Assert.False(harness.Logs.Has(1609));
    }

    [Fact]
    public async Task A_hot_table_switched_onto_the_backplane_is_copied_from_the_hot_store()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();
        for (var i = 1; i <= 5; i++)
            Count(harness, i, i);
        Count(harness, 2, 20);
        var lsn = harness.Invoke("RecordStat", ctx => ctx.Db.Stat.Insert(new Stat { Metric = "m", Value = 1, At = ctx.Timestamp }));
        await harness.WaitAppliedAsync(lsn);

        // The deploy that moves HotCounter onto the backplane. Its whole history is behind the
        // checkpoint, skipped while it was hot — replay cannot produce it.
        harness.RegistryFactory = CounterOn(StorageTier.HotWithRelationalBackplane);
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        Assert.Equal(HotCounters(harness), await PostgresCountersAsync(harness));
        var copied = Assert.Single(harness.Logs.Events, e => e.EventId == 1609);
        Assert.Contains("'HotCounter'", copied.Message, StringComparison.Ordinal);
        Assert.Contains("5 row(s)", copied.Message, StringComparison.Ordinal);
        Assert.Equal(["Account", "HotCounter", "Stat"], await RegistryAsync(harness));

        // From here on it is an ordinary backplane table: new writes project as usual.
        Count(harness, 6, 6);
        Count(harness, 1, 100);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        Assert.Equal(HotCounters(harness), await PostgresCountersAsync(harness));

        // And the copy happens once: the next start finds it registered.
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        Assert.False(harness.Logs.Has(1609));
    }

    [Fact]
    public async Task Ops_between_the_checkpoint_and_the_copy_converge_over_the_copied_rows()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(
            _postgres.ConnectionString,
            PostgresContainerFixture.NewSchema(),
            registry: CounterOn(StorageTier.HotWithRelationalBackplane));
        await harness.StartTierAsync();
        Count(harness, 1, 1);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // Writes the applier has not reached when it restarts: they sit between its checkpoint and
        // the copy's capture LSN, so they are applied twice — once inside the copy, once replayed.
        await harness.StopTierAsync();
        Count(harness, 1, 50);
        Count(harness, 2, 2);
        harness.Invoke("Drop", ctx => ctx.Db.HotCounter.Id.Delete(2L));
        Count(harness, 3, 3);

        // Clearing the registry makes the restarting applier copy HotCounter at a head past its
        // checkpoint — the window the convergence argument is about.
        await harness.ExecuteAsync($"DELETE FROM \"{harness.Schema}\".\"__melange_projected\" WHERE \"table_name\" = 'HotCounter'");
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        Assert.True(harness.Logs.Has(1609));
        Assert.Equal(HotCounters(harness), await PostgresCountersAsync(harness));
        Assert.Equal(2L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"HotCounter\""));
    }

    [Fact]
    public async Task Leaving_the_backplane_and_rejoining_it_copies_afresh_rather_than_resuming_a_stale_copy()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(
            _postgres.ConnectionString,
            PostgresContainerFixture.NewSchema(),
            registry: CounterOn(StorageTier.HotWithRelationalBackplane));
        await harness.StartTierAsync();
        Count(harness, 1, 1);
        Count(harness, 2, 2);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        harness.RegistryFactory = CounterOn(StorageTier.Hot);
        await harness.RestartAsync();
        Count(harness, 2, 22);
        Count(harness, 3, 3);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        var left = Assert.Single(harness.Logs.Events, e => e.EventId == 1610);
        Assert.Contains("'HotCounter'", left.Message, StringComparison.Ordinal);
        Assert.Equal(["Account", "Stat"], await RegistryAsync(harness));

        // Left as is: the Postgres table still holds the copy as of the change, not the hot writes since.
        Assert.Equal(2L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"HotCounter\""));

        harness.RegistryFactory = CounterOn(StorageTier.HotWithRelationalBackplane);
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        Assert.True(harness.Logs.Has(1609));
        Assert.Equal(HotCounters(harness), await PostgresCountersAsync(harness));
    }

    [Fact]
    public async Task A_projection_from_before_the_registry_seeds_it_and_copies_only_what_postgres_never_held()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();
        for (var i = 0; i < 3; i++)
            harness.Invoke("RecordStat", ctx => ctx.Db.Stat.Insert(new Stat { Metric = "m", Value = i, At = ctx.Timestamp }));
        Count(harness, 1, 1);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // A 0.2 projection: no registry. The upgrade deploy also moves HotCounter onto the backplane.
        await harness.StopTierAsync();
        await harness.ExecuteAsync($"DROP TABLE \"{harness.Schema}\".\"__melange_projected\"");
        harness.RegistryFactory = CounterOn(StorageTier.HotWithRelationalBackplane);
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // Stat and Account were already in Postgres, projected from the anchor on: seeded, never
        // copied. HotCounter was not, so it is copied like any table new to the backplane.
        var copied = Assert.Single(harness.Logs.Events, e => e.EventId == 1609);
        Assert.Contains("'HotCounter'", copied.Message, StringComparison.Ordinal);
        Assert.Equal(["Account", "HotCounter", "Stat"], await RegistryAsync(harness));
        Assert.Equal(3L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal(HotCounters(harness), await PostgresCountersAsync(harness));
    }
}
