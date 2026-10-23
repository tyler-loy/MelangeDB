using MelangeDB.Core;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The archive's own Postgres semantics (road-to-0.3 phase 25): every row carries the LSN and epoch
/// it was committed at, and an insert whose key Postgres already holds is dead-lettered — set aside
/// in the batch's own transaction, logged by key and never by value, counted, and surfaced as a
/// degraded applier — while the applier carries on. <c>HotCounter</c> plays the archive table.
/// </summary>
[Collection(PostgresCollection.Name)]
public class ArchiveDeadLetterTests
{
    private readonly PostgresContainerFixture _postgres;

    public ArchiveDeadLetterTests(PostgresContainerFixture postgres) => _postgres = postgres;

    private static SchemaRegistry WithArchive() => TierHarness.RegistryWithTier("HotCounter", StorageTier.Archive);

    private static void Archive(TierHarness harness, long id, long count) =>
        harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = id, Count = count }));

    [Fact]
    public async Task Every_archive_row_carries_the_lsn_and_epoch_it_was_committed_at()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        var lsn = harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = 7, Count = 70 }));
        await harness.WaitAppliedAsync(lsn);

        Assert.Equal((long)lsn, await harness.ScalarAsync($"SELECT \"_melange_lsn\" FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"Id\" = 7"));
        Assert.Equal(harness.Engine.Log.EpochId, await harness.ScalarAsync($"SELECT \"_melange_epoch\" FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"Id\" = 7"));
    }

    [Fact]
    public async Task A_colliding_insert_is_dead_lettered_and_the_applier_carries_on()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        Archive(harness, 1, 10);
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // A reducer cannot see that key 1 is already archived: the collision reaches Postgres.
        // The row beside it in the same transaction must still land.
        var colliding = harness.Invoke("Archive", ctx =>
        {
            ctx.Db.HotCounter.Insert(new HotCounter { Id = 1, Count = 99 });
            ctx.Db.HotCounter.Insert(new HotCounter { Id = 2, Count = 20 });
        });
        await harness.WaitAppliedAsync(colliding);

        // Nothing overwritten, nothing dropped.
        Assert.Equal(10L, await harness.ScalarAsync($"SELECT \"Count\" FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"Id\" = 1"));
        Assert.Equal(20L, await harness.ScalarAsync($"SELECT \"Count\" FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"Id\" = 2"));
        var deadLetter = $"\"{harness.Schema}\".\"__melange_archive_dead_letter\"";
        Assert.Equal(1L, await harness.ScalarAsync($"SELECT count(*) FROM {deadLetter}"));
        Assert.Equal("HotCounter", await harness.ScalarAsync($"SELECT \"table_name\" FROM {deadLetter}"));
        Assert.Equal((long)colliding, await harness.ScalarAsync($"SELECT \"lsn\" FROM {deadLetter}"));
        Assert.Equal("Archive", await harness.ScalarAsync($"SELECT \"reducer\" FROM {deadLetter}"));
        Assert.Equal("99", await harness.ScalarAsync($"SELECT \"row_values\"->>'Count' FROM {deadLetter}"));
        Assert.Equal((long)colliding, await harness.StoredCheckpointAsync());

        // Logged at Error by key and reducer — and never by value.
        var logged = Assert.Single(harness.Logs.Events, e => e.EventId == 1612);
        Assert.Equal(LogLevel.Error, logged.Level);
        Assert.Contains("HotCounter", logged.Message, StringComparison.Ordinal);
        Assert.Contains("'Archive'", logged.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("99", logged.Message, StringComparison.Ordinal);

        // Degraded until someone clears the table — and across a restart, since the count is
        // re-read from Postgres rather than remembered.
        Assert.NotNull(harness.Tier.Degraded);
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        Assert.NotNull(harness.Tier.Degraded);

        await harness.ExecuteAsync($"DELETE FROM {deadLetter}");
        Archive(harness, 3, 30);
        await TierHarness.WaitUntilAsync(() => harness.Tier.Degraded is null, "the degraded condition to clear");
    }

    [Fact]
    public async Task With_keep_keys_a_duplicate_is_refused_in_the_reducer_and_never_reaches_postgres()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(
            _postgres.ConnectionString,
            PostgresContainerFixture.NewSchema(),
            registry: () => TierHarness.RegistryWithTier("HotCounter", StorageTier.Archive, keepKeys: true));
        await harness.StartTierAsync();
        Archive(harness, 1, 10);

        var head = harness.Engine.Log.HeadLsn;
        Assert.ThrowsAny<InvalidOperationException>(() => Archive(harness, 1, 99));
        Assert.Equal(head, harness.Engine.Log.HeadLsn);

        await harness.WaitAppliedAsync(head);
        Assert.Equal(0L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"__melange_archive_dead_letter\""));
        Assert.Null(harness.Tier.Degraded);
    }

    [Fact]
    public async Task A_delete_then_insert_of_one_key_in_one_transaction_replaces_the_row()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await harness.StartTierAsync();
        Archive(harness, 5, 50);

        // The reducer replacing its own row is not a collision: the write set collapses it to an
        // update, which upserts rather than dead-letters.
        var replaced = harness.Invoke("Correct", ctx =>
        {
            ctx.Db.HotCounter.Id.Delete(5L);
            ctx.Db.HotCounter.Insert(new HotCounter { Id = 5, Count = 55 });
        });
        await harness.WaitAppliedAsync(replaced);

        Assert.Equal(55L, await harness.ScalarAsync($"SELECT \"Count\" FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"Id\" = 5"));
        Assert.Equal((long)replaced, await harness.ScalarAsync($"SELECT \"_melange_lsn\" FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"Id\" = 5"));
        Assert.Equal(0L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"__melange_archive_dead_letter\""));
        Assert.Null(harness.Tier.Degraded);
    }
}
