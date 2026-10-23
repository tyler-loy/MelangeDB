using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// Postgres restored from a plain dump (road-to-0.3 phase 25): it comes back with an older
/// checkpoint and without the archive rows committed after the dump. Within
/// <c>Postgres:ArchiveLogRetentionHours</c>, the log still holds those records and the applier
/// catches up; without it, the log has truncated past them and the refusal names the lost range
/// (EventId 1611). The "dump" is staged by rewinding the checkpoint and deleting the rows above it.
/// <c>HotCounter</c> plays the archive table.
/// </summary>
[Collection(PostgresCollection.Name)]
public class ArchiveRetentionPostgresTests(PostgresContainerFixture postgres)
{
    private static SchemaRegistry WithArchive() => TierHarness.RegistryWithTier("HotCounter", StorageTier.Archive);

    /// <summary>Archives five rows, then stages a dump restore taken after the first: checkpoint at that LSN, later rows gone.</summary>
    private static async Task<ulong> ArchiveThenRestoreDumpAsync(TierHarness harness, double retentionHours)
    {
        await harness.StartTierAsync();
        var dumpedAt = harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = 1, Count = 10 }));
        for (var id = 2; id <= 5; id++)
            harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = id, Count = id * 10 }));
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // The nightly snapshot truncates behind the applier, which is caught up — unless the
        // retention window holds the log back.
        harness.Options.Resume.RetentionWindowSeconds = 0;
        harness.Options.Postgres.ArchiveLogRetentionHours = retentionHours;
        Assert.NotNull(harness.Engine.TakeSnapshot());

        // Postgres is lost and restored from the dump: the checkpoint and the archive go back to it.
        await harness.StopTierAsync();
        await harness.ExecuteAsync($"UPDATE \"{harness.Schema}\".\"__melange_applier\" SET \"applied_lsn\" = {dumpedAt}");
        await harness.ExecuteAsync($"DELETE FROM \"{harness.Schema}\".\"HotCounter\" WHERE \"_melange_lsn\" > {dumpedAt}");
        return dumpedAt;
    }

    [Fact]
    public async Task Inside_the_retention_window_the_archive_catches_up_from_the_log()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        await ArchiveThenRestoreDumpAsync(harness, retentionHours: 26);
        Assert.Equal(0UL, harness.Engine.Log.BaseLsn);

        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        Assert.Equal(5L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"HotCounter\""));
        Assert.Null(harness.Tier.Degraded);
    }

    [Fact]
    public async Task Without_it_the_refusal_names_the_lost_range()
    {
        postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(postgres.ConnectionString, PostgresContainerFixture.NewSchema(), registry: WithArchive);
        var dumpedAt = await ArchiveThenRestoreDumpAsync(harness, retentionHours: 0);
        var baseLsn = harness.Engine.Log.BaseLsn;
        Assert.True(baseLsn > dumpedAt);

        await harness.RestartAsync();
        await TierHarness.WaitUntilAsync(() => harness.Tier.IsStalled && harness.Logs.Has(1611), "the archive-unrecoverable refusal (EventId 1611)");
        var refusal = harness.Logs.Events.First(e => e.EventId == 1611);
        Assert.Contains($"LSNs {dumpedAt + 1} to {baseLsn}", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("Postgres:ArchiveLogRetentionHours", refusal.Message, StringComparison.Ordinal);
    }
}
