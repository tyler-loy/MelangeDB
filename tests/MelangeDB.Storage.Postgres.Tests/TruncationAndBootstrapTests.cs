using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The compaction interaction: log truncation can never pass the Postgres applier's checkpoint —
/// a lagging tier pins the log rather than losing history — and a tier attached after truncation
/// bootstraps from the hot store at a consistent LSN (EventId 1606).
/// </summary>
[Collection(PostgresCollection.Name)]
public class TruncationAndBootstrapTests
{
    private readonly PostgresContainerFixture _postgres;

    public TruncationAndBootstrapTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task Log_truncation_never_passes_the_postgres_checkpoint()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();

        var applied = 0UL;
        for (var i = 0; i < 3; i++)
        {
            applied = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "floor", Value = i, At = ctx.Timestamp }));
        }

        await harness.WaitAppliedAsync(applied);

        // Freeze the checkpoint, keep committing, then snapshot: the floor must hold at the
        // checkpoint even though the snapshot itself is at the head.
        await harness.StopTierAsync();
        for (var i = 3; i < 9; i++)
        {
            harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "floor", Value = i, At = ctx.Timestamp }));
        }

        harness.Options.Resume.RetentionWindowSeconds = 0;
        var snapshotLsn = harness.Engine.TakeSnapshot();
        Assert.NotNull(snapshotLsn);
        Assert.True(harness.Engine.Log.BaseLsn <= applied,
            $"truncated to {harness.Engine.Log.BaseLsn}, past the postgres checkpoint {applied}");

        // The tier resumes from its checkpoint — the pinned records were exactly what it needed —
        // and once caught up, the next snapshot may truncate further.
        await harness.RestartAsync();
        var head = harness.Engine.Log.HeadLsn;
        await harness.WaitAppliedAsync(head);
        Assert.Equal(9L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));

        harness.Options.Resume.RetentionWindowSeconds = 0;
        harness.Engine.TakeSnapshot();
        Assert.True(harness.Engine.Log.BaseLsn >= applied);
    }

    [Fact]
    public async Task A_tier_attached_after_truncation_bootstraps_from_the_hot_store()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());

        // A deployment that ran without Postgres: rows committed, log truncated behind a snapshot.
        for (var i = 0; i < 5; i++)
        {
            harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "history", Value = i, At = ctx.Timestamp }));
        }

        harness.Options.Resume.RetentionWindowSeconds = 0;
        harness.Engine.TakeSnapshot();
        Assert.True(harness.Engine.Log.BaseLsn > 0);

        // Now the operator adds Postgres: replay cannot reach the truncated records, so the tier
        // bootstraps from the store at a consistent LSN and continues from there.
        await harness.StartTierAsync();
        var lsn = harness.Invoke("RecordStat", ctx =>
            ctx.Db.Stat.Insert(new Stat { Metric = "history", Value = 5, At = ctx.Timestamp }));
        await harness.WaitAppliedAsync(lsn);

        Assert.Equal(6L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal(15L, await harness.ScalarAsync($"SELECT sum(\"Value\")::bigint FROM \"{harness.Schema}\".\"Stat\""));
        Assert.True(
            harness.Logs.Has(1606),
            $"expected the bootstrap log (EventId 1606); events: {string.Join(" | ", harness.Logs.Events.Select(e => $"{e.EventId}:{e.Message}"))}; " +
            $"baseLsn={harness.Engine.Log.BaseLsn}, applied={harness.Tier.AppliedLsn}");
    }
}
