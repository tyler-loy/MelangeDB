using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The relational tier across a restore (road-to-0.2 phase 15): the Postgres projection is not in
/// the archive and is never silently overwritten. A restore mints a fresh epoch, so a leftover
/// checkpoint is refused as an epoch mismatch (EventId 1605) with the remediation printed — and
/// the remediation, followed literally, recovers. The same-epoch cousin — a checkpoint ahead of
/// the log's head, the hand-rolled directory swap — is refused just as loudly (EventId 1608).
/// </summary>
[Collection(PostgresCollection.Name)]
public class BackupRestorePostgresTests
{
    private readonly PostgresContainerFixture _postgres;

    public BackupRestorePostgresTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task After_a_restore_the_stale_projection_is_refused_and_the_printed_remediation_recovers()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        await using var harness = new TierHarness(_postgres.ConnectionString, schema);
        await harness.StartTierAsync();

        var applied = 0UL;
        for (var i = 0; i < 5; i++)
        {
            applied = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "world", Value = i, At = ctx.Timestamp }));
        }

        await harness.WaitAppliedAsync(applied);
        await harness.StopTierAsync();

        // A snapshot that truncates, then the nightly offline backup, then the disaster drill:
        // restore into a fresh directory. The archive carries the log's truth; Postgres carries a
        // checkpoint from the old epoch.
        harness.Options.Resume.RetentionWindowSeconds = 0;
        Assert.NotNull(harness.Engine.TakeSnapshot());
        Assert.True(harness.Engine.Log.BaseLsn > 0);
        harness.Engine.Dispose();

        var scratch = Directory.CreateTempSubdirectory("melange-pg-restore-").FullName;
        try
        {
            var archive = Path.Combine(scratch, "world.mbak");
            MelangeBackup.Create(Path.Combine(harness.Root, "log"), archive);
            var restoredRoot = Path.Combine(scratch, "restored");
            MelangeBackup.Restore(archive, Path.Combine(restoredRoot, "log"));

            // First boot against the restored directory: the applier checkpoint belongs to an
            // epoch the restored log has never seen, and the tier refuses loudly rather than
            // projecting history that no longer happened.
            await using var rebooted = new TierHarness(_postgres.ConnectionString, schema, root: restoredRoot);
            await rebooted.StartTierAsync();
            await TierHarness.WaitUntilAsync(
                () => rebooted.Tier.IsStalled && rebooted.Logs.Has(1605),
                "the epoch-mismatch refusal (EventId 1605)");

            // The printed remediation, followed literally: the clean path is an empty schema,
            // which the bootstrap machinery fills from the restored log.
            await rebooted.ExecuteAsync($"DROP SCHEMA \"{schema}\" CASCADE");
            await rebooted.RestartAsync();
            await rebooted.WaitAppliedAsync(rebooted.Engine.Log.HeadLsn);
            Assert.Equal(5L, await rebooted.ScalarAsync($"SELECT count(*) FROM \"{schema}\".\"Stat\""));
            Assert.Equal(10L, await rebooted.ScalarAsync($"SELECT sum(\"Value\")::bigint FROM \"{schema}\".\"Stat\""));
        }
        finally
        {
            try
            {
                Directory.Delete(scratch, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    [Fact]
    public async Task A_checkpoint_ahead_of_the_logs_head_is_refused_with_its_own_event()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        await using var harness = new TierHarness(_postgres.ConnectionString, schema);
        await harness.StartTierAsync();

        var applied = 0UL;
        for (var i = 0; i < 3; i++)
        {
            applied = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "future", Value = i, At = ctx.Timestamp }));
        }

        await harness.WaitAppliedAsync(applied);
        await harness.StopTierAsync();

        // The manual version of restoring beside history: the data directory is (conceptually)
        // swapped for an older copy that kept its epoch — here, the checkpoint simply jumps past
        // the head, which is the same disagreement seen from the other side.
        await harness.ExecuteAsync(
            $"UPDATE \"{schema}\".\"__melange_applier\" SET \"applied_lsn\" = {applied + 100} WHERE \"applier\" = 'postgres'");

        await harness.RestartAsync();
        await TierHarness.WaitUntilAsync(
            () => harness.Tier.IsStalled && harness.Logs.Has(1608),
            "the checkpoint-ahead refusal (EventId 1608)");

        // Same remediation, same recovery.
        await harness.ExecuteAsync($"DROP SCHEMA \"{schema}\" CASCADE");
        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);
        Assert.Equal(3L, await harness.ScalarAsync($"SELECT count(*) FROM \"{schema}\".\"Stat\""));
    }
}
