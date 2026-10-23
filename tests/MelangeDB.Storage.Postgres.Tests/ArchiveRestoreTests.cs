using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The archive across a restore (road-to-0.3 phase 25). A restore rewinds the log; Postgres keeps
/// running and holds the future that no longer happened. When the restored log's lineage names
/// the projection's epoch as an ancestor, the applier reconciles it: archive rows outside the
/// restored history are moved to <c>__melange_archive_rewound</c> — not deleted — mirrors that ran
/// past the fork are re-copied from the hot store, and the checkpoint resumes from the fork
/// (EventId 1613). A projection from an unrelated branch is still refused (1605), a resume point
/// below the restored log's base is refused for archives (1611), and a clone starts its archive
/// without the source's history (1614). <c>HotCounter</c> plays the archive table.
/// </summary>
[Collection(PostgresCollection.Name)]
public sealed class ArchiveRestoreTests(PostgresContainerFixture postgres) : IDisposable
{
    private readonly List<string> _scratch = [];

    public void Dispose()
    {
        foreach (var dir in _scratch)
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private static SchemaRegistry WithArchive() => TierHarness.RegistryWithTier("HotCounter", StorageTier.Archive);

    private string Scratch()
    {
        var dir = Directory.CreateTempSubdirectory("melange-pg-archive-restore-").FullName;
        _scratch.Add(dir);
        return dir;
    }

    private TierHarness Harness(string schema, string? root = null) =>
        new(postgres.ConnectionString, schema, root: root, registry: WithArchive);

    private static void Archive(TierHarness harness, long id) =>
        harness.Invoke("Archive", ctx => ctx.Db.HotCounter.Insert(new HotCounter { Id = id, Count = id * 10 }));

    private static void Stat(TierHarness harness, long value) =>
        harness.Invoke("RecordStat", ctx => ctx.Db.Stat.Insert(new Stat { Metric = "m", Value = value, At = ctx.Timestamp }));

    /// <summary>An online backup of the running engine, then — later — a restore of it into a fresh root.</summary>
    private string BackupOnline(TierHarness harness)
    {
        var archive = Path.Combine(Scratch(), "world.mbak");
        using (var stream = File.Create(archive))
            MelangeBackup.CreateOnline(harness.Engine, stream);
        return archive;
    }

    private string RestoreInto(string archive)
    {
        var root = Scratch();
        MelangeBackup.Restore(archive, Path.Combine(root, "log"));
        return root;
    }

    private static async Task<long[]> ArchivedIdsAsync(TierHarness harness)
    {
        var ids = await harness.ScalarAsync($"SELECT coalesce(array_agg(\"Id\" ORDER BY \"Id\"), '{{}}') FROM \"{harness.Schema}\".\"HotCounter\"");
        return (long[])ids!;
    }

    private static async Task<long> RewoundAsync(TierHarness harness) =>
        (long)(await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"__melange_archive_rewound\""))!;

    [Fact]
    public async Task A_restore_moves_the_rewound_archive_rows_aside_and_recopies_the_mirrors()
    {
        postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        string archive;
        await using (var world = Harness(schema))
        {
            await world.StartTierAsync();
            Archive(world, 1);
            Archive(world, 2);
            Stat(world, 100);
            archive = BackupOnline(world);

            // The future the restore will rewind: Postgres applies it before the disaster.
            Archive(world, 3);
            Stat(world, 200);
            await world.WaitAppliedAsync(world.Engine.Log.HeadLsn);
        }

        await using var restored = Harness(schema, RestoreInto(archive));
        await restored.StartTierAsync();
        await restored.WaitAppliedAsync(restored.Engine.Log.HeadLsn);

        Assert.True(restored.Logs.Has(1613));
        Assert.Equal(new[] { 1L, 2L }, await ArchivedIdsAsync(restored));
        Assert.Equal(1L, await RewoundAsync(restored));
        Assert.Equal("30", await restored.ScalarAsync($"SELECT \"row_values\"->>'Count' FROM \"{schema}\".\"__melange_archive_rewound\""));

        // The mirror ran past the fork, so it is the hot store's copy again: one Stat, not two.
        Assert.Equal(1L, await restored.ScalarAsync($"SELECT count(*) FROM \"{schema}\".\"Stat\""));
        Assert.Equal(restored.Engine.Log.EpochId, await restored.ScalarAsync($"SELECT \"log_epoch\" FROM \"{schema}\".\"__melange_applier\""));

        // And the restored world carries on: its own archive rows land under its own epoch.
        Archive(restored, 3);
        await restored.WaitAppliedAsync(restored.Engine.Log.HeadLsn);
        Assert.Equal(new[] { 1L, 2L, 3L }, await ArchivedIdsAsync(restored));
        Assert.Equal(restored.Engine.Log.EpochId, await restored.ScalarAsync($"SELECT \"_melange_epoch\" FROM \"{schema}\".\"HotCounter\" WHERE \"Id\" = 3"));
    }

    [Fact]
    public async Task A_restore_of_a_restore_keeps_every_ancestors_history_up_to_its_fork()
    {
        postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        string first;
        await using (var world = Harness(schema))
        {
            await world.StartTierAsync();
            Archive(world, 1);
            first = BackupOnline(world);
            Archive(world, 2);
            await world.WaitAppliedAsync(world.Engine.Log.HeadLsn);
        }

        string second;
        await using (var middle = Harness(schema, RestoreInto(first)))
        {
            await middle.StartTierAsync();
            Archive(middle, 3);
            second = BackupOnline(middle);
            Archive(middle, 4);
            await middle.WaitAppliedAsync(middle.Engine.Log.HeadLsn);
            Assert.Equal(new[] { 1L, 3L, 4L }, await ArchivedIdsAsync(middle));
        }

        // Lineage [(origin, fork 1), (middle, fork 2)]: row 1 is the origin's history, row 3 the
        // middle world's; row 4 is the middle world's rewound future, and row 2 went at the first restore.
        await using var last = Harness(schema, RestoreInto(second));
        await last.StartTierAsync();
        await last.WaitAppliedAsync(last.Engine.Log.HeadLsn);
        Assert.Equal(2, last.Engine.Lineage.Ancestors.Count);
        Assert.Equal(new[] { 1L, 3L }, await ArchivedIdsAsync(last));
        Assert.Equal(2L, await RewoundAsync(last));
    }

    [Fact]
    public async Task A_projection_from_another_branch_is_still_refused()
    {
        postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        string early;
        string late;
        await using (var world = Harness(schema))
        {
            await world.StartTierAsync();
            Archive(world, 1);
            early = BackupOnline(world);
            Archive(world, 2);
            late = BackupOnline(world);
            await world.WaitAppliedAsync(world.Engine.Log.HeadLsn);
        }

        // Postgres follows a branch restored from the early backup...
        await using (var branch = Harness(schema, RestoreInto(early)))
        {
            await branch.StartTierAsync();
            Archive(branch, 3);
            await branch.WaitAppliedAsync(branch.Engine.Log.HeadLsn);
        }

        // ...and then the late backup is restored. Its lineage does not include the branch, whose
        // rows Postgres now holds, and the origin's row 2 that the late world needs was moved aside
        // when the branch was reconciled. Nothing here can rebuild that, so it is refused.
        await using var other = Harness(schema, RestoreInto(late));
        await other.StartTierAsync();
        await TierHarness.WaitUntilAsync(() => other.Tier.IsStalled && other.Logs.Has(1605), "the epoch-mismatch refusal (EventId 1605)");
    }

    [Fact]
    public async Task A_resume_point_below_the_restored_logs_base_is_refused_for_an_archive()
    {
        postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        var root = Scratch();
        string archive;
        await using (var world = Harness(schema, root))
        {
            await world.StartTierAsync();
            Archive(world, 1);
            await world.WaitAppliedAsync(world.Engine.Log.HeadLsn);

            // Postgres falls behind; the world keeps writing and snapshots at its head. The backup's
            // snapshot then sits past Postgres's checkpoint, so the restored log starts above it.
            await world.StopTierAsync();
            Archive(world, 2);
            Archive(world, 3);
            world.Options.Resume.RetentionWindowSeconds = 0;
            Assert.NotNull(world.Engine.TakeSnapshot());
            world.Engine.Dispose();

            // Inside the block: disposing the harness deletes its data directory.
            archive = Path.Combine(Scratch(), "world.mbak");
            MelangeBackup.Create(Path.Combine(root, "log"), archive);
        }

        await using var restored = Harness(schema, RestoreInto(archive));
        await restored.StartTierAsync();
        await TierHarness.WaitUntilAsync(() => restored.Tier.IsStalled && restored.Logs.Has(1611), "the archive-unrecoverable refusal (EventId 1611)");
        Assert.Contains(restored.Logs.Events, e => e.EventId == 1611 && e.Message.Contains("below the restored log's base", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_clone_starts_its_archive_without_the_sources_history()
    {
        postgres.SkipUnlessAvailable();
        var root = Scratch();
        string archive;
        await using (var world = Harness(PostgresContainerFixture.NewSchema(), root))
        {
            await world.StartTierAsync();
            Archive(world, 1);
            Stat(world, 1);
            await world.WaitAppliedAsync(world.Engine.Log.HeadLsn);
            world.Options.Resume.RetentionWindowSeconds = 0;
            Assert.NotNull(world.Engine.TakeSnapshot());
            await world.StopTierAsync();
            world.Engine.Dispose();
            archive = Path.Combine(Scratch(), "world.mbak");
            MelangeBackup.Create(Path.Combine(root, "log"), archive);
        }

        var cloneRoot = Scratch();
        MelangeBackup.Clone(archive, Path.Combine(cloneRoot, "log"));

        // Its own Postgres schema, as BACKUP.md prescribes for a clone.
        await using var clone = Harness(PostgresContainerFixture.NewSchema(), cloneRoot);
        await clone.StartTierAsync();
        Archive(clone, 9);
        await clone.WaitAppliedAsync(clone.Engine.Log.HeadLsn);

        Assert.True(clone.Logs.Has(1614));
        Assert.False(clone.Tier.IsStalled);
        Assert.Equal(new[] { 9L }, await ArchivedIdsAsync(clone));
        Assert.Equal(1L, await clone.ScalarAsync($"SELECT count(*) FROM \"{clone.Schema}\".\"Stat\""));
    }
}
