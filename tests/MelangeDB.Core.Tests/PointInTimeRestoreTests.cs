using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <c>restore --at-lsn</c> (road-to-0.2 phase 19): the moment just before the mistake. The archive
/// already carries the tail record by record, so this verb collects rather than invents — it stops
/// writing at the named LSN, refuses the two LSNs that name nothing restorable, and refuses
/// cluster archives outright, whose engines were captured at different fences.
/// </summary>
public class PointInTimeRestoreTests : IDisposable
{
    private readonly EngineHarness _harness = new(tables: [typeof(Player), typeof(InventoryItem), typeof(TerrainChunk)]);
    private readonly List<string> _extraRoots = [];

    public void Dispose()
    {
        _harness.Dispose();
        foreach (var root in _extraRoots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private string TempDir()
    {
        var dir = Directory.CreateTempSubdirectory("melange-pitr-").FullName;
        _extraRoots.Add(dir);
        return dir;
    }

    private MelangeEngine BootRestored(string restoredDir) =>
        new(
            new MelangeDbOptions
            {
                CommitLog = { Path = restoredDir },
                HotStore = { Path = Path.Combine(TempDir(), "hot") },
            },
            EngineHarness.GeneratedRegistry(typeof(Player), typeof(InventoryItem), typeof(TerrainChunk)));

    /// <summary>
    /// Two commits, a snapshot that truncates behind them, then three more: an archive whose
    /// snapshot floor is LSN 2 and whose captured head is LSN 5, which is the shape every refusal
    /// and every cut below is measured against.
    /// </summary>
    private string PopulateAndCapture()
    {
        for (var i = 1; i <= 2; i++)
        {
            var name = $"early-{i}";
            _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, X = i, Y = 0, Name = name }));
        }

        _harness.Options.Resume.RetentionWindowSeconds = 0;
        Assert.Equal(2UL, _harness.Engine.TakeSnapshot());

        for (var i = 3; i <= 5; i++)
        {
            var name = $"late-{i}";
            _harness.Invoke("Seed", ctx =>
            {
                ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, X = i, Y = 0, Name = name });
                ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash(name), ItemName = $"item-{i}", Quantity = 1 });
            });
        }

        Assert.Equal(5UL, _harness.Engine.Log.HeadLsn);
        _harness.Engine.Dispose();

        var archive = Path.Combine(TempDir(), "world.mbak");
        var summary = MelangeBackup.Create(_harness.Options.CommitLog.Path, archive);
        Assert.Equal(2UL, summary.Engines[0].SnapshotLsn);
        Assert.Equal(5UL, summary.Engines[0].HeadLsn);
        return archive;
    }

    [Fact]
    public void A_cut_stops_the_world_at_the_named_lsn_and_the_records_above_it_are_not_in_it()
    {
        var archive = PopulateAndCapture();
        var restoredDir = Path.Combine(TempDir(), "restored");

        var summary = MelangeBackup.Restore(archive, restoredDir, new RestoreOptions { AtLsn = 4 });
        var engine = Assert.Single(summary.Engines);
        Assert.Equal(4UL, engine.HeadLsn);
        Assert.Equal(5UL, engine.CapturedHeadLsn);

        using var rebooted = BootRestored(restoredDir);
        Assert.Equal(4UL, rebooted.Log.HeadLsn);

        // Everything committed at or below the cut is here; the commit above it never happened.
        var players = rebooted.CommittedView.Scan<Player>().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.Equal(["early-1", "early-2", "late-3", "late-4"], players);
        Assert.Equal(2, rebooted.CommittedView.Count<InventoryItem>());
    }

    [Fact]
    public void A_cut_at_the_snapshot_lsn_restores_the_snapshot_alone()
    {
        var archive = PopulateAndCapture();
        var restoredDir = Path.Combine(TempDir(), "restored");

        // The floor is restorable, not merely a boundary: the world materializes at the snapshot
        // with an empty tail.
        var engine = Assert.Single(MelangeBackup.Restore(archive, restoredDir, new RestoreOptions { AtLsn = 2 }).Engines);
        Assert.Equal(2UL, engine.HeadLsn);

        using var rebooted = BootRestored(restoredDir);
        Assert.Equal(2UL, rebooted.Log.HeadLsn);
        Assert.Equal(2, rebooted.CommittedView.Count<Player>());
        Assert.Equal(0, rebooted.CommittedView.Count<InventoryItem>());
    }

    [Fact]
    public void A_cut_below_the_snapshot_floor_is_refused_and_names_the_archive_that_holds_that_moment()
    {
        var archive = PopulateAndCapture();
        var refusal = Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Restore(archive, Path.Combine(TempDir(), "restored"), new RestoreOptions { AtLsn = 1 }));

        // An archive cannot rewind below its own materialized floor — everything under it exists
        // only as snapshot state. The remediation is the operator's next action, not a shrug.
        Assert.Contains("snapshot LSN 2", refusal.Message);
        Assert.Contains("earlier archive in the series", refusal.Message);
    }

    [Fact]
    public void A_cut_above_the_captured_head_is_refused_because_there_is_nothing_up_there()
    {
        var archive = PopulateAndCapture();
        var refusal = Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Restore(archive, Path.Combine(TempDir(), "restored"), new RestoreOptions { AtLsn = 6 }));
        Assert.Contains("head LSN 5", refusal.Message);
        Assert.Contains("cannot roll forward", refusal.Message);
    }

    [Fact]
    public void A_refused_cut_leaves_no_half_world_behind()
    {
        var archive = PopulateAndCapture();
        var target = Path.Combine(TempDir(), "restored");
        Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Restore(archive, target, new RestoreOptions { AtLsn = 1 }));

        // All-or-nothing holds for the refusals too: a directory that looks bootable must never
        // survive a restore that did not finish.
        Assert.False(Directory.Exists(target) && Directory.EnumerateFileSystemEntries(target).Any());
    }

    [Fact]
    public void The_whole_archive_is_still_walked_and_checked_when_the_cut_discards_most_of_it()
    {
        // The archive's integrity claims — contiguity, the promised head, the end frame's counts —
        // are about what was captured, so a cut must not let a corrupt archive through by simply
        // not reading the corrupt part. Only the writing stops early; the walk never does.
        var archive = PopulateAndCapture();
        var bytes = File.ReadAllBytes(archive);

        // Flip a byte in the last quarter of the archive: past LSN 3's frames, inside the region a
        // cut at 3 would discard.
        var index = bytes.Length - (bytes.Length / 8);
        bytes[index] ^= 0xFF;
        var corrupted = Path.Combine(TempDir(), "corrupt.mbak");
        File.WriteAllBytes(corrupted, bytes);

        Assert.ThrowsAny<Exception>(
            () => MelangeBackup.Restore(corrupted, Path.Combine(TempDir(), "restored"), new RestoreOptions { AtLsn = 3 }));
    }

    [Fact]
    public void A_cut_rewinds_the_autoinc_allocator_with_everything_else()
    {
        // The honest reading of a rewind, and not what the plan guessed: the archive carries
        // sequences as of its snapshot, and the records above the cut are never observed, so ids
        // allocated in the discarded range are free again. Nothing inside the restored world
        // refers to them; the fresh epoch is what forces everything outside it to rebuild.
        var archive = PopulateAndCapture();
        var restoredDir = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restoredDir, new RestoreOptions { AtLsn = 3 });

        using var rebooted = BootRestored(restoredDir);
        var item = Assert.Single(rebooted.CommittedView.Scan<InventoryItem>());
        Assert.Equal(1UL, item.Id);

        rebooted.Invoke("After", EngineHarness.Caller, ctx =>
        {
            var next = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("someone"), ItemName = "lantern", Quantity = 1 });
            Assert.Equal(2UL, next.Id); // Id 2 belonged to LSN 4's item in the discarded history.
        });
    }

    [Fact]
    public void A_cluster_archive_refuses_a_cut_because_one_lsn_names_no_cross_shard_moment()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("hub"), RoomId = 1, X = 0, Y = 0, Name = "hub" }));
        var archive = Path.Combine(TempDir(), "cluster.mbak");
        using (var stream = File.Create(archive))
            MelangeBackup.CreateClusterOnline(_harness.Engine, Path.Combine(TempDir(), "no-shards"), stream);

        // Each engine was captured at its own fence. Per-shard cuts would manufacture a
        // consistency the capture never had, so the verb refuses rather than approximates.
        var refusal = Assert.Throws<InvalidOperationException>(
            () => MelangeBackup.Restore(archive, Path.Combine(TempDir(), "restored"), new RestoreOptions { AtLsn = 1 }));
        Assert.Contains("single-engine archives only", refusal.Message);

        // The whole-archive restore of the same file is unaffected.
        Assert.NotEmpty(MelangeBackup.Restore(archive, Path.Combine(TempDir(), "whole")).Engines);
    }

    [Fact]
    public void A_plain_restore_is_unchanged_by_the_option_being_absent()
    {
        var archive = PopulateAndCapture();
        var restoredDir = Path.Combine(TempDir(), "restored");
        var engine = Assert.Single(MelangeBackup.Restore(archive, restoredDir).Engines);
        Assert.Equal(5UL, engine.HeadLsn);
        Assert.Equal(5UL, engine.CapturedHeadLsn);

        using var rebooted = BootRestored(restoredDir);
        Assert.Equal(5, rebooted.CommittedView.Count<Player>());
        Assert.Equal(3, rebooted.CommittedView.Count<InventoryItem>());
    }
}
