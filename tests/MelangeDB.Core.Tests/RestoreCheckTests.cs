using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <c>restore --check</c> (road-to-0.2 phase 19): the boot-proof, ranked honestly. Verify proves
/// frames, chains, and counts; a boot additionally proves recovery's own refusals pass, that the
/// epoch and sidecars cohere, and that the stores rebuild. BACKUP.md has always said only a booted
/// server proves the world and then left the staging boot as homework — this is that homework,
/// in two rungs, each stating what it proves and what it does not.
/// </summary>
public class RestoreCheckTests : IDisposable
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
        var dir = Directory.CreateTempSubdirectory("melange-check-test-").FullName;
        _extraRoots.Add(dir);
        return dir;
    }

    private static SchemaRegistry Schema =>
        EngineHarness.GeneratedRegistry(typeof(Player), typeof(InventoryItem), typeof(TerrainChunk));

    /// <summary>A snapshot, a tail above it, and a sidecar — a directory with something to check.</summary>
    private string RestoreAWorld()
    {
        for (var i = 1; i <= 2; i++)
        {
            var name = $"player-{i}";
            _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, X = i, Y = 0, Name = name }));
        }

        _harness.Options.Resume.RetentionWindowSeconds = 0;
        Assert.NotNull(_harness.Engine.TakeSnapshot());
        _harness.Invoke("Tail", ctx => ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("player-1"), ItemName = "sword", Quantity = 1 }));
        _harness.Engine.Dispose();

        var archive = Path.Combine(TempDir(), "world.mbak");
        MelangeBackup.Create(_harness.Options.CommitLog.Path, archive);
        var restored = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restored);
        return restored;
    }

    [Fact]
    public void The_file_level_rung_runs_real_recovery_and_says_what_it_cannot_prove()
    {
        var restored = RestoreAWorld();
        var report = MelangeBackup.CheckRestore(restored);

        Assert.Equal(RestoreCheckDepth.Recovery, report.Depth);
        var engine = Assert.Single(report.Engines);
        Assert.Equal(2UL, engine.BaseLsn);
        Assert.Equal(3UL, engine.HeadLsn);
        Assert.Equal(1, engine.TailRecords);
        Assert.Equal(2, engine.SnapshotRows);
        Assert.Contains(ShapeHistory.FileName, engine.Sidecars);

        // It has no schema, so it names no tables — and says so rather than implying a whole check.
        Assert.Empty(engine.RowsByTable);
        Assert.Contains("does not prove", report.Proves);
        Assert.Contains("host-side check", report.Proves);
    }

    [Fact]
    public void A_checked_restore_is_byte_identical_to_an_unchecked_one()
    {
        // Recovery mutates: it mints epochs, deletes stale base sidecars, truncates torn tails,
        // adopts shape sidecars. The check runs against a scratch copy for exactly this reason,
        // and this is the assertion that keeps it that way.
        var restored = RestoreAWorld();
        var before = Snapshot(restored);

        MelangeBackup.CheckRestore(restored);
        Assert.Equal(before, Snapshot(restored));

        MelangeBackup.CheckRestore(restored, Schema);
        Assert.Equal(before, Snapshot(restored));
    }

    [Fact]
    public void The_host_rung_boots_the_world_and_counts_what_came_out()
    {
        var restored = RestoreAWorld();
        var report = MelangeBackup.CheckRestore(restored, Schema);

        Assert.Equal(RestoreCheckDepth.Boot, report.Depth);
        var engine = Assert.Single(report.Engines);
        Assert.Equal(3UL, engine.HeadLsn);
        Assert.Equal(2L, engine.RowsByTable["Player"]);
        Assert.Equal(1L, engine.RowsByTable["InventoryItem"]);
        Assert.Equal(0L, engine.RowsByTable["TerrainChunk"]);
        Assert.Contains("Booted with the application's schema", report.Proves);
    }

    [Fact]
    public void The_check_refuses_the_directory_recovery_would_have_refused()
    {
        var restored = RestoreAWorld();

        // The snapshot's epoch no longer matches the log's: recovery would ignore the snapshot and
        // replay from a base it cannot reach. Discovering that on the day of the outage is the
        // failure mode this verb exists to move forward in time.
        File.WriteAllBytes(Path.Combine(restored, "melange.epoch"), Guid.NewGuid().ToByteArray());

        var refusal = Assert.Throws<InvalidDataException>(() => MelangeBackup.CheckRestore(restored));
        Assert.Contains("would not boot", refusal.Message);

        // And the refusal names the directory the operator knows, never the scratch copy.
        Assert.Contains(restored, refusal.Message);
    }

    [Fact]
    public void A_corrupt_shape_sidecar_fails_the_check_because_it_records_what_the_row_bytes_mean()
    {
        var restored = RestoreAWorld();
        File.WriteAllText(Path.Combine(restored, ShapeHistory.FileName), "{ not json");

        var refusal = Assert.Throws<InvalidDataException>(() => MelangeBackup.CheckRestore(restored));
        Assert.Contains("shape sidecar", refusal.Message);
    }

    [Fact]
    public void A_corrupt_checkpoint_sidecar_fails_the_check()
    {
        var restored = RestoreAWorld();
        File.WriteAllText(Path.Combine(restored, "melange.events.json"), "{ not json");

        var refusal = Assert.Throws<InvalidDataException>(() => MelangeBackup.CheckRestore(restored));
        Assert.Contains("melange.events.json", refusal.Message);
    }

    [Fact]
    public void A_directory_that_is_not_a_data_directory_is_refused_with_what_to_point_at()
    {
        var refusal = Assert.Throws<InvalidOperationException>(() => MelangeBackup.CheckRestore(TempDir()));
        Assert.Contains("not a restored data directory", refusal.Message);
        Assert.Contains("restore's -o", refusal.Message);
    }

    [Fact]
    public void A_missing_directory_is_a_plain_refusal_rather_than_a_crash()
        => Assert.Throws<DirectoryNotFoundException>(
            () => MelangeBackup.CheckRestore(Path.Combine(TempDir(), "never-restored")));

    [Fact]
    public void A_cluster_restore_is_checked_engine_by_engine()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("hub"), RoomId = 1, X = 0, Y = 0, Name = "hub" }));
        var archive = Path.Combine(TempDir(), "cluster.mbak");
        using (var stream = File.Create(archive))
            MelangeBackup.CreateClusterOnline(_harness.Engine, Path.Combine(TempDir(), "no-shards"), stream);

        var restored = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restored);

        // The layout the restore wrote is the layout the check walks — hub/ here, and every
        // shards/shard-k/log a fuller archive would have carried.
        var engine = Assert.Single(MelangeBackup.CheckRestore(restored).Engines);
        Assert.Equal("hub", engine.Key);
        Assert.EndsWith("hub", engine.Directory);
    }

    [Fact]
    public void A_cloned_world_checks_out_with_its_provenance_listed()
    {
        _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash("solo"), RoomId = 1, X = 0, Y = 0, Name = "solo" }));
        _harness.Engine.Dispose();
        var archive = Path.Combine(TempDir(), "world.mbak");
        MelangeBackup.Create(_harness.Options.CommitLog.Path, archive);

        var cloneDir = Path.Combine(TempDir(), "staging");
        MelangeBackup.Clone(archive, cloneDir);

        var engine = Assert.Single(MelangeBackup.CheckRestore(cloneDir, Schema).Engines);
        Assert.Contains(CloneProvenance.FileName, engine.Sidecars);
        Assert.Equal(1L, engine.RowsByTable["Player"]);
    }

    /// <summary>Every file under the directory with its bytes — the byte-identity assertion's subject.</summary>
    private static List<string> Snapshot(string directory) =>
        [.. Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories)
            .OrderBy(static path => path, StringComparer.Ordinal)
            .Select(path => $"{Path.GetRelativePath(directory, path)}|{Convert.ToHexStringLower(File.ReadAllBytes(path))}")];
}
