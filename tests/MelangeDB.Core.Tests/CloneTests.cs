using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <c>melange clone</c> (road-to-0.2 phase 19): staging seeded from production, and explicitly a
/// <em>different world</em> rather than a second copy of the same one. Everything a restore does,
/// plus the two deltas that make the difference true rather than aspirational — subscriber
/// checkpoints dropped, and a provenance sidecar the server reads back at every boot.
/// </summary>
[Collection("Telemetry")]
public class CloneTests : IDisposable
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
        var dir = Directory.CreateTempSubdirectory("melange-clone-").FullName;
        _extraRoots.Add(dir);
        return dir;
    }

    private MelangeEngine Boot(string directory, ILoggerFactory? loggers = null) =>
        new(
            new MelangeDbOptions
            {
                CommitLog = { Path = directory },
                HotStore = { Path = Path.Combine(TempDir(), "hot") },
            },
            EngineHarness.GeneratedRegistry(typeof(Player), typeof(InventoryItem), typeof(TerrainChunk)),
            loggers);

    /// <summary>A world with a subscriber checkpoint sidecar beside its log, then archived.</summary>
    private string PopulateAndCapture()
    {
        for (var i = 1; i <= 3; i++)
        {
            var name = $"player-{i}";
            _harness.Invoke("Seed", ctx => ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, X = i, Y = 0, Name = name }));
        }

        // The bus's own sidecar, written the way the bus writes it: delivery state that belongs to
        // the world it was captured from and to no other.
        File.WriteAllText(
            Path.Combine(_harness.Options.CommitLog.Path, "melange.events.json"),
            """{"inventory-audit":{"Lsn":2,"UpdatedAtUnixMs":1}}""");

        _harness.Engine.Dispose();
        var archive = Path.Combine(TempDir(), "production.mbak");
        MelangeBackup.Create(_harness.Options.CommitLog.Path, archive);
        return archive;
    }

    [Fact]
    public void A_clone_carries_the_world_and_drops_the_delivery_state_that_belonged_to_the_other_one()
    {
        var archive = PopulateAndCapture();
        var cloneDir = Path.Combine(TempDir(), "staging");
        var engine = Assert.Single(MelangeBackup.Clone(archive, cloneDir).Engines);
        Assert.Equal(3UL, engine.HeadLsn);

        // Dropped, not clamped: production's handlers had delivered through LSN 2, and a clone
        // inheriting that would silently skip events this world has never emitted.
        Assert.False(File.Exists(Path.Combine(cloneDir, "melange.events.json")));

        using var cloned = Boot(cloneDir);
        Assert.Equal(3, cloned.CommittedView.Count<Player>());
    }

    [Fact]
    public void A_restore_of_the_same_archive_keeps_the_checkpoints_it_clamps()
    {
        // The contrast is the point of having two verbs: a restore is the same world rewound, so
        // its subscribers keep their place (clamped to the restored head); a clone is a different
        // world, so they have none.
        var archive = PopulateAndCapture();
        var restoredDir = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restoredDir);

        var checkpoints = File.ReadAllText(Path.Combine(restoredDir, "melange.events.json"));
        Assert.Contains("inventory-audit", checkpoints);
    }

    [Fact]
    public void The_provenance_sidecar_answers_what_this_world_is_a_clone_of()
    {
        var archive = PopulateAndCapture();
        var sourceEpoch = MelangeBackup.Verify(archive).Engines[0].Identity.SourceEpoch;
        var cloneDir = Path.Combine(TempDir(), "staging");
        var engine = Assert.Single(MelangeBackup.Clone(archive, cloneDir).Engines);

        var provenance = MelangeBackup.ReadProvenance(cloneDir);
        Assert.NotNull(provenance);
        Assert.Equal(CloneProvenance.CloneKind, provenance.Kind);
        Assert.Equal(sourceEpoch, provenance.SourceEpoch);
        Assert.Equal(3UL, provenance.SourceHeadLsn);
        Assert.Equal("production.mbak", provenance.Archive);
        Assert.Equal(engine.NewEpoch, provenance.Epoch);

        // The clone is a different world: its own epoch, never the source's.
        Assert.NotEqual(sourceEpoch, provenance.Epoch);

        // The file name, not the path — a path is this machine's business and may hold a credential.
        Assert.DoesNotContain(Path.DirectorySeparatorChar, provenance.Archive);
    }

    [Fact]
    public void A_cloned_world_says_so_in_its_own_startup_log()
    {
        var archive = PopulateAndCapture();
        var cloneDir = Path.Combine(TempDir(), "staging");
        MelangeBackup.Clone(archive, cloneDir);

        var logs = new LogCapture();
        using (Boot(cloneDir, logs))
        {
            // "Which world is this, and how stale?" is asked at the worst possible moment. A server
            // that answers it in its own startup log answers it faster than any runbook.
            var entry = logs.Single(1804);
            Assert.Equal(LogLevel.Information, entry.Level);
            Assert.Equal("production.mbak", entry.Fields["Archive"]);
            Assert.Equal(3d, entry.Number("SourceHeadLsn"));
            Assert.Contains("separate world", entry.Message);
        }
    }

    [Fact]
    public void A_world_that_is_not_a_clone_has_no_provenance_and_says_nothing()
    {
        var archive = PopulateAndCapture();
        var restoredDir = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restoredDir);

        Assert.Null(MelangeBackup.ReadProvenance(restoredDir));
        var logs = new LogCapture();
        using (Boot(restoredDir, logs))
            Assert.DoesNotContain(logs.Entries, e => e.EventId == 1804);
    }

    [Fact]
    public void An_unreadable_provenance_sidecar_never_costs_a_boot()
    {
        // A support artifact, not a correctness one: nothing about a world's ability to run
        // depends on knowing where it came from.
        var archive = PopulateAndCapture();
        var cloneDir = Path.Combine(TempDir(), "staging");
        MelangeBackup.Clone(archive, cloneDir);
        File.WriteAllText(Path.Combine(cloneDir, CloneProvenance.FileName), "{ this is not json");

        Assert.Null(MelangeBackup.ReadProvenance(cloneDir));
        using var cloned = Boot(cloneDir);
        Assert.Equal(3, cloned.CommittedView.Count<Player>());
    }

    [Fact]
    public void A_clone_refuses_a_non_empty_target_exactly_as_a_restore_does()
    {
        var archive = PopulateAndCapture();
        var occupied = TempDir();
        File.WriteAllText(Path.Combine(occupied, "keepsake.txt"), "do not overwrite me");

        var refusal = Assert.Throws<InvalidOperationException>(() => MelangeBackup.Clone(archive, occupied));
        Assert.Contains("not empty", refusal.Message);
        Assert.Equal("do not overwrite me", File.ReadAllText(Path.Combine(occupied, "keepsake.txt")));
    }

    [Fact]
    public void A_clone_can_itself_be_backed_up_and_restored()
    {
        // The provenance sidecar is directory-local by design: an archive captures a world, and
        // the sidecar set a restore understands stays exactly as it was. A clone's archive
        // round-trips through an unchanged path.
        var archive = PopulateAndCapture();
        var cloneDir = Path.Combine(TempDir(), "staging");
        MelangeBackup.Clone(archive, cloneDir);

        var cloneArchive = Path.Combine(TempDir(), "staging.mbak");
        MelangeBackup.Create(cloneDir, cloneArchive);
        MelangeBackup.Verify(cloneArchive);

        var second = Path.Combine(TempDir(), "second");
        MelangeBackup.Restore(cloneArchive, second);
        Assert.Null(MelangeBackup.ReadProvenance(second));
        using var rebooted = Boot(second);
        Assert.Equal(3, rebooted.CommittedView.Count<Player>());
    }
}
