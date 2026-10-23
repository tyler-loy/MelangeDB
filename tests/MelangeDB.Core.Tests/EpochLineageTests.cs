using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The epoch lineage (road-to-0.3 phase 25): a restore records which epoch it descends from and
/// where it forked, a backup carries the lineage so a restore of a restore extends it, and a
/// point-in-time cut rewinds inherited fork points too. What it is for — reconciling a Postgres
/// archive to the restored history — is tested against Postgres.
/// </summary>
public class EpochLineageTests : IDisposable
{
    private readonly List<string> _roots = [];

    public void Dispose()
    {
        foreach (var root in _roots)
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
        var dir = Directory.CreateTempSubdirectory("melange-lineage-").FullName;
        _roots.Add(dir);
        return dir;
    }

    private MelangeEngine Boot(string logDirectory) =>
        new(
            new MelangeDbOptions
            {
                CommitLog = { Path = logDirectory },
                HotStore = { Path = Path.Combine(TempDir(), "hot") },
            },
            EngineHarness.GeneratedRegistry(typeof(Player)));

    private static ulong Seed(MelangeEngine engine, string name) =>
        engine.Invoke("Seed", EngineHarness.Caller, ctx =>
            ctx.Db.Insert(new Player { Id = Identity.Hash(name), RoomId = 1, X = 0, Y = 0, Name = name }));

    private string Backup(string logDirectory)
    {
        var archive = Path.Combine(TempDir(), "world.mbak");
        MelangeBackup.Create(logDirectory, archive);
        return archive;
    }

    [Fact]
    public void A_world_that_was_never_restored_has_no_ancestors()
    {
        using var engine = Boot(Path.Combine(TempDir(), "log"));
        Assert.Empty(engine.Lineage.Ancestors);
    }

    [Fact]
    public void A_restore_descends_from_its_source_at_the_restored_head_and_a_restore_of_it_extends_the_chain()
    {
        var origin = Path.Combine(TempDir(), "log");
        Guid originEpoch;
        using (var engine = Boot(origin))
        {
            originEpoch = engine.Log.EpochId;
            Seed(engine, "a");
            Seed(engine, "b");
        }

        var restored = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(Backup(origin), restored);
        Guid restoredEpoch;
        using (var engine = Boot(restored))
        {
            restoredEpoch = engine.Log.EpochId;
            Assert.Equal([new EpochAncestor(originEpoch, 2)], engine.Lineage.Ancestors);
            Seed(engine, "c");
        }

        // The backup of the restored world carries its lineage; restoring it adds one generation.
        var again = Path.Combine(TempDir(), "again");
        MelangeBackup.Restore(Backup(restored), again);
        using var third = Boot(again);
        Assert.Equal([new EpochAncestor(originEpoch, 2), new EpochAncestor(restoredEpoch, 3)], third.Lineage.Ancestors);
        Assert.Equal(2UL, third.Lineage.UpToFor(originEpoch));
        Assert.Null(third.Lineage.UpToFor(third.Log.EpochId));
    }

    [Fact]
    public void A_point_in_time_cut_rewinds_inherited_fork_points_too()
    {
        // LSNs run on along a lineage, so a cut below an ancestor's fork point means that
        // ancestor's history past the cut is not this world's history either.
        var lineage = new EpochLineage([new EpochAncestor(Guid.NewGuid(), 100)]);
        var source = Guid.NewGuid();
        var extended = lineage.Extend(source, 50);
        Assert.Equal(50UL, extended.Ancestors[0].UpToLsn);
        Assert.Equal(new EpochAncestor(source, 50), extended.Ancestors[1]);
    }

    [Fact]
    public void A_corrupt_sidecar_is_a_loud_failure()
    {
        var directory = TempDir();
        File.WriteAllText(Path.Combine(directory, EpochLineage.FileName), "not json");
        var refused = Assert.Throws<InvalidDataException>(() => EpochLineage.Load(directory));
        Assert.Contains("restore it from backup", refused.Message, StringComparison.Ordinal);
    }
}
