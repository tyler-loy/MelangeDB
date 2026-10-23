using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <c>Postgres:ArchiveLogRetentionHours</c> (road-to-0.3 phase 25): while archive tables are
/// declared, the log keeps the records committed within the window, so a Postgres restored from a
/// dump taken inside it can catch its archive up from the log. A named truncation floor, searched
/// by commit timestamp like the resume window; off by default, and inert without archive tables.
/// </summary>
public class ArchiveRetentionFloorTests : IDisposable
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

    public struct Archived
    {
        public long Id;
        public long Delta;
    }

    private sealed class MovableClock(DateTimeOffset start) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = start;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private (MelangeEngine Engine, MelangeDbOptions Options) Boot(bool archive, double hours, TimeProvider? clock = null)
    {
        var root = Directory.CreateTempSubdirectory("melange-archive-floor-").FullName;
        _roots.Add(root);
        var options = new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
            Snapshots = { Enabled = true, IntervalTransactions = long.MaxValue },
            Resume = { RetentionWindowSeconds = 0 },
            Postgres = { ArchiveLogRetentionHours = hours },
        };
        var declared = SchemaShapeTests.Declare<Archived>("Archived", "Id", autoInc: "Id");
        var table = new TableSchema(declared.RowType, declared.Name, declared.Columns, tier: archive ? StorageTier.Archive : StorageTier.Hot);
        return (new MelangeEngine(options, new SchemaRegistry([table]), timeProvider: clock), options);
    }

    private static void Commit(MelangeEngine engine, int count)
    {
        for (var i = 0; i < count; i++)
            engine.Invoke("Record", EngineHarness.Caller, ctx => ctx.Db.Insert(new Archived { Delta = 1 }));
    }

    private static TruncationFloorReport Snapshot(MelangeEngine engine)
    {
        Assert.NotNull(engine.TakeSnapshot());
        return engine.TruncationFloors!;
    }

    [Fact]
    public void Off_by_default_it_holds_nothing()
    {
        var (engine, _) = Boot(archive: true, hours: 0);
        using (engine)
        {
            Commit(engine, 5);
            var report = Snapshot(engine);
            Assert.DoesNotContain(report.Floors, f => f.Name == TruncationFloorNames.ArchiveRetention);
            Assert.Equal(5UL, engine.Log.BaseLsn);
        }
    }

    [Fact]
    public void Records_inside_the_window_are_kept_and_the_floor_is_named()
    {
        var (engine, _) = Boot(archive: true, hours: 26);
        using (engine)
        {
            Commit(engine, 5);
            var report = Snapshot(engine);
            Assert.Equal(TruncationFloorNames.ArchiveRetention, report.Governing.Name);
            Assert.Equal(0UL, engine.Log.BaseLsn);
        }
    }

    [Fact]
    public void Records_older_than_the_window_are_released()
    {
        var clock = new MovableClock(new DateTimeOffset(2026, 10, 23, 0, 0, 0, TimeSpan.Zero));
        var (engine, _) = Boot(archive: true, hours: 1, clock);
        using (engine)
        {
            Commit(engine, 3);
            clock.Now = clock.Now.AddHours(2);
            Commit(engine, 2);

            // The three commits from before the window are released; the two inside it are kept.
            var report = Snapshot(engine);
            Assert.Equal(3UL, report.Floors.Single(f => f.Name == TruncationFloorNames.ArchiveRetention).Lsn);
            Assert.Equal(3UL, engine.Log.BaseLsn);
        }
    }

    [Fact]
    public void Without_archive_tables_the_setting_is_inert()
    {
        var (engine, _) = Boot(archive: false, hours: 26);
        using (engine)
        {
            Commit(engine, 5);
            var report = Snapshot(engine);
            Assert.DoesNotContain(report.Floors, f => f.Name == TruncationFloorNames.ArchiveRetention);
            Assert.Equal(5UL, engine.Log.BaseLsn);
        }
    }
}
