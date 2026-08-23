using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Issue raised by the reference workload: after an additive migration that adds two Resident
/// tables (one of them scheduled), a pre-existing interval timer on an <em>unchanged</em> scheduled
/// table stopped firing. The scheduler reads a timer's cadence by walking the current schema's
/// columns over the stored row bytes, so the question this pins is whether an additive migration
/// that only adds <em>other</em> tables leaves the scheduled table's rows — and the ScheduleAt they
/// carry — intact and correctly readable. The added tables sort alphabetically before the existing
/// one (<c>Census*</c> &lt; <c>FloraTick</c>), the shape the report suspected of shifting something.
/// </summary>
public class SchedulerMigrationTests : IDisposable
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

    // Schema N: an interval timer table and a sibling data table.
    public struct FloraTick
    {
        public ulong Id;
        public ScheduleAt Tick;
    }

    public struct Region
    {
        public ulong Id;
        public long Power;
    }

    // Schema N+1 adds two tables that sort before "FloraTick": a plain Resident table and a
    // scheduled Resident one. FloraTick and Region are unchanged.
    public struct CensusDelta
    {
        public ulong Id;
        public long Delta;
    }

    public struct CensusDeltaTick
    {
        public ulong Id;
        public ScheduleAt Tick;
    }

    [Fact]
    public void An_additive_migration_that_adds_tables_preserves_a_pre_existing_timer_and_its_interval()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        var interval = ScheduleAt.Interval(TimeSpan.FromSeconds(5));

        using (var v1 = Boot(options, Scheduled<FloraTick>("FloraTick"), Data<Region>("Region")))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new FloraTick { Id = 1, Tick = interval });
                ctx.Db.Insert(new Region { Id = 1, Power = 100 });
            });
        }

        // Reboot with the two added tables — the additive migration.
        using var v2 = Boot(
            options,
            Scheduled<FloraTick>("FloraTick"),
            Data<Region>("Region"),
            Data<CensusDelta>("CensusDelta"),
            Scheduled<CensusDeltaTick>("CensusDeltaTick"));

        var flora = v2.Schema.Get(typeof(FloraTick));

        // The timer row survives the migration...
        var pair = Assert.Single(v2.HotStore.Scan(flora.Id));
        var timer = (FloraTick)RowSerializer.Deserialize(flora, pair.Value);

        // ...and its cadence reads back exactly — the scheduler would register a 5s interval, not a
        // garbage far-future Due or a non-interval. This is the read the report suspected breaks.
        Assert.Equal(1UL, timer.Id);
        Assert.True(timer.Tick.IsInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), timer.Tick.Every);

        // The ScheduleAt column also sits where the current schema says it does — the exact walk
        // MelangeScheduler.ReadScheduleAt performs to reach it.
        Assert.Equal(ColumnKind.ScheduleAt, flora.Columns[ScheduleAtIndex(flora)].Kind);

        // The sibling data table is intact too, and the new tables are empty and harmless.
        Assert.Equal(100L, ((Region)RowSerializer.Deserialize(v2.Schema.Get(typeof(Region)), v2.HotStore.Scan(v2.Schema.Get(typeof(Region)).Id).Single().Value)).Power);
        Assert.Empty(v2.HotStore.Scan(v2.Schema.Get(typeof(CensusDelta)).Id));
        Assert.Empty(v2.HotStore.Scan(v2.Schema.Get(typeof(CensusDeltaTick)).Id));
    }

    // A second scheduled table present before the migration, to test the peer's finding that the
    // scheduler services ~one scheduled table and drops the rest.
    public struct PopulationTick
    {
        public ulong Id;
        public ScheduleAt Tick;
    }

    public struct WorldStatsTick
    {
        public ulong Id;
        public ScheduleAt Tick;
    }

    [Fact]
    public void Every_pre_existing_scheduled_table_keeps_its_timer_across_the_migration()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        var five = ScheduleAt.Interval(TimeSpan.FromSeconds(5));
        var two = ScheduleAt.Interval(TimeSpan.FromSeconds(2));
        var thirty = ScheduleAt.Interval(TimeSpan.FromSeconds(30));

        using (var v1 = Boot(
            options,
            Scheduled<FloraTick>("FloraTick"),
            Scheduled<PopulationTick>("PopulationTick"),
            Scheduled<WorldStatsTick>("WorldStatsTick")))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new FloraTick { Id = 1, Tick = five });
                ctx.Db.Insert(new PopulationTick { Id = 1, Tick = two });
                ctx.Db.Insert(new WorldStatsTick { Id = 1, Tick = thirty });
            });
        }

        using var v2 = Boot(
            options,
            Scheduled<FloraTick>("FloraTick"),
            Scheduled<PopulationTick>("PopulationTick"),
            Scheduled<WorldStatsTick>("WorldStatsTick"),
            Data<CensusDelta>("CensusDelta"),
            Scheduled<CensusDeltaTick>("CensusDeltaTick"));

        AssertAllTimersPresent(v2);
    }

    [Fact]
    public void A_restart_after_the_migration_still_has_every_scheduled_timer()
    {
        // The deploy sequence the report actually runs: boot the new build (which migrates and
        // takes a sealing snapshot), then run/restart on that snapshot. This asserts the snapshot
        // written by the migration, reloaded on the next boot with an empty tail, still carries
        // every scheduled table's timer row — the registration input the scheduler scans.
        var root = NewRoot();
        var options = OptionsFor(root);
        var five = ScheduleAt.Interval(TimeSpan.FromSeconds(5));
        var two = ScheduleAt.Interval(TimeSpan.FromSeconds(2));
        var thirty = ScheduleAt.Interval(TimeSpan.FromSeconds(30));

        using (var v1 = Boot(options, Scheduled<FloraTick>("FloraTick"), Scheduled<PopulationTick>("PopulationTick"), Scheduled<WorldStatsTick>("WorldStatsTick")))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new FloraTick { Id = 1, Tick = five });
                ctx.Db.Insert(new PopulationTick { Id = 1, Tick = two });
                ctx.Db.Insert(new WorldStatsTick { Id = 1, Tick = thirty });
            });
        }

        var n1 = new TableSchema[]
        {
            Scheduled<FloraTick>("FloraTick"), Scheduled<PopulationTick>("PopulationTick"), Scheduled<WorldStatsTick>("WorldStatsTick"),
            Data<CensusDelta>("CensusDelta"), Scheduled<CensusDeltaTick>("CensusDeltaTick"),
        };

        // Boot 1 on the new assembly: performs the migration and its sealing snapshot.
        using (var _ = Boot(options, n1)) { }

        // Boot 2: loads that snapshot, replays an empty tail. This is where the running world lives.
        using var again = Boot(options, n1);
        AssertAllTimersPresent(again);
    }

    private static void AssertAllTimersPresent(MelangeEngine engine)
    {
        foreach (var (type, every) in new[]
        {
            (typeof(FloraTick), TimeSpan.FromSeconds(5)),
            (typeof(PopulationTick), TimeSpan.FromSeconds(2)),
            (typeof(WorldStatsTick), TimeSpan.FromSeconds(30)),
        })
        {
            var table = engine.Schema.Get(type);
            var pair = Assert.Single(engine.HotStore.Scan(table.Id));
            var tick = (ScheduleAt)table.Columns[ScheduleAtIndex(table)].GetValue(RowSerializer.Deserialize(table, pair.Value))!;
            Assert.True(tick.IsInterval, $"{type.Name} lost its interval");
            Assert.Equal(every, tick.Every);
        }
    }

    // A scheduled table's own shape changing in the migration: a column added before the
    // ScheduleAt, so its byte position shifts and the row is re-encoded by the real shape mapper.
    public struct GrowthTickV1
    {
        public ulong Id;
        public ScheduleAt Tick;
    }

    public struct GrowthTickV2
    {
        public ulong Id;
        public uint NextChunk; // added mid-struct, ahead of Tick
        public ScheduleAt Tick;
    }

    [Fact]
    public void A_scheduled_table_that_gains_a_column_keeps_a_readable_interval_after_the_migration()
    {
        var root = NewRoot();
        var options = OptionsFor(root);
        var interval = ScheduleAt.Interval(TimeSpan.FromSeconds(5));

        using (var v1 = Boot(options, Declare<GrowthTickV1>("GrowthTick", "Id", scheduled: "GrowthTickReducer", residency: Residency.Resident)))
        {
            v1.Invoke("Seed", EngineHarness.Caller, ctx => ctx.Db.Insert(new GrowthTickV1 { Id = 1, Tick = interval }));
        }

        // N+1: the scheduled table gains NextChunk before Tick — its shape changed, so the shape
        // mapper re-encodes the stored row, moving where Tick's bytes sit.
        using var v2 = Boot(options, Declare<GrowthTickV2>("GrowthTick", "Id", scheduled: "GrowthTickReducer", residency: Residency.Resident));

        var table = v2.Schema.Get(typeof(GrowthTickV2));
        var pair = Assert.Single(v2.HotStore.Scan(table.Id));
        var row = (GrowthTickV2)RowSerializer.Deserialize(table, pair.Value);

        // The added column defaulted, and the ScheduleAt still reads as the 5s interval where the
        // scheduler's column walk expects it — index 2 now, not index 1.
        Assert.Equal(0u, row.NextChunk);
        Assert.True(row.Tick.IsInterval, "the migrated scheduled row lost its interval — the scheduler would read garbage and never fire");
        Assert.Equal(TimeSpan.FromSeconds(5), row.Tick.Every);
        Assert.Equal(2, ScheduleAtIndex(table));
    }

    private static int ScheduleAtIndex(TableSchema schema)
    {
        for (var i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Kind == ColumnKind.ScheduleAt)
                return i;
        }

        throw new InvalidOperationException("no ScheduleAt column");
    }

    private string NewRoot()
    {
        var root = Directory.CreateTempSubdirectory("melange-sched-migrate-").FullName;
        _roots.Add(root);
        return root;
    }

    private static MelangeDbOptions OptionsFor(string root) => new()
    {
        CommitLog = { Path = Path.Combine(root, "log") },
        HotStore = { Path = Path.Combine(root, "hot") },
        Snapshots = { Enabled = true },
        Resume = { RetentionWindowSeconds = 0 },
    };

    private static MelangeEngine Boot(MelangeDbOptions options, params TableSchema[] tables) =>
        new(options, new SchemaRegistry(tables));

    private static TableSchema Scheduled<TRow>(string name) where TRow : struct =>
        Declare<TRow>(name, "Id", scheduled: name + "Reducer", residency: Residency.Resident);

    private static TableSchema Data<TRow>(string name) where TRow : struct =>
        Declare<TRow>(name, "Id", residency: Residency.Resident);

    private static TableSchema Declare<TRow>(string name, string key, string? scheduled = null, Residency residency = Residency.Paged)
        where TRow : struct
    {
        var columns = typeof(TRow).GetFields()
            .OrderBy(f => f.MetadataToken)
            .Select(f => new ColumnSchema
            {
                Name = f.Name,
                ClrType = f.FieldType,
                Kind = KindOf(f.FieldType),
                IsPrimaryKey = f.Name == key,
                GetValue = row => f.GetValue(row),
                SetValue = (row, value) => f.SetValue(row, value),
            })
            .ToList();
        return new TableSchema(typeof(TRow), name, columns, residency: residency, scheduled: scheduled);
    }

    private static ColumnKind KindOf(Type type) => type switch
    {
        _ when type == typeof(ulong) => ColumnKind.UInt64,
        _ when type == typeof(uint) => ColumnKind.UInt32,
        _ when type == typeof(long) => ColumnKind.Int64,
        _ when type == typeof(ScheduleAt) => ColumnKind.ScheduleAt,
        _ => throw new NotSupportedException(type.Name),
    };
}
