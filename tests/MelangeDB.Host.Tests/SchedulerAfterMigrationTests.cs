using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The reference-workload report end to end: after an additive migration, a plain restart of the
/// migrated store leaves all-but-one scheduled reducer silent. The store tests (Core) prove every
/// timer row survives the migration; this drives the <em>real</em> <see cref="MelangeScheduler"/>
/// on the migrated-then-restarted store and asserts every timer actually fires — the one
/// combination the generated-schema host cannot express (it registers a whole assembly's tables at
/// once, so it cannot boot schema N and reboot schema N+1). Reflection schemas and hand-built
/// reducer descriptors stand in for two deployments of one module.
/// </summary>
public class SchedulerAfterMigrationTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-sched-mig-").FullName;
    private readonly ManualTimeProvider _time = new();

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    public struct FloraTick
    {
        public ulong Id;
        public uint NextChunk;
        public ScheduleAt Tick;
    }

    public struct PopulationTick
    {
        public ulong Id;
        public uint NextChunk;
        public ScheduleAt Tick;
    }

    public struct WorldStatsTick
    {
        public ulong Id;
        public ScheduleAt Tick;
    }

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
    public void Every_scheduled_reducer_still_fires_after_a_restart_of_a_migrated_store()
    {
        var five = ScheduleAt.Interval(TimeSpan.FromSeconds(5));
        var two = ScheduleAt.Interval(TimeSpan.FromSeconds(2));
        var thirty = ScheduleAt.Interval(TimeSpan.FromSeconds(30));

        // Schema N: three scheduled tables. Seed a timer in each.
        using (var v1 = Boot(SchemaN()))
        {
            v1.Invoke("Seed", Identity.Hash("seed"), ctx =>
            {
                ctx.Db.Insert(new FloraTick { Id = 1, Tick = five });
                ctx.Db.Insert(new PopulationTick { Id = 1, Tick = two });
                ctx.Db.Insert(new WorldStatsTick { Id = 1, Tick = thirty });
            });
        }

        // Boot on schema N+1 (adds a plain and a scheduled Resident table sorting before): migrates.
        using (var _ = Boot(SchemaNPlus1())) { }

        // Restart the migrated store — the dead scenario. Run the real scheduler on it.
        using var v3 = Boot(SchemaNPlus1());
        var ticks = new Ticks();
        using var scheduler = StartScheduler(v3, ticks);

        _time.Advance(TimeSpan.FromSeconds(65));

        // Every scheduled table's reducer must have fired. Before the reported bug is understood,
        // the failure here is the reproduction: some of these stay at zero.
        Assert.True(ticks.Count("FloraTickReducer") > 0, "FloraTick (growth) never fired after the migrated-store restart");
        Assert.True(ticks.Count("PopulationTickReducer") > 0, "PopulationTick (survivor) never fired");
        Assert.True(ticks.Count("WorldStatsTickReducer") > 0, "WorldStatsTick never fired after the migrated-store restart");
    }

    private TableSchema[] SchemaN() =>
    [
        Scheduled<FloraTick>("FloraTick"),
        Scheduled<PopulationTick>("PopulationTick"),
        Scheduled<WorldStatsTick>("WorldStatsTick"),
    ];

    private TableSchema[] SchemaNPlus1() =>
    [
        Scheduled<FloraTick>("FloraTick"),
        Scheduled<PopulationTick>("PopulationTick"),
        Scheduled<WorldStatsTick>("WorldStatsTick"),
        Data<CensusDelta>("CensusDelta"),
        Scheduled<CensusDeltaTick>("CensusDeltaTick"),
    ];

    private MelangeEngine Boot(TableSchema[] tables)
    {
        var options = new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(_root, "log") },
            HotStore = { Path = Path.Combine(_root, "hot") },
            Snapshots = { Enabled = true },
            Resume = { RetentionWindowSeconds = 0 },
        };
        return new MelangeEngine(options, new SchemaRegistry(tables), NullLoggerFactory.Instance, _time);
    }

    private IDisposable StartScheduler(MelangeEngine engine, Ticks ticks)
    {
        var services = new ServiceCollection();
        services.AddSingleton(ticks);
        services.AddScoped<Ticker>();
        var provider = services.BuildServiceProvider();

        var descriptors = engine.Schema.Tables
            .Where(t => t.Scheduled is not null)
            .Select(t =>
            {
                var name = t.Scheduled!;
                return new ReducerDescriptor(
                    name,
                    ReducerKind.Standard,
                    typeof(Ticker),
                    validate: (ref ReducerArgsReader _) => { },
                    invoke: (object instance, ReducerContext _, ref ReducerArgsReader _) => ((Ticker)instance).Fire(name));
            })
            .ToList();

        var options = new StaticOptionsMonitor(new MelangeDbOptions());
        var host = new MelangeReducerHost(engine, new ReducerRegistry(descriptors), provider.GetRequiredService<IServiceScopeFactory>(), options, _time);
        var scheduler = new MelangeScheduler(engine, host, options, NullLoggerFactory.Instance, _time);
        scheduler.Start();
        return new Composite(scheduler, provider);
    }

    private static TableSchema Scheduled<TRow>(string name) where TRow : struct =>
        Declare<TRow>(name, name + "Reducer");

    private static TableSchema Data<TRow>(string name) where TRow : struct =>
        Declare<TRow>(name, scheduled: null);

    private static TableSchema Declare<TRow>(string name, string? scheduled) where TRow : struct
    {
        var columns = typeof(TRow).GetFields()
            .OrderBy(f => f.MetadataToken)
            .Select(f => new ColumnSchema
            {
                Name = f.Name,
                ClrType = f.FieldType,
                Kind = KindOf(f.FieldType),
                IsPrimaryKey = f.Name == "Id",
                GetValue = row => f.GetValue(row),
                SetValue = (row, value) => f.SetValue(row, value),
            })
            .ToList();
        return new TableSchema(typeof(TRow), name, columns, residency: Residency.Resident, scheduled: scheduled);
    }

    private static ColumnKind KindOf(Type type) => type switch
    {
        _ when type == typeof(ulong) => ColumnKind.UInt64,
        _ when type == typeof(uint) => ColumnKind.UInt32,
        _ when type == typeof(long) => ColumnKind.Int64,
        _ when type == typeof(ScheduleAt) => ColumnKind.ScheduleAt,
        _ => throw new NotSupportedException(type.Name),
    };

    public sealed class Ticks
    {
        private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);

        public void Increment(string name) => _counts.AddOrUpdate(name, 1, static (_, c) => c + 1);

        public int Count(string name) => _counts.GetValueOrDefault(name);
    }

    public sealed class Ticker(Ticks ticks)
    {
        public void Fire(string name) => ticks.Increment(name);
    }

    private sealed class StaticOptionsMonitor(MelangeDbOptions value) : IOptionsMonitor<MelangeDbOptions>
    {
        public MelangeDbOptions CurrentValue { get; } = value;

        public MelangeDbOptions Get(string? name) => CurrentValue;

        public IDisposable OnChange(Action<MelangeDbOptions, string?> listener) => new Noop();

        private sealed class Noop : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }

    private sealed class Composite(IDisposable a, IDisposable b) : IDisposable
    {
        public void Dispose()
        {
            a.Dispose();
            b.Dispose();
        }
    }
}
