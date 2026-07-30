using MelangeDB.Core;
using MelangeDB.Storage.Faster;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// Phase 07's host surface: engine selection by registration (never by path), the per-table
/// residency configuration binding, the startup residency report, and the phase's configuration
/// defaults verified against the code — the register's row is only true if a test says so.
/// </summary>
public class PagedStorageHostTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-paged-host-").FullName;
    private readonly LogCollector _logs = new();

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

    private IHost BuildHost(IDictionary<string, string?>? settings = null, bool useFaster = false) =>
        TestApp.Build(
            _root,
            settings,
            builder => builder.Logging.AddProvider(_logs),
            events: melange =>
            {
                if (useFaster)
                    melange.UseFasterHotStore();
            });

    [Fact]
    public async Task Auto_selects_faster_when_the_package_is_registered()
    {
        using var host = BuildHost(useFaster: true);
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.IsType<FasterHotStore>(host.Engine().HotStore);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Auto_selects_in_memory_when_nothing_is_registered()
    {
        using var host = BuildHost();
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.IsType<InMemoryHotStore>(host.Engine().HotStore);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Explicit_in_memory_wins_over_a_registered_faster_package()
    {
        using var host = BuildHost(
            new Dictionary<string, string?> { ["MelangeDb:HotStore:Engine"] = "InMemory" },
            useFaster: true);
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.IsType<InMemoryHotStore>(host.Engine().HotStore);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Explicit_faster_without_the_package_fails_loudly()
    {
        using var host = BuildHost(new Dictionary<string, string?> { ["MelangeDb:HotStore:Engine"] = "Faster" });
        var exception = Assert.Throws<InvalidOperationException>(() => host.Engine());
        Assert.Contains("UseFasterHotStore", exception.Message);
    }

    [Fact]
    public async Task Per_table_residency_binds_from_configuration_and_wins_over_the_attribute()
    {
        using var host = BuildHost(
            new Dictionary<string, string?>
            {
                // Note declares Resident in code; the operator pages it and pins Audit's default.
                ["MelangeDb:Residency:Note"] = "Paged",
                ["MelangeDb:Residency:Default"] = "Paged",
            },
            useFaster: true);
        await host.StartAsync(TestContext.Current.CancellationToken);

        var statistics = host.Engine().HotStore.Statistics();
        Assert.Equal(Residency.Paged, statistics.Tables.Single(t => t.Name == "Note").Residency);
        Assert.Equal(Residency.Resident, statistics.Tables.Single(t => t.Name == "Audit").Residency);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Startup_residency_report_logs_event_1501_with_the_budget()
    {
        using var host = BuildHost(useFaster: true);
        await host.StartAsync(TestContext.Current.CancellationToken);

        var report = Assert.Single(_logs.Entries, e => e.EventId == 1501);
        Assert.Contains("buffer-pool cap", report.Message);
        Assert.Contains("Note:", report.Message); // A resident table is itemized.
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Report_can_be_disabled()
    {
        using var host = BuildHost(new Dictionary<string, string?> { ["MelangeDb:Residency:ReportOnStartup"] = "false" });
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.DoesNotContain(_logs.Entries, e => e.EventId == 1501);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Phase_07_configuration_defaults_match_the_register()
    {
        // docs/CONFIGURATION.md rows, verified against the code rather than the plan.
        var options = new MelangeDbOptions();
        Assert.Equal(HotStoreEngine.Auto, options.HotStore.Engine);
        Assert.Equal(134_217_728, options.HotStore.MemoryBudgetBytes);
        Assert.True(options.CommitLog.GroupCommit);
        Assert.Equal(Residency.Paged, options.Residency.Default);
        Assert.Equal(8_388_608, options.Residency.AutoThresholdBytes);
        Assert.True(options.Residency.ReportOnStartup);
        Assert.True(options.Snapshots.Enabled);
        Assert.Equal(100_000, options.Snapshots.IntervalTransactions);
        Assert.True(options.Snapshots.TruncateLog);
    }

    [Fact]
    public async Task Truncation_never_passes_a_wedged_live_event_subscriber()
    {
        // End-to-end through the hosted service's floor registration: a subscriber blocked on its
        // gate pins the log at its checkpoint; releasing it lets the next snapshot truncate.
        var time = new ManualTimeProvider();
        using var host = TestApp.Build(
            _root,
            new Dictionary<string, string?> { ["MelangeDb:Resume:RetentionWindowSeconds"] = "0" },
            builder => builder.Services.AddSingleton<TimeProvider>(time),
            events: melange => melange.AddEventHandler<GateHandler>());
        await host.StartAsync(TestContext.Current.CancellationToken);
        var probe = host.Services.GetRequiredService<EventProbe>();
        var reducers = host.Reducers();
        var engine = host.Engine();

        reducers.Call("PublishGate", TestApp.Caller, "wedged");
        await probe.GateEntered.WaitAsync(TestContext.Current.CancellationToken);
        for (var i = 0; i < 3; i++)
            reducers.Call("AddNote", TestApp.Caller, $"n{i}", 0.0);
        time.Advance(TimeSpan.FromMinutes(10));

        engine.TakeSnapshot();
        Assert.Equal(0UL, ((FileCommitLog)engine.Log).BaseLsn); // The wedged checkpoint pins everything.

        probe.Gate.SetResult();
        await probe.Delivered.WaitAsync(TestContext.Current.CancellationToken);
        var bus = host.Services.GetRequiredService<MelangeEventBus>();
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (bus.MinimumLiveCheckpointLsn is not { } minimum || minimum < engine.Log.HeadLsn)
        {
            Assert.True(DateTime.UtcNow < deadline, "subscriber checkpoint never caught up");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        time.Advance(TimeSpan.FromMinutes(10));
        engine.TakeSnapshot();
        Assert.Equal(engine.Log.HeadLsn, ((FileCommitLog)engine.Log).BaseLsn);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Invalid_residency_value_fails_with_the_key_named()
    {
        using var host = BuildHost(new Dictionary<string, string?> { ["MelangeDb:Residency:Note"] = "Pinned" });
        var exception = await Record.ExceptionAsync(() => host.StartAsync(TestContext.Current.CancellationToken));
        Assert.NotNull(exception);
        Assert.Contains("MelangeDb:Residency:Note", exception.Message);
    }
}
