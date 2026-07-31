using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The melange-log health check — unhealthy before startup and when the commit log is poisoned
/// (unwritable or out of disk), healthy while appends can land — and the melange-applier check:
/// unhealthy when any applier's lag exceeds <c>HealthChecks:ApplierLagThreshold</c>, which is the
/// silent-stall alarm the two-tier design demands.
/// </summary>
public class HealthCheckTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-health-").FullName;

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

    [Fact]
    public async Task Reports_unhealthy_before_start_healthy_after_and_unhealthy_when_poisoned()
    {
        using var host = TestApp.Build(_root);
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var beforeStart = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(["melange-applier", "melange-log"], beforeStart.Entries.Keys.Order().ToArray());
        Assert.Equal(HealthStatus.Unhealthy, beforeStart.Entries["melange-log"].Status);

        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Reducers().Call("AddNote", TestApp.Caller, "healthy", 0.0);
        var afterStart = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Healthy, afterStart.Entries["melange-log"].Status);

        // Poison the log the way a full disk does: the append fails and its rollback fails too.
        var log = Assert.IsType<FileCommitLog>(host.Engine().Log);
        log.AppendFaultInjection = stream => stream.Dispose();
        Assert.ThrowsAny<Exception>(() => host.Reducers().Call("AddNote", TestApp.Caller, "doomed", 0.0));

        var poisoned = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Unhealthy, poisoned.Entries["melange-log"].Status);
        Assert.Contains("poisoned", poisoned.Entries["melange-log"].Description);
    }

    [Fact]
    public async Task Applier_check_goes_unhealthy_when_lag_exceeds_the_threshold_and_recovers()
    {
        using var host = TestApp.Build(_root, new Dictionary<string, string?>
        {
            ["MelangeDb:HealthChecks:ApplierLagThreshold"] = "2",
        });
        await host.StartAsync(TestContext.Current.CancellationToken);
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var healthy = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Healthy, healthy.Entries["melange-applier"].Status);

        // A decoupled applier whose loop never runs: its checkpoint stays put while commits land,
        // which is exactly the stalled-Postgres shape.
        var stalled = new StalledApplier();
        host.Engine().Appliers.RegisterDecoupled(stalled);
        for (var i = 0; i < 4; i++)
            host.Reducers().Call("AddNote", TestApp.Caller, $"note-{i}", 0.0);

        var unhealthy = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Unhealthy, unhealthy.Entries["melange-applier"].Status);
        Assert.Contains("stalled-test", unhealthy.Entries["melange-applier"].Description);

        stalled.AppliedLsn = host.Engine().Log.HeadLsn;
        var recovered = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Healthy, recovered.Entries["melange-applier"].Status);
    }

    private sealed class StalledApplier : ILogApplier
    {
        public string Name => "stalled-test";

        public ulong AppliedLsn { get; set; }

        public void Apply(CommitRecord record) => throw new NotSupportedException();
    }
}
