using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The melange-log health check: unhealthy before startup and when the commit log is poisoned
/// (unwritable or out of disk), healthy while appends can land.
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
        var entry = Assert.Single(beforeStart.Entries);
        Assert.Equal("melange-log", entry.Key);
        Assert.Equal(HealthStatus.Unhealthy, entry.Value.Status);

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
}
