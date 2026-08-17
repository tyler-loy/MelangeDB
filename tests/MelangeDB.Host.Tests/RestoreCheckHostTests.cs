using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The host rung of <c>restore --check</c> (road-to-0.2 phase 19): the full-fidelity boot proof
/// runs where the schema lives, because indexes, residency, and the shape guard's judgement of
/// this code against these row bytes are the application's, not the archive's. One line in a
/// staging runbook — restore last night's archive, check it, alert on the throw.
/// </summary>
public class RestoreCheckHostTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-checkhost-").FullName;

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

    private string Dir(string name)
    {
        var path = Path.Combine(_root, name);
        Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>Runs a real host, archives what it wrote, and restores it into a fresh directory.</summary>
    private async Task<string> RestoreAWorldAsync()
    {
        var live = Dir("live");
        using (var host = TestApp.Build(live))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            for (var i = 0; i < 3; i++)
                host.Reducers().Call("AddNote", TestApp.Caller, $"note-{i}", (double)i);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        var archive = Path.Combine(Dir("archives"), "world.mbak");
        MelangeBackup.Create(Path.Combine(live, "log"), archive);
        var restored = Path.Combine(_root, "restored");
        MelangeBackup.Restore(archive, restored);
        return restored;
    }

    [Fact]
    public async Task The_host_rung_boots_the_restored_world_with_the_applications_own_schema()
    {
        var restored = await RestoreAWorldAsync();

        // Built, not started: starting would open this deployment's own data directory beside the
        // one under test, which is the mistake the extension's doc warns about.
        using var host = TestApp.Build(Dir("checker"));
        var report = host.CheckRestore(restored);

        Assert.Equal(RestoreCheckDepth.Boot, report.Depth);
        var engine = Assert.Single(report.Engines);
        Assert.Equal(3L, engine.RowsByTable["Note"]);
        Assert.True(engine.HeadLsn >= 3);
        Assert.Contains("Booted with the application's schema", report.Proves);
    }

    [Fact]
    public async Task The_host_rung_throws_the_refusal_recovery_would_have_thrown()
    {
        var restored = await RestoreAWorldAsync();
        File.WriteAllText(Path.Combine(restored, ShapeHistory.FileName), "{ not json");

        using var host = TestApp.Build(Dir("checker"));

        // A CI job's whole contract: the check throws, and the alert is the throw.
        var refusal = Assert.Throws<InvalidDataException>(() => host.CheckRestore(restored));
        Assert.Contains("shape sidecar", refusal.Message);
    }

    [Fact]
    public void A_host_with_no_melange_registered_says_so_rather_than_failing_obscurely()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings { DisableDefaults = true });
        using var host = builder.Build();

        var refusal = Assert.Throws<InvalidOperationException>(() => host.CheckRestore(_root));
        Assert.Contains("no MelangeDB schema registered", refusal.Message);
        Assert.Contains("MelangeBackup.CheckRestore(directory)", refusal.Message);
    }
}
