using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// <see cref="IReducerRandomProvider"/> is a DI seam: registering one replaces the seeded default
/// for every reducer the host runs, which is how a test scripts a roll through real dispatch.
/// </summary>
public class ReducerRandomProviderTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-random-").FullName;

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
    public async Task A_registered_provider_supplies_ctx_Random()
    {
        var scripted = new ScriptedProvider();
        using var host = TestApp.Build(_root, configure: builder =>
            builder.Services.AddSingleton<IReducerRandomProvider>(scripted));
        await host.StartAsync(TestContext.Current.CancellationToken);

        Random? seen = null;
        host.Engine().Invoke("Roll", TestApp.Caller, ctx => seen = ctx.Random);

        Assert.Same(scripted.Random, seen);
        Assert.Equal("Roll", scripted.LastSeed?.ReducerName);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Without_a_registration_the_seeded_default_runs()
    {
        using var host = TestApp.Build(_root);
        await host.StartAsync(TestContext.Current.CancellationToken);

        Random? seen = null;
        host.Engine().Invoke("Roll", TestApp.Caller, ctx => seen = ctx.Random);

        Assert.NotNull(seen);
        Assert.NotEqual(typeof(Random), seen.GetType());
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    private sealed class ScriptedProvider : IReducerRandomProvider
    {
        public Random Random { get; } = new(0);

        public ReducerRandomSeed? LastSeed { get; private set; }

        public Random Create(ReducerRandomSeed seed)
        {
            LastSeed = seed;
            return Random;
        }
    }
}
