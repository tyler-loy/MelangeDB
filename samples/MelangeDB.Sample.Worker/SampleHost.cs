using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MelangeDB.Sample;

/// <summary>
/// Builds the sample host. Split from <c>Program</c> so the host-integration tests can run the
/// exact composition the executable runs, config overrides included.
/// </summary>
public static class SampleHost
{
    public static IHost Build(string[] args, Action<HostApplicationBuilder>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder(args);
        configure?.Invoke(builder);

        builder.Services.Configure<GreetingOptions>(builder.Configuration.GetSection("Sample:Greeting"));
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Visitor).Assembly)
            .AddReducersFrom(typeof(GreetingReducers).Assembly));
        builder.Services.AddHealthChecks();
        builder.Services.AddHostedService<GreeterWorker>();
        return builder.Build();
    }
}
