using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MelangeDB.Sample;

/// <summary>
/// Builds the sample host. Split from <c>Program</c> so the host-integration tests can run the
/// exact composition the executable runs, config overrides included. As of phase 03 the host is an
/// ASP.NET Core app serving the MelangeDB socket at <c>/melange</c>; the greeting worker keeps
/// running unchanged beside it.
/// </summary>
public static class SampleHost
{
    public static IHost Build(string[] args, Action<IHostApplicationBuilder>? configure = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configure?.Invoke(builder);

        builder.Services.Configure<GreetingOptions>(builder.Configuration.GetSection("Sample:Greeting"));
        builder.Services.AddDevJwtAuthentication();
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Visitor).Assembly)
            .AddReducersFrom(typeof(GreetingReducers).Assembly));
        builder.Services.AddHealthChecks();
        builder.Services.AddHostedService<GreeterWorker>();

        var app = builder.Build();
        app.UseWebSockets();
        app.MapMelangeSocket();
        return app;
    }
}
