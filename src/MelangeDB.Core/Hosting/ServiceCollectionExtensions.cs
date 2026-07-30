using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>Registers MelangeDB in an ordinary .NET host.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>The configuration section every MelangeDB setting binds from.</summary>
    public const string ConfigurationSection = "MelangeDb";

    /// <summary>
    /// Adds MelangeDB: options bound from the <c>MelangeDb:</c> section (so appsettings.json,
    /// environment variables, and Azure App Configuration all work with no MelangeDB-specific
    /// code), the engine and reducer host, the hosted service owning startup and graceful
    /// shutdown, and the <c>melange-log</c> health check.
    /// </summary>
    public static IServiceCollection AddMelangeDb(this IServiceCollection services, Action<MelangeDbBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Bind first, then run the builder's Configure registrations: the builder API is sugar
        // over the same options object, and code wins over file only because it runs last.
        services.AddOptions<MelangeDbOptions>().BindConfiguration(ConfigurationSection);

        var builder = new MelangeDbBuilder(services);
        configure(builder);

        services.TryAddSingleton(new ReducerRegistry(builder.Reducers));
        foreach (var descriptor in builder.Reducers)
            services.TryAddScoped(descriptor.ReducerClass);

        var tables = builder.Tables;
        services.TryAddSingleton(_ => new SchemaRegistry(tables));
        services.TryAddSingleton(provider => new MelangeEngine(
            provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>().CurrentValue,
            provider.GetRequiredService<SchemaRegistry>(),
            provider.GetService<ILoggerFactory>(),
            provider.GetService<TimeProvider>()));
        services.TryAddSingleton<MelangeReducerHost>();
        services.TryAddSingleton<MelangeDbRuntimeState>();
        services.TryAddSingleton<MelangeLogHealthCheck>();
        services.AddHostedService<MelangeDbHostedService>();
        services.Configure<HealthCheckServiceOptions>(options =>
        {
            if (options.Registrations.All(r => r.Name != "melange-log"))
            {
                options.Registrations.Add(new HealthCheckRegistration(
                    "melange-log",
                    provider => provider.GetRequiredService<MelangeLogHealthCheck>(),
                    failureStatus: null,
                    tags: null));
            }
        });

        return services;
    }
}
