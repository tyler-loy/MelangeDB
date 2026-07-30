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

        // Residency:<TableName> keys share their section with the reserved settings, so the
        // per-table map binds by enumeration rather than by property name.
        services.AddOptions<MelangeDbOptions>().Configure<Microsoft.Extensions.Configuration.IConfiguration>(
            static (options, configuration) => BindPerTableResidency(options, configuration));

        var builder = new MelangeDbBuilder(services);
        configure(builder);

        services.TryAddSingleton(new ReducerRegistry(builder.Reducers));
        foreach (var descriptor in builder.Reducers)
        {
            services.TryAddScoped(descriptor.ReducerClass);

            // Reducer policies resolve from the call's DI scope; registering them here means an
            // attribute is all a policy needs, while an explicit registration still wins.
            if (descriptor.Policy is { } policy)
                services.TryAddScoped(policy);
        }

        var handlerTypes = builder.EventHandlers;
        services.TryAddSingleton(new EventHandlerRegistry(handlerTypes));
        foreach (var handlerType in handlerTypes)
            services.TryAddScoped(handlerType);
        services.TryAddSingleton<IEventTransport, InProcessEventTransport>();
        services.TryAddSingleton(provider => new MelangeEventBus(
            provider.GetRequiredService<MelangeEngine>(),
            provider.GetRequiredService<EventHandlerRegistry>(),
            provider.GetRequiredService<IEventTransport>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>(),
            provider.GetService<ILoggerFactory>(),
            provider.GetService<TimeProvider>()));

        var tables = builder.Tables;
        services.TryAddSingleton(_ => new SchemaRegistry(tables));
        services.TryAddSingleton(provider => new MelangeEngine(
            provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>().CurrentValue,
            provider.GetRequiredService<SchemaRegistry>(),
            provider.GetService<ILoggerFactory>(),
            provider.GetService<TimeProvider>(),
            provider.GetService<IHotStoreProvider>()));
        services.TryAddSingleton<MelangeReducerHost>();
        services.TryAddSingleton(provider => new MelangeScheduler(
            provider.GetRequiredService<MelangeEngine>(),
            provider.GetRequiredService<MelangeReducerHost>(),
            provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>(),
            provider.GetService<ILoggerFactory>(),
            provider.GetService<TimeProvider>()));
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

    private static readonly string[] ReservedResidencyKeys = ["Default", "AutoThresholdBytes", "ReportOnStartup", "PerTable"];

    private static void BindPerTableResidency(MelangeDbOptions options, Microsoft.Extensions.Configuration.IConfiguration configuration)
    {
        var section = configuration.GetSection($"{ConfigurationSection}:Residency");
        foreach (var child in section.GetChildren())
        {
            if (ReservedResidencyKeys.Contains(child.Key, StringComparer.OrdinalIgnoreCase) || child.Value is null)
                continue;
            if (!Enum.TryParse<Residency>(child.Value, ignoreCase: true, out var residency))
            {
                throw new InvalidOperationException(
                    $"Configuration key 'MelangeDb:Residency:{child.Key}' has value '{child.Value}'; " +
                    "expected Resident, Paged, or Auto.");
            }

            options.Residency.PerTable[child.Key] = residency;
        }
    }
}
