using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Storage.Postgres;

/// <summary>Registers the Postgres relational tier on the MelangeDB builder.</summary>
public static class PostgresMelangeDbBuilderExtensions
{
    /// <summary>
    /// Adds the relational tier over <paramref name="connectionString"/>: the log applier that
    /// projects <c>Tier = Relational</c> tables into Postgres with its own durable checkpoint, and
    /// the executor behind ad-hoc SQL aggregates. <b>Opt-in</b> — a deployment that never calls
    /// this needs no Postgres and loses nothing but the relational projection.
    /// </summary>
    public static MelangeDbBuilder AddPostgres(this MelangeDbBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);
        return builder.AddPostgres(options => options.ConnectionString = connectionString);
    }

    /// <summary>
    /// Adds the relational tier, configuring <see cref="PostgresOptions"/> in code — sugar over
    /// the same <c>MelangeDb:Postgres:*</c> section configuration binds, where code wins because
    /// it runs last.
    /// </summary>
    public static MelangeDbBuilder AddPostgres(this MelangeDbBuilder builder, Action<PostgresOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (configure is not null)
            builder.Services.Configure<MelangeDbOptions>(options => configure(options.Postgres));
        if (builder.Services.Any(d => d.ServiceType == typeof(PostgresRelationalTier)))
            return builder;

        builder.Services.TryAddSingleton<PostgresConnectionSource>();
        builder.Services.TryAddSingleton(provider => new PostgresRelationalTier(
            provider.GetRequiredService<MelangeEngine>(),
            provider.GetRequiredService<PostgresConnectionSource>(),
            provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>(),
            provider.GetService<ILoggerFactory>(),
            provider.GetService<TimeProvider>()));
        builder.Services.TryAddSingleton<IRelationalQueryExecutor, PostgresQueryExecutor>();
        builder.Services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<PostgresRelationalTier>());
        return builder;
    }
}
