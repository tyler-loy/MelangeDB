using Testcontainers.PostgreSql;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresContainerFixture>
{
    public const string Name = "postgres";
}

/// <summary>
/// One real Postgres container for the whole collection — container lifecycle per collection, not
/// per test, for speed. Tests isolate by a fresh schema per run (<see cref="NewSchema"/>) inside
/// the one database. When Docker is unavailable every test in the collection self-skips with the
/// reason, so the suite degrades honestly instead of failing mysteriously.
/// </summary>
public sealed class PostgresContainerFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _container;

    /// <summary>Why the container could not start, or null when Postgres is up.</summary>
    public string? UnavailableReason { get; private set; }

    public string ConnectionString => _container!.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        try
        {
            _container = new PostgreSqlBuilder("postgres:16-alpine").Build();
            await _container.StartAsync();
        }
        catch (Exception exception)
        {
            UnavailableReason = exception.Message;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    /// <summary>Call first in every test; skips the test when Docker/Postgres is unavailable.</summary>
    public void SkipUnlessAvailable() =>
        Assert.SkipWhen(UnavailableReason is not null, $"Docker/Postgres unavailable: {UnavailableReason}");

    /// <summary>A fresh schema name — the per-run isolation unit.</summary>
    public static string NewSchema() => "m" + Guid.NewGuid().ToString("N")[..12];
}
