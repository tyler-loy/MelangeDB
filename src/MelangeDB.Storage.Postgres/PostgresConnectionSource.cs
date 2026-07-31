using MelangeDB.Core;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// The one <see cref="NpgsqlDataSource"/> the tier shares between its applier and the ad-hoc
/// query executor, built lazily from <c>Postgres:ConnectionString</c>. Building it opens nothing —
/// Postgres being down at startup must not be able to fail the host.
/// </summary>
public sealed class PostgresConnectionSource : IAsyncDisposable
{
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly Lock _lock = new();
    private NpgsqlDataSource? _dataSource;

    public PostgresConnectionSource(IOptionsMonitor<MelangeDbOptions> options) => _options = options;

    public NpgsqlDataSource DataSource
    {
        get
        {
            lock (_lock)
            {
                if (_dataSource is not null)
                    return _dataSource;
                var connectionString = _options.CurrentValue.Postgres.ConnectionString;
                if (string.IsNullOrEmpty(connectionString))
                {
                    throw new InvalidOperationException(
                        "AddPostgres(...) was called but Postgres:ConnectionString is empty. " +
                        "Pass a connection string to AddPostgres or configure MelangeDb:Postgres:ConnectionString.");
                }

                return _dataSource = NpgsqlDataSource.Create(connectionString);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_lock)
        {
            var dataSource = _dataSource;
            _dataSource = null;
            return dataSource?.DisposeAsync() ?? ValueTask.CompletedTask;
        }
    }
}
