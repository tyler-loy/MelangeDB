using System.Collections.Concurrent;
using MelangeDB.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>Static options monitor: enough for components that read <c>CurrentValue</c>.</summary>
internal sealed class StaticOptionsMonitor(MelangeDbOptions value) : IOptionsMonitor<MelangeDbOptions>
{
    public MelangeDbOptions CurrentValue { get; } = value;

    public MelangeDbOptions Get(string? name) => CurrentValue;

    public IDisposable? OnChange(Action<MelangeDbOptions, string?> listener) => null;
}

/// <summary>Captures structured log events so tests can assert on stable EventIds.</summary>
internal sealed class CapturingLoggerFactory : ILoggerFactory
{
    public ConcurrentQueue<(int EventId, LogLevel Level, string Message)> Events { get; } = [];

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public ILogger CreateLogger(string categoryName) => new Logger(Events);

    public void Dispose()
    {
    }

    public bool Has(int eventId) => Events.Any(e => e.EventId == eventId);

    private sealed class Logger(ConcurrentQueue<(int, LogLevel, string)> events) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            events.Enqueue((eventId.Id, logLevel, formatter(state, exception)));
    }
}

/// <summary>
/// One engine plus one Postgres tier over a throwaway data directory and a throwaway Postgres
/// schema. <see cref="RestartAsync"/> abandons both instances and rebuilds from the log and the
/// Postgres checkpoint alone — the process-kill simulation.
/// </summary>
internal sealed class TierHarness : IAsyncDisposable
{
    public static Identity Caller { get; } = Identity.Hash("postgres-tests");

    private readonly string _connectionString;
    private readonly int _batchSize;
    private readonly bool _autoMigrate;
    private bool _tierStarted;

    public TierHarness(string connectionString, string schema, string? root = null, int batchSize = 100, bool autoMigrate = true)
    {
        _connectionString = connectionString;
        _batchSize = batchSize;
        _autoMigrate = autoMigrate;
        Schema = schema;
        Root = root ?? Directory.CreateTempSubdirectory("melange-pg-").FullName;
        Logs = new CapturingLoggerFactory();
        Options = BuildOptions();
        Engine = new MelangeEngine(Options, Registry(), Logs);
        Connections = new PostgresConnectionSource(new StaticOptionsMonitor(Options));
        Tier = new PostgresRelationalTier(Engine, Connections, new StaticOptionsMonitor(Options), Logs);
    }

    public string Root { get; }

    public string Schema { get; }

    public MelangeDbOptions Options { get; private set; }

    public MelangeEngine Engine { get; private set; }

    public PostgresConnectionSource Connections { get; private set; }

    public PostgresRelationalTier Tier { get; private set; }

    public CapturingLoggerFactory Logs { get; private set; }

    public async Task StartTierAsync()
    {
        await Tier.StartAsync(CancellationToken.None);
        _tierStarted = true;
    }

    public async Task StopTierAsync()
    {
        if (_tierStarted)
            await Tier.StopAsync(CancellationToken.None);
        _tierStarted = false;
    }

    public ulong Invoke(string reducer, Action<ReducerContext> body) => Engine.Invoke(reducer, Caller, body);

    /// <summary>Waits until the tier's checkpoint reaches <paramref name="lsn"/>, loudly bounded.</summary>
    public async Task WaitAppliedAsync(ulong lsn, int timeoutSeconds = 60)
    {
        try
        {
            await Tier.WaitForAppliedAsync(lsn, TestContext.Current.CancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(timeoutSeconds), TestContext.Current.CancellationToken);
        }
        catch (TimeoutException)
        {
            Assert.Fail(
                $"Timed out waiting for the postgres applier to reach LSN {lsn}; it is at {Tier.AppliedLsn}" +
                $"{(Tier.IsStalled ? " and stalled" : string.Empty)}. Events: {string.Join(" | ", Logs.Events.Select(e => $"{e.EventId}:{e.Message}"))}");
        }
    }

    public static async Task WaitUntilAsync(Func<bool> condition, string what, int timeoutSeconds = 60)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for: {what}");
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>Kill and revive: fresh engine and tier from the same log directory and schema.</summary>
    public async Task RestartAsync(bool startTier = true)
    {
        await StopTierAsync();
        Engine.Dispose();
        await Connections.DisposeAsync();

        Logs = new CapturingLoggerFactory();
        Options = BuildOptions();
        Engine = new MelangeEngine(Options, Registry(), Logs);
        Connections = new PostgresConnectionSource(new StaticOptionsMonitor(Options));
        Tier = new PostgresRelationalTier(Engine, Connections, new StaticOptionsMonitor(Options), Logs);
        if (startTier)
            await StartTierAsync();
    }

    /// <summary>Runs one scalar query against the test database, outside the tier's plumbing.</summary>
    public async Task<object?> ScalarAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>The tier's checkpoint row as stored in Postgres, or null when absent.</summary>
    public async Task<long?> StoredCheckpointAsync() =>
        await ScalarAsync($"SELECT \"applied_lsn\" FROM \"{Schema}\".\"__melange_applier\" WHERE \"applier\" = 'postgres'") as long?;

    public async ValueTask DisposeAsync()
    {
        await StopTierAsync();
        Engine.Dispose();
        await Connections.DisposeAsync();
        try
        {
            Directory.Delete(Root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private MelangeDbOptions BuildOptions() => new()
    {
        HotStore = { Path = Path.Combine(Root, "hot") },
        CommitLog = { Path = Path.Combine(Root, "log") },
        Snapshots = { Enabled = true, IntervalTransactions = long.MaxValue },
        Postgres =
        {
            ConnectionString = _connectionString,
            Schema = Schema,
            ApplyBatchSize = _batchSize,
            AutoMigrate = _autoMigrate,
        },
    };

    private static SchemaRegistry Registry() => new(new MelangeDB.Generated.MelangeModel().Tables());
}
