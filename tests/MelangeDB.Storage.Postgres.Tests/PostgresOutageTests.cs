using System.Net;
using System.Net.Sockets;
using Testcontainers.PostgreSql;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The done-criterion at the heart of the design: stopping Postgres does not stop the server.
/// Writes keep committing while the applier stalls loudly (EventId 1601) and its lag grows; when
/// Postgres returns, catch-up is clean — no gaps, no duplicates — and announced (EventId 1602).
/// Uses its own container on a fixed host port so the connection string survives the restart.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresOutageTests
{
    private readonly PostgresContainerFixture _postgres;

    public PostgresOutageTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task Postgres_down_is_not_server_down_and_catchup_is_clean_on_reconnect()
    {
        _postgres.SkipUnlessAvailable();
        var port = FreePort();
        await using var container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithPortBinding(port, 5432)
            .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        await using var harness = new TierHarness(container.GetConnectionString(), PostgresContainerFixture.NewSchema(), batchSize: 2);
        await harness.StartTierAsync();

        var beforeOutage = 0UL;
        for (var i = 0; i < 3; i++)
        {
            beforeOutage = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "outage", Value = i, At = ctx.Timestamp }));
        }

        await harness.WaitAppliedAsync(beforeOutage);

        // Postgres goes away mid-run.
        await container.StopAsync(TestContext.Current.CancellationToken);

        // The server is unaffected: commits keep landing and reads keep serving.
        var duringOutage = 0UL;
        for (var i = 3; i < 8; i++)
        {
            duringOutage = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "outage", Value = i, At = ctx.Timestamp }));
        }

        var statTable = harness.Engine.Schema.Get(typeof(Stat));
        Assert.Equal(8, harness.Engine.HotStore.Count(statTable.Id));

        // The stall is loud, and the lag is visible through the applier's checkpoint.
        await TierHarness.WaitUntilAsync(
            () => harness.Tier.IsStalled && harness.Logs.Has(1601),
            "the applier to stall loudly (EventId 1601)");
        Assert.True(
            harness.Tier.AppliedLsn < duringOutage,
            $"applied={harness.Tier.AppliedLsn}, head={duringOutage}; events: " +
            string.Join(" | ", harness.Logs.Events.Select(e => $"{e.EventId}:{e.Message}")));
        Assert.True(harness.Engine.Log.HeadLsn - harness.Tier.AppliedLsn > 0, "the applier's lag must be visible");

        // Postgres returns: catch-up is automatic, clean, and announced.
        await container.StartAsync(TestContext.Current.CancellationToken);
        await harness.WaitAppliedAsync(duringOutage, timeoutSeconds: 120);
        await TierHarness.WaitUntilAsync(
            () => !harness.Tier.IsStalled && harness.Logs.Has(1602),
            "the recovery log (EventId 1602)");
        Assert.Equal(8L, await ScalarAsync(container.GetConnectionString(), $"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal(28L, await ScalarAsync(container.GetConnectionString(), $"SELECT sum(\"Value\")::bigint FROM \"{harness.Schema}\".\"Stat\""));
    }

    private static async Task<object?> ScalarAsync(string connectionString, string sql)
    {
        await using var connection = new Npgsql.NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = new Npgsql.NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
