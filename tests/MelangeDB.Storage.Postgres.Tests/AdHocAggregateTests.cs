using System.Net;
using System.Text;
using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The admin console's aggregates, end to end through <c>/melange/sql</c> on a real host with a
/// real Postgres behind it: counts, hourly <c>date_trunc</c> bucketing, grouping, and predicates —
/// exactly what the reference project's hand-written ScrapeWorker existed to provide.
/// </summary>
[Collection(PostgresCollection.Name)]
public class AdHocAggregateTests
{
    private static readonly SymmetricSecurityKey Key =
        new(Encoding.UTF8.GetBytes("melange-postgres-tests-signing-key-0123456789"));

    private readonly PostgresContainerFixture _postgres;

    public AdHocAggregateTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task Owner_sql_runs_counts_grouping_and_hourly_bucketing_against_postgres()
    {
        _postgres.SkipUnlessAvailable();
        var root = Directory.CreateTempSubdirectory("melange-pg-http-").FullName;
        var schema = PostgresContainerFixture.NewSchema();
        await using var app = await StartAppAsync(root, schema);
        var port = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First()).Port;
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", OwnerToken());

        // Three stats in hour A, two in hour B; values chosen so every aggregate is checkable.
        var caller = Identity.Hash("aggregate-tests");
        var hourA = new Timestamp(1_753_800_000_000_000);
        var hourB = new Timestamp(hourA.UnixTimeMicroseconds + 3_600_000_000);
        var engine = app.Services.GetRequiredService<MelangeEngine>();
        var lsn = 0UL;
        foreach (var (at, metric, value) in new[]
        {
            (hourA, "creatures", 10L),
            (new Timestamp(hourA.UnixTimeMicroseconds + 60_000_000), "creatures", 20L),
            (new Timestamp(hourA.UnixTimeMicroseconds + 120_000_000), "players", 5L),
            (hourB, "creatures", 40L),
            (new Timestamp(hourB.UnixTimeMicroseconds + 60_000_000), "players", 7L),
        })
        {
            lsn = engine.Invoke("Seed", caller, ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = metric, Value = value, At = at }));
        }

        await app.Services.GetRequiredService<PostgresRelationalTier>()
            .WaitForAppliedAsync(lsn, TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        // COUNT(*): the simplest thing the admin console needs.
        var count = await QueryAsync(http, """{"query": "SELECT COUNT(*) FROM Stat"}""");
        Assert.Equal(["count"], Columns(count));
        Assert.Equal(5, count.GetProperty("rows")[0][0].GetInt64());

        // GROUP BY metric with the full aggregate set.
        var grouped = await QueryAsync(
            http, """{"query": "SELECT Metric, COUNT(*), SUM(Value), AVG(Value), MIN(Value), MAX(Value) FROM Stat GROUP BY Metric"}""");
        Assert.Equal(["Metric", "count", "sum_Value", "avg_Value", "min_Value", "max_Value"], Columns(grouped));
        var creatures = grouped.GetProperty("rows")[0];
        Assert.Equal("creatures", creatures[0].GetString());
        Assert.Equal(3, creatures[1].GetInt64());
        Assert.Equal(70, creatures[2].GetInt64());
        Assert.Equal(10, creatures[4].GetInt64());
        Assert.Equal(40, creatures[5].GetInt64());

        // Hourly bucketing — the date_trunc query the ScrapeWorker existed for.
        var buckets = await QueryAsync(
            http, """{"query": "SELECT DATE_TRUNC('hour', At), COUNT(*) FROM Stat GROUP BY DATE_TRUNC('hour', At)"}""");
        var bucketRows = buckets.GetProperty("rows");
        Assert.Equal(2, bucketRows.GetArrayLength());
        Assert.Equal(3, bucketRows[0][1].GetInt64());
        Assert.Equal(2, bucketRows[1][1].GetInt64());
        Assert.Equal(3_600_000_000, bucketRows[1][0].GetInt64() - bucketRows[0][0].GetInt64());

        // A predicate narrows the aggregate; parameters bind, never concatenate.
        var filtered = await QueryAsync(
            http, """{"query": "SELECT COUNT(*) FROM Stat WHERE Metric = :m", "params": {"m": "players"}}""");
        Assert.Equal(2, filtered.GetProperty("rows")[0][0].GetInt64());

        // Owner-mode row shapes on the same (private, relational) table serve from the hot store.
        var rows = await QueryAsync(http, """{"query": "SELECT Metric, Value FROM Stat WHERE Metric = :m", "params": {"m": "creatures"}}""");
        Assert.Equal(3, rows.GetProperty("rows").GetArrayLength());
    }

    [Fact]
    public async Task Sql_injection_shaped_queries_die_in_the_parser()
    {
        _postgres.SkipUnlessAvailable();
        var root = Directory.CreateTempSubdirectory("melange-pg-inj-").FullName;
        await using var app = await StartAppAsync(root, PostgresContainerFixture.NewSchema());
        var port = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First()).Port;
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", OwnerToken());

        foreach (var query in new[]
        {
            "SELECT COUNT(*) FROM Stat; DROP TABLE Stat",
            "SELECT COUNT(*) FROM Stat WHERE Metric = 'x' OR '1'='1'",
            "SELECT DATE_TRUNC('hour''); DROP TABLE Stat; --', At) FROM Stat GROUP BY Metric",
            "SELECT * FROM \"Stat\"",
        })
        {
            var response = await http.PostAsync(
                "/melange/sql",
                new StringContent(JsonSerializer.Serialize(new { query }), Encoding.UTF8, "application/json"),
                TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }
    }

    private static string[] Columns(JsonElement body) =>
        [.. body.GetProperty("columns").EnumerateArray().Select(c => c.GetString()!)];

    private static async Task<JsonElement> QueryAsync(HttpClient http, string body)
    {
        var response = await http.PostAsync(
            "/melange/sql", new StringContent(body, Encoding.UTF8, "application/json"), TestContext.Current.CancellationToken);
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.True(response.IsSuccessStatusCode, text);
        return JsonDocument.Parse(text).RootElement.Clone();
    }

    private static string OwnerToken() => new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Issuer = "melange-pg-tests",
        Claims = new Dictionary<string, object> { ["sub"] = "admin", ["role"] = "melange-owner" },
        Expires = DateTime.UtcNow.AddHours(1),
        SigningCredentials = new SigningCredentials(Key, SecurityAlgorithms.HmacSha256),
    });

    private async Task<WebApplication> StartAppAsync(string root, string schema)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(root, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(root, "hot"),
            ["MelangeDb:Sql:AdHocEnabled"] = "true",
            ["MelangeDb:Sql:AdHocMode"] = "Owner",
            ["MelangeDb:Postgres:Schema"] = schema,
            ["MelangeDb:Postgres:AutoMigrate"] = "true",
        });
        builder.Services.AddAuthentication().AddJwtBearer(jwt => jwt.TokenValidationParameters = new TokenValidationParameters
        {
            ValidIssuer = "melange-pg-tests",
            ValidateAudience = false,
            IssuerSigningKey = Key,
            RoleClaimType = "role",
        });
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Stat).Assembly)
            .AddReducersFrom(typeof(Stat).Assembly)
            .AddPostgres(_postgres.ConnectionString));

        var app = builder.Build();
        app.UseWebSockets();
        app.MapMelangeSocket();
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }
}
