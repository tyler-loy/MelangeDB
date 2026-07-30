using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The migration contract (DESIGN.md §10, settled here): create and additive-add are automatic
/// under <c>Postgres:AutoMigrate</c> and never touch existing data; destructive disagreement is
/// refused loudly (EventId 1604) in every setting; AutoMigrate off validates and refuses with the
/// exact DDL an operator would run.
/// </summary>
[Collection(PostgresCollection.Name)]
public class SchemaMigrationTests
{
    private readonly PostgresContainerFixture _postgres;

    public SchemaMigrationTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task AutoMigrate_creates_tables_with_mapped_types_and_unique_indexes()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();
        var lsn = harness.Invoke("Register", ctx => ctx.Db.Account.Insert(new Account { Email = "a@b.test" }));
        await harness.WaitAppliedAsync(lsn);

        async Task<string?> TypeOf(string column) => await harness.ScalarAsync(
            $"SELECT data_type FROM information_schema.columns WHERE table_schema = '{harness.Schema}' " +
            $"AND table_name = 'Account' AND column_name = '{column}'") as string;

        Assert.Equal("bigint", await TypeOf("Id"));
        Assert.Equal("text", await TypeOf("Email"));
        Assert.Equal("bytea", await TypeOf("Owner"));
        Assert.Equal("timestamp with time zone", await TypeOf("CreatedAt"));
        Assert.Equal("smallint", await TypeOf("Kind"));
        Assert.Equal("bytea", await TypeOf("Avatar"));
        Assert.Equal("double precision", await TypeOf("Balance"));
        Assert.Equal("real", await TypeOf("Ratio"));
        Assert.Equal("boolean", await TypeOf("Active"));
        Assert.Equal("numeric", await TypeOf("Flags"));

        Assert.Equal(1L, await harness.ScalarAsync(
            $"SELECT count(*) FROM pg_indexes WHERE schemaname = '{harness.Schema}' AND indexname = 'ux_Account_Email'"));

        // Automatic must not mean silent: the applied DDL was announced (EventId 1603).
        Assert.Contains(harness.Logs.Events, e => e.EventId == 1603 && e.Message.Contains("CREATE TABLE"));
    }

    [Fact]
    public async Task Additive_migration_adds_columns_without_dropping_existing_rows()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();

        // An "old deployment": the Stat table exists with fewer columns, and holds data.
        await using (var setup = new TierHarness(_postgres.ConnectionString, schema))
        {
            await setup.ExecuteAsync($"CREATE SCHEMA \"{schema}\"");
            await setup.ExecuteAsync(
                $"""
                CREATE TABLE "{schema}"."Stat" (
                    "Id" bigint NOT NULL PRIMARY KEY,
                    "Metric" text
                )
                """);
            await setup.ExecuteAsync($"INSERT INTO \"{schema}\".\"Stat\" (\"Id\", \"Metric\") VALUES (7, 'legacy')");
        }

        await using var harness = new TierHarness(_postgres.ConnectionString, schema);
        await harness.StartTierAsync();
        var lsn = harness.Invoke("RecordStat", ctx =>
            ctx.Db.Stat.Insert(new Stat { Metric = "fresh", Value = 1, At = ctx.Timestamp }));
        await harness.WaitAppliedAsync(lsn);

        // The legacy row survived, with the added NOT NULL column backfilled to its zero value.
        Assert.Equal("legacy", await harness.ScalarAsync($"SELECT \"Metric\" FROM \"{harness.Schema}\".\"Stat\" WHERE \"Id\" = 7"));
        Assert.Equal(0L, await harness.ScalarAsync($"SELECT \"Value\" FROM \"{harness.Schema}\".\"Stat\" WHERE \"Id\" = 7"));
        Assert.Equal(2L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
    }

    [Fact]
    public async Task Changed_column_type_is_refused_loudly_even_under_AutoMigrate()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        await using var harness = new TierHarness(_postgres.ConnectionString, schema);
        await harness.ExecuteAsync($"CREATE SCHEMA \"{schema}\"");
        await harness.ExecuteAsync(
            $"""
            CREATE TABLE "{schema}"."Stat" (
                "Id" bigint NOT NULL PRIMARY KEY,
                "Metric" text,
                "Value" text,
                "At" timestamp with time zone NOT NULL
            )
            """);

        await harness.StartTierAsync();
        harness.Invoke("RecordStat", ctx =>
            ctx.Db.Stat.Insert(new Stat { Metric = "doomed", Value = 1, At = ctx.Timestamp }));

        await TierHarness.WaitUntilAsync(() => harness.Logs.Has(1604), "the migration refusal (EventId 1604)");
        Assert.True(harness.Tier.IsStalled);
        Assert.Contains(harness.Logs.Events, e => e.EventId == 1604 && e.Message.Contains("destructive"));
    }

    [Fact]
    public async Task AutoMigrate_off_refuses_missing_tables_with_the_pending_ddl()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(
            _postgres.ConnectionString, PostgresContainerFixture.NewSchema(), autoMigrate: false);
        await harness.StartTierAsync();
        harness.Invoke("RecordStat", ctx =>
            ctx.Db.Stat.Insert(new Stat { Metric = "pending", Value = 1, At = ctx.Timestamp }));

        await TierHarness.WaitUntilAsync(() => harness.Logs.Has(1604), "the migration refusal (EventId 1604)");
        Assert.True(harness.Tier.IsStalled);
        var refusal = harness.Logs.Events.First(e => e.EventId == 1604);
        Assert.Contains("CREATE TABLE IF NOT EXISTS", refusal.Message);
        Assert.Contains("AutoMigrate", refusal.Message);

        // Nothing was applied — the projection stayed untouched rather than partially migrated.
        Assert.Null(await harness.StoredCheckpointAsync());
    }
}
