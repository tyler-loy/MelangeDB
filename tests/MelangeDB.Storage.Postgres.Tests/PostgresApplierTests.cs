using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The applier's core contract: rows appear in Postgres after commit with the checkpoint advancing
/// transactionally with each batch; kills and restarts resume from the checkpoint with no gaps and
/// no duplicates; hot-tier ops are skipped; the whole type map round-trips.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresApplierTests
{
    private readonly PostgresContainerFixture _postgres;

    public PostgresApplierTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task Committed_rows_appear_in_postgres_and_the_checkpoint_advances()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();

        var lsn = 0UL;
        for (var i = 0; i < 5; i++)
        {
            lsn = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "creatures", Value = i, At = ctx.Timestamp }));
        }

        await harness.WaitAppliedAsync(lsn);
        Assert.Equal(5L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal((long)lsn, await harness.StoredCheckpointAsync());
        Assert.Equal(lsn, harness.Tier.AppliedLsn);
    }

    [Fact]
    public async Task Batched_apply_converges_inserts_updates_and_deletes_in_order()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), batchSize: 3);
        await harness.StartTierAsync();

        var ids = new List<long>();
        for (var i = 0; i < 10; i++)
        {
            harness.Invoke("RecordStat", ctx =>
            {
                var stat = ctx.Db.Stat.Insert(new Stat { Metric = $"m{i % 2}", Value = i, At = ctx.Timestamp });
                ids.Add(stat.Id);
            });
        }

        foreach (var id in ids.Take(4))
        {
            harness.Invoke("Bump", ctx =>
            {
                var stat = ctx.Db.Stat.Id.Find(id) ?? throw new InvalidOperationException("row vanished");
                ctx.Db.Stat.Update(stat with { Value = stat.Value + 100 });
            });
        }

        var lsn = 0UL;
        foreach (var id in ids.Skip(8))
            lsn = harness.Invoke("Drop", ctx => ctx.Db.Stat.Id.Delete(id));

        await harness.WaitAppliedAsync(lsn);
        Assert.Equal(8L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal(4L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\" WHERE \"Value\" >= 100"));
        Assert.Equal((long)lsn, await harness.StoredCheckpointAsync());
    }

    [Fact]
    public async Task Kill_after_commit_converges_both_tiers_and_resumes_without_duplicates()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema(), batchSize: 2);
        await harness.StartTierAsync();

        // Mixed transactions: each commit touches a hot and a relational table atomically.
        var applied = 0UL;
        for (var i = 0; i < 3; i++)
        {
            applied = harness.Invoke("MixedWrite", ctx =>
            {
                ctx.Db.Stat.Insert(new Stat { Metric = "mixed", Value = i, At = ctx.Timestamp });
                ctx.Db.HotCounter.Insert(new HotCounter { Id = i, Count = i });
            });
        }

        await harness.WaitAppliedAsync(applied);

        // Stop the tier — its checkpoint freezes — then keep committing, then kill the process.
        await harness.StopTierAsync();
        for (var i = 3; i < 7; i++)
        {
            harness.Invoke("MixedWrite", ctx =>
            {
                ctx.Db.Stat.Insert(new Stat { Metric = "mixed", Value = i, At = ctx.Timestamp });
                ctx.Db.HotCounter.Insert(new HotCounter { Id = i, Count = i });
            });
        }

        Assert.Equal((long)applied, await harness.StoredCheckpointAsync());
        await harness.RestartAsync();

        var head = harness.Engine.Log.HeadLsn;
        await harness.WaitAppliedAsync(head);

        // Both tiers converge to the committed state: 7 rows each, no gaps, no duplicates.
        var statTable = harness.Engine.Schema.Get(typeof(Stat));
        var hotTable = harness.Engine.Schema.Get(typeof(HotCounter));
        Assert.Equal(7, harness.Engine.HotStore.Count(statTable.Id));
        Assert.Equal(7, harness.Engine.HotStore.Count(hotTable.Id));
        Assert.Equal(7L, await harness.ScalarAsync($"SELECT count(*) FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal(21L, await harness.ScalarAsync($"SELECT sum(\"Value\")::bigint FROM \"{harness.Schema}\".\"Stat\""));
        Assert.Equal((long)head, await harness.StoredCheckpointAsync());
    }

    [Fact]
    public async Task Every_column_kind_round_trips_through_the_type_map()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());
        await harness.StartTierAsync();

        var owner = Identity.Hash("account-owner");
        var lsn = harness.Invoke("Register", ctx => ctx.Db.Account.Insert(new Account
        {
            Email = "tyler@example.test",
            Owner = owner,
            CreatedAt = new Timestamp(1_753_800_000_000_000),
            Kind = AccountKind.Paid,
            Avatar = [1, 2, 3],
            Balance = 12.5,
            Ratio = 0.25f,
            Active = true,
            Flags = ulong.MaxValue,
        }));

        await harness.WaitAppliedAsync(lsn);
        var table = $"\"{harness.Schema}\".\"Account\"";
        Assert.Equal("tyler@example.test", await harness.ScalarAsync($"SELECT \"Email\" FROM {table}"));
        Assert.Equal(owner.ToByteArray(), await harness.ScalarAsync($"SELECT \"Owner\" FROM {table}"));
        Assert.Equal((short)2, await harness.ScalarAsync($"SELECT \"Kind\" FROM {table}"));
        Assert.Equal(new byte[] { 1, 2, 3 }, await harness.ScalarAsync($"SELECT \"Avatar\" FROM {table}"));
        Assert.Equal(12.5, await harness.ScalarAsync($"SELECT \"Balance\" FROM {table}"));
        Assert.Equal(0.25f, await harness.ScalarAsync($"SELECT \"Ratio\" FROM {table}"));
        Assert.Equal(true, await harness.ScalarAsync($"SELECT \"Active\" FROM {table}"));
        Assert.Equal((decimal)ulong.MaxValue, await harness.ScalarAsync($"SELECT \"Flags\" FROM {table}"));
        Assert.Equal(1_753_800_000_000_000L, await harness.ScalarAsync(
            $"SELECT (extract(epoch from \"CreatedAt\") * 1000000)::bigint FROM {table}"));
    }

    [Fact]
    public async Task Wait_for_applied_completes_only_when_the_checkpoint_reaches_the_lsn()
    {
        _postgres.SkipUnlessAvailable();
        await using var harness = new TierHarness(_postgres.ConnectionString, PostgresContainerFixture.NewSchema());

        // Tier not started: the wait must not complete on its own.
        var lsn = harness.Invoke("RecordStat", ctx =>
            ctx.Db.Stat.Insert(new Stat { Metric = "wait", Value = 1, At = ctx.Timestamp }));
        var wait = harness.Tier.WaitForAppliedAsync(lsn, TestContext.Current.CancellationToken);
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.False(wait.IsCompleted);

        await harness.StartTierAsync();
        await wait.WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        Assert.True(harness.Tier.AppliedLsn >= lsn);
    }
}
