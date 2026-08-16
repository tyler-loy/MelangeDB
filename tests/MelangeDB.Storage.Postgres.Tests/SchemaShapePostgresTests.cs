using System.Text.Json.Nodes;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Postgres.Tests;

/// <summary>
/// The decoupled applier across an additive schema migration (road-to-0.2 phase 16): a Postgres
/// checkpoint that lagged across a migration boot re-reads records written under the old shape,
/// and its own dispatch loop must route them through the same by-name transform recovery uses —
/// the decoupled half of the contract on <c>MelangeEngine.TransformToCurrentShape</c>.
/// <para>
/// The migration is staged by doctoring the shape sidecar while everything is stopped: Stat's two
/// Int64 columns' names swap, claiming the stored bytes carry Id and Value in each other's
/// positions — structurally additive (a reorder), and unmistakable, because re-applied rows reach
/// Postgres with the two columns' values traded. Without the transform, the re-applied records
/// are byte-identical to what Postgres already holds and no traded value can appear.
/// </para>
/// </summary>
[Collection(PostgresCollection.Name)]
public class SchemaShapePostgresTests
{
    private readonly PostgresContainerFixture _postgres;

    public SchemaShapePostgresTests(PostgresContainerFixture postgres) => _postgres = postgres;

    [Fact]
    public async Task A_checkpoint_that_lagged_across_a_migration_re_applies_re_encoded_rows()
    {
        _postgres.SkipUnlessAvailable();
        var schema = PostgresContainerFixture.NewSchema();
        await using var harness = new TierHarness(_postgres.ConnectionString, schema);
        await harness.StartTierAsync();

        var applied = 0UL;
        for (var i = 0; i < 5; i++)
        {
            var value = 1000L + i;
            applied = harness.Invoke("RecordStat", ctx =>
                ctx.Db.Stat.Insert(new Stat { Metric = "world", Value = value, At = ctx.Timestamp }));
        }

        await harness.WaitAppliedAsync(applied);
        await harness.StopTierAsync();

        // Five inserts allocate ids nowhere near 1000, so "an Id of 1000 or more" can only mean a
        // row whose Id column carries what was committed as Value: the transform's fingerprint.
        Assert.Equal(0L, await harness.ScalarAsync($"SELECT count(*) FROM \"{schema}\".\"Stat\" WHERE \"Id\" >= 1000"));

        // Stage the lag-across-migration: the checkpoint rewinds to 1, and the sidecar claims the
        // records it will re-read were written with Id and Value in each other's positions.
        await harness.ExecuteAsync(
            $"UPDATE \"{schema}\".\"__melange_applier\" SET \"applied_lsn\" = 1 WHERE \"applier\" = 'postgres'");
        SwapStatColumns(Path.Combine(harness.Root, "log"));

        await harness.RestartAsync();
        await harness.WaitAppliedAsync(harness.Engine.Log.HeadLsn);

        // Four, not five: the checkpoint means "applied through LSN 1", and LSN 1 is the first
        // insert, so it is rightly never re-read — the other four re-apply transformed. Without
        // the transform this count is zero (the re-applied rows are byte-identical upserts).
        Assert.Equal(4L, await harness.ScalarAsync($"SELECT count(*) FROM \"{schema}\".\"Stat\" WHERE \"Id\" >= 1000"));
    }

    private static void SwapStatColumns(string dataDirectory)
    {
        var path = Path.Combine(dataDirectory, ShapeHistory.FileName);
        var root = JsonNode.Parse(File.ReadAllBytes(path))!;
        var entries = root["Entries"]!.AsArray();
        // The Key stays "Id": the compare is by name, so renaming the key entry would read as a
        // key move and refuse; under the swapped labels, "Id" simply names the other position.
        var stat = entries[^1]!["Tables"]!["Stat"]!;
        foreach (var column in stat["Columns"]!.AsArray())
        {
            column!["Name"] = (string)column["Name"]! switch
            {
                "Id" => "Value",
                "Value" => "Id",
                var name => name,
            };
        }

        File.WriteAllText(path, root.ToJsonString());
    }
}
