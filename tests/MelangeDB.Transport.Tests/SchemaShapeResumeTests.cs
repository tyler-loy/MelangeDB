using System.Text.Json.Nodes;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The resume path across an additive schema migration (road-to-0.2 phase 16, revised for 0.4).
/// A resume sends no descriptors: the client keeps decoding with the ones it was given. A client
/// whose cursor predates a migration that changed a subscribed table's columns holds descriptors
/// for the old shape, while the server encodes with the new one, so a column added mid-table
/// would shift every column after it with no error (finding 2 in CLIENT-CONFORMANCE.md). The
/// server therefore refuses that resume, and the client resyncs with fresh descriptors.
/// <para>
/// The migration is staged by doctoring the shape sidecar while the host is stopped: the current
/// entry's Skill shape gets its two Int64 columns' <em>names</em> swapped, which claims the
/// stored bytes were written with PlayerNum and TotalXp in each other's positions. Structurally
/// additive (same names, same kinds, same key — a reorder), so the boot migrates, and recovery
/// re-encodes the stored rows: the two columns' values trade places. The trade is the proof the
/// client's rows came from the migrated store under the new descriptor.
/// </para>
/// </summary>
public class SchemaShapeResumeTests
{
    [Fact]
    public async Task A_resume_cursor_from_before_a_migration_is_refused_and_the_client_resyncs()
    {
        await using var host = await TransportTestHost.StartAsync();

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Skill", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(0, subscription.Count);

        // Records committed while the client is away, under the shape the doctored sidecar will
        // claim is old.
        client.Abort();
        host.Call("AddSkill", 7L, "mining", 1000L, 3);
        host.Call("AddSkill", 9L, "herblore", 2000L, 4);

        await host.RestartAsync(whileStopped: () => SwapSkillColumns(Path.Combine(host.Root, "log")));

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.False(resumed, "Skill's columns changed after the cursor, so the resume must be refused");
        await TransportTestHost.WaitUntilAsync(() => subscription.Count == 2, "the full resync's initial set");

        Assert.Equal(0, subscription.Inconsistencies);
        var byName = subscription.Rows.ToDictionary(row => (string)row.Columns["Name"]!);
        Assert.Equal(1000L, (long)byName["mining"].Columns["PlayerNum"]!);
        Assert.Equal(7L, (long)byName["mining"].Columns["TotalXp"]!);
        Assert.Equal(2000L, (long)byName["herblore"].Columns["PlayerNum"]!);
        Assert.Equal(9L, (long)byName["herblore"].Columns["TotalXp"]!);
    }

    [Fact]
    public async Task A_resume_cursor_from_after_the_migration_is_served()
    {
        await using var host = await TransportTestHost.StartAsync();
        await host.RestartAsync(whileStopped: () => SwapSkillColumns(Path.Combine(host.Root, "log")));

        // This client subscribes under the migrated shape, so its descriptor is current.
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Skill", cancellationToken: TestContext.Current.CancellationToken);
        client.Abort();
        var head = host.Call("AddSkill", 7L, "mining", 1000L, 3);

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "the cursor is after the migration, so the gap is served from the log");
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the gap replay to drain");
        var row = Assert.Single(subscription.Rows);
        Assert.Equal(7L, (long)row.Columns["PlayerNum"]!);
        Assert.Equal(1000L, (long)row.Columns["TotalXp"]!);
    }

    [Fact]
    public async Task A_resume_on_a_table_the_migration_did_not_touch_is_served()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        client.Abort();
        var head = host.Call("SetChunk", 1L, 2L, new byte[] { 3 });

        await host.RestartAsync(whileStopped: () => SwapSkillColumns(Path.Combine(host.Root, "log")));

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "only Skill changed; a Chunk subscription's descriptor is still current");
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the gap replay to drain");
    }

    /// <summary>
    /// Rewrites the sidecar's current entry so Skill's PlayerNum and TotalXp columns claim each
    /// other's positions. Raw JSON on purpose: the test manipulates the file an operator could,
    /// not internals.
    /// </summary>
    private static void SwapSkillColumns(string dataDirectory)
    {
        var path = Path.Combine(dataDirectory, ShapeHistory.FileName);
        var root = JsonNode.Parse(File.ReadAllBytes(path))!;
        var entries = root["Entries"]!.AsArray();
        var columns = entries[^1]!["Tables"]!["Skill"]!["Columns"]!.AsArray();
        foreach (var column in columns)
        {
            column!["Name"] = (string)column["Name"]! switch
            {
                "PlayerNum" => "TotalXp",
                "TotalXp" => "PlayerNum",
                var name => name,
            };
        }

        File.WriteAllText(path, root.ToJsonString());
    }
}
