using System.Text.Json.Nodes;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The resume path across an additive schema migration (road-to-0.2 phase 16). A cursor from
/// before the migration replays records whose rows carry the old column order; the wire
/// descriptor the resumed subscriptions declared is the new one, so the server must re-encode
/// every replayed row — the same by-name transform recovery uses.
/// <para>
/// The migration is staged by doctoring the shape sidecar while the host is stopped: the current
/// entry's Skill shape gets its two Int64 columns' <em>names</em> swapped, which claims the
/// stored bytes were written with PlayerNum and TotalXp in each other's positions. Structurally
/// additive (same names, same kinds, same key — a reorder), so the boot migrates; and the
/// transform's effect is unmistakable, because the two columns' values trade places. If the
/// resume replay ever served raw record bytes, the values would come back unswapped and the
/// asserts below would catch it.
/// </para>
/// </summary>
public class SchemaShapeResumeTests
{
    [Fact]
    public async Task A_resume_cursor_from_before_a_migration_replays_re_encoded_rows()
    {
        await using var host = await TransportTestHost.StartAsync();

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Skill", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(0, subscription.Count);

        // The gap the resume must serve: records committed while the client is away, under the
        // shape the doctored sidecar will claim is old.
        client.Abort();
        host.Call("AddSkill", 7L, "mining", 1000L, 3);
        var head = host.Call("AddSkill", 9L, "herblore", 2000L, 4);

        await host.RestartAsync(whileStopped: () => SwapSkillColumns(Path.Combine(host.Root, "log")));

        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);
        Assert.True(resumed, "the epoch survived the restart, so the gap must be served from the log");
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the gap replay to drain");

        Assert.Equal(2, subscription.Count);
        Assert.Equal(0, subscription.Inconsistencies);
        var byName = subscription.Rows.ToDictionary(row => (string)row.Columns["Name"]!);

        // The trade is the proof: PlayerNum carries what was committed as TotalXp and vice
        // versa, on the client, via the resume replay — the transform ran on the wire path.
        Assert.Equal(1000L, (long)byName["mining"].Columns["PlayerNum"]!);
        Assert.Equal(7L, (long)byName["mining"].Columns["TotalXp"]!);
        Assert.Equal(2000L, (long)byName["herblore"].Columns["PlayerNum"]!);
        Assert.Equal(9L, (long)byName["herblore"].Columns["TotalXp"]!);
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
