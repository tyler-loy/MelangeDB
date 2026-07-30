using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Ad-hoc SQL under the two-mode contract (<c>Sql:AdHocMode</c>): row policies apply in
/// <c>PolicyEnforced</c> (the default) exactly as a subscription would apply them, <c>Owner</c>
/// bypasses them deliberately, and <c>[ServerOnly]</c> columns are excluded in both modes —
/// otherwise <c>/melange/sql</c> would be the trivial bypass for the whole policy layer.
/// </summary>
public class SqlPolicyTests
{
    private static readonly Action<IServiceCollection> InventoryPolicies = services =>
    {
        services.AddSingleton<IRowPolicy<InventoryItem>, InventoryVisibility>();
        services.AddSingleton<IRowPolicy<InventoryItem>, AdminSeesAllInventory>();
    };

    [Fact]
    public async Task Policy_enforced_sql_cannot_see_rows_a_subscription_would_hide()
    {
        await using var host = await TransportTestHost.StartAsync(services: InventoryPolicies);
        host.Call("GiveItem", TestTokens.IdentityOf("alice"), 0, "alice-sword");
        host.Call("GiveItem", TestTokens.IdentityOf("bob"), 0, "bob-shield");
        host.Call("GiveItem", TestTokens.IdentityOf("bob"), 1, "chest-apple");

        using var asAlice = host.CreateHttp(TestTokens.For("alice"));
        var body = await QueryAsync(asAlice, "SELECT * FROM InventoryItem");
        Assert.Equal(["alice-sword", "chest-apple"], ItemNames(body));
    }

    [Fact]
    public async Task Owner_mode_deliberately_bypasses_row_policies()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Sql:AdHocMode"] = "Owner",
        }, services: InventoryPolicies);
        host.Call("GiveItem", TestTokens.IdentityOf("alice"), 0, "alice-sword");
        host.Call("GiveItem", TestTokens.IdentityOf("bob"), 0, "bob-shield");

        using var asAlice = host.CreateHttp(TestTokens.For("alice"));
        var body = await QueryAsync(asAlice, "SELECT * FROM InventoryItem");
        Assert.Equal(["alice-sword", "bob-shield"], ItemNames(body));
    }

    [Fact]
    public async Task ServerOnly_columns_are_absent_from_sql_results_in_both_modes()
    {
        foreach (var mode in new[] { "PolicyEnforced", "Owner" })
        {
            await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
            {
                ["MelangeDb:Sql:AdHocMode"] = mode,
            });
            host.Call("SpawnCreature", 5f, 777UL);

            using var http = host.CreateHttp();
            var body = await QueryAsync(http, "SELECT * FROM Creature");
            var columns = body.GetProperty("columns").EnumerateArray().Select(c => c.GetString()!).ToArray();
            Assert.Equal(["Id", "X"], columns);

            var explicitRequest = await http.PostAsync(
                "/melange/sql",
                Json("""{"query": "SELECT Id, NextThinkAt FROM Creature"}"""),
                TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.BadRequest, explicitRequest.StatusCode);
            var error = JsonDocument.Parse(await explicitRequest.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
            Assert.Equal("server_only_column", error.GetProperty("error").GetString());
        }
    }

    [Fact]
    public async Task Every_http_endpoint_rejects_a_missing_or_invalid_token()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var anonymous = host.CreateHttp(token: null);
        foreach (var (path, payload) in new (string, string)[]
        {
            ("/melange/call/Noop", "[]"),
            ("/melange/bulk", """{"tables": {}}"""),
            ("/melange/sql", """{"query": "SELECT * FROM Chunk"}"""),
            ("/melange/ticket", "{}"),
        })
        {
            var response = await anonymous.PostAsync(path, Json(payload), TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
        }
    }

    private static string[] ItemNames(JsonElement body)
    {
        var columns = body.GetProperty("columns").EnumerateArray().Select(c => c.GetString()!).ToList();
        var nameIndex = columns.IndexOf("ItemName");
        return [.. body.GetProperty("rows").EnumerateArray()
            .Select(row => row[nameIndex].GetString()!)
            .OrderBy(n => n, StringComparer.Ordinal)];
    }

    private static async Task<JsonElement> QueryAsync(HttpClient http, string query)
    {
        var response = await http.PostAsync(
            "/melange/sql",
            Json(JsonSerializer.Serialize(new { query })),
            TestContext.Current.CancellationToken);
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.True(response.IsSuccessStatusCode, text);
        return JsonDocument.Parse(text).RootElement.Clone();
    }

    private static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");
}
