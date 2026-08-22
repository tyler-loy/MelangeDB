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
    private const string OwnerRole = "melange-owner";

    private static readonly Dictionary<string, string?> Enabled = new()
    {
        ["MelangeDb:Sql:AdHocEnabled"] = "true",
    };

    private static readonly Dictionary<string, string?> EnabledOwner = new()
    {
        ["MelangeDb:Sql:AdHocEnabled"] = "true",
        ["MelangeDb:Sql:AdHocMode"] = "Owner",
    };

    private static readonly Action<IServiceCollection> InventoryPolicies = services =>
    {
        services.AddSingleton<IRowPolicy<InventoryItem>, InventoryVisibility>();
        services.AddSingleton<IRowPolicy<InventoryItem>, AdminSeesAllInventory>();
    };

    [Fact]
    public async Task Adhoc_sql_is_disabled_by_default()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp();
        var response = await http.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT * FROM Chunk"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
        var error = JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("sql_disabled", error.GetProperty("error").GetString());
    }

    [Fact]
    public async Task Policy_enforced_sql_cannot_see_rows_a_subscription_would_hide()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled, services: InventoryPolicies);
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
        await using var host = await TransportTestHost.StartAsync(EnabledOwner, services: InventoryPolicies);
        host.Call("GiveItem", TestTokens.IdentityOf("alice"), 0, "alice-sword");
        host.Call("GiveItem", TestTokens.IdentityOf("bob"), 0, "bob-shield");

        using var asAlice = host.CreateHttp(TestTokens.For("alice", role: OwnerRole));
        var body = await QueryAsync(asAlice, "SELECT * FROM InventoryItem");
        Assert.Equal(["alice-sword", "bob-shield"], ItemNames(body));
    }

    [Fact]
    public async Task Owner_mode_refuses_a_caller_without_the_owner_role()
    {
        await using var host = await TransportTestHost.StartAsync(EnabledOwner);
        using var asAlice = host.CreateHttp(TestTokens.For("alice"));
        var response = await asAlice.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT * FROM Chunk"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
        var error = JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("owner_required", error.GetProperty("error").GetString());
    }

    [Fact]
    public async Task Owner_mode_reads_private_relational_tables_policy_enforced_does_not()
    {
        await using var host = await TransportTestHost.StartAsync(EnabledOwner);
        host.Call("RecordStat", "creatures", 41L);

        using var asOwner = host.CreateHttp(TestTokens.For("alice", role: OwnerRole));
        var body = await QueryAsync(asOwner, "SELECT Metric, Value FROM WorldStat");
        Assert.Equal(41, body.GetProperty("rows")[0][1].GetInt64());

        await using var enforced = await TransportTestHost.StartAsync(Enabled);
        enforced.Call("RecordStat", "creatures", 41L);
        using var asAnyone = enforced.CreateHttp();
        var hidden = await asAnyone.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT * FROM WorldStat"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, hidden.StatusCode);
        var error = JsonDocument.Parse(await hidden.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("unknown_table", error.GetProperty("error").GetString());
    }

    [Fact]
    public async Task Aggregates_are_owner_mode_only_and_relational_only()
    {
        // Policy-enforced mode refuses aggregates outright: policies are in-process code that
        // cannot be pushed into Postgres, and silence here would drop enforcement silently.
        await using var enforced = await TransportTestHost.StartAsync(Enabled);
        using (var http = enforced.CreateHttp())
        {
            var refused = await http.PostAsync(
                "/melange/sql", Json("""{"query": "SELECT COUNT(*) FROM WorldStat"}"""), TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.Forbidden, refused.StatusCode);
            var error = JsonDocument.Parse(await refused.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
            Assert.Equal("owner_required", error.GetProperty("error").GetString());
        }

        await using var owner = await TransportTestHost.StartAsync(EnabledOwner);
        using var asOwner = owner.CreateHttp(TestTokens.For("alice", role: OwnerRole));

        // A hot-tier table cannot be aggregated even by an owner — the tier is the boundary.
        var hot = await asOwner.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT COUNT(*) FROM Chunk"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, hot.StatusCode);
        var hotError = JsonDocument.Parse(await hot.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("not_relational", hotError.GetProperty("error").GetString());

        // A valid aggregate with no Postgres configured is an explicit error, never empty rows.
        var missing = await asOwner.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT COUNT(*) FROM WorldStat"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, missing.StatusCode);
        var missingError = JsonDocument.Parse(await missing.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("no_relational_tier", missingError.GetProperty("error").GetString());
    }

    /// <summary>
    /// The other half of the silent-empty case (#111), which the row-shape fix did not cover:
    /// placement and tier are independent axes, so a table can be both <c>Relational</c> and
    /// <c>Partitioned</c>, and an aggregate over one on a hub counts only what the hub's own engine
    /// wrote. That is worse than the empty result the row path refuses — an empty set at least
    /// looks like nothing, while <c>COUNT(*) = 12</c> over a fraction of the rows looks like an
    /// answer. Single-node it must still be served: placement is inert there.
    /// </summary>
    [Fact]
    public async Task Partitioned_aggregates_are_refused_in_a_cluster_and_served_single_node()
    {
        await using var clustered = await TransportTestHost.StartAsync(new Dictionary<string, string?>(EnabledOwner)
        {
            ["MelangeDb:Cluster:Role"] = "Hub",
            ["MelangeDb:Cluster:NodeName"] = "hub",
            ["MelangeDb:Cluster:Secret"] = "test-cluster-secret",
        });
        using var onHub = clustered.CreateHttp(TestTokens.For("admin", role: OwnerRole));
        var refused = await onHub.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT COUNT(*) FROM WorldStat"}"""), TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.BadRequest, refused.StatusCode);
        var error = JsonDocument.Parse(await refused.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("partitioned_elsewhere", error.GetProperty("error").GetString());
        Assert.Contains("Partitioned", error.GetProperty("message").GetString()!, StringComparison.Ordinal);

        // Refused for placement, not for the missing tier: neither host here has Postgres, so the
        // single-node arm below reaches no_relational_tier and this one must not.
        Assert.DoesNotContain("no_relational_tier", error.GetProperty("error").GetString()!, StringComparison.Ordinal);

        await using var single = await TransportTestHost.StartAsync(EnabledOwner);
        using var onNode = single.CreateHttp(TestTokens.For("admin", role: OwnerRole));
        var served = await onNode.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT COUNT(*) FROM WorldStat"}"""), TestContext.Current.CancellationToken);
        var servedError = JsonDocument.Parse(await served.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal("no_relational_tier", servedError.GetProperty("error").GetString());
    }

    [Fact]
    public async Task Aggregate_shapes_are_not_subscribable()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var failure = await Assert.ThrowsAsync<MelangeDB.Client.MelangeSubscriptionException>(
            () => client.SubscribeAsync("SELECT COUNT(*) FROM Skill", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("parse", failure.Code);
    }

    [Fact]
    public async Task ServerOnly_columns_are_absent_from_sql_results_in_both_modes()
    {
        foreach (var mode in new[] { "PolicyEnforced", "Owner" })
        {
            await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
            {
                ["MelangeDb:Sql:AdHocEnabled"] = "true",
                ["MelangeDb:Sql:AdHocMode"] = mode,
            });
            host.Call("SpawnCreature", 5f, 777UL);

            using var http = host.CreateHttp(TestTokens.For(TestTokens.DefaultSubject, role: mode == "Owner" ? OwnerRole : null));
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

    /// <summary>
    /// The silent-empty case (issue #111): in a cluster, a <c>Partitioned</c> table's rows live in
    /// the shard engines, and <c>/melange/sql</c> reads the node-local one. Returning an empty
    /// result set tells an operator console "no rows" when the truth is "wrong node" — a
    /// successful answer that is false. Placement is inert single-node, so the same query against
    /// the same table must still succeed there; that half is the regression this pairs against.
    /// </summary>
    [Fact]
    public async Task Partitioned_tables_are_refused_in_a_cluster_and_served_single_node()
    {
        await using var clustered = await TransportTestHost.StartAsync(new Dictionary<string, string?>(EnabledOwner)
        {
            ["MelangeDb:Cluster:Role"] = "Hub",
            ["MelangeDb:Cluster:NodeName"] = "hub",
            ["MelangeDb:Cluster:Secret"] = "test-cluster-secret",
        });
        using var onHub = clustered.CreateHttp(TestTokens.For("admin", role: OwnerRole));
        var refused = await onHub.PostAsync(
            "/melange/sql", Json("""{"query": "SELECT * FROM Chunk"}"""), TestContext.Current.CancellationToken);

        var text = await refused.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, refused.StatusCode);
        var error = JsonDocument.Parse(text).RootElement;
        Assert.Equal("partitioned_elsewhere", error.GetProperty("error").GetString());

        // The message has to name the placement and the role: "wrong node" is only actionable if
        // the operator learns which node would have answered.
        var message = error.GetProperty("message").GetString()!;
        Assert.Contains("Partitioned", message, StringComparison.Ordinal);
        Assert.Contains("Hub", message, StringComparison.Ordinal);

        // Same query, same table, no cluster role: still served, and still empty-because-empty
        // rather than empty-because-elsewhere.
        await using var single = await TransportTestHost.StartAsync(EnabledOwner);
        using var onSingle = single.CreateHttp(TestTokens.For("admin", role: OwnerRole));
        var served = await QueryAsync(onSingle, "SELECT * FROM Chunk");
        Assert.Empty(served.GetProperty("rows").EnumerateArray());
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
