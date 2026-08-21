using System.Text;
using System.Text.Json;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The bulk ingestion gate (issue #31): <c>/melange/bulk</c> writes rows past every reducer and
/// its policies, so it is off by default (<c>Bulk:Enabled</c>) and owner-role-gated when on
/// (<c>Bulk:OwnerRole</c>) — the same posture as <c>/melange/sql</c>, for a strictly stronger
/// capability. Any-valid-token was the vulnerability: in a game, every player holds one.
/// </summary>
public class BulkGateTests
{
    private const string BulkOwnerRole = "melange-bulk-owner";
    private const string SqlOwnerRole = "melange-owner";
    private const string ClusterSecret = "bulk-gate-cluster-secret";

    private static readonly Dictionary<string, string?> Enabled = new()
    {
        ["MelangeDb:Bulk:Enabled"] = "true",
    };

    /// <summary>A shard-role host, so the authenticator accepts internal identity assertions.</summary>
    private static readonly Dictionary<string, string?> EnabledClustered = new()
    {
        ["MelangeDb:Bulk:Enabled"] = "true",
        ["MelangeDb:Cluster:Role"] = "Shard",
        ["MelangeDb:Cluster:NodeName"] = "bulk-gate-tests",
        ["MelangeDb:Cluster:Secret"] = ClusterSecret,
    };

    private const string OneRow = """{"tables": {"Chunk": [{"Id": 1, "X": 2, "Data": "AQID"}]}}""";

    /// <summary>
    /// A <c>Local</c> table, which is the only placement a shard-role node's own engine holds. The
    /// clustered tests here are about authentication, and <c>Chunk</c> is <c>Partitioned</c> —
    /// legal to bulk-load on a single node, refused on a shard node since #115.
    /// </summary>
    private const string OneLocalRow = """{"tables": {"NodeCounter": [{"Id": 1, "Count": 7}]}}""";

    [Fact]
    public async Task The_issue_repro_a_guest_token_on_a_stock_host_is_refused()
    {
        // Issue #31's exact scenario: stock host, default options, a valid token carrying the
        // guest role. Before the gate this answered 200 and wrote the row.
        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp(TestTokens.For("guest-player", role: "guest"));

        var response = await http.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("bulk_disabled", (await ReadJsonAsync(response)).GetProperty("error").GetString());
        var schema = host.Engine.Schema.Get(typeof(Chunk));
        Assert.Equal(0, host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(schema.Id).Count()));
    }

    [Fact]
    public async Task Disabled_by_default_even_for_a_token_carrying_the_bulk_owner_role()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp(TestTokens.For("loader", role: BulkOwnerRole));

        var response = await http.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("bulk_disabled", (await ReadJsonAsync(response)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task Enabled_still_refuses_a_caller_without_the_bulk_owner_claim()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);

        // No role at all, and — proving Bulk:OwnerRole is distinct from Sql:OwnerRole — the SQL
        // owner role: read-everything must not imply write-anything.
        foreach (var role in new string?[] { null, SqlOwnerRole })
        {
            using var http = host.CreateHttp(TestTokens.For("caller", role: role));
            var response = await http.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);
            Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("owner_required", (await ReadJsonAsync(response)).GetProperty("error").GetString());
        }
    }

    [Fact]
    public async Task Enabled_with_the_bulk_owner_claim_writes_rows()
    {
        await using var host = await TransportTestHost.StartAsync(Enabled);
        using var http = host.CreateHttp(TestTokens.For("loader", role: BulkOwnerRole));

        var response = await http.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);

        var body = await ReadJsonAsync(response);
        Assert.True(response.IsSuccessStatusCode, body.ToString());
        Assert.Equal(1, body.GetProperty("rows").GetInt32());
        var result = Assert.Single(body.GetProperty("results").EnumerateArray().ToArray());
        Assert.Equal(JsonValueKind.Null, result.GetProperty("shard").ValueKind);
        Assert.Equal(1, result.GetProperty("rows").GetInt32());
        Assert.True(result.GetProperty("lsn").GetUInt64() > 0);

        var schema = host.Engine.Schema.Get(typeof(Chunk));
        var row = host.Engine.ReadConsistent(_ =>
            host.Engine.HotStore.Scan(schema.Id).Select(pair => (Chunk)Core.RowSerializer.Deserialize(schema, pair.Value.ToArray())).Single());
        Assert.Equal(1, row.Id);
        Assert.Equal(2, row.X);
        Assert.Equal(new byte[] { 1, 2, 3 }, row.Data);
    }

    [Fact]
    public async Task An_assertion_minted_with_bulk_owner_authorizes_bulk()
    {
        await using var host = await TransportTestHost.StartAsync(EnabledClustered);
        var assertion = Core.InternalIdentityAssertion.Mint(
            ClusterSecret, TestTokens.IdentityOf("pipeline"),
            isGuest: false, isSqlOwner: false, isBulkOwner: true, DateTimeOffset.UtcNow.AddMinutes(5));
        using var http = host.CreateHttp(assertion);

        var response = await http.PostAsync("/melange/bulk", Json(OneLocalRow), TestContext.Current.CancellationToken);

        var body = await ReadJsonAsync(response);
        Assert.True(response.IsSuccessStatusCode, body.ToString());
        Assert.Equal(1, body.GetProperty("rows").GetInt32());
        var result = Assert.Single(body.GetProperty("results").EnumerateArray().ToArray());
        Assert.Equal(JsonValueKind.Null, result.GetProperty("shard").ValueKind);
        Assert.Equal(1, result.GetProperty("rows").GetInt32());
        Assert.True(result.GetProperty("lsn").GetUInt64() > 0);
    }

    /// <summary>
    /// A shard node's own engine is not any shard's engine, so a batch of <c>Partitioned</c> rows
    /// posted to it is refused rather than written where nothing reads it (#115). The mirror of
    /// the ad-hoc SQL refusal, and the reason the clustered tests above load a <c>Local</c> table.
    /// </summary>
    [Fact]
    public async Task A_shard_role_node_refuses_a_partitioned_batch()
    {
        await using var host = await TransportTestHost.StartAsync(EnabledClustered);
        using var http = host.CreateHttp(TestTokens.For("loader", role: BulkOwnerRole));

        var response = await http.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.BadRequest, response.StatusCode);
        var error = await ReadJsonAsync(response);
        Assert.Equal("partitioned_elsewhere", error.GetProperty("error").GetString());
        Assert.Contains("fans it out", error.GetProperty("message").GetString()!, StringComparison.Ordinal);

        var schema = host.Engine.Schema.Get(typeof(Chunk));
        Assert.Equal(0, host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(schema.Id).Count()));
    }

    [Fact]
    public async Task An_assertion_minted_without_bulk_owner_is_refused()
    {
        await using var host = await TransportTestHost.StartAsync(EnabledClustered);
        var assertion = Core.InternalIdentityAssertion.Mint(
            ClusterSecret, TestTokens.IdentityOf("pipeline"),
            isGuest: false, isSqlOwner: true, isBulkOwner: false, DateTimeOffset.UtcNow.AddMinutes(5));
        using var http = host.CreateHttp(assertion);

        var response = await http.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("owner_required", (await ReadJsonAsync(response)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task An_unauthenticated_request_is_401_before_the_gate_answers_anything()
    {
        // Auth stays first: a probe without a token learns nothing about Bulk:Enabled.
        await using var host = await TransportTestHost.StartAsync(Enabled);
        using var anonymous = host.CreateHttp(token: null);

        var response = await anonymous.PostAsync("/melange/bulk", Json(OneRow), TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");

    private static async Task<JsonElement> ReadJsonAsync(HttpResponseMessage response)
    {
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonDocument.Parse(text).RootElement.Clone();
    }
}
