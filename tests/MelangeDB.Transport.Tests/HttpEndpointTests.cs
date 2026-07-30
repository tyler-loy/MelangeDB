using System.Text;
using System.Text.Json;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The plain-HTTP endpoints: one-shot reducer calls and bulk ingestion work without opening a
/// websocket, the SQL endpoint runs the four shapes, and the ticket endpoint mints single-use
/// stubs for phase 04.
/// </summary>
public class HttpEndpointTests
{
    [Fact]
    public async Task One_shot_reducer_invocation_works_without_a_websocket()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = new HttpClient { BaseAddress = host.HttpBase };

        var response = await http.PostAsync(
            "/melange/call/SetChunk",
            Json("""[7, 3, {"$bytes": "AQID"}]"""),
            TestContext.Current.CancellationToken);
        var body = await ReadJsonAsync(response);
        Assert.True(response.IsSuccessStatusCode, body.ToString());
        Assert.True(body.GetProperty("ok").GetBoolean());
        Assert.True(body.GetProperty("lsn").GetUInt64() > 0);

        var schema = host.Engine.Schema.Get(typeof(Chunk));
        var row = host.Engine.ReadConsistent(_ =>
            host.Engine.HotStore.Scan(schema.Id).Select(pair => (Chunk)Core.RowSerializer.Deserialize(schema, pair.Value.ToArray())).Single());
        Assert.Equal(7, row.Id);
        Assert.Equal(3, row.X);
        Assert.Equal(new byte[] { 1, 2, 3 }, row.Data);
    }

    [Fact]
    public async Task Reducer_call_errors_map_to_http_codes()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = new HttpClient { BaseAddress = host.HttpBase };

        var unknown = await http.PostAsync("/melange/call/NoSuchReducer", Json("[]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, unknown.StatusCode);
        Assert.Equal("unknown_reducer", (await ReadJsonAsync(unknown)).GetProperty("error").GetString());

        var badArgs = await http.PostAsync("/melange/call/SetChunk", Json("""["nope"]"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, badArgs.StatusCode);
        Assert.Equal("invalid_args", (await ReadJsonAsync(badArgs)).GetProperty("error").GetString());

        var rejected = await http.PostAsync("/melange/call/Move", Json("[1.5]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, rejected.StatusCode);
        Assert.Equal("rejected", (await ReadJsonAsync(rejected)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task Bulk_ingestion_appends_one_write_set_not_one_transaction_per_row()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = new HttpClient { BaseAddress = host.HttpBase };
        var headBefore = host.Engine.Log.HeadLsn;

        var rows = string.Join(',', Enumerable.Range(0, 1000).Select(i =>
            $"{{\"Id\": {i}, \"X\": {i % 32}, \"Data\": \"{Convert.ToBase64String(new[] { (byte)i })}\"}}"));
        var response = await http.PostAsync("/melange/bulk", Json("{\"tables\": {\"Chunk\": [" + rows + "]}}"), TestContext.Current.CancellationToken);
        var body = await ReadJsonAsync(response);
        Assert.True(response.IsSuccessStatusCode, body.ToString());
        Assert.Equal(1000, body.GetProperty("rows").GetInt32());

        // One large write set means exactly one log record for the whole load.
        Assert.Equal(headBefore + 1, host.Engine.Log.HeadLsn);
        var schema = host.Engine.Schema.Get(typeof(Chunk));
        Assert.Equal(1000, host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(schema.Id).Count()));
    }

    [Fact]
    public async Task Adhoc_sql_runs_the_four_shapes_and_respects_table_visibility()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("AddSkill", 7L, "mining", 10L, 1);
        host.Call("AddSkill", 7L, "logging", 20L, 2);
        host.Call("AddSkill", 8L, "smithing", 30L, 3);
        host.Call("AddSecret", 1UL, "hidden");
        using var http = new HttpClient { BaseAddress = host.HttpBase };

        var projected = await http.PostAsync(
            "/melange/sql",
            Json("""{"query": "SELECT Name, TotalXp FROM Skill WHERE PlayerNum = :p", "params": {"p": 7}}"""),
            TestContext.Current.CancellationToken);
        var body = await ReadJsonAsync(projected);
        Assert.Equal(["Name", "TotalXp"], body.GetProperty("columns").EnumerateArray().Select(c => c.GetString()!).ToArray());
        var resultRows = body.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(2, resultRows.Count);
        Assert.Equal("mining", resultRows[0][0].GetString());
        Assert.Equal(10, resultRows[0][1].GetInt64());

        var whole = await http.PostAsync("/melange/sql", Json("""{"query": "SELECT * FROM Skill"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(3, (await ReadJsonAsync(whole)).GetProperty("rows").GetArrayLength());

        var range = await http.PostAsync(
            "/melange/sql",
            Json("""{"query": "SELECT * FROM Skill WHERE PlayerNum BETWEEN 8 AND 9"}"""),
            TestContext.Current.CancellationToken);
        Assert.Equal(1, (await ReadJsonAsync(range)).GetProperty("rows").GetArrayLength());

        var secret = await http.PostAsync("/melange/sql", Json("""{"query": "SELECT * FROM SecretTable"}"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, secret.StatusCode);
        Assert.Equal("unknown_table", (await ReadJsonAsync(secret)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task Ticket_endpoint_mints_single_use_short_lived_tickets()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = new HttpClient { BaseAddress = host.HttpBase };

        var first = await ReadJsonAsync(await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken));
        var second = await ReadJsonAsync(await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken));
        Assert.False(string.IsNullOrEmpty(first.GetProperty("ticket").GetString()));
        Assert.NotEqual(first.GetProperty("ticket").GetString(), second.GetProperty("ticket").GetString());
        Assert.Equal(30, first.GetProperty("expiresInSeconds").GetInt32());
    }

    [Fact]
    public async Task Http_endpoints_can_be_disabled()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:HttpEndpointsEnabled"] = "false",
        });
        using var http = new HttpClient { BaseAddress = host.HttpBase };
        var response = await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);

        // The websocket endpoint itself stays up.
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
    }

    private static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");

    private static async Task<JsonElement> ReadJsonAsync(HttpResponseMessage response)
    {
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonDocument.Parse(text).RootElement.Clone();
    }
}
