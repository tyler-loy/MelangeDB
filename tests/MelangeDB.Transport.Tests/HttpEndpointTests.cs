using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
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
        using var http = host.CreateHttp();

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
        using var http = host.CreateHttp();

        var unknown = await http.PostAsync("/melange/call/NoSuchReducer", Json("[]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, unknown.StatusCode);
        var unknownBody = await ReadJsonAsync(unknown);
        Assert.Equal("unknown_reducer", unknownBody.GetProperty("error").GetString());

        // The message is wire text: exactly one sentence, with no exception-shaped suffix. It must
        // also be the same sentence a non-client-callable reducer gets, or the difference confirms
        // that one exists — see A_client_calling_a_scheduled_reducer_is_told_unknown.
        Assert.Equal("No reducer named 'NoSuchReducer' is registered.", unknownBody.GetProperty("message").GetString());

        var badArgs = await http.PostAsync("/melange/call/SetChunk", Json("""["nope"]"""), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, badArgs.StatusCode);
        Assert.Equal("invalid_args", (await ReadJsonAsync(badArgs)).GetProperty("error").GetString());

        var rejected = await http.PostAsync("/melange/call/Move", Json("[1.5]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, rejected.StatusCode);
        Assert.Equal("rejected", (await ReadJsonAsync(rejected)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task A_reducer_that_throws_ArgumentException_from_its_body_is_a_failure_not_a_missing_reducer()
    {
        // Issue #98. The arity check passing proves resolution succeeded, so by the time a body
        // runs, "no such reducer" is not a reachable truth — and reporting it sends debugging at
        // registration, the dispatch table, and stale assemblies for a fault two layers down.
        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp();

        var failed = await http.PostAsync("/melange/call/ThrowArgumentFromBody", Json("[1]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, failed.StatusCode);
        Assert.Equal("internal", (await ReadJsonAsync(failed)).GetProperty("error").GetString());

        // And the name that genuinely does not resolve still answers 404, from the one condition
        // that can be decided before any user code runs.
        var unknown = await http.PostAsync("/melange/call/NoSuchReducer", Json("[]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, unknown.StatusCode);
        Assert.Equal("unknown_reducer", (await ReadJsonAsync(unknown)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task Bulk_ingestion_appends_one_write_set_not_one_transaction_per_row()
    {
        // Bulk is off by default and owner-gated since issue #31; this test is about the write
        // path, so it opts in and presents the owner claim.
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Bulk:Enabled"] = "true",
        });
        using var http = host.CreateHttp(TestTokens.For(TestTokens.DefaultSubject, role: "melange-bulk-owner"));
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
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Sql:AdHocEnabled"] = "true",
        });
        host.Call("AddSkill", 7L, "mining", 10L, 1);
        host.Call("AddSkill", 7L, "logging", 20L, 2);
        host.Call("AddSkill", 8L, "smithing", 30L, 3);
        host.Call("AddSecret", 1UL, "hidden");
        using var http = host.CreateHttp();

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
        using var http = host.CreateHttp();

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
        using var http = host.CreateHttp();
        var response = await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);

        // The websocket endpoint itself stays up.
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Error_responses_carry_a_request_id_a_caller_can_quote()
    {
        // Issue #136: a client that fails here could report "it didn't work" and nothing in the
        // report found the server's side of it. The request id exists in every deployment,
        // traced or not, so an error is never unquotable.
        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp(token: null);

        var response = await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);

        var body = await ReadJsonAsync(response);
        Assert.Equal("unauthorized", body.GetProperty("error").GetString());
        var requestId = body.GetProperty("requestId").GetString();
        Assert.False(string.IsNullOrEmpty(requestId));
        // Also on the headers, for a caller that never reads the body.
        Assert.Equal(requestId, Assert.Single(response.Headers.GetValues("X-Request-Id")));
        // Untraced: the id that only exists under a listener is absent rather than empty.
        Assert.False(body.TryGetProperty("traceId", out _));
    }

    [Fact]
    public async Task A_traced_error_response_carries_the_trace_id_of_its_own_request()
    {
        // A listener is what makes ASP.NET create the request Activity at all, which is the
        // deployment this matters in: the id in the body is the one the server's spans are
        // filed under, so pasting it into the log store finds this request.
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Microsoft.AspNetCore",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp(token: null);

        var response = await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);

        var traceId = (await ReadJsonAsync(response)).GetProperty("traceId").GetString();
        Assert.Matches("^[0-9a-f]{32}$", traceId);
        Assert.Equal(traceId, Assert.Single(response.Headers.GetValues("X-Trace-Id")));
    }

    [Fact]
    public async Task A_host_that_stamps_its_own_correlation_ids_keeps_them()
    {
        // The reference host's middleware already stamps these names. Ours fills a gap; it does
        // not overwrite a value the host chose, and does not append a second header the client
        // would then have to disambiguate.
        await using var host = await TransportTestHost.StartAsync(middleware: app => app.Use(async (context, next) =>
        {
            context.Response.Headers["X-Request-Id"] = "host-stamped";
            await next(context);
        }));
        using var http = host.CreateHttp(token: null);

        var response = await http.PostAsync("/melange/ticket", Json("{}"), TestContext.Current.CancellationToken);
        Assert.Equal("host-stamped", Assert.Single(response.Headers.GetValues("X-Request-Id")));
    }

    private static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");

    private static async Task<JsonElement> ReadJsonAsync(HttpResponseMessage response)
    {
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return JsonDocument.Parse(text).RootElement.Clone();
    }
}
