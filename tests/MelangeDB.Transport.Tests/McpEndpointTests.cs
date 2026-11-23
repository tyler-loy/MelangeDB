using System.Net;
using System.Text.Json;
using MelangeDB.Mcp;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Road-to-0.4 phase 29: the agent endpoint, driven by the official MCP client over the real
/// HTTP transport. It is off until Mcp:Enabled, refuses a caller without Mcp:OwnerRole, reads
/// through the same rules /melange/sql applies, and calls only reducers named in code.
/// </summary>
public class McpEndpointTests
{
    private const string AgentRole = "melange-agent";

    private static Task<TransportTestHost> StartAsync(
        Dictionary<string, string?>? settings = null,
        Action<MelangeMcpBuilder>? mcp = null) =>
        TransportTestHost.StartAsync(
            settings ?? new Dictionary<string, string?>
            {
                ["MelangeDb:Mcp:Enabled"] = "true",
                ["MelangeDb:Sql:AdHocEnabled"] = "true",
            },
            services: services => services.AddMelangeMcp(mcp),
            endpoints: app => app.MapMelangeMcp());

    private static Task<McpClient> ConnectAsync(TransportTestHost host, string token) =>
        McpClient.CreateAsync(
            new HttpClientTransport(new HttpClientTransportOptions
            {
                Endpoint = new Uri(host.HttpBase, "/melange/mcp"),
                TransportMode = HttpTransportMode.StreamableHttp,
                AdditionalHeaders = new Dictionary<string, string> { ["Authorization"] = $"Bearer {token}" },
            }),
            cancellationToken: TestContext.Current.CancellationToken);

    private static string Agent(string subject = "agent") => TestTokens.For(subject, role: AgentRole);

    private static string Text(CallToolResult result) => Assert.IsType<TextContentBlock>(Assert.Single(result.Content)).Text;

    private static async Task<CallToolResult> CallAsync(McpClient client, string tool, Dictionary<string, object?>? arguments = null) =>
        await client.CallToolAsync(tool, arguments ?? [], cancellationToken: TestContext.Current.CancellationToken);

    private static async Task<(HttpStatusCode Status, string Body)> PostRawAsync(TransportTestHost host, string? token)
    {
        using var http = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(host.HttpBase, "/melange/mcp"))
        {
            Content = new StringContent("""{"jsonrpc":"2.0","id":1,"method":"tools/list"}""", System.Text.Encoding.UTF8, "application/json"),
        };
        request.Headers.Accept.ParseAdd("application/json");
        request.Headers.Accept.ParseAdd("text/event-stream");
        if (token is not null)
            request.Headers.Authorization = new("Bearer", token);
        using var response = await http.SendAsync(request, TestContext.Current.CancellationToken);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task The_endpoint_is_off_until_enabled_and_never_open_to_a_caller_without_the_role()
    {
        await using (var off = await StartAsync(settings: []))
        {
            var (status, body) = await PostRawAsync(off, Agent());
            Assert.Equal(HttpStatusCode.Forbidden, status);
            Assert.Contains("mcp_disabled", body);
        }

        await using var host = await StartAsync();
        Assert.Equal(HttpStatusCode.Unauthorized, (await PostRawAsync(host, token: null)).Status);

        // A valid token, even one that owns SQL, is not an agent's.
        var (refused, refusal) = await PostRawAsync(host, TestTokens.For("operator", role: "melange-owner"));
        Assert.Equal(HttpStatusCode.Forbidden, refused);
        Assert.Contains("owner_required", refusal);
        Assert.Contains("Mcp:OwnerRole", refusal);
    }

    [Fact]
    public async Task An_agent_reads_the_schema_health_signals_and_rows_through_the_sql_rules()
    {
        await using var host = await StartAsync();
        host.Reducers.Call("SetChunk", TransportTestHost.Caller, 7L, 3L, new byte[] { 1, 2 });
        await using var client = await ConnectAsync(host, Agent());

        var tools = (await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken)).Select(static t => t.Name).Order().ToList();
        Assert.Equal(["describe_schema", "health", "query", "signals"], tools); // No call_reducer without an allowlist.

        Assert.Contains("\"Chunk\"", Text(await CallAsync(client, "describe_schema")));

        var rows = JsonDocument.Parse(Text(await CallAsync(client, "query", new() { ["sql"] = "SELECT * FROM Chunk WHERE Id = :id", ["parameters"] = new Dictionary<string, object?> { ["id"] = 7 } })));
        Assert.Equal(["Id", "X", "Data"], rows.RootElement.GetProperty("columns").EnumerateArray().Select(static c => c.GetString()));
        Assert.Equal("7", Assert.Single(rows.RootElement.GetProperty("rows").EnumerateArray())[0].GetString());

        var health = JsonDocument.Parse(Text(await CallAsync(client, "health"))).RootElement;
        Assert.Equal(host.Engine.Log.HeadLsn, health.GetProperty("headLsn").GetUInt64());
        Assert.True(health.GetProperty("applierLag").EnumerateObject().Any());

        var signals = JsonDocument.Parse(Text(await CallAsync(client, "signals", new() { ["prefix"] = "melange." }))).RootElement;
        Assert.True(signals.GetProperty("returned").GetInt32() > 0);
        Assert.All(signals.GetProperty("series").EnumerateArray(), static s => Assert.StartsWith("melange.", s.GetProperty("name").GetString()));
    }

    [Fact]
    public async Task Query_refuses_exactly_as_the_sql_endpoint_does()
    {
        await using var host = await StartAsync(new Dictionary<string, string?> { ["MelangeDb:Mcp:Enabled"] = "true" });
        await using var client = await ConnectAsync(host, Agent());

        var refused = await CallAsync(client, "query", new() { ["sql"] = "SELECT * FROM Chunk WHERE Id = 1" });
        Assert.True(refused.IsError);
        Assert.Contains("sql_disabled: Ad-hoc SQL is disabled", Text(refused));
    }

    [Fact]
    public async Task Only_allowlisted_reducers_are_callable_and_they_commit_as_ordinary_client_calls()
    {
        await using var host = await StartAsync(
            new Dictionary<string, string?> { ["MelangeDb:Mcp:Enabled"] = "true", ["MelangeDb:Mcp:AllowReducersInProduction"] = "true" },
            mcp => mcp.AllowReducers("SetChunk"));
        await using var client = await ConnectAsync(host, Agent());
        Assert.Contains("call_reducer", (await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken)).Select(static t => t.Name));

        var called = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = new object[] { 9, 4, new Dictionary<string, object?> { ["$bytes"] = "AQI=" } } });
        Assert.True(called.IsError is not true, Text(called));
        var lsn = JsonDocument.Parse(Text(called)).RootElement.GetProperty("lsn").GetUInt64();
        Assert.Equal(host.Engine.Log.HeadLsn, lsn);
        Assert.NotNull(host.Engine.CommittedView.Find<Chunk>(9L));

        var other = await CallAsync(client, "call_reducer", new() { ["reducer"] = "ClearCreatures" });
        Assert.True(other.IsError);
        Assert.Contains("reducer_not_allowed:", Text(other));
        Assert.Contains("SetChunk", Text(other));
        Assert.Equal(lsn, host.Engine.Log.HeadLsn);

        var badArgs = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = new object[] { "nine" } });
        Assert.True(badArgs.IsError);
        Assert.Contains("invalid_args:", Text(badArgs));
    }

    [Fact]
    public async Task On_a_cluster_hub_a_reducer_call_names_its_shard_and_is_routed_there()
    {
        var router = new RecordingRouter();
        await using var host = await TransportTestHost.StartAsync(
            new Dictionary<string, string?> { ["MelangeDb:Mcp:Enabled"] = "true", ["MelangeDb:Mcp:AllowReducersInProduction"] = "true" },
            services: services =>
            {
                services.AddMelangeMcp(mcp => mcp.AllowReducers("SetChunk"));
                services.AddSingleton<MelangeDB.Server.IShardCallRouter>(router);
            },
            endpoints: app => app.MapMelangeMcp());
        await using var client = await ConnectAsync(host, Agent());
        var head = host.Engine.Log.HeadLsn;
        var arguments = new object[] { 9, 4, new Dictionary<string, object?> { ["$bytes"] = "AQI=" } };

        // The hub's own engine is not where a shard's reducers commit, so a call without a shard
        // is refused rather than run there.
        var unnamed = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = arguments });
        Assert.True(unnamed.IsError);
        Assert.Contains("invalid_args:", Text(unnamed));
        Assert.Contains("shard", Text(unnamed));
        Assert.Empty(router.Calls);

        var routed = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = arguments, ["shard"] = 61 });
        Assert.True(routed.IsError is not true, Text(routed));
        Assert.Equal(42UL, JsonDocument.Parse(Text(routed)).RootElement.GetProperty("lsn").GetUInt64());
        var call = Assert.Single(router.Calls);
        Assert.Equal(61UL, call.Shard);
        Assert.Equal("SetChunk", call.Reducer);
        Assert.Equal(TestTokens.IdentityOf("agent"), call.Caller);
        Assert.Equal(MelangeDB.Core.CallSource.Client(), call.Source);
        Assert.Equal(head, host.Engine.Log.HeadLsn);

        // The node's failures keep their codes.
        router.Failure = new MelangeDB.Core.ReducerDeniedException("SetChunk is not yours to call.");
        var denied = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = arguments, ["shard"] = 61 });
        Assert.Contains("denied:", Text(denied));
        router.Failure = new ArgumentException("Shard 9999 does not exist; a call never creates one.", "shard");
        var missing = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = arguments, ["shard"] = 9999 });
        Assert.Contains("invalid_args:", Text(missing));
        Assert.Contains("9999", Text(missing));
    }

    private sealed class RecordingRouter : MelangeDB.Server.IShardCallRouter
    {
        public List<(ulong Shard, string Reducer, Identity Caller, MelangeDB.Core.CallSource Source)> Calls { get; } = [];

        public Exception? Failure { get; set; }

        public bool RoutesByShard => true;

        public Task<ulong> CallAsync(
            ulong shard, string reducer, Identity caller, ReadOnlyMemory<byte> arguments, MelangeDB.Core.CallSource source, CallerClaims claims, CancellationToken cancellationToken)
        {
            if (Failure is { } failure)
                throw failure;
            lock (Calls)
            {
                Calls.Add((shard, reducer, caller, source));
            }

            return Task.FromResult(42UL);
        }
    }

    [Fact]
    public async Task Outside_Development_reducer_calls_need_their_own_flag_and_reads_do_not()
    {
        // The test host runs as Production: the endpoint is on, the allowlist names a reducer,
        // and still nothing is written until Mcp:AllowReducersInProduction says so.
        await using var host = await StartAsync(mcp: mcp => mcp.AllowReducers("SetChunk"));
        await using var client = await ConnectAsync(host, Agent());
        var head = host.Engine.Log.HeadLsn;

        var refused = await CallAsync(client, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = new object[] { 9, 4, new Dictionary<string, object?> { ["$bytes"] = "AQI=" } } });
        Assert.True(refused.IsError);
        Assert.Contains("Mcp:AllowReducersInProduction", Text(refused));
        Assert.Equal(head, host.Engine.Log.HeadLsn);
        Assert.True((await CallAsync(client, "health")).IsError is not true);
    }

    [Fact]
    public async Task An_allowlist_naming_a_reducer_that_does_not_exist_fails_at_startup()
    {
        var failure = await Assert.ThrowsAnyAsync<Exception>(() => StartAsync(mcp: mcp => mcp.AllowReducers("NoSuchReducer")));
        Assert.Contains("NoSuchReducer", failure.ToString());
    }

    [Fact]
    public async Task The_cli_bridge_forwards_an_agents_stdio_to_the_hosts_endpoint_unchanged()
    {
        await using var host = await StartAsync(
            new Dictionary<string, string?> { ["MelangeDb:Mcp:Enabled"] = "true", ["MelangeDb:Mcp:AllowReducersInProduction"] = "true" },
            mcp => mcp.AllowReducers("SetChunk"));
        var agentToBridge = new System.IO.Pipelines.Pipe();
        var bridgeToAgent = new System.IO.Pipelines.Pipe();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var bridge = Cli.McpBridge.RunAsync(
            host.HttpBase, // No path: the bridge adds /melange/mcp.
            Agent(),
            new ModelContextProtocol.Server.StreamServerTransport(agentToBridge.Reader.AsStream(), bridgeToAgent.Writer.AsStream()),
            stop.Token);

        await using (var agent = await McpClient.CreateAsync(
            new StreamClientTransport(agentToBridge.Writer.AsStream(), bridgeToAgent.Reader.AsStream()),
            cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("MelangeDB", agent.ServerInfo?.Name);
            var tools = (await agent.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken)).Select(static t => t.Name).Order();
            Assert.Equal(["call_reducer", "describe_schema", "health", "query", "signals"], tools);

            var called = await CallAsync(agent, "call_reducer", new() { ["reducer"] = "SetChunk", ["arguments"] = new object[] { 11, 1, new Dictionary<string, object?> { ["$bytes"] = "AQ==" } } });
            Assert.True(called.IsError is not true, Text(called));
            Assert.NotNull(host.Engine.CommittedView.Find<Chunk>(11L));

            // The host's refusals come back through the bridge as they are.
            var refused = await CallAsync(agent, "call_reducer", new() { ["reducer"] = "ClearCreatures" });
            Assert.Contains("reducer_not_allowed:", Text(refused));
        }

        await stop.CancelAsync();
        try
        {
            await bridge;
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Fact]
    public void The_bridge_adds_the_default_path_only_to_a_bare_host()
    {
        Assert.Equal("https://game.example/melange/mcp", Cli.McpBridge.EndpointOf(new Uri("https://game.example")).ToString());
        Assert.Equal("https://game.example/custom/mcp", Cli.McpBridge.EndpointOf(new Uri("https://game.example/custom/mcp")).ToString());
    }
}
