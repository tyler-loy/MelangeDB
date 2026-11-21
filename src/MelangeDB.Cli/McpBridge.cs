using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace MelangeDB.Cli;

/// <summary>
/// <c>melange mcp &lt;url&gt;</c> (road-to-0.4 phase 29): a stdio MCP server for agents that expect a
/// subprocess, forwarding every tool listing and tool call to a running host's MCP endpoint over
/// HTTP, authenticated as the other <c>melange</c> verbs are (<c>--token</c> or
/// <c>MELANGE_TOKEN</c>). It holds no tools of its own and decides nothing: the host's gate, role
/// check, allowlist and policies apply to every call exactly as they would over HTTP.
/// <b>Stdout is the protocol channel</b>; diagnostics go to stderr only.
/// </summary>
internal static class McpBridge
{
    internal const string DefaultEndpointPath = "/melange/mcp";

    /// <summary>The host's endpoint: <paramref name="source"/> as given, or with the default path when it names none.</summary>
    internal static Uri EndpointOf(Uri source) =>
        source.AbsolutePath is "" or "/" ? new Uri(source, DefaultEndpointPath) : source;

    public static Task<int> RunAsync(Uri source, string? token, CancellationToken ct) =>
        RunAsync(source, token, new StdioServerTransport("melange"), ct);

    /// <summary>The bridge over any server-side transport: stdio in the verb, in-memory pipes in its test.</summary>
    internal static async Task<int> RunAsync(Uri source, string? token, ITransport agentSide, CancellationToken ct)
    {
        var endpoint = EndpointOf(source);
        var headers = new Dictionary<string, string>();
        if (!string.IsNullOrEmpty(token))
            headers["Authorization"] = $"Bearer {token}";

        McpClient upstream;
        try
        {
            upstream = await McpClient.CreateAsync(
                new HttpClientTransport(new HttpClientTransportOptions
                {
                    Endpoint = endpoint,
                    TransportMode = HttpTransportMode.StreamableHttp,
                    AdditionalHeaders = headers,
                }),
                cancellationToken: ct).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is HttpRequestException or InvalidOperationException or ModelContextProtocol.McpException)
        {
            await Console.Error.WriteLineAsync(
                $"Could not open the MCP endpoint at {endpoint}: {exception.Message} " +
                "(is Mcp:Enabled on, and does the token carry the host's Mcp:OwnerRole claim?)").ConfigureAwait(false);
            return 1;
        }

        await using (upstream.ConfigureAwait(false))
        {
            var options = new McpServerOptions
            {
                ServerInfo = upstream.ServerInfo,
                ServerInstructions = upstream.ServerInstructions,
                Handlers = new McpServerHandlers
                {
                    ListToolsHandler = async (request, cancel) => new ListToolsResult
                    {
                        Tools = [.. (await upstream.ListToolsAsync(cancellationToken: cancel).ConfigureAwait(false)).Select(static tool => tool.ProtocolTool)],
                    },
                    CallToolHandler = async (request, cancel) =>
                    {
                        var call = request.Params ?? throw new ModelContextProtocol.McpException("A tool call needs a name.");
                        return await upstream.CallToolAsync(
                            call.Name,
                            call.Arguments?.ToDictionary(static pair => pair.Key, static pair => (object?)pair.Value),
                            cancellationToken: cancel).ConfigureAwait(false);
                    },
                },
            };

            await using var server = McpServer.Create(agentSide, options);
            await server.RunAsync(ct).ConfigureAwait(false);
        }

        return 0;
    }
}
