using MelangeDB.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Server;

/// <summary>Maps the MelangeDB transport into the developer's own ASP.NET Core app.</summary>
public static class MelangeEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the MelangeDB websocket endpoint at <paramref name="path"/> (default
    /// <c>Transport:Path</c>, <c>/melange</c>) plus the HTTP endpoints
    /// <c>{path}/call/{reducer}</c>, <c>{path}/bulk</c>, <c>{path}/sql</c>, and
    /// <c>{path}/ticket</c> when <c>Transport:HttpEndpointsEnabled</c> is on.
    /// <para>
    /// Every connection authenticates — <b>the IdP is the gate</b>. Tokens are validated against
    /// the host's own JWT bearer scheme (<c>Auth:Scheme</c>), presented as an
    /// <c>Authorization: Bearer</c> header on the upgrade request, a single-use connect ticket
    /// (<c>?ticket=</c>, minted at <c>{path}/ticket</c> — the path browsers use, since their
    /// WebSocket API cannot set headers), or a token in the <c>Hello</c> frame.
    /// </para>
    /// <para>
    /// The socket endpoint accepts <b>CONNECT as well as GET</b>: WebSockets over HTTP/2
    /// (RFC 8441) arrive as extended CONNECT, and a GET-only mapping would make them silently
    /// fall back to HTTP/1.1. TLS and HTTP version are deliberately the host's Kestrel
    /// configuration — MelangeDB maps an endpoint; it does not own a listener. The host must have
    /// called <c>app.UseWebSockets()</c>.
    /// </para>
    /// </summary>
    public static IEndpointConventionBuilder MapMelangeSocket(this IEndpointRouteBuilder endpoints, string? path = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var services = endpoints.ServiceProvider;
        var options = services.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>();
        var engine = services.GetRequiredService<MelangeEngine>();
        var authenticator = new MelangeAuthenticator(services, () => options.CurrentValue.Auth);
        authenticator.EnsureSchemeConfigured();
        var transport = new MelangeTransport(
            engine,
            services.GetRequiredService<MelangeReducerHost>(),
            options,
            services.GetService<TimeProvider>(),
            services.GetRequiredService<ILoggerFactory>(),
            authenticator,
            services.GetService<MelangeSessions>() ?? new MelangeSessions(),
            new PolicySet(services, engine.Schema),
            services.GetService<Microsoft.Extensions.Hosting.IHostApplicationLifetime>()?.ApplicationStopping ?? default);
        var basePath = (path ?? options.CurrentValue.Transport.Path).TrimEnd('/');

        // endpoints.Map (not MapGet) carries no method constraint, so both the HTTP/1.1 GET
        // upgrade and the HTTP/2 extended CONNECT reach the handler.
        var socket = endpoints.Map(basePath, context => HandleSocketAsync(context, transport));

        if (options.CurrentValue.Transport.HttpEndpointsEnabled)
        {
            endpoints.MapPost(basePath + "/call/{reducer}", context => MelangeHttpEndpoints.CallAsync(context, transport));
            endpoints.MapPost(basePath + "/bulk", context => MelangeHttpEndpoints.BulkAsync(context, transport));
            endpoints.MapPost(basePath + "/sql", context => MelangeHttpEndpoints.SqlAsync(context, transport));
            endpoints.MapPost(basePath + "/ticket", context => MelangeHttpEndpoints.TicketAsync(context, transport));
        }

        return socket;
    }

    private static async Task HandleSocketAsync(HttpContext context, MelangeTransport transport)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync(
                "This endpoint speaks the MelangeDB websocket protocol. If this was a websocket " +
                "handshake, ensure the host calls app.UseWebSockets() before routing.").ConfigureAwait(false);
            return;
        }

        // Header and ticket credentials resolve before the socket is accepted, so a bad one is an
        // HTTP status rather than a mute upgrade. A connection with neither authenticates in
        // Hello instead.
        AuthResult? session = null;
        if (context.Request.Query.TryGetValue("ticket", out var tickets) && tickets.ToString() is { Length: > 0 } ticket)
        {
            if (!transport.Tickets.TryRedeem(ticket, out var redeemed))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                await context.Response.WriteAsync("The connect ticket is unknown, already used, or expired.").ConfigureAwait(false);
                return;
            }

            session = redeemed;
        }
        else if (BearerToken(context) is { } token)
        {
            switch (await transport.Authenticator.ValidateAsync(token).ConfigureAwait(false))
            {
                case AuthFailure failure:
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    await context.Response.WriteAsync(failure.Reason).ConfigureAwait(false);
                    return;
                case AuthResult validated:
                    session = validated;
                    break;
            }
        }

        var slotReserved = false;
        if (session is not null)
        {
            if (transport.Sessions.IsRevoked(session.Identity))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                await context.Response.WriteAsync("This identity is revoked.").ConfigureAwait(false);
                return;
            }

            if (!transport.TryReserveConnectionSlot(session.Identity))
            {
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                await context.Response.WriteAsync(
                    $"This identity already holds Auth:MaxConnectionsPerIdentity ({transport.Options.Auth.MaxConnectionsPerIdentity}) connections.").ConfigureAwait(false);
                return;
            }

            slotReserved = true;
        }

        var accept = new WebSocketAcceptContext
        {
            DangerousEnableCompression = transport.Options.Transport.CompressionEnabled,
        };
        using var socket = await context.WebSockets.AcceptWebSocketAsync(accept).ConfigureAwait(false);
        var connection = new MelangeSocketConnection(socket, transport, context.Request.Protocol, session, slotReserved);
        transport.OnConnectionOpened(connection);
        await connection.RunAsync(context.RequestAborted).ConfigureAwait(false);
    }

    internal static string? BearerToken(HttpContext context)
    {
        var authorization = context.Request.Headers.Authorization.ToString();
        return authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..]
            : null;
    }
}
