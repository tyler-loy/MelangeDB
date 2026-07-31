using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The public face of the hub's cluster operations — what application code (an event handler
/// reacting to a portal, an admin tool) uses to move players and create instances. Resolvable
/// anywhere, usable only where the hub runs.
/// </summary>
public sealed class MelangeClusterCoordinator
{
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    internal MelangeClusterCoordinator(IServiceProvider services, IOptionsMonitor<MelangeDbOptions> options)
    {
        _services = services;
        _options = options;
    }

    private HubRuntime Hub =>
        _options.CurrentValue.Cluster.Role == ClusterRole.Hub
            ? _services.GetRequiredService<HubRuntime>()
            : throw new InvalidOperationException("Cluster operations run on the hub; this node's Cluster:Role is not Hub.");

    /// <summary>
    /// Moves a player's partitioned rows between shards as the explicit handoff saga: freeze on
    /// origin, import on destination, confirm, release on origin. The player is writable on at
    /// most one node at every instant, and a crash at any step recovers to exactly one owner.
    /// </summary>
    public Task TransferPlayerAsync(Identity player, ShardKey from, ShardKey to, CancellationToken ct = default) =>
        Hub.TransferPlayerAsync(player, from, to, ct);

    /// <summary>Creates (or looks up) a shard and returns its assignment — instancing's "spin up instance 7".</summary>
    public void EnsureShard(ShardKey shard) => Hub.ResolveShard(shard);
}

/// <summary>Registers MelangeDB clustering in the host. Call after <c>AddMelangeDb</c>.</summary>
public static class MelangeClusterServiceCollectionExtensions
{
    /// <summary>
    /// Adds the cluster layer. Everything is driven by <c>MelangeDb:Cluster:Role</c>: <c>None</c>
    /// (the default) leaves single-node behavior untouched except that a registered
    /// <see cref="IShardStrategy"/> arms the shard-span debug check; <c>Hub</c> runs membership,
    /// the node-link listener, replication, and the handoff coordinator; <c>Shard</c> connects to
    /// the hub and opens per-shard engines as shards are assigned.
    /// </summary>
    public static IServiceCollection AddMelangeCluster(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IMembershipStore, InMemoryMembershipStore>();
        services.Replace(ServiceDescriptor.Singleton<IEventTransport, ClusterEventTransport>());
        services.TryAddSingleton<HubRuntime>();
        services.TryAddSingleton<ShardNodeRuntime>();
        services.TryAddSingleton(static provider => new MelangeClusterCoordinator(
            provider, provider.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>()));
        services.AddHostedService<ClusterRuntimeHost>();
        return services;
    }
}

/// <summary>
/// Starts the role's runtime after the ordinary MelangeDB hosted service has recovered the
/// DI-registered engine (registration order guarantees the ordering). Role None only arms the
/// shard-span debug check when a strategy is registered — the single-node deployment must
/// otherwise not notice this package exists.
/// </summary>
internal sealed class ClusterRuntimeHost : IHostedService
{
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private HubRuntime? _hub;
    private ShardNodeRuntime? _node;

    public ClusterRuntimeHost(IServiceProvider services, IOptionsMonitor<MelangeDbOptions> options)
    {
        _services = services;
        _options = options;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        switch (_options.CurrentValue.Cluster.Role)
        {
            case ClusterRole.Hub:
                _hub = _services.GetRequiredService<HubRuntime>();
                _hub.Start();
                break;
            case ClusterRole.Shard:
                _node = _services.GetRequiredService<ShardNodeRuntime>();
                _node.Start();
                break;
            default:
                ArmSingleNodeSpanCheck();
                break;
        }

        return Task.CompletedTask;
    }

    private void ArmSingleNodeSpanCheck()
    {
        if (_services.GetService<IShardStrategy>() is not { } strategy)
            return;
        var engine = _services.GetRequiredService<MelangeEngine>();
        engine.AddCommitGuard(new SingleNodeSpanGuard(
            engine.Schema,
            () => engine.HotStore,
            strategy,
            () => ShardSpanCheck.IsEnabled(_options.CurrentValue.Cluster.ShardSpanCheck)));
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _node?.Dispose();
        _hub?.Dispose();
        return Task.CompletedTask;
    }
}

/// <summary>Maps the cluster's endpoints into the host's ASP.NET Core app.</summary>
public static class MelangeClusterEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the gateway — the one endpoint clients connect to — on the hub. Terminates client
    /// sockets, authenticates against the host's JWT scheme, and routes reducer calls and
    /// subscriptions to the owning nodes; the client never learns the topology. The hub app must
    /// also map its own <c>MapMelangeSocket()</c>, which the gateway uses as the permanent hub
    /// attachment.
    /// </summary>
    public static IEndpointConventionBuilder MapMelangeGateway(this IEndpointRouteBuilder endpoints, string path = "/gateway")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var gateway = new GatewayRuntime(
            endpoints.ServiceProvider,
            endpoints.ServiceProvider.GetRequiredService<HubRuntime>());
        return endpoints.Map(path, context => HandleGatewayAsync(context, gateway));
    }

    /// <summary>
    /// Maps the per-shard websocket endpoints on a shard node at
    /// <c>{Transport:Path}/shard/{shardKey}</c>. Internal infrastructure: reachable by the
    /// gateway, authenticated by hub-minted assertions, never exposed to clients.
    /// </summary>
    public static IEndpointConventionBuilder MapMelangeShardSockets(this IEndpointRouteBuilder endpoints, string? basePath = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var services = endpoints.ServiceProvider;
        var options = services.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>();
        var path = (basePath ?? options.CurrentValue.Transport.Path).TrimEnd('/') + "/shard/{shardKey:long}";
        return endpoints.Map(path, context => HandleShardSocketAsync(context, services));
    }

    private static async Task HandleGatewayAsync(HttpContext context, GatewayRuntime gateway)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("This endpoint speaks the MelangeDB websocket protocol.").ConfigureAwait(false);
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        await new GatewayConnection(socket, gateway).RunAsync(context.RequestAborted).ConfigureAwait(false);
    }

    private static async Task HandleShardSocketAsync(HttpContext context, IServiceProvider services)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("This endpoint speaks the MelangeDB websocket protocol.").ConfigureAwait(false);
            return;
        }

        var node = services.GetRequiredService<ShardNodeRuntime>();
        var shardValue = ulong.Parse(
            (string)context.Request.RouteValues["shardKey"]!, System.Globalization.CultureInfo.InvariantCulture);
        var shard = node.TryGetShard(new ShardKey(shardValue));
        if (shard is null)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            await context.Response.WriteAsync($"This node does not own shard:{shardValue}.").ConfigureAwait(false);
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        var connection = new MelangeSocketConnection(socket, shard.Transport, context.Request.Protocol);
        shard.Transport.OnConnectionOpened(connection);
        await connection.RunAsync(context.RequestAborted).ConfigureAwait(false);
    }
}
