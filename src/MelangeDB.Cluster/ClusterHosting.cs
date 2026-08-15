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

    /// <summary>
    /// Executes one reducer on the shard owning <paramref name="shard"/> — the building block of
    /// the rare genuine cross-shard interaction, composed as a saga over the event bus:
    /// eventually consistent steps with compensating actions, <b>explicitly not ACID</b> (see
    /// docs/CLUSTERING.md). Each call is one ordinary local transaction on the owning shard; a
    /// step that throws a peer error definitively did not commit, and the caller compensates.
    /// Co-location is always the first choice — this exists for the interactions spatial locality
    /// genuinely cannot cover.
    /// </summary>
    public Task<ulong> ExecuteOnShardAsync(
        ShardKey shard, string reducer, Identity caller, object?[] arguments, CancellationToken ct = default) =>
        Hub.ExecuteOnShardAsync(shard, reducer, caller, arguments, ct);

    /// <summary>The cluster's shard ownership map: every shard, its owner, and its fencing term.</summary>
    public IReadOnlyList<ShardAssignment> OwnershipMap() => Hub.Membership.AllAssignments();

    /// <summary>
    /// Moves one shard to another live node while both are up — the planned drain (road-to-0.2
    /// phase 13), an operator-facing primitive in its own right. The origin takes a fresh
    /// snapshot and closes the shard, membership moves it under a bumped fencing token, the
    /// destination recovers it from the shard's own log on shared storage, and the gateways swap
    /// attached clients invisibly: calls issued during the window are queued (bounded by
    /// <c>Cluster:DrainQueueTimeoutMs</c>) and flush in order on the destination; subscriptions
    /// re-scope, so each client's cache is atomically replaced with the destination's state. The
    /// shard's writes pause for the handover window; every other shard is untouched.
    /// <paramref name="destinationNode"/> null picks the live node owning the fewest shards.
    /// Throws when the shard, its owner, or the destination is not in a drainable state — a
    /// failed drain leaves the shard where it was.
    /// </summary>
    public Task DrainShardAsync(ShardKey shard, string? destinationNode = null, CancellationToken ct = default) =>
        Hub.DrainShardAsync(shard, destinationNode, ct);

    /// <summary>
    /// The cluster's per-shard load view, fed by every node's heartbeats: which shard is hot,
    /// where it lives, and what it weighs. The operator's "which island is busy" answer, and the
    /// feed the rebalance loop decides from. Utilization is the busy fraction of the shard
    /// engine's write lock over its last heartbeat interval — the resource the published hotspot
    /// ceilings (docs/CLUSTERING.md) are ceilings on.
    /// </summary>
    public IReadOnlyList<ShardLoad> LoadView() => Hub.Load.Snapshot();
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
        services.TryAddSingleton<MelangeShardHealthCheck>();
        services.TryAddSingleton<MelangeCapacityHealthCheck>();
        services.Configure<Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckServiceOptions>(static options =>
        {
            if (options.Registrations.All(static r => r.Name != "melange-shard"))
            {
                options.Registrations.Add(new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckRegistration(
                    "melange-shard",
                    static provider => provider.GetRequiredService<MelangeShardHealthCheck>(),
                    failureStatus: null,
                    tags: null));
            }

            if (options.Registrations.All(static r => r.Name != "melange-capacity"))
            {
                options.Registrations.Add(new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckRegistration(
                    "melange-capacity",
                    static provider => provider.GetRequiredService<MelangeCapacityHealthCheck>(),
                    failureStatus: null,
                    tags: null));
            }
        });
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
