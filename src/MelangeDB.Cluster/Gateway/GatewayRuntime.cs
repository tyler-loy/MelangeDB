using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// The gateway's shared state: the serializer, the client-facing authenticator (the IdP is still
/// the gate — the gateway validates JWTs exactly as a single node would), reducer and schema
/// registries for routing, the shard strategy, and the hub runtime for assertions and shard
/// resolution. Also the upstream frame counters the acceptance tests read: routed traffic is
/// counted as network messages, not inferred.
/// </summary>
internal sealed class GatewayRuntime
{
    private readonly IServiceProvider _services;
    private long _upstreamMessagesSent;
    private long _upstreamMessagesReceived;
    private Uri? _hubSocketUri;

    public GatewayRuntime(IServiceProvider services, HubRuntime hub)
    {
        _services = services;
        Hub = hub;
        Options = services.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>();
        Serializer = new MessagePackFrameSerializer();
        Reducers = services.GetRequiredService<ReducerRegistry>();
        Schema = services.GetRequiredService<SchemaRegistry>();
        Strategy = services.GetService<IShardStrategy>();
        Logger = (services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance)
            .CreateLogger("MelangeDB.Cluster.Gateway");
        Time = services.GetService<TimeProvider>() ?? TimeProvider.System;
        Authenticator = new MelangeAuthenticator(
            services,
            () => Options.CurrentValue.Auth,
            () => Options.CurrentValue.Sql,
            () => Options.CurrentValue.Cluster,
            Time);
    }

    public HubRuntime Hub { get; }

    public IOptionsMonitor<MelangeDbOptions> Options { get; }

    public IMelangeSerializer Serializer { get; }

    public ReducerRegistry Reducers { get; }

    public SchemaRegistry Schema { get; }

    public IShardStrategy? Strategy { get; }

    public MelangeAuthenticator Authenticator { get; }

    public ILogger Logger { get; }

    public TimeProvider Time { get; }

    /// <summary>Total websocket messages the gateway has exchanged with upstream nodes.</summary>
    public long UpstreamMessages =>
        Interlocked.Read(ref _upstreamMessagesSent) + Interlocked.Read(ref _upstreamMessagesReceived);

    public void CountUpstreamSent() => Interlocked.Increment(ref _upstreamMessagesSent);

    public void CountUpstreamReceived() => Interlocked.Increment(ref _upstreamMessagesReceived);

    /// <summary>
    /// The hub's own melange websocket endpoint — the gateway's permanent upstream. Resolved from
    /// the server's bound addresses, since gateway and hub endpoint live in the same host.
    /// </summary>
    public Uri HubSocketUri()
    {
        if (_hubSocketUri is { } cached)
            return cached;
        var addresses = _services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()?.Addresses
            ?? throw new InvalidOperationException("The server exposes no addresses; cannot locate the hub's melange endpoint.");
        var address = addresses.FirstOrDefault(static a => a.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            ?? addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("The server exposes no addresses; cannot locate the hub's melange endpoint.");
        return _hubSocketUri = ToWebSocketUri(address, Options.CurrentValue.Transport.Path);
    }

    /// <summary>The websocket endpoint serving one shard on its owning node.</summary>
    public Uri ShardSocketUri(string publicAddress, ShardKey shard) =>
        ToWebSocketUri(publicAddress, $"{Options.CurrentValue.Transport.Path}/shard/{shard.Value}");

    private static Uri ToWebSocketUri(string httpAddress, string path)
    {
        var builder = new UriBuilder(httpAddress.Replace("*", "127.0.0.1").Replace("+", "127.0.0.1"));
        builder.Scheme = builder.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ? "wss" : "ws";
        builder.Path = path;
        return builder.Uri;
    }
}
