using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using MelangeDB.Cluster;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MelangeDB.LoadTest;

/// <summary>
/// The serve side: a hub plus N shard nodes, each a real Kestrel host in this process — the
/// cluster acceptance tests' topology (real websockets between client, gateway, and shard
/// endpoints; real TCP node links), sized by flags instead of a fixture. On start it creates
/// every world block's shard, waits for owners, seeds one terrain row per chunk, and then
/// announces <c>GATEWAY</c> and <c>READY</c> lines a driver (or the <c>all</c> parent process)
/// can parse. The hub app additionally serves <c>/loadtest/stats</c> so a driver on another
/// machine can pull server-side counters instead of guessing.
/// </summary>
internal sealed class LoadTestServer : IAsyncDisposable
{
    private const string ClusterSecret = "melange-loadtest-cluster-secret";

    private readonly LoadTestOptions _options;
    private readonly string _root;
    private readonly bool _ownsRoot;
    private readonly List<WebApplication> _nodeApps = [];
    private WebApplication _hubApp = null!;
    private int _hubHttpPort;
    private int _nodeListenPort;

    private LoadTestServer(LoadTestOptions options, string root, bool ownsRoot)
    {
        _options = options;
        _root = root;
        _ownsRoot = ownsRoot;
    }

    public Uri GatewayUri => new($"ws://{ReachableHost()}:{_hubHttpPort}/gateway");

    public HubRuntime Hub => _hubApp.Services.GetRequiredService<HubRuntime>();

    public static async Task<LoadTestServer> StartAsync(LoadTestOptions options, TextWriter output, CancellationToken ct = default)
    {
        var ownsRoot = options.DataPath is null;
        var root = options.DataPath ?? Directory.CreateTempSubdirectory("melange-loadtest-").FullName;
        var server = new LoadTestServer(options, root, ownsRoot);
        try
        {
            await server.StartCoreAsync(output, ct).ConfigureAwait(false);
            return server;
        }
        catch
        {
            await server.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task StartCoreAsync(TextWriter output, CancellationToken ct)
    {
        _hubHttpPort = _options.Port != 0 ? _options.Port : FreePort();
        _nodeListenPort = FreePort();
        output.WriteLine(
            $"Serve: hub + {_options.Nodes} shard node(s), world {_options.World} " +
            $"({_options.WorldChunksX}x{_options.WorldChunksY} chunks), band {_options.BandChunks}, " +
            $"fsync {_options.Fsync}{(_options.Fsync == "interval" ? $" ({_options.FsyncIntervalMs} ms)" : string.Empty)}.");
        _hubApp = await StartAppAsync("hub", ClusterRole.Hub, _hubHttpPort).ConfigureAwait(false);
        for (var i = 0; i < _options.Nodes; i++)
            _nodeApps.Add(await StartAppAsync($"node-{i}", ClusterRole.Shard, FreePort()).ConfigureAwait(false));

        var hub = Hub;
        await WaitUntilAsync(
            () => hub.Membership.Nodes().Count(n => n.Alive) == _options.Nodes,
            "all shard nodes registered", ct).ConfigureAwait(false);

        // Create every block's shard up front and seed one terrain row per chunk on its owner, so
        // drivers start against a fully owned, populated world instead of racing shard creation.
        var seeder = DevTokens.IdentityOf("server-seed");
        for (var bx = 0; bx < _options.WorldBlocksX; bx++)
        {
            for (var by = 0; by < _options.WorldBlocksY; by++)
            {
                var shard = SpatialShardStrategy.ShardOfBlock(bx, by);
                hub.ResolveShard(shard);
                ShardRuntime? runtime = null;
                await WaitUntilAsync(
                    () => (runtime = TryGetShard(shard)) is not null,
                    $"shard {shard.Value} opened by its owner", ct).ConfigureAwait(false);
                for (var cx = bx * _options.BlockChunksX; cx < (bx + 1) * _options.BlockChunksX; cx++)
                {
                    for (var cy = by * _options.BlockChunksY; cy < (by + 1) * _options.BlockChunksY; cy++)
                        runtime!.ReducerHost.Call("PlaceTerrain", seeder, Chunks.Id(cx, cy));
                }
            }
        }

        output.WriteLine($"GATEWAY {GatewayUri}");
        output.WriteLine($"STATS http://{ReachableHost()}:{_hubHttpPort}/loadtest/stats");
        output.WriteLine("READY");
    }

    private ShardRuntime? TryGetShard(ShardKey shard) =>
        _nodeApps.Select(app => app.Services.GetRequiredService<ShardNodeRuntime>().TryGetShard(shard))
            .FirstOrDefault(static r => r is not null);

    /// <summary>The same counters the stats endpoint serves, for the serve console's periodic line.</summary>
    public ServerStats Stats() => CollectStats();

    private ServerStats CollectStats()
    {
        using var process = Process.GetCurrentProcess();
        var metrics = Hub.Metrics;
        return new ServerStats
        {
            TimestampUtc = DateTimeOffset.UtcNow,
            WorldBlocksX = _options.WorldBlocksX,
            WorldBlocksY = _options.WorldBlocksY,
            BlockChunksX = _options.BlockChunksX,
            BlockChunksY = _options.BlockChunksY,
            HandoffsStarted = metrics.HandoffsStarted,
            HandoffsCompleted = metrics.HandoffsCompleted,
            HandoffsAborted = metrics.HandoffsAborted,
            HandoffsUnresolved = metrics.HandoffsUnresolved,
            HandoffsRateLimited = metrics.HandoffsRateLimited,
            HandoffsInFlight = metrics.HandoffsInFlight,
            WorkingSetBytes = process.WorkingSet64,
            GcHeapBytes = GC.GetTotalMemory(forceFullCollection: false),
            Gen0Collections = GC.CollectionCount(0),
            Gen1Collections = GC.CollectionCount(1),
            Gen2Collections = GC.CollectionCount(2),
        };
    }

    private async Task<WebApplication> StartAppAsync(string nodeName, ClusterRole role, int httpPort)
    {
        var nodeRoot = Path.Combine(_root, nodeName);
        var settings = new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(nodeRoot, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(nodeRoot, "hot"),
            ["MelangeDb:Events:DeadLetterPath"] = Path.Combine(nodeRoot, "deadletter"),
            ["MelangeDb:CommitLog:FsyncPolicy"] = _options.Fsync == "commit" ? "OnCommit" : "Interval",
            ["MelangeDb:CommitLog:FsyncIntervalMs"] = _options.FsyncIntervalMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Cluster:Role"] = role.ToString(),
            ["MelangeDb:Cluster:NodeName"] = nodeName,
            ["MelangeDb:Cluster:Secret"] = ClusterSecret,

            // The measurement should include the guards a correct deployment runs with; the
            // hotspot-ceiling methodology (HotspotMeasurementTests) measured with them on too.
            ["MelangeDb:Cluster:ShardSpanCheck"] = "Always",
            ["MelangeDb:Cluster:HeartbeatIntervalMs"] = "250",
            ["MelangeDb:Cluster:FailureTimeoutMs"] = "15000",
            ["MelangeDb:Cluster:BorderBandChunks"] = _options.BandChunks.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Cluster:HandoffMarginChunks"] = _options.MarginChunks.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Cluster:HandoffMinIntervalMs"] = _options.HandoffMinIntervalMs.ToString(System.Globalization.CultureInfo.InvariantCulture),

            // One shared root: the phase 09 shared-storage assumption for reassignment.
            ["MelangeDb:Cluster:ShardDataPath"] = Path.Combine(_root, "shards"),
        };
        if (role == ClusterRole.Hub)
        {
            settings["MelangeDb:Cluster:NodeListenPort"] = _nodeListenPort.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        else
        {
            settings["MelangeDb:Cluster:HubAddress"] = $"127.0.0.1:{_nodeListenPort}";

            // The gateway (hub process, same machine) is the only consumer of this address, so
            // shard nodes stay on loopback even when the hub accepts a remote driver.
            settings["MelangeDb:Cluster:PublicAddress"] = $"http://127.0.0.1:{httpPort}";
        }

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        var listen = role == ClusterRole.Hub ? ListenAddress() : IPAddress.Loopback;
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(listen, httpPort, static l => l.Protocols = HttpProtocols.Http1));
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddAuthentication().AddJwtBearer(jwt =>
        {
            jwt.TokenValidationParameters = new Microsoft.IdentityModel.Tokens.TokenValidationParameters
            {
                ValidIssuer = DevTokens.Issuer,
                ValidateAudience = false,
                IssuerSigningKey = new Microsoft.IdentityModel.Tokens.SymmetricSecurityKey(
                    System.Text.Encoding.UTF8.GetBytes(DevTokens.SigningKey)),
            };
        });
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Terrain).Assembly)
            .AddReducersFrom(typeof(WalkerReducers).Assembly));
        builder.Services.AddSingleton<IShardStrategy>(provider => new SpatialShardStrategy(
            provider.GetRequiredService<SchemaRegistry>(),
            new SpatialGeometry
            {
                BlockWidthChunks = _options.BlockChunksX,
                BlockHeightChunks = _options.BlockChunksY,
                DecodeChunk = Chunks.At,
            },
            static session => new ShardKey(
                session.HubDb.Find<PlayerShardMap>(session.Identity)?.Shard
                ?? SpatialShardStrategy.ShardOfBlock(0, 0).Value),
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<MelangeDbOptions>>()));
        builder.Services.AddSingleton<IHandoffSet, WalkerHandoffSet>();
        builder.Services.AddSingleton<IMigrationAnchors, WalkerAnchors>();
        builder.Services.AddSingleton<IShardTransferListener>(
            static provider => new WalkerTransferListener(provider));
        builder.Services.AddMelangeCluster();

        var app = builder.Build();
        app.UseWebSockets();
        if (role == ClusterRole.Hub)
        {
            app.MapMelangeSocket();
            app.MapMelangeGateway();
            app.MapGet("/loadtest/stats", () => Results.Json(CollectStats()));
        }
        else
        {
            app.MapMelangeShardSockets();
        }

        await app.StartAsync().ConfigureAwait(false);
        return app;
    }

    private IPAddress ListenAddress() =>
        IPAddress.TryParse(_options.Listen, out var address) ? address : IPAddress.Loopback;

    /// <summary>The host a driver dials: a wildcard bind is announced as this machine's loopback.</summary>
    private string ReachableHost()
    {
        var listen = ListenAddress();
        return listen.Equals(IPAddress.Any) || listen.Equals(IPAddress.IPv6Any)
            ? "127.0.0.1"
            : _options.Listen;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what, CancellationToken ct, int timeoutSeconds = 60)
    {
        var clock = Stopwatch.StartNew();
        while (!condition())
        {
            if (clock.Elapsed > TimeSpan.FromSeconds(timeoutSeconds))
                throw new TimeoutException($"Timed out waiting for: {what}");
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var app in _nodeApps)
        {
            await StopAsync(app).ConfigureAwait(false);
        }

        if (_hubApp is not null)
            await StopAsync(_hubApp).ConfigureAwait(false);
        if (_ownsRoot)
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
            }
        }

        static async Task StopAsync(WebApplication app)
        {
            try
            {
                // Bounded, like the cluster fixture: the gateway holds live websockets that only
                // close when their client does, and a full graceful drain would wait for them.
                using var abrupt = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                await app.StopAsync(abrupt.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Shutdown is best-effort; disposal below releases the log handles regardless.
            }
            finally
            {
                await app.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
