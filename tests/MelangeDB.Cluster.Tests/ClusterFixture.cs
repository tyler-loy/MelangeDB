using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using MelangeDB.Client;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>One node of the in-process cluster: a real Kestrel host on a loopback port.</summary>
internal sealed class ClusterNode
{
    public required string Name { get; init; }

    public required int HttpPort { get; set; }

    public WebApplication? App { get; set; }

    public ShardNodeRuntime Runtime => App!.Services.GetRequiredService<ShardNodeRuntime>();

    public ClusterMetrics Metrics => Runtime.Metrics;

    public EventReceipts Receipts => App!.Services.GetRequiredService<EventReceipts>();
}

/// <summary>
/// A hub plus N shard nodes, each a real Kestrel host in this process: real websockets between
/// client, gateway, and shard endpoints; real TCP node links between nodes. Every shard node
/// roots its per-shard data under one shared directory — the shared-storage assumption
/// reassignment relies on in phase 09 — so killing a node and letting the hub reassign means the
/// new owner recovers the shard from its own log.
/// </summary>
internal sealed class ClusterFixture : IAsyncDisposable
{
    public const string Secret = "cluster-test-secret";

    private readonly string _root;
    private readonly int _heartbeatMs;
    private readonly int _failureTimeoutMs;
    private readonly bool _spatial;
    private readonly IReadOnlyDictionary<string, string?>? _extraSettings;
    private int _hubHttpPort;
    private int _nodeListenPort;

    private ClusterFixture(
        string root, int heartbeatMs, int failureTimeoutMs, bool spatial, IReadOnlyDictionary<string, string?>? extraSettings)
    {
        _root = root;
        _heartbeatMs = heartbeatMs;
        _failureTimeoutMs = failureTimeoutMs;
        _spatial = spatial;
        _extraSettings = extraSettings;
    }

    public WebApplication HubApp { get; private set; } = null!;

    public List<ClusterNode> Nodes { get; } = [];

    public HubRuntime Hub => HubApp.Services.GetRequiredService<HubRuntime>();

    public MelangeClusterCoordinator Coordinator => HubApp.Services.GetRequiredService<MelangeClusterCoordinator>();

    public MelangeEngine HubEngine => HubApp.Services.GetRequiredService<MelangeEngine>();

    public MelangeReducerHost HubReducers => HubApp.Services.GetRequiredService<MelangeReducerHost>();

    public EventReceipts HubReceipts => HubApp.Services.GetRequiredService<EventReceipts>();

    public Uri GatewayUri => new($"ws://127.0.0.1:{_hubHttpPort}/gateway");

    public static Identity Caller { get; } = TestTokens.IdentityOf(TestTokens.DefaultSubject);

    public static async Task<ClusterFixture> StartAsync(
        int shardNodes = 2,
        int heartbeatMs = 200,
        int failureTimeoutMs = 10_000,
        bool spatial = false,
        IReadOnlyDictionary<string, string?>? extraSettings = null)
    {
        var root = Directory.CreateTempSubdirectory("melange-cluster-").FullName;

        // The failure timeout dilates with the wait deadlines (MELANGE_TEST_TIME_SCALE), and for
        // the same reason: on starved shared vCPUs a heartbeat can stall long enough for the hub
        // to declare a healthy node dead mid-test, which is the environment failing, not the
        // cluster. Ratios are preserved — failover tests that provoke detection still see it well
        // inside their equally-dilated deadlines — and at scale 1 nothing changes.
        var fixture = new ClusterFixture(root, heartbeatMs, TestTime.Scale * failureTimeoutMs, spatial, extraSettings);
        fixture.HubApp = await WithFreshPortsRetryAsync(() =>
        {
            fixture._hubHttpPort = FreePort();
            fixture._nodeListenPort = FreePort();
            return fixture.StartAppAsync("hub", ClusterRole.Hub, fixture._hubHttpPort);
        });
        for (var i = 0; i < shardNodes; i++)
        {
            var name = $"node-{(char)('a' + i)}";
            var port = 0;
            var app = await WithFreshPortsRetryAsync(() =>
            {
                port = FreePort();
                return fixture.StartAppAsync(name, ClusterRole.Shard, port);
            });
            fixture.Nodes.Add(new ClusterNode { Name = name, HttpPort = port, App = app });
        }

        // Every node registered before any test proceeds.
        await WaitUntilAsync(
            () => fixture.Hub.Membership.Nodes().Count(n => n.Alive) == shardNodes,
            "all shard nodes registered");
        return fixture;
    }

    public ClusterNode Node(string name) => Nodes.First(n => n.Name == name);

    /// <summary>Creates the shard, waits for its owner to open it, and returns the owning node.</summary>
    public async Task<ClusterNode> EnsureShardOwnedAsync(ulong shard)
    {
        Coordinator.EnsureShard(new ShardKey(shard));
        ClusterNode? owner = null;
        await WaitUntilAsync(
            () =>
            {
                var assignment = Hub.Membership.GetAssignment(new ShardKey(shard));
                if (assignment?.NodeName is not { } name)
                    return false;
                owner = Nodes.FirstOrDefault(n => n.Name == name && n.App is not null);
                return owner?.Runtime.TryGetShard(new ShardKey(shard)) is not null;
            },
            $"shard {shard} opened by its owner");
        return owner!;
    }

    public ShardRuntime ShardOf(ulong shard) =>
        Nodes.Where(static n => n.App is not null)
            .Select(n => n.Runtime.TryGetShard(new ShardKey(shard)))
            .FirstOrDefault(static r => r is not null)
        ?? throw new InvalidOperationException($"No live node owns shard {shard}.");

    public MelangeClient CreateClient(string? token = null) =>
        new(new MelangeClientOptions { Uri = GatewayUri, Token = token ?? TestTokens.Default });

    /// <summary>
    /// Kills a node. Graceful stop is attempted but not required — these tests kill nodes
    /// mid-request on purpose, and a stop that trips over its own in-flight work must still
    /// release the node's resources (above all its log file handles), or the "revive the node"
    /// half of the test fails on a leaked lock instead of testing recovery.
    /// </summary>
    public async Task StopNodeAsync(string name)
    {
        var node = Node(name);
        try
        {
            // Two seconds of grace, then abort: phase 10's gateway holds live websockets into
            // shard nodes, and a default graceful stop would wait the host's full shutdown
            // timeout for connections that only close when the process dies.
            using var abrupt = new CancellationTokenSource(TestTime.Dilated(TimeSpan.FromSeconds(2)));
            await node.App!.StopAsync(abrupt.Token);
        }
        catch (Exception)
        {
            // A kill, not a shutdown; disposal below still releases everything.
        }
        finally
        {
            await node.App!.DisposeAsync();
            node.App = null;
        }
    }

    public async Task StartNodeAsync(string name)
    {
        // A revived node need not keep its old port: it announces Cluster:PublicAddress on every
        // auth, RegisterNode replaces the membership record wholesale, and the gateway re-resolves
        // the address on every connection attempt — a fresh port is legal by the product's own
        // design. Keeping the old port raced every concurrent fixture's FreePort over a number
        // another process may have won during the stop-to-restart gap (observed as "address
        // already in use" on a truncation-recovery revival under the full suite).
        var node = Node(name);
        node.App = await WithFreshPortsRetryAsync(() =>
        {
            node.HttpPort = FreePort();
            return StartAppAsync(node.Name, ClusterRole.Shard, node.HttpPort);
        });
    }

    /// <summary>Total non-heartbeat node-link messages across the whole cluster — the network-call count.</summary>
    public long TotalLinkMessages()
    {
        var total = Hub.Metrics.TotalSentExcept("heartbeat", "heartbeat-ok");
        foreach (var node in Nodes)
        {
            if (node.App is not null)
                total += node.Metrics.TotalSentExcept("heartbeat", "heartbeat-ok");
        }

        return total;
    }

    /// <summary>
    /// Waits until the cluster's background traffic (replication subscriptions, event
    /// forwarding) quiesces: no non-heartbeat message for longer than a heartbeat interval, so
    /// nothing assignment-piggybacked is still on its way.
    /// </summary>
    public async Task QuiesceAsync()
    {
        var stable = TotalLinkMessages();
        var quietFor = Stopwatch.StartNew();
        var overall = Stopwatch.StartNew();
        while (overall.Elapsed < TestTime.Dilated(TimeSpan.FromSeconds(15)))
        {
            await Task.Delay(100);
            var now = TotalLinkMessages();
            if (now != stable)
            {
                stable = now;
                quietFor.Restart();
            }
            else if (quietFor.Elapsed > TimeSpan.FromMilliseconds(800))
            {
                return;
            }
        }
    }

    private async Task<WebApplication> StartAppAsync(string nodeName, ClusterRole role, int httpPort)
    {
        var nodeRoot = Path.Combine(_root, nodeName);
        var settings = new Dictionary<string, string?>
        {
            ["MelangeDb:CommitLog:Path"] = Path.Combine(nodeRoot, "log"),
            ["MelangeDb:HotStore:Path"] = Path.Combine(nodeRoot, "hot"),
            ["MelangeDb:Events:DeadLetterPath"] = Path.Combine(nodeRoot, "deadletter"),

            // Truncation tests take snapshots on demand; a retention window would floor every
            // truncation at "now minus five minutes", which in a test is the whole log.
            ["MelangeDb:Resume:RetentionWindowSeconds"] = "0",
            ["MelangeDb:Cluster:Role"] = role.ToString(),
            ["MelangeDb:Cluster:NodeName"] = nodeName,
            ["MelangeDb:Cluster:Secret"] = Secret,
            ["MelangeDb:Cluster:ShardSpanCheck"] = "Always",
            ["MelangeDb:Cluster:HeartbeatIntervalMs"] = _heartbeatMs.ToString(),
            ["MelangeDb:Cluster:FailureTimeoutMs"] = _failureTimeoutMs.ToString(),

            // One shared root for every node: the phase 09 shared-storage assumption, which is
            // what lets a reassigned shard's new owner open the shard's log and recover it.
            ["MelangeDb:Cluster:ShardDataPath"] = Path.Combine(_root, "shards"),
        };
        foreach (var (key, value) in _extraSettings ?? new Dictionary<string, string?>())
            settings[key] = value;
        if (role == ClusterRole.Hub)
        {
            settings["MelangeDb:Cluster:NodeListenPort"] = _nodeListenPort.ToString();
        }
        else
        {
            settings["MelangeDb:Cluster:HubAddress"] = $"127.0.0.1:{_nodeListenPort}";
            settings["MelangeDb:Cluster:PublicAddress"] = $"http://127.0.0.1:{httpPort}";
        }

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        if (Environment.GetEnvironmentVariable("MELANGE_TEST_LOG") is { Length: > 0 } path)
        {
            builder.Logging.AddProvider(new FileLoggerProvider(path, nodeName));
            builder.Logging.AddFilter("MelangeDB.Cluster", LogLevel.Debug);
        }
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, httpPort, static listen => listen.Protocols = HttpProtocols.Http1));
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddAuthentication().AddJwtBearer(jwt =>
        {
            jwt.TokenValidationParameters = new Microsoft.IdentityModel.Tokens.TokenValidationParameters
            {
                ValidIssuer = TestTokens.Issuer,
                ValidateAudience = false,
                IssuerSigningKey = TestTokens.Key,
                RoleClaimType = "role",
            };
        });
        builder.Services.AddSingleton<EventReceipts>();
        builder.Services.AddMelangeDb(melange => melange
            .AddTablesFrom(typeof(Mob).Assembly)
            .AddReducersFrom(typeof(ClusterReducers).Assembly)
            .AddEventHandler<MobDiedHandler>()
            .AddEventHandler<GiftSagaHandler>());
        if (_spatial)
        {
            builder.Services.AddSingleton<IShardStrategy>(static provider => new SpatialShardStrategy(
                provider.GetRequiredService<SchemaRegistry>(),
                new SpatialGeometry
                {
                    BlockWidthChunks = SpatialReducers.BlockW,
                    BlockHeightChunks = SpatialReducers.BlockH,
                    DecodeChunk = Chunks.At,
                },
                static session => new ShardKey(
                    session.HubDb.Find<PlayerShardMap>(session.Identity)?.Shard
                    ?? SpatialShardStrategy.ShardOfBlock(0, 0).Value),
                provider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<MelangeDbOptions>>()));
            builder.Services.AddSingleton<IHandoffSet, SpatialHandoffSet>();
            builder.Services.AddSingleton<IMigrationAnchors, SpatialAnchors>();
            builder.Services.AddSingleton<IShardTransferListener>(
                static provider => new SpatialTransferListener(provider));
        }
        else
        {
            builder.Services.AddSingleton<IShardStrategy>(static provider => new InstancingShardStrategy(
                provider.GetRequiredService<SchemaRegistry>(),
                static session => new ShardKey(session.HubDb.Find<PlayerLocation>(session.Identity)?.InstanceId ?? 1)));
            builder.Services.AddSingleton<IHandoffSet, PlayerStateHandoffSet>();
        }

        builder.Services.AddMelangeCluster();

        var app = builder.Build();
        app.UseWebSockets();
        if (role == ClusterRole.Hub)
        {
            app.MapMelangeSocket();
            app.MapMelangeGateway();
        }
        else
        {
            app.MapMelangeShardSockets();
        }

        try
        {
            await app.StartAsync();
        }
        catch
        {
            // A failed start (port stolen between FreePort and Kestrel's bind) must release
            // everything the host already opened — engines hold the node's log files, and a
            // fresh-port retry reuses the same data directories.
            await app.DisposeAsync();
            throw;
        }

        return app;
    }

    /// <summary>Diagnostics only: MELANGE_TEST_LOG routes cluster-layer logs to one shared file.</summary>
    private sealed class FileLoggerProvider(string path, string node) : ILoggerProvider
    {
        private static readonly Lock Sync = new();

        public ILogger CreateLogger(string categoryName) => new FileLogger(path, node, categoryName);

        public void Dispose()
        {
        }

        private sealed class FileLogger(string path, string node, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                lock (Sync)
                {
                    File.AppendAllText(
                        path,
                        $"{DateTime.UtcNow:HH:mm:ss.fff} [{node}] {category} {eventId.Name}: {formatter(state, exception)}\n");
                }
            }
        }
    }

    public static async Task WaitUntilAsync(Func<bool> condition, string what, int timeoutSeconds = 20)
    {
        // Deadlines dilate on slow hardware (MELANGE_TEST_TIME_SCALE); the condition does not.
        var deadline = TestTime.Dilated(TimeSpan.FromSeconds(timeoutSeconds));
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(stopwatch.Elapsed < deadline, $"Timed out waiting for: {what}");
            await Task.Delay(25, TestContext.Current.CancellationToken);
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

    /// <summary>
    /// <see cref="FreePort"/> is allocate-close-rebind, so another process can win the port in
    /// the gap and the app's real bind then fails; every start — initial and revival alike —
    /// retries with fresh ports. Revivals may move ports because the product lets them: a node
    /// announces its address on every auth and the hub replaces the record (see
    /// <see cref="StartNodeAsync"/>).
    /// </summary>
    private static async Task<WebApplication> WithFreshPortsRetryAsync(Func<Task<WebApplication>> start)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await start();
            }
            catch (IOException) when (attempt < 5)
            {
            }
            catch (SocketException) when (attempt < 5)
            {
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in Nodes)
        {
            if (node.App is { } app)
            {
                await app.StopAsync();
                await app.DisposeAsync();
            }
        }

        await HubApp.StopAsync();
        await HubApp.DisposeAsync();
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
