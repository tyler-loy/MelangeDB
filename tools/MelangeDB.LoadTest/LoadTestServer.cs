using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using MelangeDB.Cluster;
using MelangeDB.Core;
using MelangeDB.Server;
using MelangeDB.Storage.Faster;
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
    private readonly ServeTuning _tuning;
    private readonly string _root;
    private readonly bool _ownsRoot;
    private readonly List<(string Name, WebApplication? App)> _nodeApps = [];
    private readonly Lock _lagLock = new();
    private readonly List<long> _lagRecords = [];
    private readonly List<double> _stalenessMs = [];

    // Per owner shard: when each durable LSN was first seen, oldest first, trimmed to two minutes.
    private readonly Dictionary<ulong, Queue<(long At, ulong Durable)>> _durableHistory = [];
    private Timer? _lagSampler;
    private LockSplitListener? _lockSplit;
    private WebApplication _hubApp = null!;
    private int _hubHttpPort;
    private int _nodeListenPort;

    private LoadTestServer(LoadTestOptions options, ServeTuning tuning, string root, bool ownsRoot)
    {
        _options = options;
        _tuning = tuning;
        _root = root;
        _ownsRoot = ownsRoot;
    }

    public Uri GatewayUri => new($"ws://{ReachableHost()}:{_hubHttpPort}/gateway");

    public HubRuntime Hub => _hubApp.Services.GetRequiredService<HubRuntime>();

    public static async Task<LoadTestServer> StartAsync(
        LoadTestOptions options, TextWriter output, CancellationToken ct = default, ServeTuning? tuning = null)
    {
        var ownsRoot = options.DataPath is null;
        var root = options.DataPath ?? Directory.CreateTempSubdirectory("melange-loadtest-").FullName;
        var server = new LoadTestServer(options, tuning ?? ServeTuning.Serve, root, ownsRoot);
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
        output.WriteLine(
            $"Serve: hub + {_options.Nodes} shard node(s), world {_options.World} " +
            $"({_options.WorldChunksX}x{_options.WorldChunksY} chunks), band {_options.BandChunks}, " +
            $"fsync {_options.Fsync}{(_options.Fsync == "interval" ? $" ({_options.FsyncIntervalMs} ms)" : string.Empty)}, " +
            $"hot store {_options.HotStore}, heartbeat {HeartbeatMs} ms, failure timeout {FailureTimeoutMs} ms.");

        // FreePort is allocate-close-rebind, so another process can win a port in the gap and the
        // real bind then fails — one stolen port out of the dozens churning on a busy CI machine
        // must not fail a whole run. Ephemeral allocations retry with fresh ports (the fixture in
        // the cluster tests does the same); an explicitly requested --port is never moved — it is
        // the caller's scripted address — so if that port itself is taken, the retries exhaust
        // and the failure stays loud.
        if (_options.LockSplit)
            _lockSplit = new LockSplitListener("Move");
        _hubApp = await WithFreshPortsRetryAsync(() =>
        {
            _hubHttpPort = _options.Port != 0 ? _options.Port : FreePort();
            _nodeListenPort = FreePort();
            return StartAppAsync("hub", ClusterRole.Hub, _hubHttpPort);
        }).ConfigureAwait(false);
        for (var i = 0; i < _options.Nodes; i++)
        {
            var name = $"node-{i}";
            _nodeApps.Add((name, await WithFreshPortsRetryAsync(() => StartAppAsync(name, ClusterRole.Shard, FreePort())).ConfigureAwait(false)));
        }

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

        _lagSampler = new Timer(_ => SampleReplicaLag(), null, 100, 100);
        output.WriteLine($"GATEWAY {GatewayUri}");
        output.WriteLine($"STATS http://{ReachableHost()}:{_hubHttpPort}/loadtest/stats");
        output.WriteLine("READY");
    }

    private int HeartbeatMs => _options.HeartbeatMs ?? _tuning.HeartbeatMs;

    private int FailureTimeoutMs => _options.FailureTimeoutMs ?? _tuning.FailureTimeoutMs;

    public ShardRuntime? TryGetShard(ShardKey shard) =>
        _nodeApps.Where(static n => n.App is not null)
            .Select(n => n.App!.Services.GetRequiredService<ShardNodeRuntime>().TryGetShard(shard))
            .FirstOrDefault(static r => r is not null);

    /// <summary>The live shard node runtime by name, or null when that node is down.</summary>
    public ShardNodeRuntime? Node(string name) =>
        _nodeApps.Single(n => n.Name == name).App?.Services.GetRequiredService<ShardNodeRuntime>();

    /// <summary>
    /// Kills a shard node the way the cluster tests do: a bounded stop, then disposal, with no
    /// goodbye to the hub — the hub learns only from heartbeat silence, exactly as it would from a
    /// crashed process. Its data directory stays, as a dead machine's shared storage would.
    /// </summary>
    /// <param name="dead">Invoked the moment the runtime is gone, before the host teardown: the instant after which nothing on this node can commit.</param>
    public async Task KillNodeAsync(string name, Action? dead = null)
    {
        var index = _nodeApps.FindIndex(n => n.Name == name);
        var app = _nodeApps[index].App ?? throw new InvalidOperationException($"Node {name} is already down.");
        _nodeApps[index] = (name, null);

        // The runtime first, synchronously: its heartbeats and shard engines stop at once, the
        // way a crash stops them. A host stop alone keeps heartbeating while Kestrel drains the
        // gateway's upstream sockets, which would add up to its grace period to the window.
        app.Services.GetRequiredService<ShardNodeRuntime>().Dispose();
        dead?.Invoke();
        await StopAsync(app, TimeSpan.Zero).ConfigureAwait(false);
    }

    /// <summary>Starts a killed node again under its old name; it re-registers and owns nothing until assigned.</summary>
    public async Task RestartNodeAsync(string name)
    {
        var index = _nodeApps.FindIndex(n => n.Name == name);
        if (_nodeApps[index].App is not null)
            throw new InvalidOperationException($"Node {name} is already running.");
        var app = await WithFreshPortsRetryAsync(() => StartAppAsync(name, ClusterRole.Shard, FreePort())).ConfigureAwait(false);
        _nodeApps[index] = (name, app);
    }

    /// <summary>The same counters the stats endpoint serves, for the serve console's periodic line.</summary>
    public ServerStats Stats() => CollectStats();

    private ServerStats CollectStats()
    {
        using var process = Process.GetCurrentProcess();
        var metrics = Hub.Metrics;
        var nodes = _nodeApps
            .Where(static n => n.App is not null)
            .Select(static n => n.App!.Services.GetRequiredService<ShardNodeRuntime>().Metrics)
            .ToList();
        List<long> lag;
        List<double> staleness;
        lock (_lagLock)
        {
            lag = [.. _lagRecords.Order()];
            staleness = [.. _stalenessMs.Order()];
        }

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
            ServerGc = System.Runtime.GCSettings.IsServerGC,
            ProcessorCount = Environment.ProcessorCount,
            LockSplitTransactions = _lockSplit?.Transactions ?? 0,
            LockedMsSum = _lockSplit?.LockedMs ?? 0,
            PostCommitMsSum = _lockSplit?.PostCommitMs ?? 0,
            HeartbeatGapCount = metrics.HeartbeatGaps.Count,
            HeartbeatGapMaxMs = metrics.HeartbeatGaps.MaxMs,
            HeartbeatGapsOverMs = LivenessGaps.ThresholdsMs.ToDictionary(static t => t, metrics.HeartbeatGaps.Over),
            LeaseRenewalGapCount = nodes.Sum(static n => n.LeaseRenewalGaps.Count),
            LeaseRenewalGapMaxMs = nodes.Count == 0 ? 0 : nodes.Max(static n => n.LeaseRenewalGaps.MaxMs),
            LeaseRenewalGapsOverMs = LivenessGaps.ThresholdsMs.ToDictionary(static t => t, t => nodes.Sum(n => n.LeaseRenewalGaps.Over(t))),
            NodesDeclaredDead = metrics.NodesDeclaredDead,
            ReplicaLagSamples = lag.Count,
            ReplicaLagP50Records = Quantile(lag, 0.50),
            ReplicaLagP99Records = Quantile(lag, 0.99),
            ReplicaLagMaxRecords = lag.Count == 0 ? 0 : lag[^1],
            ReplicaStalenessMaxMs = staleness.Count == 0 ? 0 : staleness[^1],
            ReplicaStalenessP99Ms = Quantile(staleness, 0.99),
            LeaseLapses = nodes.Sum(static n => n.LeaseLapses),
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
            ["MelangeDb:Cluster:HeartbeatIntervalMs"] = HeartbeatMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Cluster:FailureTimeoutMs"] = FailureTimeoutMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:HotStore:Engine"] = _options.HotStore == "faster" ? "Faster" : "InMemory",
            ["MelangeDb:Cluster:BorderBandChunks"] = _options.BandChunks.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Cluster:HandoffMarginChunks"] = _options.MarginChunks.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MelangeDb:Cluster:HandoffMinIntervalMs"] = _options.HandoffMinIntervalMs.ToString(System.Globalization.CultureInfo.InvariantCulture),

            // One shared root: the phase 09 shared-storage assumption for reassignment.
            ["MelangeDb:Cluster:ShardDataPath"] = Path.Combine(_root, "shards"),
        };
        foreach (var (key, value) in _tuning.Settings)
            settings[key] = value;
        if (_options.LockSplit)
            settings["MelangeDb:Telemetry:SlowReducerMs"] = "0";
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

        // --lock-split makes every transaction a "slow" one so its split is recorded; the warning
        // each would log is the measurement's mechanism, not a finding.
        if (_options.LockSplit)
            builder.Logging.AddFilter("MelangeDB.Core.MelangeEngine", LogLevel.Error);
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
        builder.Services.AddMelangeDb(melange =>
        {
            melange
                .AddTablesFrom(typeof(Terrain).Assembly)
                .AddReducersFrom(typeof(WalkerReducers).Assembly);
            if (_options.HotStore == "faster")
                melange.UseFasterHotStore();
        });
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

        try
        {
            await app.StartAsync().ConfigureAwait(false);
        }
        catch
        {
            // A failed start (stolen port above all) must release everything the host already
            // opened — the engines hold this node's log files, and the fresh-port retry reuses
            // the same data directories.
            await app.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return app;
    }

    /// <summary>Retries an app start whose ephemeral port was stolen between allocation and bind.</summary>
    private static async Task<WebApplication> WithFreshPortsRetryAsync(Func<Task<WebApplication>> start)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await start().ConfigureAwait(false);
            }
            catch (IOException) when (attempt < 5)
            {
            }
            catch (SocketException) when (attempt < 5)
            {
            }
        }
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

    /// <summary>
    /// One tick of the replica lag sampler: for every replica on every live node, the owner's
    /// durable LSN (read from the owner's engine in this same process) minus what the replica has
    /// applied. Read without the hub's relay, so the ceiling's own delay counts as lag. Staleness
    /// is how long the oldest record the replica has not applied has been durable, from a history
    /// of the owner's durable LSN at this sampler's 100 ms resolution.
    /// </summary>
    private void SampleReplicaLag()
    {
        try
        {
            var now = System.Diagnostics.Stopwatch.GetTimestamp();
            foreach (var (_, app) in _nodeApps.ToArray())
            {
                if (app is null)
                    continue;
                foreach (var (shard, applied, _) in app.Services.GetRequiredService<ShardNodeRuntime>().ReplicaProgress())
                {
                    if (TryGetShard(shard) is not { } owner || applied == 0)
                        continue; // No owner up, or the replica has not bootstrapped yet.
                    var durable = owner.Engine.Log.DurableLsn;
                    var behind = durable > applied ? (long)(durable - applied) : 0;
                    lock (_lagLock)
                    {
                        if (!_durableHistory.TryGetValue(shard.Value, out var history))
                            _durableHistory[shard.Value] = history = new Queue<(long, ulong)>();
                        if (history.Count == 0 || history.Last().Durable < durable)
                            history.Enqueue((now, durable));
                        while (history.Count > 1 && System.Diagnostics.Stopwatch.GetElapsedTime(history.Peek().At, now) > TimeSpan.FromMinutes(2))
                            history.Dequeue();

                        _lagRecords.Add(behind);
                        if (behind > 0)
                        {
                            // The first moment the owner was seen durable past what the replica holds.
                            var since = history.FirstOrDefault(entry => entry.Durable > applied).At;
                            _stalenessMs.Add(since == 0 ? 0 : System.Diagnostics.Stopwatch.GetElapsedTime(since, now).TotalMilliseconds);
                        }
                    }
                }
            }
        }
        catch (Exception exception) when (exception is ObjectDisposedException or InvalidOperationException)
        {
            // A node stopping under the sampler; the next tick reads the survivors.
        }
    }

    private static T Quantile<T>(List<T> sorted, double q) =>
        sorted.Count == 0 ? default! : sorted[Math.Min(sorted.Count - 1, (int)(q * sorted.Count))];

    public async ValueTask DisposeAsync()
    {
        if (_lagSampler is not null)
            await _lagSampler.DisposeAsync().ConfigureAwait(false);
        foreach (var (_, app) in _nodeApps)
        {
            if (app is not null)
                await StopAsync(app).ConfigureAwait(false);
        }

        if (_hubApp is not null)
            await StopAsync(_hubApp).ConfigureAwait(false);
        _lockSplit?.Dispose();
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
    }

    /// <param name="grace">
    /// How long the host may drain. A kill passes zero: Kestrel aborts its connections and stops
    /// accepting at once, as a crashed process's port would — with any grace, the dead node's
    /// listener keeps accepting the gateway's reconnects for that long and the window measures it.
    /// </param>
    private static async Task StopAsync(WebApplication app, TimeSpan? grace = null)
    {
        try
        {
            // Bounded, like the cluster fixture: the gateway holds live websockets that only
            // close when their client does, and a full graceful drain would wait for them.
            using var abrupt = new CancellationTokenSource(grace ?? TimeSpan.FromSeconds(3));
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
