using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using MelangeDB.Client;
using MelangeDB.Cluster;
using MelangeDB.Core;
using MelangeDB.Storage.Faster;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MelangeDB.LoadTest;

/// <summary>
/// The reassignment window (road-to-0.3 phase 20; phase 30's design input): kill the node that
/// owns a crowded shard and time how long the shard is unwritable, split into the parts that
/// behave differently.
/// <list type="bullet">
/// <item><b>detect</b> — kill until the hub names a new owner. Set by Cluster:FailureTimeoutMs and
/// the hub's sweep cadence, not by the shard: a warm replica cannot shorten it.</item>
/// <item><b>reopen</b> — new owner named until the shard is open on it: the heartbeat that carries
/// the assignment, then recovery from the snapshot plus the log tail. This is the part that
/// grows with shard heat, and the part a warm replica removes.</item>
/// <item><b>first write</b> — open until a reducer commits there.</item>
/// <item><b>client restore</b> — kill until each gateway client's first acked call that it sent
/// after the kill: what a player on the shard actually waits.</item>
/// </list>
/// Recovery is also timed in isolation, by opening the shard's directory with nothing else
/// running, so the heat-dependent part can be read without the heartbeat wait folded in.
/// Synthetic by construction: the reference workload has no cluster deployment to kill.
/// </summary>
internal static class FailoverMeasurement
{
    private const int SeedBatch = 5_000;
    private const int TailIdentities = 1_000;

    public static async Task<int> RunAsync(LoadTestOptions options, TextWriter output, CancellationToken ct)
    {
        // One shard, two nodes: the owner, and the survivor that takes the shard over.
        options.Nodes = 2;
        options.WorldBlocksX = 1;
        options.WorldBlocksY = 1;
        options.RowBytes = Math.Max(8, options.RowBytes);
        // The library defaults, read rather than copied: the window measured is the one a
        // deployment on defaults gets, and a copy silently kept measuring the old ones.
        var defaults = new MelangeDB.Core.ClusterOptions();
        var tuning = new ServeTuning(
            HeartbeatMs: defaults.HeartbeatIntervalMs,
            FailureTimeoutMs: defaults.FailureTimeoutMs,
            new Dictionary<string, string?>
            {
                // Snapshots are taken by the measurement, before each kill, so the tail recovery
                // replays is the --tail it asked for and never an automatic snapshot's leftovers.
                ["MelangeDb:Snapshots:IntervalTransactions"] = "1000000000000",
                ["MelangeDb:Cluster:FastFailoverOnLinkLoss"] = options.FastFailover ? "true" : "false",
                ["MelangeDb:Cluster:ShardReplicas"] = options.Replicas.ToString(CultureInfo.InvariantCulture),
            });
        var heartbeat = options.HeartbeatMs ?? tuning.HeartbeatMs;
        var failureTimeout = options.FailureTimeoutMs ?? tuning.FailureTimeoutMs;
        output.WriteLine(
            $"Failover: rows [{string.Join(", ", options.FailoverRows)}] x {options.RowBytes} B, tail {options.TailRecords} records, " +
            $"{options.Repeat} kill(s) each, {options.Clients} client(s) at {options.TickHz} Hz, hot store {options.HotStore} ({options.PagedEngine}), " +
            $"heartbeat {heartbeat} ms, failure timeout {failureTimeout} ms (deadline check every {Math.Clamp(failureTimeout / 20, 25, 250)} ms), " +
            $"fast failover {(options.FastFailover ? "on" : "off")}, replicas {options.Replicas}.");
        output.WriteLine(
            $"Machine: {Environment.ProcessorCount} logical cores, Server GC {System.Runtime.GCSettings.IsServerGC}, " +
            $"{System.Runtime.InteropServices.RuntimeInformation.OSDescription}, .NET {Environment.Version}.");

        var allPassed = true;
        foreach (var rows in options.FailoverRows)
            allPassed &= await RunSizeAsync(options, tuning, rows, output, ct).ConfigureAwait(false);
        return allPassed ? 0 : 1;
    }

    private static async Task<bool> RunSizeAsync(
        LoadTestOptions options, ServeTuning tuning, int rows, TextWriter output, CancellationToken ct)
    {
        // An explicit data root, so the shard directory outlives the cluster for the isolated
        // open below; the server only deletes roots it created itself.
        var root = Directory.CreateTempSubdirectory("melange-failover-").FullName;
        options.DataPath = root;
        var shard = SpatialShardStrategy.ShardOfBlock(0, 0);
        var shardDirectory = Path.Combine(root, "shards", $"shard-{shard.Value}");
        var runs = new List<FailoverRun>();
        ushort originator;
        ServerStats liveness;
        try
        {
            await using (var server = await LoadTestServer.StartAsync(options, output, ct, tuning).ConfigureAwait(false))
            {
                var seeder = DevTokens.IdentityOf("failover-seed");
                var seedClock = Stopwatch.StartNew();
                var owner = server.TryGetShard(shard) ?? throw new InvalidOperationException("The measured shard has no owner.");
                var chunks = options.BlockChunksX * options.BlockChunksY;
                for (long first = 0; first < rows; first += SeedBatch)
                {
                    var count = (int)Math.Min(SeedBatch, rows - first);
                    var chunk = (int)(first / SeedBatch % chunks);
                    owner.ReducerHost.Call(
                        "SeedBallast", seeder, Chunks.Id(chunk % options.BlockChunksX, chunk / options.BlockChunksX),
                        first, count, options.RowBytes);
                }

                output.WriteLine($"[rows {rows}] seeded in {seedClock.Elapsed.TotalSeconds:F1} s.");

                var probes = new List<ClientProbe>();
                try
                {
                    for (var i = 0; i < options.Clients; i++)
                        probes.Add(await ClientProbe.StartAsync(i, server.GatewayUri, shard, options, ct).ConfigureAwait(false));

                    for (var run = 1; run <= options.Repeat; run++)
                    {
                        var result = await KillOnceAsync(server, shard, options, probes, rows, run, ct).ConfigureAwait(false);
                        runs.Add(result);
                        output.WriteLine(result.Line());
                    }
                }
                finally
                {
                    foreach (var probe in probes)
                        await probe.DisposeAsync().ConfigureAwait(false);
                }

                originator = server.Hub.Membership.GetAssignment(shard)!.Originator;
                liveness = server.Stats();
            }

            // Recovery alone, with no cluster running: the heat-dependent part of the window.
            var isolated = new List<double>();
            for (var i = 0; i < 3; i++)
                isolated.Add(OpenIsolated(options, shardDirectory, originator));
            var footprint = DirectoryBytes(shardDirectory);
            var restores = runs.SelectMany(static r => r.ClientRestoreMs).ToList();

            // Every kill is one death; any beyond that is a healthy node (the adopter, mid-recovery,
            // most likely) declared dead. Lapses are counted on the nodes still running.
            var falseDeaths = liveness.NodesDeclaredDead - runs.Count;
            var windows = runs.Select(static r => r.ServerWindowMs).ToList();
            var line =
                FormattableString.Invariant($"FAILOVER rows={rows} row_bytes={options.RowBytes} tail={options.TailRecords} hot_store={options.HotStore} paged_engine={options.PagedEngine} replicas={options.Replicas} ") +
                FormattableString.Invariant($"promoted={runs.Count(static r => r.Promoted)}/{runs.Count} replica_behind_records={Range(runs.Select(static r => (double)r.ReplicaBehind))} ") +
                FormattableString.Invariant($"kills={runs.Count} shard_dir_mib={footprint / (1024.0 * 1024):F1} ") +
                FormattableString.Invariant($"detect_ms={Range(runs.Select(static r => r.DetectMs))} ") +
                FormattableString.Invariant($"reopen_ms={Range(runs.Select(static r => r.ReopenMs))} ") +
                FormattableString.Invariant($"open_isolated_ms={Range(isolated)} ") +
                FormattableString.Invariant($"server_window_ms={Range(windows)} ") +
                FormattableString.Invariant($"client_restore_p50_ms={Percentile(restores, 0.50):F0} client_restore_p99_ms={Percentile(restores, 0.99):F0} ") +
                FormattableString.Invariant($"client_restore_max_ms={(restores.Count == 0 ? 0 : restores.Max()):F0} ") +
                FormattableString.Invariant($"clients_unrestored={runs.Sum(static r => r.ClientsUnrestored)} ") +
                FormattableString.Invariant($"resync_errors={runs.Sum(static r => r.ResyncErrors)} failed_calls={runs.Sum(static r => r.FailedCalls)} ") +
                FormattableString.Invariant($"false_deaths={falseDeaths} lease_lapses={liveness.LeaseLapses} ") +
                FormattableString.Invariant($"hb_gap_max_ms={liveness.HeartbeatGapMaxMs:F0} lease_gap_max_ms={liveness.LeaseRenewalGapMaxMs:F0}");
            output.WriteLine(line);
            return runs.All(static r => r.ClientsUnrestored == 0) && falseDeaths == 0 && liveness.LeaseLapses == 0;
        }
        finally
        {
            options.DataPath = null;
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
            }
        }
    }

    private static async Task<FailoverRun> KillOnceAsync(
        LoadTestServer server, ShardKey shard, LoadTestOptions options, List<ClientProbe> probes,
        int rows, int run, CancellationToken ct)
    {
        var hub = server.Hub;
        var ownerName = hub.Membership.GetAssignment(shard)!.NodeName
            ?? throw new InvalidOperationException("The measured shard is unowned before the kill.");
        var survivorName = ownerName == "node-0" ? "node-1" : "node-0";
        var owner = server.Node(ownerName)!.TryGetShard(shard)!;

        // The owner's heartbeat staleness, as the hub sees it, through the whole pre-kill phase.
        // A live node that goes quiet for most of Cluster:FailureTimeoutMs is a finding of its
        // own (it is one stall from being fenced while healthy), and it also shortens "detect"
        // below the timeout, so the run must say when it happened rather than report the
        // shorter window as if the hub had been faster.
        // Steady state first: with replicas on, the survivor's follower is placed and caught up (the
        // restarted node from the previous kill has to bootstrap its replica from the snapshot).
        // The tail below is then written with no wait, so the kill finds the replica as far behind
        // as live traffic leaves it.
        if (options.Replicas > 0)
        {
            // Up to where the owner stood when the wait began: the clients keep writing, so the
            // replica is always a heartbeat or so behind the live head.
            var target = owner.Engine.Log.DurableLsn;
            await WaitUntilAsync(
                () => server.Node(survivorName)?.ReplicaAppliedLsn(shard) is { } applied && applied >= target,
                "the survivor's replica caught up before the kill", ct).ConfigureAwait(false);
        }

        using var watch = new HeartbeatWatch(hub, ownerName);
        var pauseBefore = GC.GetTotalPauseDuration();
        var phase = Stopwatch.StartNew();

        // A fresh snapshot, then exactly --tail records after it: the recovery the kill forces.
        owner.Engine.TakeSnapshot();
        var snapshotMs = phase.Elapsed.TotalMilliseconds;
        for (var i = 0; i < options.TailRecords; i++)
        {
            owner.ReducerHost.Call(
                "Move", DevTokens.IdentityOf($"failover-tail-{i % TailIdentities:D4}"),
                Chunks.Id(i % options.BlockChunksX, 0), (long)i);
        }

        var tailMs = phase.Elapsed.TotalMilliseconds - snapshotMs;

        // Every client acking before the kill, so a restore is a restore and not a slow start.
        await WaitUntilAsync(() => probes.All(static p => p.AckedRecently), "every client acking before the kill", ct).ConfigureAwait(false);
        var preKill = new PreKill(
            snapshotMs, tailMs, watch.MaxStalenessMs, watch.StalenessAtKillMs,
            (GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds, GC.CollectionCount(2));

        // The poll runs on its own thread from before the kill: the kill's host stop takes up to
        // its grace period after the runtime is already dead, and the hub and survivor move
        // during it. Spinning rather than Task.Delay, whose ~15 ms Windows granularity is the size
        // of a small shard's whole reopen.
        long assignedAt = 0, openedAt = 0, writtenAt = 0;
        var probeIdentity = DevTokens.IdentityOf("failover-probe");
        var replicaBehind = server.Node(survivorName)?.ReplicaAppliedLsn(shard) is { } kept
            ? (long)(owner.Engine.Log.DurableLsn - Math.Min(kept, owner.Engine.Log.DurableLsn))
            : -1;
        var killedAt = Stopwatch.GetTimestamp();
        var poll = Task.Factory.StartNew(
            () =>
            {
                var timeout = Stopwatch.StartNew();
                var spinner = default(SpinWait);
                while (true)
                {
                    if (timeout.Elapsed > TimeSpan.FromMinutes(5) || ct.IsCancellationRequested)
                        throw new TimeoutException($"rows {rows} run {run}: the shard never became writable on the survivor.");
                    if (assignedAt == 0 && hub.Membership.GetAssignment(shard)?.NodeName is { } named && named != ownerName)
                        assignedAt = Stopwatch.GetTimestamp();
                    if (assignedAt != 0 && openedAt == 0 && server.Node(survivorName)?.TryGetShard(shard) is not null)
                        openedAt = Stopwatch.GetTimestamp();
                    if (openedAt != 0)
                    {
                        try
                        {
                            server.Node(survivorName)!.TryGetShard(shard)!.ReducerHost.Call(
                                "Move", probeIdentity, Chunks.Id(0, 0), (long)run);
                            writtenAt = Stopwatch.GetTimestamp();
                            return;
                        }
                        catch (Exception exception) when (exception is ShardFencedException or InvalidOperationException)
                        {
                            // Not yet: the survivor's lease or fencing token has not caught up.
                        }
                    }

                    spinner.SpinOnce(sleep1Threshold: -1);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        // Probes arm once the runtime is gone, not at the kill's first instant: a call sent in
        // between can still be acked by the dying node, and would read as a 30 ms "restore".
        // The window itself is still measured from killedAt.
        await server.KillNodeAsync(ownerName, () =>
        {
            var deadAt = Stopwatch.GetTimestamp();
            foreach (var probe in probes)
                probe.Arm(deadAt);
        }).ConfigureAwait(false);
        await poll.ConfigureAwait(false);

        // Clients get as long as the server took plus a generous margin; a client that never comes
        // back inside that is a finding, reported as unrestored rather than waited on forever.
        var clientDeadline = Stopwatch.StartNew();
        while (!probes.All(static p => p.RestoredAt != 0) && clientDeadline.Elapsed < TimeSpan.FromSeconds(60))
            await Task.Delay(20, ct).ConfigureAwait(false);

        var result = new FailoverRun(
            rows,
            run,
            DetectMs: Ms(killedAt, assignedAt),
            ReopenMs: Ms(assignedAt, openedAt),
            FirstWriteMs: Ms(openedAt, writtenAt),
            ServerWindowMs: Ms(killedAt, writtenAt),
            ClientRestoreMs: probes.Where(static p => p.RestoredAt != 0).Select(p => Ms(killedAt, p.RestoredAt)).ToList(),
            ClientsUnrestored: probes.Count(static p => p.RestoredAt == 0),
            ResyncErrors: probes.Sum(static p => p.ResyncErrorsSinceArm),
            FailedCalls: probes.Sum(static p => p.FailedSinceArm),
            preKill)
        {
            Promoted = server.Node(survivorName)?.TryGetShard(shard)?.WarmStartedFrom is not null,
            ReplicaBehind = replicaBehind,
            FailureReasons = probes
                .SelectMany(static p => p.FailureReasonsSinceArm)
                .GroupBy(static pair => pair.Key, StringComparer.Ordinal)
                .ToDictionary(static g => g.Key, static g => g.Sum(static pair => pair.Value), StringComparer.Ordinal),
        };

        await server.RestartNodeAsync(ownerName).ConfigureAwait(false);
        await WaitUntilAsync(
            () => hub.Membership.Nodes().Count(static n => n.Alive) == 2,
            "the killed node re-registered", ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>Opens the shard directory with a bare engine — recovery and nothing else — and returns the milliseconds it took.</summary>
    private static double OpenIsolated(LoadTestOptions options, string shardDirectory, ushort originator)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMelangeDb(melange =>
        {
            melange
                .AddTablesFrom(typeof(Terrain).Assembly)
                .AddReducersFrom(typeof(WalkerReducers).Assembly);
            if (options.HotStore == "faster")
                melange.UseFasterHotStore();
        });
        using var provider = services.BuildServiceProvider();
        var engineOptions = new MelangeDbOptions();
        engineOptions.CommitLog.Path = Path.Combine(shardDirectory, "log");
        engineOptions.HotStore.Path = Path.Combine(shardDirectory, "hot");
        engineOptions.HotStore.Engine = options.HotStore == "faster" ? HotStoreEngine.Faster : HotStoreEngine.InMemory;
        engineOptions.HotStore.PagedEngine = Enum.Parse<PagedEngine>(LoadTestServer.PagedEngineName(options.PagedEngine));
        if (options.BudgetMb is { } budget)
            engineOptions.HotStore.MemoryBudgetBytes = budget * (1L << 20);
        engineOptions.Events.DeadLetterPath = Path.Combine(shardDirectory, "deadletter");
        engineOptions.Snapshots.IntervalTransactions = 1_000_000_000_000;
        engineOptions.Residency.ReportOnStartup = false;
        var started = Stopwatch.GetTimestamp();
        using var engine = new MelangeEngine(
            engineOptions,
            provider.GetRequiredService<SchemaRegistry>(),
            hotStoreProvider: provider.GetService<IHotStoreProvider>(),
            originator: originator);
        return Stopwatch.GetElapsedTime(started).TotalMilliseconds;
    }

    private static long DirectoryBytes(string directory) =>
        Directory.Exists(directory)
            ? new DirectoryInfo(directory).EnumerateFiles("*", SearchOption.AllDirectories).Sum(static f => f.Length)
            : 0;

    private static double Ms(long from, long to) => Stopwatch.GetElapsedTime(from, to).TotalMilliseconds;

    private static string Range(IEnumerable<double> values)
    {
        var list = values.Order().ToList();
        return list.Count == 0
            ? "none"
            : list.Count == 1
                ? list[0].ToString("F0", CultureInfo.InvariantCulture)
                : $"{list[0].ToString("F0", CultureInfo.InvariantCulture)}..{list[^1].ToString("F0", CultureInfo.InvariantCulture)}" +
                  $"(med {Percentile(list, 0.5).ToString("F0", CultureInfo.InvariantCulture)})";
    }

    private static double Percentile(IReadOnlyList<double> values, double p)
    {
        if (values.Count == 0)
            return 0;
        var sorted = values.Order().ToList();
        return sorted[Math.Clamp((int)Math.Ceiling(p * sorted.Count) - 1, 0, sorted.Count - 1)];
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        while (!condition())
        {
            if (clock.Elapsed > TimeSpan.FromSeconds(60))
                throw new TimeoutException($"Timed out waiting for: {what}");
            await Task.Delay(20, ct).ConfigureAwait(false);
        }
    }

    private sealed record FailoverRun(
        int Rows,
        int Run,
        double DetectMs,
        double ReopenMs,
        double FirstWriteMs,
        double ServerWindowMs,
        IReadOnlyList<double> ClientRestoreMs,
        int ClientsUnrestored,
        int ResyncErrors,
        int FailedCalls,
        PreKill PreKill)
    {
        /// <summary>Whether the survivor started from its replica (step 2e) rather than recovering cold.</summary>
        public bool Promoted { get; init; }

        /// <summary>Records the replica had not applied at the kill; -1 when the survivor had no replica.</summary>
        public long ReplicaBehind { get; init; }

        /// <summary>Why the failed calls failed: exception type and message head, with counts.</summary>
        public IReadOnlyDictionary<string, int> FailureReasons { get; init; } = new Dictionary<string, int>();

        public string Line() => Body() +
            (FailureReasons.Count == 0 ? string.Empty : " Failures: " + string.Join("; ",
                FailureReasons.OrderByDescending(static pair => pair.Value).Take(4).Select(static pair => $"{pair.Value}x {pair.Key}"))) +
            (PreKill.StalenessAtKillMs > 1_500 ? " WARNING: the owner was already silent at the kill, so detect is short by that much." : string.Empty);

        private string Body() => string.Create(
            CultureInfo.InvariantCulture,
            $"[rows {Rows} kill {Run}] detect {DetectMs:F0} ms, {(Promoted ? "promote" : "reopen")} {ReopenMs:F0} ms" +
            $"{(ReplicaBehind >= 0 ? $" (replica {ReplicaBehind} behind)" : string.Empty)}, first write {FirstWriteMs:F0} ms " +
            $"=> server window {ServerWindowMs:F0} ms; clients restored p50 {Percentile(ClientRestoreMs, 0.5):F0} / " +
            $"max {(ClientRestoreMs.Count == 0 ? 0 : ClientRestoreMs.Max()):F0} ms, {ClientsUnrestored} unrestored, " +
            $"{ResyncErrors} resync error(s), {FailedCalls} failed call(s). Pre-kill: snapshot {PreKill.SnapshotMs:F0} ms, " +
            $"tail {PreKill.TailMs:F0} ms, owner heartbeat staleness max {PreKill.MaxStalenessMs:F0} ms / at kill " +
            $"{PreKill.StalenessAtKillMs:F0} ms, GC pause {PreKill.GcPauseMs:F0} ms (gen2 total {PreKill.Gen2}).");
    }

    private sealed record PreKill(double SnapshotMs, double TailMs, double MaxStalenessMs, double StalenessAtKillMs, double GcPauseMs, int Gen2);

    /// <summary>Samples the hub's last-seen time for one node every 50 ms on its own thread.</summary>
    private sealed class HeartbeatWatch : IDisposable
    {
        private readonly HubRuntime _hub;
        private readonly string _node;
        private readonly CancellationTokenSource _stop = new();
        private readonly Thread _thread;
        private double _max;
        private double _last;

        public HeartbeatWatch(HubRuntime hub, string node)
        {
            _hub = hub;
            _node = node;
            _thread = new Thread(Run) { IsBackground = true, Name = "failover-heartbeat-watch" };
            _thread.Start();
        }

        public double MaxStalenessMs => Volatile.Read(ref _max);

        public double StalenessAtKillMs => Sample();

        private double Sample()
        {
            var record = _hub.Membership.Nodes().FirstOrDefault(n => n.NodeName == _node);
            return record is null ? 0 : (DateTimeOffset.UtcNow - record.LastSeen).TotalMilliseconds;
        }

        private void Run()
        {
            while (!_stop.IsCancellationRequested)
            {
                var staleness = Sample();
                Volatile.Write(ref _last, staleness);
                if (staleness > Volatile.Read(ref _max))
                    Volatile.Write(ref _max, staleness);
                _stop.Token.WaitHandle.WaitOne(50);
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _thread.Join();
            _stop.Dispose();
        }
    }

    /// <summary>
    /// One player on the measured shard, through the gateway: calls Move every tick and records
    /// the first acked call it sent after the kill. A resync error (the gateway telling it an
    /// attachment was re-established) is answered the way a client must: subscribe again.
    /// </summary>
    private sealed class ClientProbe : IAsyncDisposable
    {
        private readonly MelangeClient _client;
        private readonly uint _chunk;
        private readonly CancellationTokenSource _stop = new();
        private Task _loop = Task.CompletedTask;
        private long _seq;
        private long _armedAt = long.MaxValue;
        private long _restoredAt;
        private long _lastAckAt;
        private int _resyncErrors;
        private int _failed;

        private ClientProbe(int index, Uri gateway, LoadTestOptions options)
        {
            var subject = $"failover-client-{index:D4}";
            _client = new MelangeClient(new MelangeClientOptions { Uri = gateway, Token = DevTokens.For(subject) });
            _chunk = Chunks.Id(index % options.BlockChunksX, index / options.BlockChunksX % options.BlockChunksY);
            _client.OnError += error =>
            {
                if (Interlocked.Read(ref _armedAt) == long.MaxValue)
                    return;
                Interlocked.Increment(ref _resyncErrors);
                _ = ResubscribeAsync();
            };
        }

        public long RestoredAt => Interlocked.Read(ref _restoredAt);

        public int ResyncErrorsSinceArm => Volatile.Read(ref _resyncErrors);

        public int FailedSinceArm => Volatile.Read(ref _failed);

        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _failureReasons = new(StringComparer.Ordinal);

        public IEnumerable<KeyValuePair<string, int>> FailureReasonsSinceArm => _failureReasons;

        public bool AckedRecently =>
            Interlocked.Read(ref _lastAckAt) is var last and not 0 && Stopwatch.GetElapsedTime(last) < TimeSpan.FromMilliseconds(500);

        public static async Task<ClientProbe> StartAsync(int index, Uri gateway, ShardKey shard, LoadTestOptions options, CancellationToken ct)
        {
            var probe = new ClientProbe(index, gateway, options);
            await probe._client.ConnectAsync(ct).ConfigureAwait(false);
            await probe._client.CallReducerAsync("SetPlayerShard", [shard.Value], ct).ConfigureAwait(false);
            await probe._client.SubscribeAsync("SELECT * FROM PlayerPos", null, ct).ConfigureAwait(false);
            probe._loop = probe.RunAsync(options.TickHz);
            return probe;
        }

        public void Arm(long killedAt)
        {
            Interlocked.Exchange(ref _restoredAt, 0);
            Interlocked.Exchange(ref _resyncErrors, 0);
            Interlocked.Exchange(ref _failed, 0);
            _failureReasons.Clear();
            Interlocked.Exchange(ref _armedAt, killedAt);
        }

        private async Task RunAsync(double tickHz)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1 / tickHz));
            try
            {
                while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
                    _ = CallAsync();
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task CallAsync()
        {
            var sentAt = Stopwatch.GetTimestamp();
            try
            {
                await _client.CallReducerAsync("Move", [_chunk, Interlocked.Increment(ref _seq)]).ConfigureAwait(false);
                var ackedAt = Stopwatch.GetTimestamp();
                Interlocked.Exchange(ref _lastAckAt, ackedAt);
                if (sentAt > Interlocked.Read(ref _armedAt))
                    Interlocked.CompareExchange(ref _restoredAt, ackedAt, 0);
            }
            catch (Exception exception) when (!_stop.IsCancellationRequested)
            {
                if (sentAt > Interlocked.Read(ref _armedAt))
                {
                    Interlocked.Increment(ref _failed);
                    var message = exception.Message.Length > 80 ? exception.Message[..80] : exception.Message;
                    _failureReasons.AddOrUpdate($"{exception.GetType().Name}: {message}", 1, static (_, count) => count + 1);
                }
            }
        }

        private async Task ResubscribeAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                try
                {
                    await _client.SubscribeAsync("SELECT * FROM PlayerPos", null, _stop.Token).ConfigureAwait(false);
                    return;
                }
                catch (Exception) when (!_stop.IsCancellationRequested)
                {
                    await Task.Delay(250, _stop.Token).ConfigureAwait(false);
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            await _loop.ConfigureAwait(false);
            await _client.DisposeAsync().ConfigureAwait(false);
            _stop.Dispose();
        }
    }
}
