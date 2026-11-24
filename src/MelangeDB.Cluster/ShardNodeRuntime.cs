using System.Net.Sockets;
using System.Text.Json;
using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// Everything the shard-node role runs: the node link to the hub (mutually authenticated,
/// heartbeating, lease-renewing), one <see cref="ShardRuntime"/> per assigned shard, the
/// replication client applying the hub's Replicated write sets into every shard engine, event
/// forwarding, and the node-side halves of the handoff saga. The node's DI-registered engine
/// stays what it was — a node-local engine holding only Local tables — while shard state lives
/// in the per-shard engines this runtime opens and closes as assignments change.
/// </summary>
internal sealed partial class ShardNodeRuntime : IDisposable
{
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly MelangeAuthenticator _authenticator;
    private readonly MelangeSessions _sessions;
    private readonly Lock _shardsLock = new();
    private readonly Dictionary<ShardKey, ShardRuntime> _shards = [];

    /// <summary>
    /// Shards being opened, off the heartbeat path. Recovery is a full replay of the shard's
    /// snapshot and log tail, which for a crowded shard takes seconds, and it used to run inline in
    /// the heartbeat loop: the adopting node stopped heartbeating and renewing its lease for the
    /// whole recovery, so a recovery longer than Cluster:FailureTimeoutMs got the healthy adopter
    /// declared dead and self-fenced on every shard it owned (#198). An open now runs as its own
    /// task; this map is what keeps a second heartbeat from starting the same open twice.
    /// </summary>
    private readonly Dictionary<ShardKey, Task> _opening = [];

    /// <summary>The hub's latest assignment list as this node last received it, for opens that complete after it changed.</summary>
    private Dictionary<ShardKey, ShardAssignment> _assigned = [];

    /// <summary>
    /// Shards this node quiesced for a planned drain, with when. While marked, an assignment
    /// still naming this node does not reopen the shard — the hub is between quiesce and
    /// reassign, and reopening would race the destination onto one log. The mark clears when the
    /// assignment moves elsewhere, when the hub abandons the drain (<c>shard-drain-abort</c>), or
    /// by expiry (2 x Cluster:FailureTimeoutMs) — the self-healing bound for a hub that died
    /// mid-drain, after which the shard reopens on the next heartbeat.
    /// </summary>
    private readonly Dictionary<ShardKey, DateTimeOffset> _draining = [];

    /// <summary>
    /// Shards this node has sealed and closed for a reap but not yet deleted, with when and where.
    /// The mark holds the door shut over the window the hub needs to retire the shard's membership
    /// row: while it is set, an assignment still naming this node does not reopen the shard, and a
    /// repeated <c>shard-reap</c> answers "already done" rather than starting over. It clears when
    /// the delete arrives, or by expiry (2 x Cluster:FailureTimeoutMs) — after which the shard
    /// reopens from its untouched directory, which is what "the reap did not happen" has to mean.
    /// </summary>
    private readonly Dictionary<ShardKey, (DateTimeOffset At, string Directory)> _reaping = [];
    private readonly Dictionary<ShardKey, EventForwarder> _forwarders = [];
    private readonly Dictionary<ShardKey, BorderPublisher> _borderPublishers = [];
    private readonly Dictionary<ShardKey, BoundaryMonitor> _boundaryMonitors = [];
    private readonly Dictionary<(ulong Observer, ulong Owner), (int Band, long LastSentTicks)> _borderSubscriptions = [];
    private readonly CancellationTokenSource _stopped = new();
    private volatile NodeLink? _link;

    /// <summary>Test hook: whether this node's link to the hub sends binary frames (negotiated at authentication).</summary>
    internal bool HubLinkSendsBinary => _link?.SendsBinary ?? false;
    private IShardStrategy? _strategy;
    /// <summary>
    /// When the lease ends, as a <see cref="TimeProvider.GetTimestamp"/> value: monotonic, so a
    /// wall-clock step cannot stretch or shrink it. Zero (never renewed) is always expired.
    /// </summary>
    private long _leaseValidUntilTimestamp;

    /// <summary>
    /// The failure timeout the lease is measured against: the hub's, from <see cref="AuthReply"/>,
    /// once registered. Until then (and against a hub too old to send it) this node's own setting.
    /// </summary>
    private int _leaseTimeoutMs;

    private bool _timeoutMismatchLogged;

    private ITimer? _leaseWatchdog;
    private long _lastRenewalSentTimestamp;
    private readonly System.Diagnostics.Metrics.Meter _livenessMeter = new("MelangeDB");
    private readonly System.Diagnostics.Metrics.Histogram<double> _renewalGap;
    private readonly System.Diagnostics.Metrics.Counter<long> _leaseLapses;
    private Task? _connectLoop;
    private Task? _reconcileLoop;
    private Task? _borderLoop;
    private ulong _replicaSubscribedFrom = ulong.MaxValue;

    public ShardNodeRuntime(IServiceProvider services)
    {
        _services = services;
        _options = services.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>();
        _time = services.GetService<TimeProvider>() ?? TimeProvider.System;
        _loggerFactory = services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger("MelangeDB.Cluster.Node");
        _sessions = services.GetService<MelangeSessions>() ?? new MelangeSessions();
        _authenticator = new MelangeAuthenticator(
            services,
            () => _options.CurrentValue.Auth,
            () => _options.CurrentValue.Sql,
            () => _options.CurrentValue.Bulk,
            () => _options.CurrentValue.Cluster,
            _time);
        _renewalGap = _livenessMeter.CreateHistogram<double>(
            "melange.cluster.lease.renewal_gap",
            unit: "ms",
            description: "Time from sending an acknowledged heartbeat to the reply to the next one: how long the node's lease had to last. It lapses past nine tenths of the hub's Cluster:FailureTimeoutMs.");
        _livenessMeter.CreateObservableGauge(
            "melange.cluster.replica.lag",
            () =>
            {
                lock (_followersLock)
                {
                    return _followers.Select(pair => new System.Diagnostics.Metrics.Measurement<long>(
                        (long)(pair.Value.Ceiling > pair.Value.AppliedLsn ? pair.Value.Ceiling - pair.Value.AppliedLsn : 0),
                        new KeyValuePair<string, object?>("shard", pair.Key))).ToList();
                }
            },
            unit: "{record}",
            description: "Records a shard replica's follower is behind its owner's durable LSN, as last relayed by the hub.");
        _leaseLapses = _livenessMeter.CreateCounter<long>(
            "melange.cluster.lease.lapses",
            unit: "{lapse}",
            description: "Times a shard node's lease lapsed and it parked its shards.");
        _livenessMeter.CreateObservableGauge(
            "melange.shard.owned",
            () =>
            {
                int owned;
                lock (_shardsLock)
                {
                    owned = _shards.Count;
                }

                return new System.Diagnostics.Metrics.Measurement<long>(owned, new KeyValuePair<string, object?>("node", NodeName));
            },
            unit: "{shard}",
            description: "Shards this node currently owns and serves, tagged by node.");
    }

    public ClusterMetrics Metrics { get; } = new();

    /// <summary>Test hook: while true, heartbeats and pings stop — a partition, as the hub sees it.</summary>
    internal bool SuspendHeartbeats { get; set; }

    /// <summary>
    /// Test hook: while true, only the thread-pool heartbeat loop stops; the liveness pinger keeps
    /// going. A node whose pool is starved, as the hub sees it.
    /// </summary>
    internal bool StallHeartbeatLoop { get; set; }

    /// <summary>
    /// Test hook: runs at the start of every shard open, on the open's own task — a slow (or
    /// failing) recovery without having to build a shard big enough to be one.
    /// </summary>
    internal Action<ShardKey>? BeforeOpen { get; set; }

    /// <summary>
    /// Test hook: runs before every slice of Replicated records this node applies, pushed or
    /// pulled, on the applying thread. Slow replication without a seed big enough to be slow.
    /// </summary>
    internal Action? BeforeReplicaSlice { get; set; }

    /// <summary>Test hook: the estimated bytes per border-reset part this node's publishers send, so a test band splits.</summary>
    internal int BorderResetPartBytes { get; set; } = BorderPublisher.DefaultResetPartBytes;

    private ClusterOptions Cluster => _options.CurrentValue.Cluster;

    /// <summary>This node's own scratch (Cluster:LocalScratchPath): shard hot stores, owned and replica.</summary>
    private string LocalScratch => string.IsNullOrEmpty(Cluster.LocalScratchPath)
        ? Path.Combine(_options.CurrentValue.HotStore.Path, "cluster")
        : Cluster.LocalScratchPath;

    private readonly Lock _followersLock = new();
    private readonly Dictionary<ulong, LogFollower> _followers = [];
    private Thread? _followerThread;

    /// <summary>Test hook: the follower keeping this node's replica of <paramref name="shard"/>, if any.</summary>
    internal LogFollower? TryGetFollower(ShardKey shard)
    {
        lock (_followersLock)
        {
            return _followers.GetValueOrDefault(shard.Value);
        }
    }

    /// <summary>
    /// For the load tool's lag sampler: every replica this node keeps, with how far it has applied
    /// and the commit time of the last record it applied.
    /// </summary>
    internal List<(ShardKey Shard, ulong AppliedLsn, Timestamp? Tail)> ReplicaProgress()
    {
        lock (_followersLock)
        {
            return [.. _followers.Select(static pair => (new ShardKey(pair.Key), pair.Value.AppliedLsn, pair.Value.TailTimestamp))];
        }
    }

    /// <summary>For the failover tool: how far this node's replica of <paramref name="shard"/> has applied; null without one.</summary>
    internal ulong? ReplicaAppliedLsn(ShardKey shard) => TryGetFollower(shard)?.AppliedLsn;

    /// <summary>
    /// Diffs the hub's replica list against the running followers (phase 30 step 2d): new ones
    /// start, dropped ones stop, and each ceiling rises to the owner's latest durable LSN. A shard
    /// this node owns is never followed here, whatever a stale plan says.
    /// </summary>
    private void ApplyReplicas(ReplicaDto[] replicas)
    {
        HashSet<ShardKey> owned;
        HashSet<ShardKey> promoting;
        lock (_shardsLock)
        {
            owned = [.. _assigned.Keys];

            // Assigned here but not open yet: the open promotes the follower (step 2e), so it is
            // left for the open to take rather than stopped under it.
            promoting = [.. _assigned.Keys.Where(shard => !_shards.ContainsKey(shard))];
        }

        var wanted = replicas.Where(r => !owned.Contains(new ShardKey(r.Shard))).ToDictionary(static r => r.Shard);
        List<LogFollower> stopped = [];
        lock (_followersLock)
        {
            foreach (var (shard, follower) in _followers.ToList())
            {
                if (wanted.ContainsKey(shard) || promoting.Contains(new ShardKey(shard)))
                    continue;
                _followers.Remove(shard);
                stopped.Add(follower);
            }

            foreach (var replica in wanted.Values)
            {
                if (!_followers.TryGetValue(replica.Shard, out var follower))
                {
                    follower = CreateFollower(replica);
                    _followers[replica.Shard] = follower;
                }

                follower.Ceiling = Math.Max(follower.Ceiling, replica.Ceiling);
            }

            if (_followers.Count > 0 && _followerThread is null)
            {
                _followerThread = new Thread(FollowReplicas) { IsBackground = true, Name = "MelangeDB replica follower" };
                _followerThread.Start();
            }
        }

        foreach (var follower in stopped)
        {
            lock (follower)
                follower.Dispose();
        }
    }

    private LogFollower CreateFollower(ReplicaDto replica)
    {
        var options = MelangeOptionsClone.DeepClone(_options.CurrentValue);
        options.CommitLog.Path = Path.Combine(Cluster.ShardDataPath, $"shard-{replica.Shard}", "log");
        options.HotStore.Path = Path.Combine(LocalScratch, $"replica-{replica.Shard}");
        var schema = _services.GetRequiredService<SchemaRegistry>();
        ApplyReplicaResidency(options, schema, Cluster.ReplicaResidency);
        return new LogFollower(
            options,
            schema,
            replica.Originator,
            _loggerFactory,
            _services.GetService<IHotStoreProvider>());
    }

    /// <summary>
    /// Promotion's first half (phase 30 step 2e): stops this node's follower of
    /// <paramref name="shard"/>, if it has one, and hands over its projection for the owner's engine
    /// to start from. Null when there is none to hand over (no follower, not bootstrapped yet, or a
    /// place in the log that can no longer be trusted); the open then recovers cold.
    /// </summary>
    private WarmStart? TakeReplica(ShardKey shard)
    {
        LogFollower? follower;
        lock (_followersLock)
        {
            if (_followers.Remove(shard.Value, out follower) is false)
                return null;
        }

        // The poll loop may hold it mid-poll; the lock waits that out, and the loop skips a
        // disposed follower afterwards.
        lock (follower)
        {
            try
            {
                return follower.Release();
            }
            catch (Exception exception)
            {
                LogReplicaFollowFailed(_logger, shard.Value, exception.Message);
                follower.Dispose();
                return null;
            }
        }
    }

    /// <summary>
    /// <c>Cluster:ReplicaResidency</c> on a replica's options: <see cref="ReplicaResidency.Paged"/>
    /// pins every table to paged, overriding attributes and <c>Residency:*</c> alike. A promoted
    /// replica keeps the residency it was built with until the shard next opens cold.
    /// </summary>
    internal static void ApplyReplicaResidency(MelangeDbOptions options, SchemaRegistry schema, ReplicaResidency residency)
    {
        if (residency != ReplicaResidency.Paged)
            return;
        options.Residency.Default = Residency.Paged;
        foreach (var table in schema.Tables)
            options.Residency.PerTable[table.Name] = Residency.Paged;
    }

    /// <summary>The followers' poll loop: one thread for all of this node's replicas.</summary>
    private void FollowReplicas()
    {
        while (!_stopped.IsCancellationRequested)
        {
            List<(ulong Shard, LogFollower Follower)> followers;
            lock (_followersLock)
            {
                followers = [.. _followers.Select(static pair => (pair.Key, pair.Value))];
            }

            foreach (var (shard, follower) in followers)
            {
                lock (follower)
                {
                    try
                    {
                        follower.Poll();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Stopped since the list was read.
                    }
                    catch (Exception exception)
                    {
                        LogReplicaFollowFailed(_logger, shard, exception.Message);
                    }
                }
            }

            _stopped.Token.WaitHandle.WaitOne(50);
        }
    }

    private string NodeName => Cluster.NodeName;

    /// <summary>The shards this node currently owns.</summary>
    public IReadOnlyList<ShardKey> OwnedShards
    {
        get
        {
            lock (_shardsLock)
            {
                return [.. _shards.Keys.OrderBy(static s => s.Value)];
            }
        }
    }

    public ShardRuntime? TryGetShard(ShardKey shard)
    {
        lock (_shardsLock)
        {
            return _shards.GetValueOrDefault(shard);
        }
    }

    /// <summary>Test hook: the boundary monitor for one owned shard, if any.</summary>
    internal BoundaryMonitor? TryGetMonitor(ShardKey shard)
    {
        lock (_shardsLock)
        {
            return _boundaryMonitors.GetValueOrDefault(shard);
        }
    }

    /// <summary>
    /// Whether this node's lease is live. The lease runs from when the last acknowledged heartbeat
    /// (or registration) was <em>sent</em>, for the hub's failure timeout less a margin (see
    /// <see cref="RenewLease"/>), so it always ends before the hub can declare this node dead.
    /// Expired means self-fenced: every commit is refused and the shards are parked.
    /// </summary>
    public bool LeaseValid() =>
        _time.GetTimestamp() < Interlocked.Read(ref _leaseValidUntilTimestamp);

    public void Start()
    {
        var cluster = Cluster;
        if (string.IsNullOrEmpty(cluster.Secret))
            throw new InvalidOperationException("Cluster:Role is Shard but Cluster:Secret is empty.");
        if (string.IsNullOrEmpty(cluster.NodeName))
            throw new InvalidOperationException("Cluster:Role is Shard but Cluster:NodeName is empty.");
        if (string.IsNullOrEmpty(cluster.HubAddress))
            throw new InvalidOperationException("Cluster:Role is Shard but Cluster:HubAddress is empty.");

        // The node-local DI engine holds only Local tables; everything else has a home elsewhere.
        var engine = _services.GetRequiredService<MelangeEngine>();
        engine.SetTableAccessGuard(PlacementGuards.NodeLocalAccess(cluster.NodeName));

        // And the commit-point half, which the access guard cannot cover: bulk ingestion never
        // touches a table handle, so without this a batch of Partitioned rows posted to this
        // node's own endpoint committed into an engine nothing reads them from. The hub has had
        // the equivalent guard all along.
        engine.AddCommitGuard(new NodeLocalCommitGuard(engine.Schema, cluster.NodeName));

        _strategy = _services.GetService<IShardStrategy>();
        _leaseTimeoutMs = cluster.FailureTimeoutMs;
        var watchdog = TimeSpan.FromMilliseconds(Math.Max(25, Math.Min(cluster.HeartbeatIntervalMs, cluster.FailureTimeoutMs / 10)));
        _leaseWatchdog = _time.CreateTimer(_ => ParkShardsIfLeaseExpired(), null, watchdog, watchdog);
        _connectLoop = Task.Run(RunAsync);
        _reconcileLoop = Task.Run(ReconcileHandoffsAsync);
        _borderLoop = Task.Run(MaintainBorderSubscriptionsAsync);
    }

    private async Task RunAsync()
    {
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await ConnectAndServeAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogHubLinkLost(_logger, exception.Message);
            }

            try
            {
                await Task.Delay(500, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ConnectAndServeAsync(CancellationToken ct)
    {
        var cluster = Cluster;
        var parts = cluster.HubAddress.Split(':');
        var client = new TcpClient();
        await client.ConnectAsync(parts[0], int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture), ct)
            .ConfigureAwait(false);
        var link = new NodeLink(client, Metrics);
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        link.Closed += _ => closed.TrySetResult();
        var challenge = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        link.Handler = (l, type, body) => HandleAsync(l, type, body, challenge);
        link.Start();
        try
        {
            var hubNonce = await challenge.Task.WaitAsync(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
            var nodeNonce = LinkProof.NewNonce();
            var authSent = _time.GetTimestamp();
            var reply = await link.RequestAsync("auth", new AuthRequest(
                cluster.NodeName,
                cluster.PublicAddress,
                nodeNonce,
                LinkProof.Compute(cluster.Secret, hubNonce, cluster.NodeName),
                LinkCodec.BinaryFormat), ct).ConfigureAwait(false);
            var auth = reply!.Value.Read<AuthReply>()!;
            if (!LinkProof.Verify(cluster.Secret, nodeNonce, "hub", auth.Proof))
                throw new InvalidOperationException("The hub failed to prove possession of the cluster secret.");

            // Binary frames only to a hub that said it reads them; an older hub never says so.
            link.SendsBinary = auth.LinkFormat >= LinkCodec.BinaryFormat;

            _link = link;
            AdoptHubTimeout(auth.FailureTimeoutMs);
            RenewLease(authSent);
            StartPinger(link, ct);
            ApplyAssignments(auth.Assignments);
            await SubscribeReplicationAsync(link, ct).ConfigureAwait(false);
            KickForwarders();

            // Heartbeat until the link dies. A successful heartbeat renews the lease and applies
            // any assignment changes (reassignments, new shards) the hub decided meanwhile.
            while (!ct.IsCancellationRequested && link.IsAlive)
            {
                await Task.Delay(cluster.HeartbeatIntervalMs, ct).ConfigureAwait(false);
                if (SuspendHeartbeats || StallHeartbeatLoop)
                    continue;
                var loads = CollectLoads();
                var sent = _time.GetTimestamp();
                var beat = await link.RequestAsync("heartbeat", new HeartbeatRequest(loads), ct).ConfigureAwait(false);
                RenewLease(sent);
                var beatReply = beat!.Value.Read<HeartbeatReply>()!;
                ApplyAssignments(beatReply.Assignments);
                ApplyReplicas(beatReply.Replicas ?? []);
                await SubscribeReplicationAsync(link, ct).ConfigureAwait(false);
            }

            await Task.WhenAny(closed.Task, Task.Delay(Timeout.Infinite, ct)).ConfigureAwait(false);
        }
        finally
        {
            if (ReferenceEquals(_link, link))
                _link = null;
            link.Dispose();
        }
    }

    /// <summary>
    /// Renews the lease from <paramref name="sentTimestamp"/>, when the acknowledged request left
    /// this node, not from when its reply arrived (#214). The hub receives a heartbeat no earlier
    /// than it was sent and declares death no earlier than <c>FailureTimeoutMs</c> after receiving
    /// it, on its own monotonic clock; starting the lease at the send, and shortening it by a
    /// tenth for the two clocks' rates to disagree, makes the lease end strictly first. Measured
    /// from the reply instead, it outlived the hub's earliest declaration by the hub's processing
    /// time plus the return trip. Never moves the lease backwards (a late reply to an earlier
    /// request is not news).
    /// </summary>
    private void RenewLease(long sentTimestamp)
    {
        // Two sources renew (the pinger and the heartbeat), so a reply can arrive for a request
        // older than one already counted; only a newer send is a new renewal.
        var previous = Interlocked.Read(ref _lastRenewalSentTimestamp);
        if (sentTimestamp > previous
            && Interlocked.CompareExchange(ref _lastRenewalSentTimestamp, sentTimestamp, previous) == previous
            && previous != 0)
        {
            var gap = _time.GetElapsedTime(previous);
            Metrics.LeaseRenewalGaps.Record(gap);
            _renewalGap.Record(gap.TotalMilliseconds, new KeyValuePair<string, object?>("node", NodeName));
        }

        var timeoutMs = Volatile.Read(ref _leaseTimeoutMs);
        var lengthMs = timeoutMs - timeoutMs / 10;
        var until = sentTimestamp + (long)(lengthMs * (double)_time.TimestampFrequency / 1000d);
        long current;
        do
        {
            current = Interlocked.Read(ref _leaseValidUntilTimestamp);
            if (until <= current)
                return;
        }
        while (Interlocked.CompareExchange(ref _leaseValidUntilTimestamp, until, current) != current);
    }

    /// <summary>
    /// Starts the liveness pinger for one link: a dedicated thread, above normal priority, that
    /// pings the hub every heartbeat interval and renews the lease from each acknowledged ping. The
    /// heartbeat loop runs on the thread pool, which is what starves first under load; phase 30's
    /// soak measured its 250 ms delay firing up to 2.4 s late on a healthy node. The pinger depends
    /// on nothing the pool runs: the link's reader is its own thread, and the hub answers pings on
    /// its reader too. The heartbeat still runs for loads, assignments and replication, and still
    /// renews the lease as well. The thread ends with the link.
    /// </summary>
    private void StartPinger(NodeLink link, CancellationToken ct) =>
        new Thread(() => Ping(link, ct))
        {
            IsBackground = true,
            Name = "MelangeDB liveness pinger",
            Priority = ThreadPriority.AboveNormal,
        }.Start();

    private void Ping(NodeLink link, CancellationToken ct)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct, link.ClosedToken);
        while (!stop.Token.WaitHandle.WaitOne(Cluster.HeartbeatIntervalMs))
        {
            if (SuspendHeartbeats)
                continue;
            var sent = _time.GetTimestamp();
            try
            {
                link.Request("ping", null, Volatile.Read(ref _leaseTimeoutMs));
                RenewLease(sent);
            }
            catch (NodeLinkException)
            {
                // Refused (the hub has declared this node dead: only a heartbeat, whose reply
                // carries the assignments, may bring it back), timed out, or the link is closing.
                // Either way the lease is not renewed, and the next ping tries again.
            }
            catch (Exception exception) when (exception is IOException or ObjectDisposedException or OperationCanceledException)
            {
                return; // The link is gone; the heartbeat loop reconnects and starts a new pinger.
            }
        }
    }

    /// <summary>
    /// The lease is measured against the hub's failure timeout, not this node's setting: the hub
    /// is the one that declares death, and a node configured with a longer timeout held its lease
    /// past the declaration (#214). A mismatch is logged once, since it is a deployment error.
    /// </summary>
    private void AdoptHubTimeout(int hubFailureTimeoutMs)
    {
        if (hubFailureTimeoutMs <= 0)
            return; // A hub that predates the field: keep this node's own setting.
        Volatile.Write(ref _leaseTimeoutMs, hubFailureTimeoutMs);
        if (hubFailureTimeoutMs != Cluster.FailureTimeoutMs && !_timeoutMismatchLogged)
        {
            _timeoutMismatchLogged = true;
            LogFailureTimeoutMismatch(_logger, NodeName, Cluster.FailureTimeoutMs, hubFailureTimeoutMs);
        }
    }

    /// <summary>
    /// The lease watchdog's tick: once the lease has lapsed, park every shard (#216). The lease
    /// is already the write fence (every commit is refused), but an open engine also holds its
    /// shard's log lock, and on storage that honours the lock a partitioned-but-alive owner kept
    /// the new owner from opening the shard indefinitely. Parking closes the engines and releases
    /// the locks; registering again reopens whatever the hub still assigns here, from the log.
    /// </summary>
    private void ParkShardsIfLeaseExpired()
    {
        try
        {
            if (!LeaseValid())
                ParkShards();
        }
        catch (Exception exception)
        {
            LogHubLinkLost(_logger, $"parking shards after the lease lapsed failed: {exception.Message}");
        }
    }

    private void ParkShards()
    {
        List<ShardRuntime> parked;
        lock (_shardsLock)
        {
            if (_shards.Count == 0 && _assigned.Count == 0)
                return;

            // Nothing is assigned to a node without a lease: an open still recovering sees this
            // when it finishes and abandons (1754) instead of publishing into a fenced node.
            _assigned = [];
            parked = [.. _shards.Values];
            foreach (var shard in _shards.Keys)
            {
                if (_forwarders.Remove(shard, out var forwarder))
                    forwarder.Dispose();
                if (_borderPublishers.Remove(shard, out var publisher))
                    publisher.Dispose();
                if (_boundaryMonitors.Remove(shard, out var monitor))
                    monitor.Dispose();
            }

            _shards.Clear();
            _replicaSubscribedFrom = ulong.MaxValue;
        }

        Metrics.LeaseLapsed();
        _leaseLapses.Add(1, new KeyValuePair<string, object?>("node", NodeName));
        foreach (var runtime in parked)
            runtime.Dispose();
        if (parked.Count > 0)
            LogShardsParked(_logger, NodeName, parked.Count);
    }

    /// <summary>
    /// One load sample per owned shard, taken by the heartbeat loop (its only caller, so the
    /// per-shard sampling state on each runtime needs no synchronization of its own).
    /// </summary>
    private ShardLoadDto[] CollectLoads()
    {
        List<ShardRuntime> owned;
        List<ShardKey> draining;
        lock (_shardsLock)
        {
            owned = [.. _shards.Values];

            // A quiesced shard is not in _shards — QuiesceShard removes it in the same lock that
            // sets the drain mark — so the two sets are disjoint and a drain can only be reported
            // from the mark. Without this the hub keeps the last pre-quiesce sample, which says
            // Draining = false, and an empty shard stays on the holding-nothing list for a whole
            // freshness window while it is mid-drain. Read on the owner because IsDrainingLocked
            // expires the mark as it inspects it; the hub cannot age it for us.
            draining = [.. _draining.Keys.ToList().Where(IsDrainingLocked)];
        }

        var loads = new ShardLoadDto[owned.Count + draining.Count];
        for (var i = 0; i < owned.Count; i++)
            loads[i] = owned[i].SampleLoad();

        // A drained shard's engine is closed, so there is nothing to measure and the zeroes are
        // honest rather than a stand-in: the flag is the payload, and the hub reads no numbers off
        // a sample that carries it.
        for (var i = 0; i < draining.Count; i++)
            loads[owned.Count + i] = new ShardLoadDto(draining[i].Value, 0, 0, 0, 0, 0, Draining: true);
        return loads;
    }

    /// <summary>
    /// Diffs the hub's assignment list against the open shard runtimes: new shards open (their
    /// engines recover from the shard's own log — reassignment is recovery), lost shards close,
    /// and fencing tokens refresh. Opening happens here, on the node, because the shard's log
    /// directory is the shard — whoever the hub names simply opens it.
    /// </summary>
    private void ApplyAssignments(ShardAssignmentDto[] assignments)
    {
        var assigned = assignments.Select(static a => a.ToAssignment()).ToDictionary(static a => a.Shard);
        List<ShardRuntime> closed = [];
        var shardSetChanged = false;
        lock (_shardsLock)
        {
            foreach (var (shard, runtime) in _shards.ToList())
            {
                if (assigned.TryGetValue(shard, out var assignment))
                {
                    runtime.FencingToken = assignment.FencingToken;
                }
                else
                {
                    _shards.Remove(shard);
                    if (_forwarders.Remove(shard, out var forwarder))
                        forwarder.Dispose();
                    if (_borderPublishers.Remove(shard, out var publisher))
                        publisher.Dispose();
                    if (_boundaryMonitors.Remove(shard, out var monitor))
                        monitor.Dispose();
                    closed.Add(runtime);
                    shardSetChanged = true;
                    LogShardReleased(_logger, shard.Value, NodeName);
                }
            }

            // Draining marks whose shard the hub has since moved elsewhere (or dropped) are done;
            // the ones still naming this node are checked for expiry below.
            foreach (var draining in _draining.Keys.ToList())
            {
                if (!assigned.ContainsKey(draining))
                    _draining.Remove(draining);
            }

            // A reap mark outlives its assignment on purpose — the hub removes the membership row
            // between the close and the delete, and the mark is what keeps the directory closed
            // over that window. Only expiry clears it, so sweep for that here; nothing else ever
            // asks about a shard the hub has stopped naming.
            foreach (var reaping in _reaping.Keys.ToList())
                IsReapingLocked(reaping);

            // A reply that arrived after the lease it renewed had already run out (the lease runs
            // from the request's send) leaves this node fenced: it opens nothing, or the watchdog
            // would park what it opened and the two would alternate. The next heartbeat retries.
            var leaseValid = LeaseValid();
            _assigned = leaseValid ? assigned : [];
            foreach (var assignment in leaseValid ? assigned.Values : Enumerable.Empty<ShardAssignment>())
            {
                if (_shards.ContainsKey(assignment.Shard) || _opening.ContainsKey(assignment.Shard))
                    continue;
                if (IsDrainingLocked(assignment.Shard))
                    continue;

                // Sealed and closed for a reap the hub has not finished. Reopening now would
                // either race the delete or — if the hub got as far as removing the membership row
                // and this is a stale assignment list — mint ids under a retired originator.
                if (IsReapingLocked(assignment.Shard))
                    continue;
                var shard = assignment.Shard;
                _opening[shard] = Task.Run(() => OpenShard(shard));
            }

            // A shard-set change moves the replication low-water mark; force a re-subscribe.
            if (shardSetChanged)
                _replicaSubscribedFrom = ulong.MaxValue;
        }

        foreach (var runtime in closed)
            runtime.Dispose();
    }

    /// <summary>
    /// Opens one assigned shard: recovery runs here, outside every lock and off the heartbeat
    /// path, and only the finished runtime is published into the shard set — under the same checks
    /// <see cref="ApplyAssignments"/> applies, re-read now, because the hub may have moved the
    /// shard, a drain or reap may have started, or this node may be stopping while recovery ran.
    /// A failed open (the previous owner still holding the log, a damaged directory) is logged and
    /// left for the next heartbeat to retry; it no longer takes the hub link, and with it the
    /// lease of every other shard on this node, down with it.
    /// </summary>
    private void OpenShard(ShardKey shard)
    {
        ShardRuntime? runtime = null;
        try
        {
            ShardAssignment assignment;
            lock (_shardsLock)
            {
                if (_stopped.IsCancellationRequested || !_assigned.TryGetValue(shard, out assignment!))
                    return;
            }

            BeforeOpen?.Invoke(shard);
            var started = _time.GetTimestamp();
            var directory = Path.Combine(Cluster.ShardDataPath, $"shard-{shard.Value}");
            Directory.CreateDirectory(directory);
            runtime = new ShardRuntime(
                shard,
                assignment.FencingToken,
                assignment.Originator,
                directory,
                Path.Combine(LocalScratch, $"shard-{shard.Value}"),
                NodeName,
                _services.GetRequiredService<SchemaRegistry>(),
                _services.GetRequiredService<ReducerRegistry>(),
                _services,
                _options,
                LeaseValid,
                _loggerFactory,
                _time,
                _authenticator,
                _sessions,
                () => TakeReplica(shard));
            CatchUpReplicated(shard, runtime);
            var openMs = _time.GetElapsedTime(started).TotalMilliseconds;

            lock (_shardsLock)
            {
                if (_stopped.IsCancellationRequested
                    || !_assigned.TryGetValue(shard, out var current)
                    || IsDrainingLocked(shard)
                    || IsReapingLocked(shard))
                {
                    LogShardOpenAbandoned(_logger, shard.Value, NodeName);
                    return; // runtime disposed in finally
                }

                // The token the hub holds now, not the one recovery started under: a heartbeat
                // that bumped it while recovery ran must not be lost.
                runtime.FencingToken = current.FencingToken;
                PublishLocked(shard, runtime);
                if (runtime.Engine.WarmStartedFrom is { } from)
                    LogShardPromoted(_logger, shard.Value, NodeName, openMs, from, runtime.Engine.Log.HeadLsn);
                else
                    LogShardOpened(_logger, shard.Value, NodeName, openMs, runtime.Engine.Log.HeadLsn);
                runtime = null; // Published: the shard set owns it now.
            }

            KickForwarders();
        }
        catch (Exception exception)
        {
            LogShardOpenFailed(_logger, shard.Value, NodeName, exception.Message);
        }
        finally
        {
            runtime?.Dispose();
            lock (_shardsLock)
            {
                _opening.Remove(shard);
            }
        }
    }

    /// <summary>
    /// Brings an opened, not yet published shard's Replicated tables up to the hub's head before
    /// it serves anything (#283). The pushed stream only reaches shards in service, and a newly
    /// published shard waited a heartbeat for its re-subscribe and then the whole history from
    /// its cursor, so its first calls read empty or partly applied Replicated tables. Pulling here
    /// means that once the shard is in the shard set (and so in the hub's load view), it holds
    /// every Replicated row the hub had committed when the open asked. Later writes reach it by
    /// the stream, one hop behind, as on every other shard.
    /// <para>
    /// A failure throws, and the open fails and is retried on the next heartbeat like a failed
    /// recovery. A hub too old to answer is logged once and the shard opens as before. A shard
    /// unassigned while this runs stops pulling; the publish check after it abandons the open.
    /// </para>
    /// </summary>
    private void CatchUpReplicated(ShardKey shard, ShardRuntime runtime)
    {
        if (!runtime.Engine.Schema.Tables.Any(static t => t.Placement == Placement.Replicated))
            return;
        var link = _link
            ?? throw new InvalidOperationException($"No hub link to catch {shard}'s Replicated tables up from.");
        var started = _time.GetTimestamp();
        var from = ReadReplicaCursor(runtime.Directory);
        var cursor = from;
        ulong? target = null;
        var records = 0;
        while (true)
        {
            ReplicaReadReply reply;
            try
            {
                reply = link.Request("replica-read", new ReplicaRead(cursor), ReplicaReadTimeoutMs)!.Value
                    .Read<ReplicaReadReply>()!;
            }
            catch (NodeLinkException failure)
                when (failure.IsPeerError && failure.Message.StartsWith("Unknown node-link message", StringComparison.Ordinal))
            {
                if (!_replicaReadUnsupportedLogged)
                {
                    _replicaReadUnsupportedLogged = true;
                    LogReplicaCatchUpUnsupported(_logger, NodeName);
                }

                return;
            }

            target ??= reply.HeadLsn;
            BeforeReplicaSlice?.Invoke();
            if (reply.Reset is { } reset)
                ApplyReplicaReset(runtime, cursor, reset);
            var applied = ApplyReplicaRecords(runtime, Math.Max(cursor, reply.Reset?.Lsn ?? 0), reply.Lsns, reply.Records);
            records += reply.Lsns.Count(lsn => lsn > cursor);

            // The scan position, not only the last Replicated record: a cursor at the head is what
            // makes the stream's next re-subscribe start there, rather than re-reading every record
            // since the last one that carried a Replicated op.
            var next = Math.Max(applied, reply.ScannedTo);
            if (next > cursor)
                WriteReplicaCursor(runtime.Directory, next);
            if (next >= target)
                break;
            if (next == cursor)
                throw new InvalidOperationException(
                    $"The hub's Replicated stream made no progress past LSN {cursor} toward {target} while opening {shard}.");
            cursor = next;

            lock (_shardsLock)
            {
                if (_stopped.IsCancellationRequested || !_assigned.ContainsKey(shard))
                    return;
            }
        }

        LogShardReplicaCaughtUp(_logger, shard.Value, NodeName, records, from, target!.Value, _time.GetElapsedTime(started).TotalMilliseconds);
    }

    /// <summary>One <c>replica-read</c>'s timeout: a reset carries every Replicated row, so it gets longer than an ordinary request.</summary>
    private const int ReplicaReadTimeoutMs = 60_000;

    private bool _replicaReadUnsupportedLogged;

    /// <summary>Applies an assignment list and completes when every shard it put into opening has finished (opened, failed, or abandoned).</summary>
    private async Task<object?> ApplyAndAwaitOpensAsync(ShardAssignmentDto[] assignments)
    {
        ApplyAssignments(assignments);
        Task[] opening;
        lock (_shardsLock)
        {
            opening = [.. assignments
                .Select(static a => new ShardKey(a.Shard))
                .Select(shard => _opening.GetValueOrDefault(shard))
                .OfType<Task>()];
        }

        await Task.WhenAll(opening).ConfigureAwait(false);
        return null;
    }

    /// <summary>Puts an opened runtime into service: the shard set, its forwarder, border publisher, and boundary monitor. Caller holds <see cref="_shardsLock"/>.</summary>
    private void PublishLocked(ShardKey shard, ShardRuntime runtime)
    {
        _shards[shard] = runtime;
        var forwarder = new EventForwarder(
            runtime.Engine,
            shard.Value,
            Path.Combine(runtime.Directory, "events.cursor"),
            () => _link,
            _logger);
        _forwarders[shard] = forwarder;
        forwarder.Start();
        if (_strategy is { } strategy)
        {
            var publisher = new BorderPublisher(
                runtime.Engine, shard, strategy, runtime.BorrowedOwnerOf, () => _link, _logger, BorderResetPartBytes);
            _borderPublishers[shard] = publisher;
            publisher.Start();

            // Seamless (walking-triggered) handoff needs a strategy that can judge
            // boundaries and an application that names its migratable entities; with
            // both, the origin decides — its committed rows are the only trusted position.
            if (strategy is IBoundaryStrategy && _services.GetService<IMigrationAnchors>() is { } anchors)
            {
                var monitor = new BoundaryMonitor(
                    runtime.Engine, shard, strategy, anchors,
                    () => _link, () => runtime.FencingToken, runtime.BorrowedOwnerOf, runtime.IsFrozen,
                    _options, _time, Metrics);
                _boundaryMonitors[shard] = monitor;
                monitor.Start();
            }
        }

        // A shard-set change moves the replication low-water mark; the next heartbeat re-subscribes.
        _replicaSubscribedFrom = ulong.MaxValue;
    }

    /// <summary>Whether the shard is drain-quiesced, clearing an expired mark on the way past. Caller holds <see cref="_shardsLock"/>.</summary>
    private bool IsDrainingLocked(ShardKey shard)
    {
        if (!_draining.TryGetValue(shard, out var since))
            return false;
        if (_time.GetUtcNow() - since <= TimeSpan.FromMilliseconds(2L * Math.Max(1, Cluster.FailureTimeoutMs)))
            return true;
        _draining.Remove(shard);
        LogDrainMarkExpired(_logger, shard.Value, NodeName);
        return false;
    }

    /// <summary>
    /// Whether the shard is sealed and closed awaiting its reap's delete, clearing an expired mark
    /// on the way past. Caller holds <see cref="_shardsLock"/>. Expiry is the self-healing bound
    /// for a hub that died between the close and the membership removal: the directory was never
    /// touched, so the shard simply reopens.
    /// </summary>
    private bool IsReapingLocked(ShardKey shard)
    {
        if (!_reaping.TryGetValue(shard, out var pending))
            return false;
        if (_time.GetUtcNow() - pending.At <= TimeSpan.FromMilliseconds(2L * Math.Max(1, Cluster.FailureTimeoutMs)))
            return true;
        _reaping.Remove(shard);
        LogReapMarkExpired(_logger, shard.Value, NodeName);
        return false;
    }

    /// <summary>
    /// The node half of a planned drain: verify the term, mark the shard draining (so this node's
    /// own heartbeat cannot reopen it while the hub is between quiesce and reassign), close it
    /// exactly the way a reassignment closes it, take a fresh snapshot so the destination's
    /// recovery tail is short, and report the head the destination will recover to. The snapshot
    /// and close run outside the shard-set lock — the node's other shards must keep serving.
    /// </summary>
    /// <summary>
    /// Internal rather than private for the drain-reporting test: a quiesced shard leaves
    /// <c>_shards</c> here, which is precisely why its drain mark has to be reported from
    /// <c>_draining</c> and why the omission was invisible until something drove this directly.
    /// </summary>
    internal ShardDrainReply QuiesceShard(ShardDrain drain)
    {
        var shard = new ShardKey(drain.Shard);
        ShardRuntime runtime;
        lock (_shardsLock)
        {
            runtime = RequireShard(shard, drain.FencingToken);
            _draining[shard] = _time.GetUtcNow();
            _shards.Remove(shard);
            if (_forwarders.Remove(shard, out var forwarder))
                forwarder.Dispose();
            if (_borderPublishers.Remove(shard, out var publisher))
                publisher.Dispose();
            if (_boundaryMonitors.Remove(shard, out var monitor))
                monitor.Dispose();
            _replicaSubscribedFrom = ulong.MaxValue;
        }

        runtime.Engine.TakeSnapshot();
        var head = runtime.Engine.Log.HeadLsn;
        runtime.Dispose();
        LogShardQuiesced(_logger, shard.Value, NodeName, head);
        return new ShardDrainReply(head);
    }

    /// <summary>
    /// Truncation floors that do not stand in the way of removing a shard, each for its own
    /// reason: the snapshot is the healthy ceiling, the sidecar floor never pins, the hot store's
    /// applier is a projection of the very log being deleted, and the resume window only keeps
    /// records for clients reconnecting to a shard that is about to stop existing. Every other
    /// floor — a streaming backup, an unsettled handoff, a lagging Postgres applier, an
    /// unforwarded cluster event, or a name this build does not know — is an outstanding claim.
    /// </summary>
    private static readonly HashSet<string> ReapIgnorableFloors = new(StringComparer.Ordinal)
    {
        TruncationFloorNames.Snapshot,
        TruncationFloorNames.ShardSidecar,
        TruncationFloorNames.ResumeWindow,
        TruncationFloorNames.ClusterEvents,
        "hot-store",
    };

    /// <summary>
    /// The first half of removing a shard this node owns: verify it holds nothing of its own and
    /// nothing pins its log, then seal it against further writes and close it. The directory is
    /// left alone — <see cref="DeleteReapedShard"/> removes it once the hub has retired the
    /// membership row. Refusing is an ordinary answer: the hub asks from a sampled view and only
    /// the owner can settle it.
    /// <para>
    /// Emptiness is decided and the engine sealed in <em>one hold of the engine write lock</em>
    /// (<see cref="ShardRuntime.SealIfEmpty"/>), because nothing weaker separates "holds nothing"
    /// from "will still hold nothing when the directory goes". <c>_shardsLock</c> cannot do it:
    /// it guards the shard map, and a reducer call resolves its shard under it and then commits
    /// with it released. Nor can the heartbeat's row gauge, which is throttled to ten seconds.
    /// </para>
    /// <para>
    /// A snapshot is forced first so the floors are evaluated — they are only legal to read
    /// inside a truncation decision — which on a shard with no rows costs almost nothing.
    /// </para>
    /// </summary>
    internal ShardReapReply ReapShard(ShardReap reap)
    {
        var shard = new ShardKey(reap.Shard);

        // A repeat of a reap this node already sealed and closed is answered, not redone: the hub
        // retries after a lost reply, and there is nothing left here to inspect or refuse.
        lock (_shardsLock)
        {
            if (IsReapingLocked(shard))
                return new ShardReapReply(true, null);
        }

        // Ship anything this shard has published before its log is deleted, unsealed and before
        // anything else, so that the backlog leaves while the shard is still serving its callers
        // normally. Only the tail that lands during the decision is shipped under the seal.
        EventForwarder? forwarder;
        lock (_shardsLock)
        {
            _forwarders.TryGetValue(shard, out forwarder);
        }

        if (forwarder is not null && TryGetShard(shard) is { } pending
            && !ShipEvents(forwarder, pending, TimeSpan.FromSeconds(5)))
        {
            return new ShardReapReply(
                false,
                $"{shard} has events forwarded only to LSN {forwarder.Cursor} of {pending.Engine.Log.DurableLsn}; "
                + "they would be deleted with the shard. Check the hub link and retry.");
        }

        ShardRuntime runtime;
        string directory;
        lock (_shardsLock)
        {
            if (IsDrainingLocked(shard))
                return new ShardReapReply(false, $"{shard} is mid-drain; a reap and a drain cannot both own the outcome.");
            runtime = RequireShard(shard, reap.FencingToken);
            directory = runtime.Directory;

            // Forces a truncation decision, which is the only place floors may be evaluated.
            runtime.Engine.TakeSnapshot();
            if (runtime.Engine.TruncationFloors is not { } floors)
            {
                return new ShardReapReply(
                    false,
                    $"{shard} has no truncation-floor reading, so nothing can be said about what is holding its log. "
                    + "Snapshots:TruncateLog is off; a reap will not guess.");
            }

            // Judged by which floors are *present*, never by where they sit. A floor whose provider
            // has nothing outstanding returns null and is omitted from the report entirely, so
            // presence is the signal — and position is not: PinTruncation pins at the current base,
            // so a streaming backup and a cluster-event cursor that has never forwarded anything
            // report the very same LSN with opposite meanings.
            //
            // Cast to the nullable form before FirstOrDefault: TruncationFloor is a readonly record
            // struct, so the plain overload yields default(TruncationFloor) — name "", LSN 0 — and
            // `is { }` is true for every value of a non-nullable struct, which reads as a phantom
            // floor and refuses every reap.
            if (floors.Floors
                    .Where(f => !ReapIgnorableFloors.Contains(f.Name))
                    .Cast<TruncationFloor?>()
                    .FirstOrDefault()
                is { } claim)
            {
                return new ShardReapReply(
                    false,
                    $"{shard}'s log is held by '{claim.Name}' at LSN {claim.Lsn}; something still needs records "
                    + "this would delete.");
            }

            // Live, not sampled, and the seal engages in the same hold of the engine write lock
            // that finds the zero — so a commit is either counted here or refused from here on.
            if (runtime.SealIfEmpty() is var rows and > 0)
                return new ShardReapReply(false, $"{shard} still owns {rows} row(s) of its own.");
        }

        // The tail of the shipping above, now that the seal has stopped the watermark moving. The
        // first pass raced anything still committing, and a write touching no partitioned table
        // leaves the row count at zero while still publishing an event whose record would go with
        // the directory. It is a *second* pass rather than the only one because the seal refuses
        // writes while it is engaged: doing the whole backlog under it would fail a shard's live
        // callers for as long as the hub took, where this waits only for what landed mid-decision.
        // Outside _shardsLock, because sleeping there holds every other shard on this node still.
        if (forwarder is not null && !ShipEvents(forwarder, runtime, TimeSpan.FromSeconds(1)))
        {
            runtime.Unseal();
            return new ShardReapReply(
                false,
                $"{shard} published events while the reap was deciding — forwarded to LSN {forwarder.Cursor} of "
                + $"{runtime.Engine.Log.DurableLsn}. Retry once the hub has taken them.");
        }

        lock (_shardsLock)
        {
            // Reassignment could have closed the shard under us while the tail shipped. There is
            // then nothing here to reap and nothing to unseal — that engine is already gone.
            if (!_shards.TryGetValue(shard, out var current) || !ReferenceEquals(current, runtime))
            {
                return new ShardReapReply(
                    false,
                    $"{shard} stopped being this node's while the reap was deciding; the hub reassigned it. "
                    + "Nothing was removed.");
            }

            _draining.Remove(shard);
            _shards.Remove(shard);
            if (_forwarders.Remove(shard, out var shipped))
                shipped.Dispose();
            if (_borderPublishers.Remove(shard, out var publisher))
                publisher.Dispose();
            if (_boundaryMonitors.Remove(shard, out var monitor))
                monitor.Dispose();
            runtime.Dispose();

            // Closed but not destroyed, and marked so this node's own heartbeat cannot reopen it
            // while the hub retires the membership row. Everything up to here is undoable.
            _reaping[shard] = (_time.GetUtcNow(), directory);
        }

        LogShardSealedForReap(_logger, shard.Value, NodeName, directory);
        return new ShardReapReply(true, null);
    }

    /// <summary>
    /// Kicks the shard's event forwarder and waits for it to reach the durable watermark, up to
    /// <paramref name="within"/>. False means it did not get there.
    /// <para>
    /// Kicked and waited on rather than read off a truncation floor, because the cluster-event
    /// cursor only advances when the pump runs and the pump only wakes on an event-bearing commit:
    /// a resting cursor means "not asked lately", not "nothing to send". A cursor that will not
    /// reach the watermark means the hub is not taking the events, and they would be deleted with
    /// the shard.
    /// </para>
    /// </summary>
    private bool ShipEvents(EventForwarder forwarder, ShardRuntime runtime, TimeSpan within)
    {
        forwarder.Kick();
        var deadline = _time.GetUtcNow() + within;
        while (forwarder.Cursor < runtime.Engine.Log.DurableLsn && _time.GetUtcNow() < deadline)
            Thread.Sleep(25);

        return forwarder.Cursor >= runtime.Engine.Log.DurableLsn;
    }

    /// <summary>
    /// The second half of a reap: delete the directory of a shard this node already sealed and
    /// closed. Split from <see cref="ReapShard"/> so the destruction happens <em>after</em> the
    /// hub has removed the membership row, never before — a shard whose directory is gone but
    /// whose row survives is reopened by <see cref="ApplyAssignments"/> as a fresh empty engine
    /// under the very originator it just retired, and ids minted under that originator are alive
    /// elsewhere. Deleting last inverts the failure: a lost reply strands a directory nobody owns,
    /// which is garbage rather than a collision, and is logged as such.
    /// <para>
    /// Idempotent, and refuses rather than guesses: a shard that reopened because the mark expired
    /// is serving again, and its directory is not a leftover.
    /// </para>
    /// </summary>
    internal ShardReapReply DeleteReapedShard(ShardReapDelete request)
    {
        var shard = new ShardKey(request.Shard);
        string directory;
        lock (_shardsLock)
        {
            if (_shards.ContainsKey(shard))
            {
                return new ShardReapReply(
                    false,
                    $"{shard} is open and serving on '{NodeName}'; its reap did not complete and its directory is "
                    + "not a leftover. Reap it again from the start.");
            }

            // The path is derived, not remembered, so a node that restarted between the two halves
            // still finishes the job rather than reporting success over a directory it left behind.
            directory = _reaping.TryGetValue(shard, out var pending)
                ? pending.Directory
                : Path.Combine(Cluster.ShardDataPath, $"shard-{shard.Value}");

            // Re-marked with a fresh timestamp: the delete runs outside the lock, and an assignment
            // arriving mid-delete must not open an engine over a directory being emptied.
            _reaping[shard] = (_time.GetUtcNow(), directory);
        }

        try
        {
            // Outside the lock: a slow delete must not hold every other shard on this node still.
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
        finally
        {
            lock (_shardsLock)
            {
                _reaping.Remove(shard);
            }
        }

        LogShardReaped(_logger, shard.Value, NodeName, directory);
        return new ShardReapReply(true, null);
    }

    private void KickForwarders()
    {
        lock (_shardsLock)
        {
            foreach (var forwarder in _forwarders.Values)
                forwarder.Kick();
        }
    }

    /// <summary>
    /// Subscribes (or re-subscribes) the replication stream from the minimum of the shards'
    /// persisted cursors. Batches apply per shard engine with a per-shard cursor, so a shard
    /// that moved here carries its replication progress with it in its own directory.
    /// </summary>
    private async Task SubscribeReplicationAsync(NodeLink link, CancellationToken ct)
    {
        ulong from;
        lock (_shardsLock)
        {
            if (_shards.Count == 0)
                return;
            from = _shards.Values.Min(r => ReadReplicaCursor(r.Directory));
            if (_replicaSubscribedFrom <= from)
                return;
            _replicaSubscribedFrom = from;
        }

        await link.RequestAsync("replica-subscribe", new ReplicaSubscribe(from), ct).ConfigureAwait(false);
    }

    private static ulong ReadReplicaCursor(string shardDirectory)
    {
        var path = Path.Combine(shardDirectory, "replica.cursor");
        try
        {
            if (File.Exists(path) && ulong.TryParse(File.ReadAllText(path), out var lsn))
                return lsn;
        }
        catch (IOException)
        {
        }

        return 0;
    }

    private static void WriteReplicaCursor(string shardDirectory, ulong lsn) =>
        File.WriteAllText(Path.Combine(shardDirectory, "replica.cursor"), lsn.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private Task<object?> HandleAsync(NodeLink link, string type, LinkBody? body, TaskCompletionSource<string> challenge)
    {
        switch (type)
        {
            case "challenge":
                challenge.TrySetResult(body!.Value.Json.GetProperty("ServerNonce").GetString()!);
                return Task.FromResult<object?>(null);
            case "replica-batch":
                ApplyReplicaBatch(body!.Value.Read<ReplicaBatch>()!);
                return Task.FromResult<object?>(null);
            case "replica-reset":
                ApplyReplicaReset(body!.Value.Read<ReplicaReset>()!);
                return Task.FromResult<object?>(null);
            case "handoff-freeze":
            {
                var freeze = body!.Value.Read<HandoffFreeze>()!;
                var runtime = RequireShard(new ShardKey(freeze.FromShard), freeze.FencingToken);
                var rows = runtime.FreezeAndCollect(
                    freeze.HandoffId, new Identity(Convert.FromHexString(freeze.PlayerHex)), new ShardKey(freeze.ToShard));
                return Task.FromResult<object?>(new HandoffFrozenRows(rows));
            }

            case "handoff-import":
            {
                var import = body!.Value.Read<HandoffImport>()!;
                var runtime = RequireShard(new ShardKey(import.ToShard), import.FencingToken);
                runtime.Import(import.HandoffId, import.PlayerHex, import.FromShard, import.Rows);
                return Task.FromResult<object?>(null);
            }

            case "handoff-release":
            {
                var release = body!.Value.Read<HandoffRelease>()!;
                RequireShard(new ShardKey(release.FromShard), release.FencingToken).Release(release.HandoffId);
                return Task.FromResult<object?>(null);
            }

            case "handoff-abort":
            {
                var abort = body!.Value.Read<HandoffRelease>()!;
                RequireShard(new ShardKey(abort.FromShard), abort.FencingToken).Abort(abort.HandoffId);
                return Task.FromResult<object?>(null);
            }

            case "border-subscribe-owner":
            {
                var subscribe = body!.Value.Read<BorderSubscribe>()!;
                BorderPublisher? publisher;
                lock (_shardsLock)
                {
                    publisher = _borderPublishers.GetValueOrDefault(new ShardKey(subscribe.OwnerShard));
                }

                if (publisher is null)
                    throw new InvalidOperationException($"This node does not own shard {subscribe.OwnerShard} (or runs no shard strategy).");
                publisher.Subscribe(subscribe);
                return Task.FromResult<object?>(null);
            }

            case "border-apply":
            {
                var batch = body!.Value.Read<BorderBatch>()!;
                var observer = TryGetShard(new ShardKey(batch.ObserverShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {batch.ObserverShard}.");
                observer.ApplyBorder(batch);
                return Task.FromResult<object?>(null);
            }

            case "border-reset-apply":
            {
                var reset = body!.Value.Read<BorderReset>()!;
                var observer = TryGetShard(new ShardKey(reset.ObserverShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {reset.ObserverShard}.");
                observer.ApplyBorderReset(reset);
                return Task.FromResult<object?>(null);
            }

            case "border-reset-part-apply":
            {
                var part = body!.Value.Read<BorderResetPart>()!;
                var observer = TryGetShard(new ShardKey(part.Reset.ObserverShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {part.Reset.ObserverShard}.");
                observer.ApplyBorderResetPart(part);
                return Task.FromResult<object?>(null);
            }

            case "shard-drain":
                return Task.FromResult<object?>(QuiesceShard(body!.Value.Read<ShardDrain>()!));
            case "shard-reap":
                return Task.FromResult<object?>(ReapShard(body!.Value.Read<ShardReap>()!));
            case "shard-reap-delete":
                return Task.FromResult<object?>(DeleteReapedShard(body!.Value.Read<ShardReapDelete>()!));
            case "shard-drain-abort":
            {
                var abort = body!.Value.Read<ShardDrainAbort>()!;
                lock (_shardsLock)
                {
                    _draining.Remove(new ShardKey(abort.Shard));
                }

                LogDrainAborted(_logger, abort.Shard, NodeName);
                return Task.FromResult<object?>(null);
            }

            case "assignments-apply":
                // The drain's fast path: the hub pushes the destination's assignments instead of
                // waiting out the heartbeat clock, and the reply doubles as "the shard is open".
                // Opens run off the heartbeat path now (#198), so the reply waits for the ones this
                // push started — on this request's own task, which the link dispatches apart from
                // its read loop, so heartbeat replies keep flowing while it waits. The hub bounds
                // the wait with its step timeout and falls back to the heartbeat clock past it.
                return ApplyAndAwaitOpensAsync(body!.Value.Read<AssignmentsApply>()!.Assignments);
            case "shard-execute":
            {
                var execute = body!.Value.Read<ShardExecute>()!;
                ShardRuntime runtime;
                try
                {
                    runtime = RequireShard(new ShardKey(execute.Shard), execute.FencingToken);
                }
                catch (InvalidOperationException exception) when (execute.ClientOriginated)
                {
                    // The shard moved, or this owner's term ended, since the hub resolved it: the
                    // call did nothing and a retry resolves afresh. Said as transient so a routed
                    // client call is told to retry rather than that the server failed.
                    throw new TransientRejectionException(exception.Message);
                }

                var lsn = runtime.ReducerHost.Call(
                    execute.Reducer,
                    new Identity(Convert.FromHexString(execute.CallerHex)),
                    ConnectionId.None,
                    Convert.FromBase64String(execute.ArgsB64),
                    source: new CallSource(execute.ClientOriginated, execute.CallerIsGuest),
                    claims: execute.Claims is { Count: > 0 } claims ? CallerClaims.From(claims) : CallerClaims.Empty);
                return Task.FromResult<object?>(new ShardExecuteReply(lsn));
            }

            case "shard-bulk":
            {
                var bulk = body!.Value.Read<ShardBulk>()!;
                var runtime = RequireShard(new ShardKey(bulk.Shard), bulk.FencingToken);
                return Task.FromResult<object?>(ApplyBulkGroup(
                    runtime,
                    new ShardKey(bulk.Shard),
                    new Identity(Convert.FromHexString(bulk.CallerHex)),
                    bulk.Rows));
            }

            case "handoff-query-owner":
            {
                var query = body!.Value.Read<HandoffQuery>()!;
                var runtime = TryGetShard(new ShardKey(query.ToShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {query.ToShard}.");
                return Task.FromResult<object?>(new HandoffQueryReply(runtime.WasImported(query.HandoffId)));
            }

            case "handoff-freeze-query-owner":
            {
                var query = body!.Value.Read<HandoffFreezeQuery>()!;
                var runtime = TryGetShard(new ShardKey(query.FromShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {query.FromShard}.");
                return Task.FromResult<object?>(new HandoffFreezeQueryReply(runtime.IsFreezePending(query.HandoffId)));
            }

            default:
                throw new InvalidOperationException($"Unknown node-link message '{type}'.");
        }
    }

    /// <summary>
    /// Takes one group of a fanned-out bulk batch: re-resolve every row, then write the group as
    /// one transaction on this shard's engine.
    ///
    /// <para><b>The re-resolution is the correctness of the whole feature, and it is not
    /// redundant.</b> It is tempting to think the existing shard-span check already covers a
    /// misrouted row — it does include the executing shard in the set it compares, so a row
    /// belonging elsewhere would widen the span and fail. Two things make that not enough.
    /// <c>Cluster:ShardSpanCheck</c> defaults to <c>DebugOnly</c>, so in a Release cluster — the
    /// only kind that has a bake to run — the check is off. And a spatial strategy's
    /// <c>MayCommit</c> deliberately admits the seam, so a row a border band's depth across the
    /// line is admitted by design. So the guard that would catch this is absent exactly where it
    /// would be needed. One <c>ShardForRow</c> per row, unconditionally, is the price of the hub
    /// being allowed to decide destinations at all.</para>
    ///
    /// <para>Refused whole and before any write, naming the row: a bake that half-lands rows into
    /// the wrong engine is worse than one that lands nothing, because nothing is a re-post and
    /// half is an investigation.</para>
    /// </summary>
    private ShardBulkReply ApplyBulkGroup(ShardRuntime runtime, ShardKey shard, Identity caller, BulkRowDto[] rows)
    {
        var schema = runtime.Engine.Schema;
        var strategy = _strategy
            ?? throw new InvalidOperationException(
                $"Node '{NodeName}' has no IShardStrategy registered; a forwarded bulk group cannot be verified.");

        List<BulkRow> staged = new(rows.Length);
        foreach (var dto in rows)
        {
            if (!schema.TryGetByName(dto.Table, out var table))
                throw new InvalidOperationException($"No table named '{dto.Table}' is registered on node '{NodeName}'.");

            var resolved = strategy.ShardForRow(table.Id, table.ToRowRef(dto.Row));
            if (resolved != shard)
            {
                throw new InvalidOperationException(
                    $"A forwarded bulk row of '{table.Name}' resolves to {resolved} on node '{NodeName}', but the hub "
                    + $"routed it to {shard}. The hub's shard map and this node's strategy disagree; nothing in this "
                    + "group was written.");
            }

            staged.Add(new BulkRow(table.Name, ColumnsOf(table, dto)));
        }

        var record = runtime.Engine.BulkInsert(caller, staged);
        return new ShardBulkReply(record?.Lsn ?? 0, staged.Count);
    }

    /// <summary>
    /// Rebuilds the caller's column dictionary from the routing preimage and the list of columns
    /// they actually supplied. Only the supplied ones are put back, which is what lets an
    /// <c>[AutoInc]</c> column the caller omitted be allocated here — by this shard, under its own
    /// originator prefix — rather than arriving as the preimage's zero and colliding with every
    /// other shard's zero.
    /// </summary>
    private static Dictionary<string, object?> ColumnsOf(TableSchema table, BulkRowDto dto)
    {
        var boxed = RowSerializer.Deserialize(table, dto.Row);
        var columns = new Dictionary<string, object?>(dto.Supplied.Length, StringComparer.Ordinal);
        foreach (var name in dto.Supplied)
            columns[name] = table.Column(name).GetValue(boxed);
        return columns;
    }

    /// <summary>
    /// Resolves a shard this node must own under a current fencing token. A stale token is a
    /// message from a previous ownership term — the fencing guarantee, enforced at every
    /// saga step.
    /// </summary>
    private ShardRuntime RequireShard(ShardKey shard, long fencingToken)
    {
        var runtime = TryGetShard(shard)
            ?? throw new InvalidOperationException($"Node '{NodeName}' does not own {shard}.");
        if (runtime.FencingToken != fencingToken)
        {
            throw new InvalidOperationException(
                $"Fencing token mismatch for {shard}: message carries {fencingToken}, current term is " +
                $"{runtime.FencingToken}. The sender's view of ownership is stale.");
        }

        return runtime;
    }

    private void ApplyReplicaBatch(ReplicaBatch batch)
    {
        List<(ShardRuntime Runtime, ulong Cursor)> shards;
        lock (_shardsLock)
        {
            shards = [.. _shards.Values.Select(r => (r, ReadReplicaCursor(r.Directory)))];
        }

        BeforeReplicaSlice?.Invoke();
        foreach (var (runtime, cursor) in shards)
        {
            // A shard that has not applied everything up to the slice's predecessor would take
            // the slice and advance its cursor over records it never saw. A shard published while
            // a stream from a higher low-water mark was in flight did exactly that, and kept a
            // permanent hole in its Replicated tables. Skipping is safe: publishing the shard reset
            // the subscription mark, so the next heartbeat re-subscribes from this shard's own
            // cursor and the stream comes back for it.
            if (cursor < batch.PrevLsn)
                continue;
            var applied = ApplyReplicaRecords(runtime, cursor, batch.Lsns, batch.Records);
            if (applied > cursor)
                WriteReplicaCursor(runtime.Directory, applied);
        }
    }

    /// <summary>
    /// Applies the records after <paramref name="cursor"/> to one shard engine and returns the
    /// last LSN applied (<paramref name="cursor"/> if none was new). The caller persists it.
    /// </summary>
    private static ulong ApplyReplicaRecords(ShardRuntime runtime, ulong cursor, ulong[] lsns, WireOp[][] records)
    {
        var applied = cursor;
        for (var i = 0; i < lsns.Length; i++)
        {
            if (lsns[i] <= applied)
                continue;
            runtime.Engine.ApplyInternal(
                ClusterRecordNames.Replica,
                ReplicaCaller,
                [.. records[i].Select(static op => op.ToRowOp())],
                reconcile: true);
            applied = lsns[i];
        }

        return applied;
    }

    /// <summary>
    /// Applies a full-state replication reset (see <see cref="ReplicaReset"/>): upsert every
    /// snapshot row, delete every local Replicated row the snapshot lacks — the hub deleted it
    /// during the gap the truncated log can no longer serve — then jump the shard's cursor to the
    /// snapshot LSN and resume streaming from there.
    /// </summary>
    private void ApplyReplicaReset(ReplicaReset reset)
    {
        List<(ShardRuntime Runtime, ulong Cursor)> shards;
        lock (_shardsLock)
        {
            shards = [.. _shards.Values.Select(r => (r, ReadReplicaCursor(r.Directory)))];
        }

        foreach (var (runtime, cursor) in shards)
            ApplyReplicaReset(runtime, cursor, reset);
    }

    /// <summary>Test hook: how many full Replicated resets this node applied to a shard, pushed or pulled.</summary>
    internal int ReplicaResetsApplied => Volatile.Read(ref _replicaResetsApplied);

    private int _replicaResetsApplied;

    /// <summary>Applies a reset to one shard engine whose cursor is behind it, and persists the cursor.</summary>
    private void ApplyReplicaReset(ShardRuntime runtime, ulong cursor, ReplicaReset reset)
    {
        if (cursor >= reset.Lsn)
            return;
        Interlocked.Increment(ref _replicaResetsApplied);
        var ops = new List<RowOp>();
        foreach (var table in reset.Tables)
        {
            var tableId = new TableId(table.Table);
            var snapshotKeys = new HashSet<RowKey>();
            foreach (var row in table.Rows)
            {
                snapshotKeys.Add(new RowKey(row.Key));
                ops.Add(row.ToRowOp());
            }

            foreach (var (key, _) in runtime.Engine.HotStore.Scan(tableId).ToList())
            {
                if (!snapshotKeys.Contains(key))
                    ops.Add(new RowOp(RowOpKind.Delete, tableId, key));
            }
        }

        runtime.Engine.ApplyInternal(ClusterRecordNames.Replica, ReplicaCaller, ops, reconcile: true);
        WriteReplicaCursor(runtime.Directory, reset.Lsn);
    }

    private static readonly Identity ReplicaCaller = Identity.Hash("melange/replica");

    /// <summary>
    /// The handoff reconciler: a periodic sweep resolving every saga this node holds a live half
    /// of, whether it got there by crash recovery or by a coordinator that died (or lost the
    /// link) mid-saga. Origin side: a pending freeze asks the hub whether the destination's
    /// import became durable — release if it did, abort if it definitively did not, wait while
    /// the saga is still in flight or the destination is unreachable. Destination side: an
    /// unsettled import asks whether the origin's freeze is still pending — once it is not (or
    /// there never was an origin), the import settles and its marker stops pinning log
    /// truncation. Every step is idempotent, so the sweep needs no state of its own — which is
    /// what makes it correct across any combination of crashes.
    /// </summary>
    private async Task ReconcileHandoffsAsync()
    {
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(HandoffReconcileIntervalMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (_link is not { } link)
                continue;
            List<ShardRuntime> runtimes;
            lock (_shardsLock)
            {
                runtimes = [.. _shards.Values];
            }

            foreach (var runtime in runtimes)
            {
                foreach (var pending in runtime.PendingFreezes)
                {
                    try
                    {
                        var reply = await link.RequestAsync(
                            "handoff-query", new HandoffQuery(pending.HandoffId, pending.ToShard), ct).ConfigureAwait(false);
                        var imported = reply!.Value.Read<HandoffQueryReply>()!.Imported;
                        if (imported)
                            runtime.Release(pending.HandoffId);
                        else
                            runtime.Abort(pending.HandoffId);

                        // The coordinator that would have told the world is gone (that is why this
                        // reconciler resolved it), so this node reports the outcome: the hub runs
                        // its transfer listeners and gateway notifications late but correctly.
                        await link.NotifyAsync(
                            "handoff-resolved",
                            new HandoffResolved(pending.HandoffId, pending.PlayerHex, pending.FromShard, pending.ToShard, imported),
                            ct).ConfigureAwait(false);
                    }
                    catch (Exception) when (!ct.IsCancellationRequested)
                    {
                        // Still in flight, or the destination is unreachable: the freeze stays,
                        // the player stays unwritable everywhere, and the next sweep retries.
                    }
                }

                foreach (var import in runtime.UnsettledImports)
                {
                    try
                    {
                        if (import.FromShard == HandoffMarker.NoOrigin)
                        {
                            runtime.Settle(import.HandoffId); // First entry: no origin to wait on.
                            continue;
                        }

                        var reply = await link.RequestAsync(
                            "handoff-freeze-query", new HandoffFreezeQuery(import.HandoffId, import.FromShard), ct).ConfigureAwait(false);
                        if (!reply!.Value.Read<HandoffFreezeQueryReply>()!.Pending)
                            runtime.Settle(import.HandoffId);
                    }
                    catch (Exception) when (!ct.IsCancellationRequested)
                    {
                        // Origin unreachable: the marker keeps pinning truncation, honestly.
                    }
                }
            }
        }
    }

    private const int HandoffReconcileIntervalMs = 1_000;

    /// <summary>
    /// The observer half of interest-driven replication: a periodic sweep keeping one border
    /// subscription alive per (owned shard, interesting neighbour) pair. Re-sent when the band
    /// depth changes (forcing a reset — a widened band has rows the stream already scanned past)
    /// and otherwise refreshed on a slow cadence, which is what re-establishes streams after an
    /// owner node restarts and lost them. A neighbour shard that does not exist yet is a benign
    /// "no" — empty world regions are not an error, and nothing subscribes to them.
    /// </summary>
    private async Task MaintainBorderSubscriptionsAsync()
    {
        const int SweepMs = 1_000;
        const long RefreshTicks = 5 * TimeSpan.TicksPerSecond;
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(SweepMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (_strategy is not { } strategy || _link is not { } link)
                continue;
            var band = Math.Max(1, Cluster.BorderBandChunks);
            List<(ShardKey Shard, ShardRuntime Runtime)> owned;
            lock (_shardsLock)
            {
                owned = [.. _shards.Select(static pair => (pair.Key, pair.Value))];
            }

            foreach (var (shard, runtime) in owned)
            {
                foreach (var owner in strategy.InterestOf(shard))
                {
                    var key = (shard.Value, owner.Value);
                    var now = _time.GetUtcNow().UtcTicks;
                    lock (_shardsLock)
                    {
                        if (_borderSubscriptions.TryGetValue(key, out var last)
                            && last.Band == band && now - last.LastSentTicks < RefreshTicks)
                        {
                            continue;
                        }
                    }

                    var (epoch, lsn, storedBand) = runtime.ReadBorderCursor(owner.Value, band);
                    runtime.SetBorderCursorBand(band);
                    try
                    {
                        var reply = await link.RequestAsync(
                            "border-subscribe",
                            new BorderSubscribe(owner.Value, shard.Value, epoch, lsn, band, ForceReset: storedBand != band),
                            ct).ConfigureAwait(false);
                        if (reply!.Value.Read<BorderSubscribeReply>()!.Exists)
                        {
                            lock (_shardsLock)
                            {
                                _borderSubscriptions[key] = (band, now);
                            }
                        }
                    }
                    catch (Exception) when (!ct.IsCancellationRequested)
                    {
                        // Hub or owner unreachable; the next sweep retries.
                    }
                }
            }
        }
    }

    public void Dispose()
    {
        _stopped.Cancel();
        _leaseWatchdog?.Dispose();
        _link?.Dispose();
        _livenessMeter.Dispose();
        lock (_followersLock)
        {
            foreach (var follower in _followers.Values)
            {
                lock (follower)
                    follower.Dispose();
            }

            _followers.Clear();
        }

        // An open in flight holds the shard's log and store; let it finish and close itself (it
        // sees the stop and abandons) rather than leave those handles to a node that no longer
        // exists. Bounded, because a recovery has no cancellation point — past the bound the
        // handles are released when the task ends.
        Task[] opening;
        lock (_shardsLock)
        {
            opening = [.. _opening.Values];
        }

        if (opening.Length > 0)
            Task.WaitAll(opening, TimeSpan.FromSeconds(30));
        lock (_shardsLock)
        {
            foreach (var forwarder in _forwarders.Values)
                forwarder.Dispose();
            _forwarders.Clear();
            foreach (var publisher in _borderPublishers.Values)
                publisher.Dispose();
            _borderPublishers.Clear();
            foreach (var monitor in _boundaryMonitors.Values)
                monitor.Dispose();
            _boundaryMonitors.Clear();
            foreach (var runtime in _shards.Values)
                runtime.Dispose();
            _shards.Clear();
        }
    }

    [LoggerMessage(EventId = 1707, EventName = "ShardOpened", Level = LogLevel.Information,
        Message = "Shard {Shard} opened on node '{NodeName}' in {OpenMs:F0}ms, recovered to LSN {HeadLsn} from its own log.")]
    private static partial void LogShardOpened(ILogger logger, ulong shard, string nodeName, double openMs, ulong headLsn);

    [LoggerMessage(EventId = 1762, EventName = "ShardPromoted", Level = LogLevel.Information,
        Message = "Shard {Shard} promoted from its replica on node '{NodeName}' in {OpenMs:F0}ms: the replica stood at LSN {FromLsn}, and the shard is at LSN {HeadLsn}.")]
    private static partial void LogShardPromoted(ILogger logger, ulong shard, string nodeName, double openMs, ulong fromLsn, ulong headLsn);

    [LoggerMessage(EventId = 1761, EventName = "ReplicaFollowFailed", Level = LogLevel.Warning,
        Message = "Following shard {Shard}'s log for its replica failed ({Reason}); the next poll tries again.")]
    private static partial void LogReplicaFollowFailed(ILogger logger, ulong shard, string reason);

    [LoggerMessage(EventId = 1756, EventName = "ShardsParkedOnLeaseExpiry", Level = LogLevel.Warning,
        Message = "Node '{NodeName}' parked {Count} shard(s): its lease lapsed without a hub heartbeat, so it may already have been declared dead. Their engines are closed and their logs released for whichever node the hub names; registering again reopens any still assigned here.")]
    private static partial void LogShardsParked(ILogger logger, string nodeName, int count);

    [LoggerMessage(EventId = 1757, EventName = "FailureTimeoutMismatch", Level = LogLevel.Warning,
        Message = "Node '{NodeName}' is configured with Cluster:FailureTimeoutMs {NodeTimeoutMs} but the hub uses {HubTimeoutMs}; the lease follows the hub's. Set them the same.")]
    private static partial void LogFailureTimeoutMismatch(ILogger logger, string nodeName, int nodeTimeoutMs, int hubTimeoutMs);

    [LoggerMessage(EventId = 1753, EventName = "ShardOpenFailed", Level = LogLevel.Warning,
        Message = "Shard {Shard} could not be opened on node '{NodeName}' ({Reason}); the next heartbeat retries. The hub link and this node's other shards are unaffected.")]
    private static partial void LogShardOpenFailed(ILogger logger, ulong shard, string nodeName, string reason);

    [LoggerMessage(EventId = 1754, EventName = "ShardOpenAbandoned", Level = LogLevel.Information,
        Message = "Shard {Shard} finished recovering on node '{NodeName}' after the hub had moved it (or a drain, reap, or shutdown began); the recovered runtime was closed unused.")]
    private static partial void LogShardOpenAbandoned(ILogger logger, ulong shard, string nodeName);

    [LoggerMessage(EventId = 1708, EventName = "ShardReleased", Level = LogLevel.Information,
        Message = "Shard {Shard} released by node '{NodeName}': the hub reassigned it.")]
    private static partial void LogShardReleased(ILogger logger, ulong shard, string nodeName);

    [LoggerMessage(EventId = 1709, EventName = "HubLinkLost", Level = LogLevel.Warning,
        Message = "The hub link failed ({Reason}); reconnecting. If Cluster:FailureTimeoutMs passes first, this node self-fences its shards.")]
    private static partial void LogHubLinkLost(ILogger logger, string reason);

    [LoggerMessage(EventId = 1726, EventName = "ShardQuiesced", Level = LogLevel.Information,
        Message = "Shard {Shard} quiesced on node '{NodeName}' for a planned drain: snapshot taken, engine closed at LSN {HeadLsn}. " +
            "The shard reopens here only if the hub abandons the drain or dies mid-drain (the draining mark expires after 2x Cluster:FailureTimeoutMs).")]
    private static partial void LogShardQuiesced(ILogger logger, ulong shard, string nodeName, ulong headLsn);

    [LoggerMessage(EventId = 1747, EventName = "ShardReaped", Level = LogLevel.Warning,
        Message = "Shard {Shard} was reaped on node '{NodeName}': it held no rows of its own and nothing pinned its log, " +
            "so its engine was closed and '{Directory}' deleted. Warning rather than information because this is the one " +
            "cluster operation that destroys durable state; the shard key is not reserved, and visiting it again creates a " +
            "new shard with a new originator.")]
    private static partial void LogShardReaped(ILogger logger, ulong shard, string nodeName, string directory);

    [LoggerMessage(EventId = 1727, EventName = "ShardDrainAborted", Level = LogLevel.Warning,
        Message = "The hub abandoned the drain of shard {Shard}; node '{NodeName}' cleared the draining mark and reopens the shard on its next heartbeat.")]
    private static partial void LogDrainAborted(ILogger logger, ulong shard, string nodeName);

    [LoggerMessage(EventId = 1728, EventName = "ShardDrainMarkExpired", Level = LogLevel.Warning,
        Message = "Shard {Shard}'s draining mark on node '{NodeName}' outlived 2x Cluster:FailureTimeoutMs with the assignment still naming this node — " +
            "the hub likely died between quiesce and reassign. The mark expired and the shard reopens; the interrupted drain healed itself in favour of the origin.")]
    private static partial void LogDrainMarkExpired(ILogger logger, ulong shard, string nodeName);

    [LoggerMessage(EventId = 1750, EventName = "ShardSealedForReap", Level = LogLevel.Warning,
        Message = "Shard {Shard} was sealed and closed for a reap on node '{NodeName}': it held no rows of its own and nothing pinned its log. " +
            "'{Directory}' is untouched until the hub has retired the membership row, and the shard reopens from it if the reap does not finish.")]
    private static partial void LogShardSealedForReap(ILogger logger, ulong shard, string nodeName, string directory);

    [LoggerMessage(EventId = 1751, EventName = "ShardReapMarkExpired", Level = LogLevel.Warning,
        Message = "Shard {Shard}'s reap mark on node '{NodeName}' outlived 2x Cluster:FailureTimeoutMs — the hub likely died between the close and the " +
            "delete. The mark expired; the directory was never touched, so the shard reopens on the next assignment and the reap simply did not happen.")]
    private static partial void LogReapMarkExpired(ILogger logger, ulong shard, string nodeName);

    [LoggerMessage(EventId = 1763, EventName = "ShardReplicaCaughtUp", Level = LogLevel.Information,
        Message = "Shard {Shard} on node '{NodeName}' caught its Replicated tables up before serving: {Records} records, from LSN {FromLsn} to the hub's {HeadLsn}, in {ElapsedMs:F0} ms.")]
    private static partial void LogShardReplicaCaughtUp(ILogger logger, ulong shard, string nodeName, int records, ulong fromLsn, ulong headLsn, double elapsedMs);

    [LoggerMessage(EventId = 1764, EventName = "ReplicaCatchUpUnsupported", Level = LogLevel.Warning,
        Message = "The hub does not answer replica-read, so node '{NodeName}' opens shards without catching their Replicated tables up first: a newly opened shard's " +
            "first calls can read Replicated tables the stream has not filled yet. Upgrade the hub to match its nodes.")]
    private static partial void LogReplicaCatchUpUnsupported(ILogger logger, string nodeName);
}
