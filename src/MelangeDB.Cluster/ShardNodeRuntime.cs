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
    private readonly Dictionary<ShardKey, EventForwarder> _forwarders = [];
    private readonly Dictionary<ShardKey, BorderPublisher> _borderPublishers = [];
    private readonly Dictionary<ShardKey, BoundaryMonitor> _boundaryMonitors = [];
    private readonly Dictionary<(ulong Observer, ulong Owner), (int Band, long LastSentTicks)> _borderSubscriptions = [];
    private readonly CancellationTokenSource _stopped = new();
    private volatile NodeLink? _link;
    private IShardStrategy? _strategy;
    private long _leaseValidUntilTicks;
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
            () => _options.CurrentValue.Cluster,
            _time);
    }

    public ClusterMetrics Metrics { get; } = new();

    /// <summary>Test hook: while true, heartbeats stop — a partition, as the hub sees it.</summary>
    internal bool SuspendHeartbeats { get; set; }

    private ClusterOptions Cluster => _options.CurrentValue.Cluster;

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
    /// Whether this node's lease is live: a heartbeat (or registration) succeeded within
    /// Cluster:FailureTimeoutMs. Expired means self-fenced — the hub has, by the same clock,
    /// begun to suspect this node dead.
    /// </summary>
    public bool LeaseValid() =>
        _time.GetUtcNow().UtcTicks < Interlocked.Read(ref _leaseValidUntilTicks);

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

        _strategy = _services.GetService<IShardStrategy>();
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
            var reply = await link.RequestAsync("auth", new AuthRequest(
                cluster.NodeName,
                cluster.PublicAddress,
                nodeNonce,
                LinkProof.Compute(cluster.Secret, hubNonce, cluster.NodeName)), ct).ConfigureAwait(false);
            var auth = reply!.Value.Deserialize<AuthReply>()!;
            if (!LinkProof.Verify(cluster.Secret, nodeNonce, "hub", auth.Proof))
                throw new InvalidOperationException("The hub failed to prove possession of the cluster secret.");

            _link = link;
            RenewLease();
            ApplyAssignments(auth.Assignments);
            await SubscribeReplicationAsync(link, ct).ConfigureAwait(false);
            KickForwarders();

            // Heartbeat until the link dies. A successful heartbeat renews the lease and applies
            // any assignment changes (reassignments, new shards) the hub decided meanwhile.
            while (!ct.IsCancellationRequested && link.IsAlive)
            {
                await Task.Delay(cluster.HeartbeatIntervalMs, ct).ConfigureAwait(false);
                if (SuspendHeartbeats)
                    continue;
                var beat = await link.RequestAsync("heartbeat", null, ct).ConfigureAwait(false);
                RenewLease();
                var assignments = beat!.Value.Deserialize<HeartbeatReply>()!.Assignments;
                ApplyAssignments(assignments);
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

    private void RenewLease() =>
        Interlocked.Exchange(
            ref _leaseValidUntilTicks,
            _time.GetUtcNow().AddMilliseconds(Cluster.FailureTimeoutMs).UtcTicks);

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

            foreach (var assignment in assigned.Values)
            {
                if (_shards.ContainsKey(assignment.Shard))
                    continue;
                var directory = Path.Combine(Cluster.ShardDataPath, $"shard-{assignment.Shard.Value}");
                Directory.CreateDirectory(directory);
                var runtime = new ShardRuntime(
                    assignment.Shard,
                    assignment.FencingToken,
                    assignment.Originator,
                    directory,
                    NodeName,
                    _services.GetRequiredService<SchemaRegistry>(),
                    _services.GetRequiredService<ReducerRegistry>(),
                    _services,
                    _options,
                    LeaseValid,
                    _loggerFactory,
                    _time,
                    _authenticator,
                    _sessions);
                _shards[assignment.Shard] = runtime;
                var forwarder = new EventForwarder(
                    runtime.Engine,
                    assignment.Shard.Value,
                    Path.Combine(directory, "events.cursor"),
                    () => _link,
                    _logger);
                _forwarders[assignment.Shard] = forwarder;
                forwarder.Start();
                if (_strategy is { } strategy)
                {
                    var publisher = new BorderPublisher(
                        runtime.Engine, assignment.Shard, strategy, runtime.BorrowedOwnerOf, () => _link, _logger);
                    _borderPublishers[assignment.Shard] = publisher;
                    publisher.Start();

                    // Seamless (walking-triggered) handoff needs a strategy that can judge
                    // boundaries and an application that names its migratable entities; with
                    // both, the origin decides — its committed rows are the only trusted position.
                    if (strategy is IBoundaryStrategy && _services.GetService<IMigrationAnchors>() is { } anchors)
                    {
                        var monitor = new BoundaryMonitor(
                            runtime.Engine, assignment.Shard, strategy, anchors,
                            () => _link, () => runtime.FencingToken, runtime.BorrowedOwnerOf, runtime.IsFrozen,
                            _options, _time);
                        _boundaryMonitors[assignment.Shard] = monitor;
                        monitor.Start();
                    }
                }

                shardSetChanged = true;
                LogShardOpened(_logger, assignment.Shard.Value, NodeName, runtime.Engine.Log.HeadLsn);
            }

            // A shard-set change moves the replication low-water mark; force a re-subscribe.
            if (shardSetChanged)
                _replicaSubscribedFrom = ulong.MaxValue;
        }

        foreach (var runtime in closed)
            runtime.Dispose();
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

    private Task<object?> HandleAsync(NodeLink link, string type, JsonElement? body, TaskCompletionSource<string> challenge)
    {
        switch (type)
        {
            case "challenge":
                challenge.TrySetResult(body!.Value.GetProperty("ServerNonce").GetString()!);
                return Task.FromResult<object?>(null);
            case "replica-batch":
                ApplyReplicaBatch(body!.Value.Deserialize<ReplicaBatch>()!);
                return Task.FromResult<object?>(null);
            case "replica-reset":
                ApplyReplicaReset(body!.Value.Deserialize<ReplicaReset>()!);
                return Task.FromResult<object?>(null);
            case "handoff-freeze":
            {
                var freeze = body!.Value.Deserialize<HandoffFreeze>()!;
                var runtime = RequireShard(new ShardKey(freeze.FromShard), freeze.FencingToken);
                var rows = runtime.FreezeAndCollect(
                    freeze.HandoffId, new Identity(Convert.FromHexString(freeze.PlayerHex)), new ShardKey(freeze.ToShard));
                return Task.FromResult<object?>(new HandoffFrozenRows(rows));
            }

            case "handoff-import":
            {
                var import = body!.Value.Deserialize<HandoffImport>()!;
                var runtime = RequireShard(new ShardKey(import.ToShard), import.FencingToken);
                runtime.Import(import.HandoffId, import.PlayerHex, import.FromShard, import.Rows);
                return Task.FromResult<object?>(null);
            }

            case "handoff-release":
            {
                var release = body!.Value.Deserialize<HandoffRelease>()!;
                RequireShard(new ShardKey(release.FromShard), release.FencingToken).Release(release.HandoffId);
                return Task.FromResult<object?>(null);
            }

            case "handoff-abort":
            {
                var abort = body!.Value.Deserialize<HandoffRelease>()!;
                RequireShard(new ShardKey(abort.FromShard), abort.FencingToken).Abort(abort.HandoffId);
                return Task.FromResult<object?>(null);
            }

            case "border-subscribe-owner":
            {
                var subscribe = body!.Value.Deserialize<BorderSubscribe>()!;
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
                var batch = body!.Value.Deserialize<BorderBatch>()!;
                var observer = TryGetShard(new ShardKey(batch.ObserverShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {batch.ObserverShard}.");
                observer.ApplyBorder(batch);
                return Task.FromResult<object?>(null);
            }

            case "border-reset-apply":
            {
                var reset = body!.Value.Deserialize<BorderReset>()!;
                var observer = TryGetShard(new ShardKey(reset.ObserverShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {reset.ObserverShard}.");
                observer.ApplyBorderReset(reset);
                return Task.FromResult<object?>(null);
            }

            case "handoff-query-owner":
            {
                var query = body!.Value.Deserialize<HandoffQuery>()!;
                var runtime = TryGetShard(new ShardKey(query.ToShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {query.ToShard}.");
                return Task.FromResult<object?>(new HandoffQueryReply(runtime.WasImported(query.HandoffId)));
            }

            case "handoff-freeze-query-owner":
            {
                var query = body!.Value.Deserialize<HandoffFreezeQuery>()!;
                var runtime = TryGetShard(new ShardKey(query.FromShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {query.FromShard}.");
                return Task.FromResult<object?>(new HandoffFreezeQueryReply(runtime.IsFreezePending(query.HandoffId)));
            }

            default:
                throw new InvalidOperationException($"Unknown node-link message '{type}'.");
        }
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

        foreach (var (runtime, cursor) in shards)
        {
            var applied = cursor;
            for (var i = 0; i < batch.Lsns.Length; i++)
            {
                if (batch.Lsns[i] <= applied)
                    continue;
                runtime.Engine.ApplyInternal(
                    ClusterRecordNames.Replica,
                    ReplicaCaller,
                    [.. batch.Records[i].Select(static op => op.ToRowOp())],
                    reconcile: true);
                applied = batch.Lsns[i];
            }

            if (applied > cursor)
                WriteReplicaCursor(runtime.Directory, applied);
        }
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
        {
            if (cursor >= reset.Lsn)
                continue;
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
                        var imported = reply!.Value.Deserialize<HandoffQueryReply>()!.Imported;
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
                        if (!reply!.Value.Deserialize<HandoffFreezeQueryReply>()!.Pending)
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
                        if (reply!.Value.Deserialize<BorderSubscribeReply>()!.Exists)
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
        _link?.Dispose();
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
        Message = "Shard {Shard} opened on node '{NodeName}', recovered to LSN {HeadLsn} from its own log.")]
    private static partial void LogShardOpened(ILogger logger, ulong shard, string nodeName, ulong headLsn);

    [LoggerMessage(EventId = 1708, EventName = "ShardReleased", Level = LogLevel.Information,
        Message = "Shard {Shard} released by node '{NodeName}': the hub reassigned it.")]
    private static partial void LogShardReleased(ILogger logger, ulong shard, string nodeName);

    [LoggerMessage(EventId = 1709, EventName = "HubLinkLost", Level = LogLevel.Warning,
        Message = "The hub link failed ({Reason}); reconnecting. If Cluster:FailureTimeoutMs passes first, this node self-fences its shards.")]
    private static partial void LogHubLinkLost(ILogger logger, string reason);
}
