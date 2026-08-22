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
            () => _options.CurrentValue.Bulk,
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

        // And the commit-point half, which the access guard cannot cover: bulk ingestion never
        // touches a table handle, so without this a batch of Partitioned rows posted to this
        // node's own endpoint committed into an engine nothing reads them from. The hub has had
        // the equivalent guard all along.
        engine.AddCommitGuard(new NodeLocalCommitGuard(engine.Schema, cluster.NodeName));

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
                var beat = await link.RequestAsync("heartbeat", new HeartbeatRequest(CollectLoads()), ct).ConfigureAwait(false);
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

            foreach (var assignment in assigned.Values)
            {
                if (_shards.ContainsKey(assignment.Shard))
                    continue;
                if (IsDrainingLocked(assignment.Shard))
                    continue;

                // Sealed and closed for a reap the hub has not finished. Reopening now would
                // either race the delete or — if the hub got as far as removing the membership row
                // and this is a stale assignment list — mint ids under a retired originator.
                if (IsReapingLocked(assignment.Shard))
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

            case "shard-drain":
                return Task.FromResult<object?>(QuiesceShard(body!.Value.Deserialize<ShardDrain>()!));
            case "shard-reap":
                return Task.FromResult<object?>(ReapShard(body!.Value.Deserialize<ShardReap>()!));
            case "shard-reap-delete":
                return Task.FromResult<object?>(DeleteReapedShard(body!.Value.Deserialize<ShardReapDelete>()!));
            case "shard-drain-abort":
            {
                var abort = body!.Value.Deserialize<ShardDrainAbort>()!;
                lock (_shardsLock)
                {
                    _draining.Remove(new ShardKey(abort.Shard));
                }

                LogDrainAborted(_logger, abort.Shard, NodeName);
                return Task.FromResult<object?>(null);
            }

            case "assignments-apply":
                // The drain's fast path: the hub pushes the destination's assignments instead of
                // waiting out the heartbeat clock, and the reply doubles as "the shard is open"
                // because ApplyAssignments recovers synchronously.
                ApplyAssignments(body!.Value.Deserialize<AssignmentsApply>()!.Assignments);
                return Task.FromResult<object?>(null);
            case "shard-execute":
            {
                var execute = body!.Value.Deserialize<ShardExecute>()!;
                var runtime = RequireShard(new ShardKey(execute.Shard), execute.FencingToken);
                var lsn = runtime.ReducerHost.Call(
                    execute.Reducer,
                    new Identity(Convert.FromHexString(execute.CallerHex)),
                    ConnectionId.None,
                    Convert.FromBase64String(execute.ArgsB64));
                return Task.FromResult<object?>(new ShardExecuteReply(lsn));
            }

            case "shard-bulk":
            {
                var bulk = body!.Value.Deserialize<ShardBulk>()!;
                var runtime = RequireShard(new ShardKey(bulk.Shard), bulk.FencingToken);
                return Task.FromResult<object?>(ApplyBulkGroup(
                    runtime,
                    new ShardKey(bulk.Shard),
                    new Identity(Convert.FromHexString(bulk.CallerHex)),
                    bulk.Rows));
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
}
