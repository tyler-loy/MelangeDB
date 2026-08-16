using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MelangeDB.Cluster;

/// <summary>
/// Everything the hub role runs beyond its ordinary single-node stack: the node-link listener
/// with mutual (cluster-secret) authentication, the membership store and failure sweep, per-link
/// replication pumps shipping Replicated write sets from the hub's log, foreign-event dispatch,
/// internal identity assertions, and the handoff saga coordinator. The hub's own engine is the
/// DI-registered engine — its Global and Replicated tables are exactly the single-node tables it
/// always had, now guarded so a Partitioned touch fails with the placement rule in the message.
/// </summary>
internal sealed partial class HubRuntime : IDisposable
{
    private sealed class NodeSession
    {
        private long _replicaCursor = -1;

        public required string ServerNonce { get; init; }

        public string? NodeName { get; set; }

        public SemaphoreSlim ReplicaSignal { get; } = new(0, 1);

        public Task? ReplicaPump { get; set; }

        /// <summary>The pump's cursor; -1 means not subscribed. A re-subscribe may move it back.</summary>
        public long ReplicaCursor
        {
            get => Interlocked.Read(ref _replicaCursor);
            set => Interlocked.Exchange(ref _replicaCursor, value);
        }

        /// <summary>Moves the cursor backwards only — a re-subscribe for a shard that needs older records.</summary>
        public void LowerReplicaCursor(long cursor)
        {
            long current;
            while ((current = Interlocked.Read(ref _replicaCursor)) < 0 || cursor < current)
            {
                if (Interlocked.CompareExchange(ref _replicaCursor, cursor, current) == current)
                    return;
            }
        }
    }

    /// <summary>An in-flight handoff saga on the coordinator; its presence alone means "still driving".</summary>
    private sealed record Saga(string HandoffId, Identity Player, ShardKey From, ShardKey To);

    private readonly IServiceProvider _services;
    private readonly MelangeEngine _engine;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly IMembershipStore _membership;
    private readonly TimeProvider _time;
    private readonly INodeProvisioner? _provisioner;
    private readonly ILogger _logger;
    private readonly ForeignEventDispatcher _foreignEvents;
    private readonly ConcurrentDictionary<string, NodeLink> _linksByNode = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Saga> _sagas = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Identity, string> _sagasByPlayer = new();
    private readonly ConcurrentDictionary<Identity, long> _lastHandoffStartTicks = new();

    /// <summary>
    /// The hub's own record of where each transferred entity lives — written at every
    /// destination-authoritative moment, consulted before starting a boundary-triggered saga. A
    /// request naming a stale origin is dropped outright: an origin that no longer owns the
    /// entity can only be offering a stale copy, and a saga built on one re-imports the past over
    /// the present. In-memory by design — after a hub restart the node-side defenses (borrowed
    /// registry, frozen set, empty-collect abort) carry the same invariant until this map
    /// repopulates.
    /// </summary>
    private readonly ConcurrentDictionary<Identity, ShardKey> _entityOwner = new();
    private readonly CancellationTokenSource _stopped = new();
    private TcpListener? _listener;
    private Task? _acceptLoop;
    private ITimer? _sweepTimer;
    private ITimer? _rebalanceTimer;
    private int _rebalanceMoveInFlight;
    private readonly ConcurrentDictionary<ShardKey, long> _lastShardMoveTicks = new();
    private readonly ConcurrentDictionary<string, long> _rebalanceStuckLogTicks = new(StringComparer.Ordinal);

    /// <summary>A ticket the hub is waiting on, with when it asked and which attempt this is.</summary>
    private sealed record OutstandingTicket(ProvisionTicket Ticket, DateTimeOffset RequestedAt, int Attempt);

    // Capacity state (road-to-0.2 phase 14), guarded by _capacityLock: the rebalance tick and
    // node registrations race, and ticket state is money-shaped — it must never double-spend.
    private readonly Lock _capacityLock = new();
    private OutstandingTicket? _ticket;
    private int _provisionAttempts;
    private bool _provisionGaveUp;
    private int _provisionCallInFlight;
    private readonly Dictionary<string, string> _expiredTickets = new(StringComparer.Ordinal);
    private int _scaleInInFlight;
    private long _lastScaleInTicks;
    private readonly ConcurrentDictionary<string, long> _nodeProvisionedTicks = new(StringComparer.Ordinal);
    private readonly System.Diagnostics.Metrics.Meter _capacityMeter = new("MelangeDB");
    private readonly System.Diagnostics.Metrics.Histogram<double> _provisionLatency;
    private readonly System.Diagnostics.Metrics.Counter<long> _decommissions;

    public HubRuntime(IServiceProvider services)
    {
        _services = services;
        _engine = services.GetRequiredService<MelangeEngine>();
        _options = services.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>();
        _membership = services.GetRequiredService<IMembershipStore>();
        _time = services.GetService<TimeProvider>() ?? TimeProvider.System;
        _provisioner = services.GetService<INodeProvisioner>();
        var loggers = services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
        _logger = loggers.CreateLogger("MelangeDB.Cluster.Hub");
        _foreignEvents = new ForeignEventDispatcher(
            services.GetRequiredService<EventHandlerRegistry>(),
            services.GetRequiredService<IServiceScopeFactory>(),
            _options,
            _logger);
        _provisionLatency = _capacityMeter.CreateHistogram<double>(
            "melange.cluster.provision.latency",
            unit: "ms",
            description: "Time from a provision request to the ticket's named node joining membership.");
        _decommissions = _capacityMeter.CreateCounter<long>(
            "melange.cluster.decommissions",
            unit: "{node}",
            description: "DecommissionAsync calls this hub issued — scale-in and expired-ticket late arrivals alike.");
        _capacityMeter.CreateObservableGauge(
            "melange.cluster.nodes",
            () => (long)_membership.Nodes().Count(static n => n.Alive),
            unit: "{node}",
            description: "Live shard nodes in membership — the fleet size the capacity loop steers.");
        _capacityMeter.CreateObservableGauge(
            "melange.cluster.provision.outstanding",
            () =>
            {
                lock (_capacityLock)
                {
                    return _ticket is null ? 0L : 1L;
                }
            },
            unit: "{ticket}",
            description: "Provision tickets outstanding — 0 or 1 by construction.");
    }

    public ClusterMetrics Metrics { get; } = new();

    /// <summary>The gateway connections' view into in-flight handoffs, keyed by player identity.</summary>
    public HandoffNotifier Handoffs { get; } = new();

    /// <summary>The per-shard load view, fed by every node's heartbeats; see <see cref="ClusterLoadView"/>.</summary>
    public ClusterLoadView Load { get; } = new();

    /// <summary>The gateway connections' view into in-flight shard drains; see <see cref="ShardMoveNotifier"/>.</summary>
    public ShardMoveNotifier ShardMoves { get; } = new();

    /// <summary>Test hook: awaited before each named drain step ("reassign", "apply").</summary>
    internal Func<string, Task>? DrainStepHook { get; set; }

    /// <summary>Whether a node provisioner is registered — the capacity seam's on/off fact.</summary>
    internal bool HasProvisioner => _provisioner is not null;

    /// <summary>The provision ticket the hub is currently waiting on, if any.</summary>
    internal ProvisionTicket? OutstandingProvision
    {
        get
        {
            lock (_capacityLock)
            {
                return _ticket?.Ticket;
            }
        }
    }

    /// <summary>
    /// True once two provision attempts failed or expired: the loop has stopped asking (EventId
    /// 1738, the <c>melange-capacity</c> health check) and a human must intervene. Cleared when a
    /// ticket-named node finally joins.
    /// </summary>
    internal bool ProvisionHasGivenUp
    {
        get
        {
            lock (_capacityLock)
            {
                return _provisionGaveUp;
            }
        }
    }

    public MelangeEngine Engine => _engine;

    public IMembershipStore Membership => _membership;

    /// <summary>The node-link listener's bound port — the configured one, or the ephemeral pick.</summary>
    public int NodeListenPort { get; private set; }

    /// <summary>Test hook: awaited before each named saga step ("import", "release").</summary>
    internal Func<string, Task>? HandoffStepHook { get; set; }

    private ClusterOptions Cluster => _options.CurrentValue.Cluster;

    public void Start()
    {
        var cluster = Cluster;
        if (string.IsNullOrEmpty(cluster.Secret))
            throw new InvalidOperationException("Cluster:Role is Hub but Cluster:Secret is empty; nodes cannot authenticate.");
        if (_provisioner is not null && cluster.MaxNodes < 1)
        {
            // Startup-fatal on purpose: a deployment that registered a provisioner meant to use
            // it, and every default ceiling is wrong — low silently caps a deployment that meant
            // to scale, high is a silent spending authorization.
            throw new InvalidOperationException(
                "An INodeProvisioner is registered but Cluster:MaxNodes is not set. The provisioning loop can spend " +
                "money, so its ceiling must be an explicit decision: set Cluster:MaxNodes to the largest fleet this " +
                "deployment is willing to pay for, or remove the provisioner registration to keep the fleet fixed.");
        }

        if (_provisioner is not null && cluster.ScaleInEnabled && Math.Max(1, cluster.MinNodes) > cluster.MaxNodes)
        {
            throw new InvalidOperationException(
                $"Cluster:MinNodes ({cluster.MinNodes}) exceeds Cluster:MaxNodes ({cluster.MaxNodes}); the fleet's floor " +
                "cannot sit above its ceiling.");
        }

        _engine.SetTableAccessGuard(PlacementGuards.HubAccess());
        _engine.AddCommitGuard(new HubCommitGuard(_engine.Schema));
        _engine.AddCommitObserver(new ReplicaSignaller(this));

        // Hub-executed init reducers seed the hub's own fresh engine — its Global and Replicated
        // tables. Deliberately after the guards, so a seed that reaches for a Partitioned table
        // fails with the placement rule in the message rather than writing a row the hub must not
        // own. Shard-executed ones belong to the shards and fire as each shard's engine opens.
        MelangeInitReducers.Fire(
            _engine, _services.GetRequiredService<MelangeReducerHost>(), _logger, "the hub", ReducerSite.Hub);

        if (!IPAddress.TryParse(cluster.NodeListenAddress, out var listenAddress))
        {
            throw new InvalidOperationException(
                $"Cluster:NodeListenAddress '{cluster.NodeListenAddress}' is not an IP address. Use 127.0.0.1 " +
                "(the default), 0.0.0.0 to accept nodes from other machines, or a specific interface address — " +
                "and read the docs/THREAT-MODEL.md note before widening it.");
        }

        _listener = new TcpListener(listenAddress, cluster.NodeListenPort);
        _listener.Start();
        NodeListenPort = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);

        var sweep = TimeSpan.FromMilliseconds(Math.Max(250, cluster.FailureTimeoutMs / 2));
        _sweepTimer = _time.CreateTimer(_ => SweepDeadNodes(), null, sweep, sweep);

        // Always armed, gated per tick on the live Cluster:RebalanceEnabled — hysteresis lives in
        // the window, the per-shard floor, and the decision rule, not in the tick rate.
        _rebalanceTimer = _time.CreateTimer(
            _ => EvaluateRebalance(), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        LogHubStarted(_logger, NodeListenPort);
    }

    /// <summary>Mints the internal identity assertion the gateway attaches to upstream sessions.</summary>
    public string MintAssertion(Identity identity, bool isGuest, bool isSqlOwner, bool isBulkOwner, DateTimeOffset tokenExpiresAt, bool firesLifecycle, bool isBackupOwner = false)
    {
        var ttlCap = _time.GetUtcNow().AddSeconds(Cluster.AssertionTtlSeconds);
        var expires = tokenExpiresAt < ttlCap ? tokenExpiresAt : ttlCap;
        return InternalIdentityAssertion.Mint(Cluster.Secret, identity, isGuest, isSqlOwner, isBulkOwner, expires, firesLifecycle, isBackupOwner);
    }

    /// <summary>
    /// The shard's current owner and where its websocket endpoint lives, creating and assigning
    /// the shard on first use — instances are created at runtime by playing the game.
    /// </summary>
    public (ShardAssignment Assignment, string PublicAddress) ResolveShard(ShardKey shard)
    {
        var assignment = _membership.EnsureShard(shard, _time.GetUtcNow());
        if (assignment.NodeName is null)
        {
            _membership.AssignUnowned(_time.GetUtcNow());
            assignment = _membership.GetAssignment(shard)!;
        }

        if (assignment.NodeName is null)
            throw new InvalidOperationException($"{shard} has no owner: no live shard node is registered.");
        var node = _membership.Nodes().FirstOrDefault(n => n.NodeName == assignment.NodeName)
            ?? throw new InvalidOperationException($"Node '{assignment.NodeName}' is not registered.");
        return (assignment, node.PublicAddress);
    }

    /// <summary>
    /// The explicit handoff saga: freeze on origin, import on destination, confirm, release on
    /// origin. Each half appended a marker to its own log before acknowledging, so a crash at any
    /// point recovers to exactly one owner — the origin's recovery asks this coordinator (or,
    /// failing that, the destination's log) whether the import happened. A shard with no prior
    /// assignment (the player's first entry) skips the origin half.
    /// </summary>
    public async Task TransferPlayerAsync(Identity player, ShardKey from, ShardKey to, CancellationToken ct = default)
    {
        if (from == to)
            return;
        var handoffId = Guid.NewGuid().ToString("n");
        if (!_sagasByPlayer.TryAdd(player, handoffId))
            throw new InvalidOperationException($"A transfer is already in flight for {player}; one entity, one saga at a time.");
        _sagas[handoffId] = new Saga(handoffId, player, from, to);
        _lastHandoffStartTicks[player] = _time.GetUtcNow().UtcTicks;
        Metrics.HandoffStarted();
        Handoffs.NotifyStarted(player, from, to);
        var completed = false;
        var aborted = false;
        try
        {
            var origin = _membership.GetAssignment(from);
            var originLink = origin?.NodeName is { } originNode
                ? LinkOf(originNode)
                    ?? throw new InvalidOperationException(
                        $"No live link to '{originNode}', the owner of {from}; the player cannot be frozen, so the transfer cannot start.")
                : null;
            WireOp[] rows = [];
            if (origin is not null && originLink is not null)
            {
                var frozen = await originLink.RequestAsync(
                    "handoff-freeze",
                    new HandoffFreeze(handoffId, from.Value, to.Value, origin.FencingToken, player.ToString()),
                    ct).ConfigureAwait(false);
                rows = frozen!.Value.Deserialize<HandoffFrozenRows>()!.Rows;
            }

            var (destination, _) = ResolveShard(to);
            var destinationLink = LinkOf(destination.NodeName!)
                ?? throw new InvalidOperationException($"No live link to '{destination.NodeName}', the owner of {to}.");

            if (HandoffStepHook is { } beforeImport)
                await beforeImport("import").ConfigureAwait(false);
            try
            {
                await destinationLink.RequestAsync(
                    "handoff-import",
                    new HandoffImport(
                        handoffId,
                        origin is null ? HandoffMarker.NoOrigin : from.Value,
                        to.Value,
                        destination.FencingToken,
                        player.ToString(),
                        rows),
                    ct).ConfigureAwait(false);
            }
            catch (NodeLinkException failure) when (failure.IsPeerError)
            {
                // The destination *replied* with an error: the import definitively did not
                // persist, so aborting the origin's freeze is safe and the player stays put.
                if (originLink is not null && origin is not null)
                {
                    await originLink.RequestAsync(
                        "handoff-abort", new HandoffRelease(handoffId, from.Value, origin.FencingToken), ct).ConfigureAwait(false);
                }

                aborted = true;
                LogHandoffAborted(_logger, handoffId, player.ToString(), from.Value, to.Value);
                throw;
            }
            catch (Exception)
            {
                // A timeout or link death: the destination may or may not have durably imported —
                // an ack lost in transit looks exactly like a dead node. Aborting here could make
                // two owners, so the origin's freeze deliberately STAYS: the player is writable
                // nowhere until the origin's reconciler learns the truth from the destination's
                // log and releases or aborts. Unavailable beats duplicated.
                LogHandoffUnresolved(_logger, handoffId, player.ToString(), from.Value, to.Value);
                throw;
            }

            // The destination's import is durable: it owns the player from this instant. The
            // session map flips and the gateways swap BEFORE the release is requested, so the
            // release's row deletions on the origin can never reach a client still attached there.
            completed = true;
            NotifyTransferred(player, from, to);

            if (originLink is not null && origin is not null)
            {
                if (HandoffStepHook is { } beforeRelease)
                    await beforeRelease("release").ConfigureAwait(false);
                await originLink.RequestAsync(
                    "handoff-release", new HandoffRelease(handoffId, from.Value, origin.FencingToken), ct).ConfigureAwait(false);
            }

            LogHandoffCompleted(_logger, handoffId, player.ToString(), from.Value, to.Value);
        }
        finally
        {
            _sagas.TryRemove(handoffId, out _);
            _sagasByPlayer.TryRemove(new KeyValuePair<Identity, string>(player, handoffId));
            Metrics.HandoffEnded(completed, aborted, unresolved: !completed && !aborted);
            if (completed || aborted)
                Handoffs.NotifyClosed(player, from, to, completed);

            // An unresolved exit notifies nothing: the gateway keeps queueing until this node's
            // or the origin's reconciler learns the truth and reports it (handoff-resolved).
        }
    }

    /// <summary>
    /// The destination-authoritative moment: application transfer listeners update the session
    /// map, and gateway observers swap the client's attachment. Listener failures are logged, not
    /// fatal — the transfer itself is already durable.
    /// </summary>
    private void NotifyTransferred(Identity player, ShardKey from, ShardKey to)
    {
        _entityOwner[player] = to;
        foreach (var listener in _services.GetServices<IShardTransferListener>())
        {
            try
            {
                listener.OnTransferred(player, from, to);
            }
            catch (Exception exception)
            {
                LogTransferListenerFailed(_logger, listener.GetType().Name, player.ToString(), exception);
            }
        }

        Handoffs.NotifyDestinationAuthoritative(player, from, to);
    }

    /// <summary>
    /// A boundary-triggered transfer request from an origin node. The hub owns hysteresis: an
    /// in-flight saga for the entity dedupes, and a start within Cluster:HandoffMinIntervalMs of
    /// the entity's previous one is suppressed (EventId 1713) — pacing across a boundary triggers
    /// a bounded number of transfers, never one per step.
    /// </summary>
    private void HandleHandoffRequest(NodeSession session, HandoffRequest request)
    {
        Metrics.HandoffRequestReceived();
        var from = new ShardKey(request.FromShard);
        var to = new ShardKey(request.ToShard);
        var assignment = _membership.GetAssignment(from);
        if (assignment is null || assignment.NodeName != session.NodeName || assignment.FencingToken != request.FencingToken)
        {
            // A previous owner's view of the world; the fencing rule, applied to triggers.
            LogHandoffRequestStale(_logger, request.PlayerHex, from.Value, session.NodeName ?? "?", assignment?.NodeName, assignment?.FencingToken ?? -1, request.FencingToken);
            return;
        }

        var player = new Identity(Convert.FromHexString(request.PlayerHex));
        if (_sagasByPlayer.ContainsKey(player))
            return; // Already moving; the saga's outcome supersedes this trigger.
        if (_entityOwner.TryGetValue(player, out var currentOwner) && currentOwner != from)
        {
            // The requesting shard is not where the last completed transfer put the entity: a
            // stale trigger (a lingering copy, a delayed sweep). A saga from a stale origin would
            // re-import the past over the present, so it never starts.
            LogHandoffRequestStale(_logger, request.PlayerHex, from.Value, session.NodeName ?? "?", $"owner {currentOwner}", -1, request.FencingToken);
            return;
        }

        var minInterval = TimeSpan.TicksPerMillisecond * Math.Max(0, Cluster.HandoffMinIntervalMs);
        if (_lastHandoffStartTicks.TryGetValue(player, out var last)
            && _time.GetUtcNow().UtcTicks - last < minInterval)
        {
            Metrics.HandoffRateLimited();
            LogHandoffRateLimited(_logger, request.PlayerHex, from.Value, to.Value, Cluster.HandoffMinIntervalMs);
            return;
        }

        LogHandoffRequested(_logger, request.PlayerHex, from.Value, to.Value);
        _ = Task.Run(async () =>
        {
            try
            {
                await TransferPlayerAsync(player, from, to, _stopped.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Logged by the saga itself (aborted or unresolved); the trigger owes no reply.
            }
        });
    }

    /// <summary>
    /// A node's reconciler resolved a stranded saga this hub lost track of (a crash, a dead
    /// link). Released means the destination owns the entity: the session map and the gateways
    /// learn it now, late but correct — and idempotently, since the listeners' contract says so.
    /// </summary>
    private void HandleHandoffResolved(HandoffResolved resolved)
    {
        var player = new Identity(Convert.FromHexString(resolved.PlayerHex));
        var from = new ShardKey(resolved.FromShard);
        var to = new ShardKey(resolved.ToShard);
        LogHandoffResolvedRemotely(_logger, resolved.HandoffId, resolved.PlayerHex, resolved.Released, from.Value, to.Value);
        Metrics.HandoffResolvedRemotely(resolved.Released);
        if (resolved.Released)
        {
            NotifyTransferred(player, from, to);
            Handoffs.NotifyClosed(player, from, to, success: true);
        }
        else
        {
            Handoffs.NotifyClosed(player, from, to, success: false);
        }
    }

    /// <summary>
    /// Executes one reducer on the node owning <paramref name="shard"/> — the building block of a
    /// cross-shard saga. One call is one ordinary local transaction on that shard; the
    /// distributed part is only the composition, which is the application's saga to drive (and
    /// compensate). Throws when the owner is unreachable or the reducer rejects; a
    /// <see cref="NodeLinkException.IsPeerError"/> means the step definitively did not commit.
    /// </summary>
    public async Task<ulong> ExecuteOnShardAsync(
        ShardKey shard, string reducer, Identity caller, object?[] arguments, CancellationToken ct = default)
    {
        var (assignment, _) = ResolveShard(shard);
        var link = LinkOf(assignment.NodeName!)
            ?? throw new InvalidOperationException($"No live link to '{assignment.NodeName}', the owner of {shard}.");
        var reply = await link.RequestAsync(
            "shard-execute",
            new ShardExecute(
                shard.Value,
                assignment.FencingToken,
                reducer,
                caller.ToString(),
                Convert.ToBase64String(ReducerArguments.Encode(arguments))),
            ct).ConfigureAwait(false);
        return reply!.Value.Deserialize<ShardExecuteReply>()!.Lsn;
    }

    private readonly ConcurrentDictionary<ShardKey, byte> _drainsInFlight = new();

    /// <summary>
    /// The planned drain — the node-death reassignment path made polite (road-to-0.2 phase 13).
    /// In order: gateways start queueing the shard's calls (and mute their origin attachments, so
    /// the quiesce's socket closures never surface as client errors); the origin quiesces the
    /// shard — fresh snapshot, engine closed — under its current fencing token; membership moves
    /// the shard to the destination under a bumped token; the destination is pushed its
    /// assignments and opens the shard by ordinary recovery (the push is a fast path — the
    /// membership record is already the truth, and a missed push means the destination opens on
    /// its own next heartbeat); gateways swap, re-scope subscriptions, and flush. On any failure
    /// before the reassign, the origin is told to abandon the drain and reopens; a hub death
    /// mid-drain heals by the origin's draining-mark expiry. The shard is writable on at most one
    /// node at every instant: it is closed on the origin before the destination ever learns of it.
    /// </summary>
    public async Task DrainShardAsync(ShardKey shard, string? destinationNode = null, CancellationToken ct = default)
    {
        var assignment = _membership.GetAssignment(shard)
            ?? throw new InvalidOperationException($"{shard} was never created; nothing to drain.");
        if (assignment.NodeName is not { } origin)
            throw new InvalidOperationException($"{shard} has no owner; it needs assignment, not a drain.");
        var destination = destinationNode
            ?? LeastLoadedNodeExcept(origin)
            ?? throw new InvalidOperationException($"No live node other than '{origin}' can take {shard}.");
        if (destination == origin)
            throw new InvalidOperationException($"{shard} already lives on '{destination}'; a drain to the current owner is refused rather than silently done.");
        if (!_membership.Nodes().Any(n => n.NodeName == destination && n.Alive))
            throw new InvalidOperationException($"Node '{destination}' is not registered and alive; a drain must never assign to a corpse.");
        var originLink = LinkOf(origin)
            ?? throw new InvalidOperationException(
                $"No live link to '{origin}', the owner of {shard}. A planned drain needs a cooperative origin — a dead origin is the failure detector's job, not the drain's.");
        if (!_drainsInFlight.TryAdd(shard, 0))
            throw new InvalidOperationException($"A drain of {shard} is already in flight; one shard, one drain at a time.");

        Metrics.DrainStarted();
        ShardMoves.NotifyStarted(shard);
        var quiesced = false;
        try
        {
            // Quiesce and recovery scale with shard size; the link's default request timeout does
            // not. The queue cap is the deployment's statement of drain patience, so borrow it.
            var stepTimeoutMs = Math.Max(30_000, Cluster.DrainQueueTimeoutMs);
            var quiesceStarted = Stopwatch.GetTimestamp();
            await originLink.RequestAsync(
                "shard-drain", new ShardDrain(shard.Value, assignment.FencingToken), ct, stepTimeoutMs).ConfigureAwait(false);
            quiesced = true;
            var quiesceMs = Stopwatch.GetElapsedTime(quiesceStarted).TotalMilliseconds;

            if (DrainStepHook is { } beforeReassign)
                await beforeReassign("reassign").ConfigureAwait(false);
            var next = _membership.Reassign(shard, destination, _time.GetUtcNow());

            if (DrainStepHook is { } beforeApply)
                await beforeApply("apply").ConfigureAwait(false);
            var openStarted = Stopwatch.GetTimestamp();
            if (LinkOf(destination) is { } destinationLink)
            {
                try
                {
                    await destinationLink.RequestAsync(
                        "assignments-apply", new AssignmentsApply(AssignmentsDto(destination)), ct, stepTimeoutMs).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    // Fast path only: membership already records the move, so the destination
                    // opens the shard on its own next heartbeat and the gateways' connect
                    // retries ride out the gap.
                    LogDrainApplyPushFailed(_logger, shard.Value, destination, exception.Message);
                }
            }

            var openMs = Stopwatch.GetElapsedTime(openStarted).TotalMilliseconds;
            ShardMoves.NotifyMoved(shard);
            Metrics.DrainEnded(completed: true);

            // Every completed drain — operator or loop — starts the shard's move floor, so the
            // loop cannot immediately re-move what an operator just placed.
            _lastShardMoveTicks[shard] = _time.GetUtcNow().UtcTicks;
            LogDrainCompleted(_logger, shard.Value, origin, destination, next.FencingToken, quiesceMs, openMs);
        }
        catch (Exception exception)
        {
            if (quiesced)
            {
                // Quiesced but never reassigned: hand the shard back. Best effort — if this
                // notify is lost too, the origin's draining mark expires and the shard reopens on
                // an ordinary heartbeat, which is the same conclusion arrived at slower.
                try
                {
                    await originLink.NotifyAsync(
                        "shard-drain-abort", new ShardDrainAbort(shard.Value), CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }

            ShardMoves.NotifyFailed(shard);
            Metrics.DrainEnded(completed: false);
            LogDrainFailed(_logger, shard.Value, origin, destination, exception.Message);
            throw;
        }
        finally
        {
            _drainsInFlight.TryRemove(shard, out _);
        }
    }

    /// <summary>
    /// One rebalance tick (road-to-0.2 phase 13). The decision rule, in full: a node is hot when
    /// the sum of its shards' sustained write-lock utilizations over
    /// <c>Cluster:RebalanceWindowSeconds</c> exceeds <c>Cluster:RebalanceHotUtilization</c> — and
    /// only shards whose history covers the whole window count, so a spike is never mistaken for
    /// sustained. The move is the largest-load shard on the hottest node for which
    /// <c>target + shard &lt; origin</c> — the pair's maximum strictly improves — sent to the
    /// least-loaded live node. That strict inequality is the anti-flap core: relocating a whole
    /// hotspot to an emptier node fails it, so the loop refuses moves that merely shuffle. Layered
    /// on top: the per-shard move floor (<c>Cluster:ShardMoveMinIntervalMs</c>), one automatic
    /// move in flight at a time, and a hot node the rule cannot help is logged (rate-limited),
    /// never churned — the single-shard case being the granularity ceiling the design record
    /// warns about.
    /// </summary>
    private void EvaluateRebalance()
    {
        try
        {
            var cluster = Cluster;
            var now = _time.GetUtcNow();

            // Ticket bookkeeping never freezes: an expiry must be noticed even while a move is in
            // flight or the loop has been toggled off mid-episode.
            SweepProvisionTicket(cluster, now);

            if (!cluster.RebalanceEnabled
                || Interlocked.CompareExchange(ref _rebalanceMoveInFlight, 0, 0) != 0
                || Interlocked.CompareExchange(ref _scaleInInFlight, 0, 0) != 0
                || !_drainsInFlight.IsEmpty)
            {
                return;
            }

            var window = TimeSpan.FromSeconds(Math.Max(1, cluster.RebalanceWindowSeconds));
            var live = _membership.Nodes()
                .Where(static n => n.Alive)
                .Select(static n => n.NodeName)
                .ToHashSet(StringComparer.Ordinal);
            if (live.Count == 0)
                return;

            var byNode = _membership.AllAssignments()
                .Where(a => a.NodeName is not null && live.Contains(a.NodeName))
                .GroupBy(static a => a.NodeName!, StringComparer.Ordinal)
                .ToDictionary(static g => g.Key, static g => g.Select(static a => a.Shard).ToList(), StringComparer.Ordinal);

            var shardUtilization = new Dictionary<ShardKey, double>();
            var nodeUtilization = live.ToDictionary(static node => node, static _ => 0d, StringComparer.Ordinal);
            var fullCoverage = true;
            foreach (var (node, shards) in byNode)
            {
                foreach (var shard in shards)
                {
                    if (Load.SustainedUtilization(shard, window, now) is { } sustained)
                    {
                        shardUtilization[shard] = sustained;
                        nodeUtilization[node] += sustained;
                    }
                    else
                    {
                        // A shard without whole-window history makes the fleet's aggregate an
                        // underestimate — hot decisions tolerate that (they only need the covered
                        // shards to be hot); the cold decision must not act on it.
                        fullCoverage = false;
                    }
                }
            }

            var hottest = nodeUtilization.OrderByDescending(static pair => pair.Value).ThenBy(static pair => pair.Key, StringComparer.Ordinal).First();
            if (hottest.Value <= cluster.RebalanceHotUtilization)
            {
                // Episode over: the pressure receded without (or after) provisioning. A fresh
                // episode gets a fresh attempt budget; an outstanding ticket keeps its count.
                lock (_capacityLock)
                {
                    if (_ticket is null && !_provisionGaveUp)
                        _provisionAttempts = 0;
                }

                EvaluateScaleIn(cluster, live, byNode, nodeUtilization, fullCoverage, now);
                return;
            }

            var origin = hottest.Key;
            var originShards = byNode.GetValueOrDefault(origin) ?? [];
            if (live.Count >= 2 && originShards.Count > 1)
            {
                var target = nodeUtilization
                    .Where(pair => pair.Key != origin)
                    .OrderBy(static pair => pair.Value)
                    .ThenBy(static pair => pair.Key, StringComparer.Ordinal)
                    .First();
                var floorTicks = TimeSpan.TicksPerMillisecond * Math.Max(0, cluster.ShardMoveMinIntervalMs);
                var candidate = originShards
                    .Where(shard => shardUtilization.GetValueOrDefault(shard) > 0)
                    .Where(shard => !_lastShardMoveTicks.TryGetValue(shard, out var moved) || now.UtcTicks - moved >= floorTicks)
                    .Where(shard => target.Value + shardUtilization[shard] < hottest.Value)
                    .OrderByDescending(shard => shardUtilization[shard])
                    .Select(static shard => (ShardKey?)shard)
                    .FirstOrDefault();
                if (candidate is { } moving)
                {
                    LogRebalanceMoving(
                        _logger, moving.Value, origin, hottest.Value, target.Key, target.Value, shardUtilization[moving]);
                    Interlocked.Exchange(ref _rebalanceMoveInFlight, 1);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await DrainShardAsync(moving, target.Key, _stopped.Token).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // DrainShardAsync already logged the failure (1725) and handed the shard back.
                        }
                        finally
                        {
                            Interlocked.Exchange(ref _rebalanceMoveInFlight, 0);
                        }
                    });
                    return;
                }

                if (ShouldLogStuck(origin, now, cluster.ShardMoveMinIntervalMs))
                    LogRebalanceNoFit(_logger, origin, hottest.Value, target.Key, target.Value);
            }
            else if (live.Count >= 2 && ShouldLogStuck(origin, now, cluster.ShardMoveMinIntervalMs))
            {
                LogRebalanceSingleShardHot(_logger, origin, hottest.Value, originShards.Count == 1 ? originShards[0].Value : 0);
            }

            // The loop's first move — rearranging the nodes it has — is unavailable this tick.
            // The second move exists only behind the capacity seam.
            EvaluateScaleOut(cluster, live, byNode, nodeUtilization, hottest.Value, now);
        }
        catch (Exception exception)
        {
            LogRebalanceEvaluationFailed(_logger, exception.Message);
        }
    }

    /// <summary>A stuck-hot situation persists across ticks; its warning repeats on the move floor's cadence, not per second.</summary>
    private bool ShouldLogStuck(string node, DateTimeOffset now, int minIntervalMs)
    {
        var cadence = TimeSpan.TicksPerMillisecond * Math.Max(1_000, minIntervalMs);
        var last = _rebalanceStuckLogTicks.GetValueOrDefault(node);
        if (now.UtcTicks - last < cadence)
            return false;
        _rebalanceStuckLogTicks[node] = now.UtcTicks;
        return true;
    }

    /// <summary>
    /// The loop's second move (road-to-0.2 phase 14): obtain one more node through the capacity
    /// seam. Taken only when the first move is unavailable — every live node sustained-hot — and
    /// bounded twice over: never past <c>Cluster:MaxNodes</c>, and never while a ticket is already
    /// outstanding or the loop has given up (EventId 1738). A fleet where shards no longer
    /// outnumber nodes is refused too: a new node that cannot receive a whole shard is spend
    /// without relief, the granularity ceiling wearing its capacity face.
    /// </summary>
    private void EvaluateScaleOut(
        ClusterOptions cluster,
        HashSet<string> live,
        Dictionary<string, List<ShardKey>> byNode,
        Dictionary<string, double> nodeUtilization,
        double hottestUtilization,
        DateTimeOffset now)
    {
        if (_provisioner is null)
            return;
        if (nodeUtilization.Count == 0 || nodeUtilization.Values.Any(utilization => utilization <= cluster.RebalanceHotUtilization))
            return;

        var totalShards = byNode.Values.Sum(static shards => shards.Count);
        if (totalShards <= live.Count)
        {
            if (ShouldLogStuck("(capacity-granularity)", now, cluster.ShardMoveMinIntervalMs))
                LogProvisionSkippedGranularity(_logger, live.Count, totalShards);
            return;
        }

        if (live.Count >= cluster.MaxNodes)
        {
            if (ShouldLogStuck("(capacity-ceiling)", now, cluster.ShardMoveMinIntervalMs))
                LogProvisionAtCeiling(_logger, live.Count, cluster.MaxNodes, hottestUtilization);
            return;
        }

        int attempt;
        lock (_capacityLock)
        {
            if (_provisionGaveUp || _ticket is not null || _provisionCallInFlight != 0 || _provisionAttempts >= 2)
                return;
            _provisionCallInFlight = 1;
            attempt = _provisionAttempts + 1;
        }

        var request = new CapacityRequest(
            live.Count,
            cluster.MaxNodes,
            $"every live node sustained-hot over Cluster:RebalanceWindowSeconds ({cluster.RebalanceWindowSeconds}s), " +
            $"hottest at {hottestUtilization:P0}; fleet {live.Count} of Cluster:MaxNodes {cluster.MaxNodes}");
        _ = Task.Run(() => RequestNodeIsolatedAsync(request, attempt, cluster.ProvisionTicketTimeoutMs));
    }

    /// <summary>
    /// The provisioner call, isolated: never on the loop's tick thread, bounded by the ticket
    /// timeout, and a throw counts as a spent attempt — user code on this seam can degrade the
    /// fleet to fixed, never the hub to dead.
    /// </summary>
    private async Task RequestNodeIsolatedAsync(CapacityRequest request, int attempt, int timeoutMs)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stopped.Token);
            cts.CancelAfter(Math.Max(1_000, timeoutMs));
            var ticket = await _provisioner!.RequestNodeAsync(request, cts.Token).ConfigureAwait(false);
            lock (_capacityLock)
            {
                _provisionAttempts = attempt;
                _ticket = new OutstandingTicket(ticket, _time.GetUtcNow(), attempt);
            }

            Metrics.ProvisionRequested();
            LogProvisionTicketIssued(_logger, ticket.TicketId, ticket.NodeName, attempt, request.Reason);
        }
        catch (Exception exception)
        {
            bool gaveUp;
            lock (_capacityLock)
            {
                _provisionAttempts = attempt;
                gaveUp = _provisionGaveUp = attempt >= 2;
            }

            LogProvisionerCallFailed(_logger, "RequestNodeAsync", exception.Message);
            if (gaveUp)
                LogProvisionGaveUp(_logger, attempt);
        }
        finally
        {
            lock (_capacityLock)
            {
                _provisionCallInFlight = 0;
            }
        }
    }

    /// <summary>
    /// Expires an outstanding ticket whose named node never joined. The first expiry leaves the
    /// attempt budget open, so the loop re-requests exactly once if the pressure persists; the
    /// second latches the give-up alert — money is involved, and the posture on repeated failure
    /// is <em>tell a human</em>, never <em>keep trying</em>. The expired name is remembered: an
    /// instance limping in later is surplus, not capacity (see <see cref="OnNodeRegistered"/>).
    /// </summary>
    private void SweepProvisionTicket(ClusterOptions cluster, DateTimeOffset now)
    {
        OutstandingTicket? outstanding;
        lock (_capacityLock)
        {
            outstanding = _ticket;
        }

        if (outstanding is null)
            return;

        // Fulfillment by membership, not only by the registration hook: the node may have joined
        // in the instant between the provisioner returning its ticket and the hub recording it —
        // registration would have checked the slot before the slot existed. The tick notices.
        if (_membership.Nodes().Any(n => n.Alive && n.NodeName == outstanding.Ticket.NodeName))
        {
            var fulfilled = false;
            lock (_capacityLock)
            {
                if (ReferenceEquals(_ticket, outstanding))
                {
                    _ticket = null;
                    _provisionAttempts = 0;
                    _provisionGaveUp = false;
                    fulfilled = true;
                }
            }

            if (fulfilled)
            {
                _nodeProvisionedTicks[outstanding.Ticket.NodeName] = now.UtcTicks;
                var latencyMs = (now - outstanding.RequestedAt).TotalMilliseconds;
                _provisionLatency.Record(latencyMs);
                Metrics.ProvisionFulfilled();
                LogProvisionFulfilled(_logger, outstanding.Ticket.NodeName, outstanding.Ticket.TicketId, latencyMs);
            }

            return;
        }

        if (now - outstanding.RequestedAt <= TimeSpan.FromMilliseconds(Math.Max(1_000, cluster.ProvisionTicketTimeoutMs)))
            return;

        var expired = false;
        var gaveUp = false;
        lock (_capacityLock)
        {
            if (ReferenceEquals(_ticket, outstanding))
            {
                _ticket = null;
                _expiredTickets[outstanding.Ticket.NodeName] = outstanding.Ticket.TicketId;
                gaveUp = _provisionGaveUp = outstanding.Attempt >= 2;
                expired = true;
            }
        }

        if (!expired)
            return;
        Metrics.ProvisionExpired();
        LogProvisionTicketExpired(
            _logger, outstanding.Ticket.TicketId, outstanding.Ticket.NodeName, cluster.ProvisionTicketTimeoutMs, outstanding.Attempt);
        if (gaveUp)
            LogProvisionGaveUp(_logger, outstanding.Attempt);
    }

    /// <summary>
    /// Capacity bookkeeping on every node registration. A node fulfilling the outstanding ticket
    /// clears the whole episode — attempts, give-up latch — and records the provision latency. A
    /// node named by an <em>expired</em> ticket is the at-least-once contract's surplus: if
    /// registration handed it nothing it is decommissioned (fencing already guarantees it can
    /// write nothing it was never assigned); if unowned shards existed and registration gave it
    /// some, capacity arrived late but arrived, and it stays.
    /// </summary>
    private void OnNodeRegistered(string nodeName)
    {
        OutstandingTicket? fulfilled;
        string? expiredTicketId = null;
        lock (_capacityLock)
        {
            if (_ticket is { } ticket && ticket.Ticket.NodeName == nodeName)
            {
                fulfilled = ticket;
                _ticket = null;
                _provisionAttempts = 0;
                _provisionGaveUp = false;
            }
            else
            {
                fulfilled = null;
                _expiredTickets.Remove(nodeName, out expiredTicketId);
            }
        }

        if (fulfilled is { } issued)
        {
            _nodeProvisionedTicks[nodeName] = _time.GetUtcNow().UtcTicks;
            var latencyMs = (_time.GetUtcNow() - issued.RequestedAt).TotalMilliseconds;
            _provisionLatency.Record(latencyMs);
            Metrics.ProvisionFulfilled();
            LogProvisionFulfilled(_logger, nodeName, issued.Ticket.TicketId, latencyMs);
            return;
        }

        if (expiredTicketId is null)
            return;

        if (_membership.AssignmentsFor(nodeName).Count > 0)
        {
            LogProvisionLateArrivalKept(_logger, nodeName, expiredTicketId);
            return;
        }

        LogProvisionLateArrivalDecommissioned(_logger, nodeName, expiredTicketId);
        _ = Task.Run(() => DecommissionIsolatedAsync(nodeName));
    }

    /// <summary>The decommission call, isolated the same way as the request call.</summary>
    private async Task DecommissionIsolatedAsync(string nodeName)
    {
        Metrics.DecommissionRequested();
        _decommissions.Add(1);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stopped.Token);
            cts.CancelAfter(Math.Max(1_000, Cluster.ProvisionTicketTimeoutMs));
            await _provisioner!.DecommissionAsync(nodeName, cts.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            LogProvisionerCallFailed(_logger, "DecommissionAsync", exception.Message);
        }
    }

    /// <summary>
    /// Scale-in (road-to-0.2 phase 14), evaluated only when nothing is hot: when the fleet's
    /// aggregate sustained load would fit under <c>Cluster:RebalanceColdUtilization</c> on one
    /// node fewer, the emptiest live node is consolidated away and handed back to the
    /// provisioner. Behind its own switch, floored by <c>Cluster:MinNodes</c>, refused on partial
    /// load-view coverage (never consolidate on an underestimate), and paced by
    /// <c>Cluster:ScaleInCooldownMs</c> — which also exempts freshly provisioned nodes, because
    /// the newest node is the emptiest by definition and the two fleet moves must never take
    /// turns.
    /// </summary>
    private void EvaluateScaleIn(
        ClusterOptions cluster,
        HashSet<string> live,
        Dictionary<string, List<ShardKey>> byNode,
        Dictionary<string, double> nodeUtilization,
        bool fullCoverage,
        DateTimeOffset now)
    {
        if (_provisioner is null || !cluster.ScaleInEnabled || !fullCoverage)
            return;
        if (live.Count <= Math.Max(1, cluster.MinNodes))
            return;

        // Consolidation only in a fully connected fleet: a membership-alive node the hub cannot
        // reach is either dying (a decommission still settling toward the failure sweep) or
        // partitioned — either way the fleet is in flux, and scale-in is the one move with no
        // urgency whatsoever.
        if (live.Any(node => LinkOf(node) is null))
            return;
        lock (_capacityLock)
        {
            if (_ticket is not null)
                return;
        }

        var cooldownTicks = TimeSpan.TicksPerMillisecond * Math.Max(0, cluster.ScaleInCooldownMs);
        if (now.UtcTicks - Interlocked.Read(ref _lastScaleInTicks) < cooldownTicks)
            return;

        var aggregate = nodeUtilization.Values.Sum();
        var remainder = aggregate / (live.Count - 1);
        if (remainder >= cluster.RebalanceColdUtilization)
            return;

        var victim = nodeUtilization
            .Where(pair => now.UtcTicks - _nodeProvisionedTicks.GetValueOrDefault(pair.Key) >= cooldownTicks)
            .Where(pair => LinkOf(pair.Key) is not null)
            .OrderBy(static pair => pair.Value)
            .ThenBy(static pair => pair.Key, StringComparer.Ordinal)
            .Select(static pair => (string?)pair.Key)
            .FirstOrDefault();
        if (victim is null || Interlocked.CompareExchange(ref _scaleInInFlight, 1, 0) != 0)
            return;

        LogScaleInStarting(
            _logger, victim, nodeUtilization[victim], aggregate, live.Count, remainder,
            cluster.RebalanceColdUtilization, byNode.GetValueOrDefault(victim)?.Count ?? 0);
        _ = Task.Run(() => ConsolidateAsync(victim));
    }

    /// <summary>
    /// One consolidation session: drain the victim's shards onto the rest one at a time — phase
    /// 13 drains, nothing new touches a shard — re-checking the cold condition before every drain
    /// and once more at the last moment, because decommissioning a node the loop now needs is the
    /// one mistake players would see. Aborting is free at every step: a partially drained node is
    /// just an emptier node, and the loop's ordinary rules take it from there.
    /// </summary>
    private async Task ConsolidateAsync(string victim)
    {
        try
        {
            var cluster = Cluster;
            var budget = Math.Max(4, 2 * _membership.AssignmentsFor(victim).Count);
            while (true)
            {
                var owned = _membership.AssignmentsFor(victim);
                if (owned.Count == 0)
                    break;
                if (budget-- <= 0)
                {
                    LogScaleInAborted(_logger, victim, "the node kept receiving new shards faster than the drains removed them");
                    return;
                }

                if (!FleetIsColdOnRemainder(cluster))
                {
                    LogScaleInAborted(_logger, victim, "the fleet warmed while consolidating");
                    return;
                }

                await DrainShardAsync(owned[0].Shard, destinationNode: null, _stopped.Token).ConfigureAwait(false);
            }

            if (!FleetIsColdOnRemainder(cluster) || _membership.AssignmentsFor(victim).Count > 0)
            {
                LogScaleInAborted(_logger, victim, "the last-moment re-check failed");
                return;
            }

            Interlocked.Exchange(ref _lastScaleInTicks, _time.GetUtcNow().UtcTicks);
            LogScaleInDecommissioning(_logger, victim);
            await DecommissionIsolatedAsync(victim).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            LogScaleInAborted(_logger, victim, exception.Message);
        }
        finally
        {
            Interlocked.Exchange(ref _scaleInInFlight, 0);
        }
    }

    /// <summary>
    /// The scale-in condition, recomputed from the stores: aggregate sustained load across the
    /// live fleet fits under the cold threshold on one node fewer. False on any shard without
    /// whole-window coverage — a cold call made on partial data is how a loop decommissions a
    /// node it needs.
    /// </summary>
    private bool FleetIsColdOnRemainder(ClusterOptions cluster)
    {
        var now = _time.GetUtcNow();
        var window = TimeSpan.FromSeconds(Math.Max(1, cluster.RebalanceWindowSeconds));
        var live = _membership.Nodes()
            .Where(static n => n.Alive)
            .Select(static n => n.NodeName)
            .ToHashSet(StringComparer.Ordinal);
        if (live.Count <= Math.Max(1, cluster.MinNodes))
            return false;

        var aggregate = 0d;
        foreach (var assignment in _membership.AllAssignments())
        {
            if (assignment.NodeName is not { } owner || !live.Contains(owner))
                continue;
            if (Load.SustainedUtilization(assignment.Shard, window, now) is not { } sustained)
                return false;
            aggregate += sustained;
        }

        return aggregate / (live.Count - 1) < cluster.RebalanceColdUtilization;
    }

    /// <summary>The live node owning the fewest shards, excluding the drain's origin — the default destination.</summary>
    private string? LeastLoadedNodeExcept(string origin)
    {
        var counts = _membership.AllAssignments()
            .Where(static a => a.NodeName is not null)
            .GroupBy(static a => a.NodeName!, StringComparer.Ordinal)
            .ToDictionary(static g => g.Key, static g => g.Count(), StringComparer.Ordinal);
        return _membership.Nodes()
            .Where(n => n.Alive && n.NodeName != origin)
            .OrderBy(n => counts.GetValueOrDefault(n.NodeName))
            .ThenBy(static n => n.NodeName, StringComparer.Ordinal)
            .Select(static n => n.NodeName)
            .FirstOrDefault();
    }

    private NodeLink? LinkOf(string nodeName) =>
        _linksByNode.TryGetValue(nodeName, out var link) && link.IsAlive ? link : null;

    private async Task AcceptLoopAsync()
    {
        var ct = _stopped.Token;
        while (!ct.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener!.AcceptTcpClientAsync(ct).ConfigureAwait(false);
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                return;
            }

            var link = new NodeLink(client, Metrics);
            var session = new NodeSession { ServerNonce = LinkProof.NewNonce() };
            link.Tag = session;
            link.Handler = (l, type, body) => HandleAsync(l, session, type, body);
            link.Closed += OnLinkClosed;
            link.Start();
            _ = link.NotifyAsync("challenge", new { session.ServerNonce }, ct);
        }
    }

    private void OnLinkClosed(NodeLink link)
    {
        if (link.Tag is NodeSession { NodeName: { } name })
            _linksByNode.TryRemove(new KeyValuePair<string, NodeLink>(name, link));
    }

    private async Task<object?> HandleAsync(NodeLink link, NodeSession session, string type, JsonElement? body)
    {
        if (type == "auth")
            return HandleAuth(link, session, body!.Value.Deserialize<AuthRequest>()!);

        if (session.NodeName is null)
            throw new InvalidOperationException("The link is not authenticated.");

        switch (type)
        {
            case "heartbeat":
                _membership.Heartbeat(session.NodeName, _time.GetUtcNow());
                if (body?.Deserialize<HeartbeatRequest>() is { Loads.Length: > 0 } heartbeat)
                    Load.Record(session.NodeName, heartbeat.Loads, _time.GetUtcNow());

                // A node returning from a partition is alive again; shards orphaned while it was
                // suspected dead get owners before the reply reports this node's assignments.
                _membership.AssignUnowned(_time.GetUtcNow());
                return new HeartbeatReply(AssignmentsDto(session.NodeName));
            case "replica-subscribe":
                var subscribe = body!.Value.Deserialize<ReplicaSubscribe>()!;
                StartReplicaPump(link, session, subscribe.FromLsn);
                return null;
            case "events-forward":
                var forward = body!.Value.Deserialize<EventsForward>()!;
                await _foreignEvents.DispatchAsync(session.NodeName, forward.ShardValue, forward, _stopped.Token).ConfigureAwait(false);
                return null;
            case "border-subscribe":
            {
                // Routed, not answered: the hub knows who owns the owner shard; the stream itself
                // is owner node -> hub -> observer node, so the star topology carries it.
                var borderSubscribe = body!.Value.Deserialize<BorderSubscribe>()!;
                var owner = _membership.GetAssignment(new ShardKey(borderSubscribe.OwnerShard));
                if (owner?.NodeName is not { } ownerNode || LinkOf(ownerNode) is not { } ownerLink)
                    return new BorderSubscribeReply(false); // Empty world region, or its owner is down: benign, retry later.
                await ownerLink.RequestAsync("border-subscribe-owner", borderSubscribe, _stopped.Token).ConfigureAwait(false);
                return new BorderSubscribeReply(true);
            }

            case "border-batch":
            {
                var batch = body!.Value.Deserialize<BorderBatch>()!;
                var observer = _membership.GetAssignment(new ShardKey(batch.ObserverShard));
                if (observer?.NodeName is not { } observerNode || LinkOf(observerNode) is not { } observerLink)
                    throw new InvalidOperationException($"Shard {batch.ObserverShard}'s owner is unreachable; the owner retries.");
                await observerLink.RequestAsync("border-apply", batch, _stopped.Token).ConfigureAwait(false);
                return null;
            }

            case "border-reset":
            {
                var reset = body!.Value.Deserialize<BorderReset>()!;
                var observer = _membership.GetAssignment(new ShardKey(reset.ObserverShard));
                if (observer?.NodeName is not { } observerNode || LinkOf(observerNode) is not { } observerLink)
                    throw new InvalidOperationException($"Shard {reset.ObserverShard}'s owner is unreachable; the owner retries.");
                await observerLink.RequestAsync("border-reset-apply", reset, _stopped.Token).ConfigureAwait(false);
                return null;
            }

            case "handoff-request":
                HandleHandoffRequest(session, body!.Value.Deserialize<HandoffRequest>()!);
                return null;
            case "handoff-approach":
            {
                var approach = body!.Value.Deserialize<HandoffApproach>()!;
                var player = new Identity(Convert.FromHexString(approach.PlayerHex));
                foreach (var target in approach.ToShards)
                {
                    // Ensure the destination exists and has an owner, so a pre-opened session (and
                    // the eventual import) has somewhere to land — approach is what wakes up an
                    // empty neighbouring region.
                    try
                    {
                        ResolveShard(new ShardKey(target));
                    }
                    catch (InvalidOperationException)
                    {
                        continue; // No live node can own it right now; the approach re-fires.
                    }

                    Handoffs.NotifyApproach(player, new ShardKey(approach.FromShard), new ShardKey(target));
                }

                return null;
            }

            case "handoff-resolved":
                HandleHandoffResolved(body!.Value.Deserialize<HandoffResolved>()!);
                return null;
            case "handoff-query":
                return await HandleHandoffQueryAsync(body!.Value.Deserialize<HandoffQuery>()!).ConfigureAwait(false);
            case "handoff-freeze-query":
                return await HandleFreezeQueryAsync(body!.Value.Deserialize<HandoffFreezeQuery>()!).ConfigureAwait(false);
            default:
                throw new InvalidOperationException($"Unknown node-link message '{type}'.");
        }
    }

    private AuthReply HandleAuth(NodeLink link, NodeSession session, AuthRequest request)
    {
        var cluster = Cluster;
        if (!LinkProof.Verify(cluster.Secret, session.ServerNonce, request.NodeName, request.Proof))
        {
            LogLinkAuthFailed(_logger, request.NodeName);
            throw new InvalidOperationException("Node authentication failed: the proof does not verify against the cluster secret.");
        }

        session.NodeName = request.NodeName;
        _linksByNode[request.NodeName] = link;
        _membership.RegisterNode(request.NodeName, request.PublicAddress, _time.GetUtcNow());
        _membership.AssignUnowned(_time.GetUtcNow());
        LogNodeRegistered(_logger, request.NodeName, request.PublicAddress);
        OnNodeRegistered(request.NodeName);
        return new AuthReply(
            LinkProof.Compute(cluster.Secret, request.NodeNonce, "hub"),
            AssignmentsDto(request.NodeName),
            cluster.FailureTimeoutMs);
    }

    private async Task<HandoffQueryReply> HandleHandoffQueryAsync(HandoffQuery query)
    {
        // An in-flight saga always answers "wait": the coordinator is still driving — the origin's
        // reconciler must neither abort (the import may be about to land) nor release (racing the
        // coordinator's own release step). It takes over only once the saga object is gone, which
        // the coordinator guarantees on every exit path.
        if (_sagas.ContainsKey(query.HandoffId))
            throw new InvalidOperationException("The handoff is still in flight; retry.");

        var destination = _membership.GetAssignment(new ShardKey(query.ToShard));
        if (destination?.NodeName is not { } owner || LinkOf(owner) is not { } link)
            throw new InvalidOperationException($"The destination shard's owner is unreachable; retry.");
        var reply = await link.RequestAsync("handoff-query-owner", query, _stopped.Token).ConfigureAwait(false);
        return reply!.Value.Deserialize<HandoffQueryReply>()!;
    }

    /// <summary>
    /// Routes a destination reconciler's "is the origin's freeze still pending?" to the origin
    /// shard's current owner. An in-flight saga answers "pending" without asking — the freeze
    /// exists by construction while its saga runs.
    /// </summary>
    private async Task<HandoffFreezeQueryReply> HandleFreezeQueryAsync(HandoffFreezeQuery query)
    {
        if (_sagas.ContainsKey(query.HandoffId))
            return new HandoffFreezeQueryReply(true);

        var origin = _membership.GetAssignment(new ShardKey(query.FromShard));
        if (origin?.NodeName is not { } owner || LinkOf(owner) is not { } link)
            throw new InvalidOperationException("The origin shard's owner is unreachable; retry.");
        var reply = await link.RequestAsync("handoff-freeze-query-owner", query, _stopped.Token).ConfigureAwait(false);
        return reply!.Value.Deserialize<HandoffFreezeQueryReply>()!;
    }

    private ShardAssignmentDto[] AssignmentsDto(string nodeName) =>
        [.. _membership.AssignmentsFor(nodeName).Select(ShardAssignmentDto.From)];

    /// <summary>
    /// One pump per subscribed link: reads the hub log from the node's cursor, filters Replicated
    /// ops, and ships batches — each awaited, so delivery is ordered and flow-controlled by the
    /// node's ack. Log-driven on purpose: there is no queue to overflow or to disagree with the
    /// log, and a reconnecting node just re-subscribes from its persisted cursor.
    /// </summary>
    private void StartReplicaPump(NodeLink link, NodeSession session, ulong fromLsn)
    {
        session.LowerReplicaCursor((long)fromLsn);
        session.ReplicaPump ??= Task.Run(async () =>
        {
            var ct = _stopped.Token;
            while (!ct.IsCancellationRequested && link.IsAlive)
            {
                try
                {
                    var cursor = (ulong)Math.Max(0, session.ReplicaCursor);

                    // A cursor below the truncation base cannot be served from the log — the
                    // gap's records are gone, and streaming from BaseLsn+1 would silently lose
                    // every Replicated update in between (the phase 08 silent-gap bug class).
                    // Bootstrap instead: full current Replicated state at one LSN.
                    if (cursor < _engine.Log.BaseLsn)
                    {
                        await BootstrapReplicaAsync(link, session, cursor, ct).ConfigureAwait(false);
                        continue;
                    }

                    // Durable, not head: ReadFrom serves nothing beyond the durability watermark,
                    // so judging availability by the head would busy-spin through the gap while a
                    // commit's fsync is still in flight — and a replica must not receive an LSN a
                    // crash on this hub could untell anyway.
                    var head = _engine.Log.DurableLsn;
                    if (cursor < head)
                    {
                        var lsns = new List<ulong>();
                        var records = new List<WireOp[]>();
                        var scanned = cursor;
                        foreach (var record in _engine.Log.ReadFrom(cursor + 1))
                        {
                            scanned = record.Lsn;

                            // A node whose replica cursor lagged across an additive schema
                            // migration must receive current-shape rows — its engine stores what
                            // this stream sends verbatim.
                            var replicated = ReplicatedOps(_engine.TransformToCurrentShape(record));
                            if (replicated.Length > 0)
                            {
                                lsns.Add(record.Lsn);
                                records.Add(replicated);
                            }

                            if (records.Count >= 128)
                                break;
                        }

                        if (records.Count > 0)
                        {
                            await link.RequestAsync("replica-batch", new ReplicaBatch([.. lsns], [.. records]), ct)
                                .ConfigureAwait(false);
                        }

                        // Advance only if no re-subscribe moved the cursor back meanwhile.
                        if (session.ReplicaCursor == (long)cursor)
                            session.ReplicaCursor = (long)scanned;
                        continue;
                    }

                    await session.ReplicaSignal.WaitAsync(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                    if (!link.IsAlive)
                        return; // The node re-subscribes on reconnect, from its persisted cursors.
                    try
                    {
                        // A transient batch failure must not end replication for the link's life.
                        await Task.Delay(250, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
            }
        });
    }

    /// <summary>
    /// Sends the full current Replicated table set, captured under the write lock at one LSN, as
    /// a reset the node applies with upsert-plus-absent-delete semantics — matching phase 08's
    /// Postgres bootstrap rigor, where a pure upsert would resurrect rows deleted during the gap.
    /// The stream then resumes from the snapshot LSN.
    /// </summary>
    private async Task BootstrapReplicaAsync(NodeLink link, NodeSession session, ulong staleCursor, CancellationToken ct)
    {
        ulong resetLsn = 0;
        var tables = new List<ReplicaTableSnapshot>();
        _engine.ReadConsistent(head =>
        {
            resetLsn = head;
            foreach (var table in _engine.Schema.Tables)
            {
                if (table.Placement != Placement.Replicated)
                    continue;
                var rows = _engine.HotStore.Scan(table.Id)
                    .Select(pair => new WireOp((byte)RowOpKind.Insert, table.Id.Value, pair.Key.ToArray(), pair.Value.ToArray()))
                    .ToArray();
                tables.Add(new ReplicaTableSnapshot(table.Id.Value, rows));
            }
        });

        await link.RequestAsync("replica-reset", new ReplicaReset(resetLsn, [.. tables]), ct).ConfigureAwait(false);
        LogReplicaBootstrapped(
            _logger,
            (link.Tag as NodeSession)?.NodeName ?? "?",
            staleCursor,
            _engine.Log.BaseLsn,
            resetLsn,
            tables.Sum(static t => t.Rows.Length));
        if (session.ReplicaCursor == (long)staleCursor)
            session.ReplicaCursor = (long)resetLsn;
    }

    private WireOp[] ReplicatedOps(CommitRecord record)
    {
        List<WireOp>? ops = null;
        foreach (var op in record.WriteSet)
        {
            if (_engine.Schema.TryGet(op.Table, out var table) && table.Placement == Placement.Replicated)
                (ops ??= []).Add(WireOp.From(op));
        }

        return ops is null ? [] : [.. ops];
    }

    private sealed class ReplicaSignaller(HubRuntime hub) : ICommitObserver
    {
        public void OnCommit(CommitRecord record)
        {
            foreach (var link in hub._linksByNode.Values)
            {
                if (link.Tag is NodeSession session)
                {
                    try
                    {
                        session.ReplicaSignal.Release();
                    }
                    catch (SemaphoreFullException)
                    {
                    }
                }
            }
        }
    }

    private void SweepDeadNodes()
    {
        var now = _time.GetUtcNow();
        var timeout = TimeSpan.FromMilliseconds(Cluster.FailureTimeoutMs);
        foreach (var node in _membership.Nodes())
        {
            if (!node.Alive || now - node.LastSeen <= timeout)
                continue;
            var moved = _membership.MarkDead(node.NodeName, now);
            LogNodeSuspectedDead(_logger, node.NodeName, (now - node.LastSeen).TotalMilliseconds, moved.Count);
        }

        // The rate-limit memory is bounded: entries older than any conceivable window are noise.
        var floor = now.AddMilliseconds(-10L * Math.Max(1, Cluster.HandoffMinIntervalMs)).UtcTicks;
        foreach (var (player, ticks) in _lastHandoffStartTicks)
        {
            if (ticks < floor)
                _lastHandoffStartTicks.TryRemove(new KeyValuePair<Identity, long>(player, ticks));
        }
    }

    public void Dispose()
    {
        _stopped.Cancel();
        _sweepTimer?.Dispose();
        _rebalanceTimer?.Dispose();
        _listener?.Stop();
        foreach (var link in _linksByNode.Values)
            link.Dispose();
        Load.Dispose();
        _capacityMeter.Dispose();
    }

    [LoggerMessage(EventId = 1700, EventName = "ClusterHubStarted", Level = LogLevel.Information,
        Message = "Cluster hub started: node-link listener on port {Port}.")]
    private static partial void LogHubStarted(ILogger logger, int port);

    [LoggerMessage(EventId = 1701, EventName = "ClusterNodeRegistered", Level = LogLevel.Information,
        Message = "Shard node '{NodeName}' registered at {PublicAddress}.")]
    private static partial void LogNodeRegistered(ILogger logger, string nodeName, string publicAddress);

    [LoggerMessage(EventId = 1702, EventName = "ClusterNodeSuspectedDead", Level = LogLevel.Warning,
        Message = "Shard node '{NodeName}' silent for {SilenceMs:F0}ms (past Cluster:FailureTimeoutMs); marked dead and {MovedShards} shard(s) reassigned with bumped fencing tokens.")]
    private static partial void LogNodeSuspectedDead(ILogger logger, string nodeName, double silenceMs, int movedShards);

    [LoggerMessage(EventId = 1703, EventName = "ClusterLinkAuthFailed", Level = LogLevel.Warning,
        Message = "A node link claiming to be '{NodeName}' failed cluster-secret authentication and was refused.")]
    private static partial void LogLinkAuthFailed(ILogger logger, string nodeName);

    [LoggerMessage(EventId = 1705, EventName = "HandoffCompleted", Level = LogLevel.Information,
        Message = "Handoff {HandoffId}: player {Player} moved shard {FromShard} -> {ToShard}.")]
    private static partial void LogHandoffCompleted(ILogger logger, string handoffId, string player, ulong fromShard, ulong toShard);

    [LoggerMessage(EventId = 1706, EventName = "HandoffAborted", Level = LogLevel.Warning,
        Message = "Handoff {HandoffId}: import failed; player {Player} stays on shard {FromShard} (destination was {ToShard}).")]
    private static partial void LogHandoffAborted(ILogger logger, string handoffId, string player, ulong fromShard, ulong toShard);

    [LoggerMessage(EventId = 1710, EventName = "HandoffUnresolved", Level = LogLevel.Warning,
        Message = "Handoff {HandoffId}: the import request to shard {ToShard}'s owner timed out or the link died — the " +
            "destination may or may not hold the import. Player {Player} stays frozen on shard {FromShard} until the " +
            "origin's reconciler learns the truth from the destination's log; unavailable beats duplicated.")]
    private static partial void LogHandoffUnresolved(ILogger logger, string handoffId, string player, ulong fromShard, ulong toShard);

    [LoggerMessage(EventId = 1722, EventName = "HandoffRequestStale", Level = LogLevel.Debug,
        Message = "Dropped a transfer request for {Player} from shard {FromShard}: sender '{SenderNode}' vs owner '{OwnerNode}', token {OwnerToken} vs {RequestToken}.")]
    private static partial void LogHandoffRequestStale(
        ILogger logger, string player, ulong fromShard, string senderNode, string? ownerNode, long ownerToken, long requestToken);

    [LoggerMessage(EventId = 1712, EventName = "HandoffRequested", Level = LogLevel.Information,
        Message = "Player {Player} crossed shard {FromShard}'s boundary past the margin; the origin requested a transfer to shard {ToShard}.")]
    private static partial void LogHandoffRequested(ILogger logger, string player, ulong fromShard, ulong toShard);

    [LoggerMessage(EventId = 1713, EventName = "HandoffRateLimited", Level = LogLevel.Debug,
        Message = "Suppressed a boundary-triggered transfer of player {Player} (shard {FromShard} -> {ToShard}): within " +
            "Cluster:HandoffMinIntervalMs ({MinIntervalMs}ms) of the previous transfer. Hysteresis working as intended.")]
    private static partial void LogHandoffRateLimited(ILogger logger, string player, ulong fromShard, ulong toShard, int minIntervalMs);

    [LoggerMessage(EventId = 1714, EventName = "HandoffResolvedRemotely", Level = LogLevel.Information,
        Message = "Handoff {HandoffId}: a node's reconciler resolved the stranded saga for player {Player} — released: " +
            "{Released} (shard {FromShard} -> {ToShard}); session maps and gateways were notified late but correctly.")]
    private static partial void LogHandoffResolvedRemotely(
        ILogger logger, string handoffId, string player, bool released, ulong fromShard, ulong toShard);

    [LoggerMessage(EventId = 1717, EventName = "TransferListenerFailed", Level = LogLevel.Error,
        Message = "IShardTransferListener '{Listener}' threw for player {Player}; the transfer itself is durable, but the " +
            "application's session map may be stale until the listener is invoked again (it is idempotent by contract).")]
    private static partial void LogTransferListenerFailed(ILogger logger, string listener, string player, Exception exception);

    [LoggerMessage(EventId = 1724, EventName = "ShardDrainCompleted", Level = LogLevel.Information,
        Message = "Shard {Shard} drained from node '{Origin}' to node '{Destination}' under fencing token {FencingToken}: " +
            "quiesce (snapshot + close) {QuiesceMs:F0}ms, destination open (recovery) {OpenMs:F0}ms. Gateways swapped; clients observed a pause, not a disconnect.")]
    private static partial void LogDrainCompleted(
        ILogger logger, ulong shard, string origin, string destination, long fencingToken, double quiesceMs, double openMs);

    [LoggerMessage(EventId = 1725, EventName = "ShardDrainFailed", Level = LogLevel.Warning,
        Message = "The drain of shard {Shard} from node '{Origin}' to node '{Destination}' failed ({Reason}). " +
            "If the shard was already quiesced it was handed back to the origin (or reopens there when the draining mark expires); queued gateway calls flushed to the current owner.")]
    private static partial void LogDrainFailed(ILogger logger, ulong shard, string origin, string destination, string reason);

    [LoggerMessage(EventId = 1729, EventName = "ShardDrainApplyPushFailed", Level = LogLevel.Warning,
        Message = "The drain of shard {Shard} could not push assignments to destination '{Destination}' ({Reason}); membership already records the move, " +
            "so the destination opens the shard on its next heartbeat and gateway connect retries cover the gap.")]
    private static partial void LogDrainApplyPushFailed(ILogger logger, ulong shard, string destination, string reason);

    [LoggerMessage(EventId = 1731, EventName = "RebalanceMoving", Level = LogLevel.Information,
        Message = "Rebalance: node '{Origin}' is sustained-hot ({OriginUtilization:P0} over Cluster:RebalanceWindowSeconds, past Cluster:RebalanceHotUtilization); " +
            "moving shard {Shard} (its own sustained load {ShardUtilization:P0}) to '{Target}' ({TargetUtilization:P0}) — the pair's maximum strictly improves.")]
    private static partial void LogRebalanceMoving(
        ILogger logger, ulong shard, string origin, double originUtilization, string target, double targetUtilization, double shardUtilization);

    [LoggerMessage(EventId = 1732, EventName = "RebalanceSingleShardHot", Level = LogLevel.Warning,
        Message = "Node '{Node}' is sustained-hot ({Utilization:P0}) but owns a single shard ({Shard}) — nothing can be shed. This is the granularity " +
            "ceiling: the unit of elasticity is the shard, and this node's whole load lives in one. Finer split lines at strategy registration are the fix " +
            "(docs/design/elastic-rebalancing.md); no cluster size changes this. Logged at most once per Cluster:ShardMoveMinIntervalMs.")]
    private static partial void LogRebalanceSingleShardHot(ILogger logger, string node, double utilization, ulong shard);

    [LoggerMessage(EventId = 1733, EventName = "RebalanceNoFit", Level = LogLevel.Warning,
        Message = "Node '{Node}' is sustained-hot ({Utilization:P0}) but no move helps: every candidate shard is inside its move floor, has no sustained " +
            "load of its own, or would leave the pair's maximum no better against '{Target}' ({TargetUtilization:P0}) — relocating a whole hotspot is not " +
            "a rebalance. Logged at most once per Cluster:ShardMoveMinIntervalMs.")]
    private static partial void LogRebalanceNoFit(ILogger logger, string node, double utilization, string target, double targetUtilization);

    [LoggerMessage(EventId = 1735, EventName = "ProvisionTicketIssued", Level = LogLevel.Information,
        Message = "Capacity: provision ticket '{TicketId}' issued for node '{NodeName}' (attempt {Attempt}): {Reason}. " +
                  "Waiting Cluster:ProvisionTicketTimeoutMs for it to join membership.")]
    private static partial void LogProvisionTicketIssued(ILogger logger, string ticketId, string nodeName, int attempt, string reason);

    [LoggerMessage(EventId = 1736, EventName = "ProvisionFulfilled", Level = LogLevel.Information,
        Message = "Capacity: node '{NodeName}' joined membership, fulfilling ticket '{TicketId}' after {LatencyMs:F0}ms; " +
                  "the rebalance loop spreads onto it from here.")]
    private static partial void LogProvisionFulfilled(ILogger logger, string nodeName, string ticketId, double latencyMs);

    [LoggerMessage(EventId = 1737, EventName = "ProvisionTicketExpired", Level = LogLevel.Warning,
        Message = "Capacity: ticket '{TicketId}' expired — node '{NodeName}' did not join within Cluster:ProvisionTicketTimeoutMs " +
                  "({TimeoutMs}ms). This was attempt {Attempt}; should it still arrive, it will be decommissioned unless shards were waiting for it.")]
    private static partial void LogProvisionTicketExpired(ILogger logger, string ticketId, string nodeName, int timeoutMs, int attempt);

    [LoggerMessage(EventId = 1738, EventName = "ProvisionGaveUp", Level = LogLevel.Error,
        Message = "Capacity: {Attempts} provision attempts failed or expired; the loop has stopped asking and the " +
                  "melange-capacity health check is now unhealthy. Money is involved, so repeated failure means a human: " +
                  "fix the provisioner (or add a node by hand) — a ticket-named node joining clears this.")]
    private static partial void LogProvisionGaveUp(ILogger logger, int attempts);

    [LoggerMessage(EventId = 1739, EventName = "ProvisionLateArrivalDecommissioned", Level = LogLevel.Warning,
        Message = "Capacity: node '{NodeName}' arrived after its ticket '{TicketId}' expired and owns nothing; " +
                  "decommissioning the surplus. At-least-once provisioning made safe by fencing — a cost, never a correctness problem.")]
    private static partial void LogProvisionLateArrivalDecommissioned(ILogger logger, string nodeName, string ticketId);

    [LoggerMessage(EventId = 1740, EventName = "ProvisionerCallFailed", Level = LogLevel.Warning,
        Message = "Capacity: the provisioner's {Operation} threw: {Reason}")]
    private static partial void LogProvisionerCallFailed(ILogger logger, string operation, string reason);

    [LoggerMessage(EventId = 1741, EventName = "ProvisionSkippedGranularity", Level = LogLevel.Warning,
        Message = "Capacity: every one of the {LiveNodes} live node(s) is sustained-hot, but only {TotalShards} shard(s) exist — " +
                  "a new node could not receive a whole shard, so provisioning is refused. The granularity ceiling: " +
                  "relief needs more shards (finer boundaries at strategy registration), not more nodes.")]
    private static partial void LogProvisionSkippedGranularity(ILogger logger, int liveNodes, int totalShards);

    [LoggerMessage(EventId = 1742, EventName = "ProvisionAtCeiling", Level = LogLevel.Warning,
        Message = "Capacity: every one of the {LiveNodes} live node(s) is sustained-hot (hottest {HottestUtilization:P0}) and the " +
                  "fleet is at Cluster:MaxNodes ({MaxNodes}). The ceiling is doing its job; raising it is a spending decision, so it is yours.")]
    private static partial void LogProvisionAtCeiling(ILogger logger, int liveNodes, int maxNodes, double hottestUtilization);

    [LoggerMessage(EventId = 1743, EventName = "ProvisionLateArrivalKept", Level = LogLevel.Information,
        Message = "Capacity: node '{NodeName}' arrived after its ticket '{TicketId}' expired, but registration assigned it " +
                  "shards that were waiting for an owner — capacity arrived late but arrived, so it stays.")]
    private static partial void LogProvisionLateArrivalKept(ILogger logger, string nodeName, string ticketId);

    [LoggerMessage(EventId = 1744, EventName = "ScaleInStarting", Level = LogLevel.Information,
        Message = "Scale-in: aggregate sustained load {Aggregate:F2} across {LiveNodes} node(s) fits on one fewer at " +
                  "{Remainder:P0}, under Cluster:RebalanceColdUtilization ({Cold}); consolidating the emptiest node " +
                  "'{Victim}' ({VictimUtilization:P0}, {Shards} shard(s)) onto the rest.")]
    private static partial void LogScaleInStarting(
        ILogger logger, string victim, double victimUtilization, double aggregate, int liveNodes, double remainder, double cold, int shards);

    [LoggerMessage(EventId = 1745, EventName = "ScaleInDecommissioning", Level = LogLevel.Information,
        Message = "Scale-in: node '{NodeName}' owns nothing and the fleet is still cold at the last-moment re-check; " +
                  "handing it back to the provisioner.")]
    private static partial void LogScaleInDecommissioning(ILogger logger, string nodeName);

    [LoggerMessage(EventId = 1746, EventName = "ScaleInAborted", Level = LogLevel.Warning,
        Message = "Scale-in of '{NodeName}' stopped: {Reason}. The node stays and nothing was lost — consolidation is an " +
                  "optimization, and aborting one is free.")]
    private static partial void LogScaleInAborted(ILogger logger, string nodeName, string reason);

    [LoggerMessage(EventId = 1734, EventName = "RebalanceEvaluationFailed", Level = LogLevel.Warning,
        Message = "A rebalance tick failed to evaluate ({Reason}); the loop continues on its next tick.")]
    private static partial void LogRebalanceEvaluationFailed(ILogger logger, string reason);

    [LoggerMessage(EventId = 1711, EventName = "ReplicaStreamBootstrapped", Level = LogLevel.Warning,
        Message = "Node '{NodeName}' subscribed replication from LSN {StaleCursor}, below the hub log's truncation base " +
            "{BaseLsn}; the gap cannot be served from the log, so the full Replicated state ({Rows} row(s)) was sent as a " +
            "reset at LSN {ResetLsn} and the stream resumes from there.")]
    private static partial void LogReplicaBootstrapped(
        ILogger logger, string nodeName, ulong staleCursor, ulong baseLsn, ulong resetLsn, int rows);
}
