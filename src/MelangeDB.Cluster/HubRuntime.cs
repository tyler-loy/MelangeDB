using System.Collections.Concurrent;
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
        public required string ServerNonce { get; init; }

        public string? NodeName { get; set; }

        public SemaphoreSlim ReplicaSignal { get; } = new(0, 1);

        public Task? ReplicaPump { get; set; }
    }

    /// <summary>An in-flight handoff saga on the coordinator.</summary>
    private sealed record Saga(string HandoffId, Identity Player, ShardKey From, ShardKey To)
    {
        public bool Imported { get; set; }
    }

    private readonly IServiceProvider _services;
    private readonly MelangeEngine _engine;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly IMembershipStore _membership;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly ForeignEventDispatcher _foreignEvents;
    private readonly ConcurrentDictionary<string, NodeLink> _linksByNode = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Saga> _sagas = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _stopped = new();
    private TcpListener? _listener;
    private Task? _acceptLoop;
    private ITimer? _sweepTimer;

    public HubRuntime(IServiceProvider services)
    {
        _services = services;
        _engine = services.GetRequiredService<MelangeEngine>();
        _options = services.GetRequiredService<IOptionsMonitor<MelangeDbOptions>>();
        _membership = services.GetRequiredService<IMembershipStore>();
        _time = services.GetService<TimeProvider>() ?? TimeProvider.System;
        var loggers = services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
        _logger = loggers.CreateLogger("MelangeDB.Cluster.Hub");
        _foreignEvents = new ForeignEventDispatcher(
            services.GetRequiredService<EventHandlerRegistry>(),
            services.GetRequiredService<IServiceScopeFactory>(),
            _options,
            _logger);
    }

    public ClusterMetrics Metrics { get; } = new();

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

        _engine.SetTableAccessGuard(PlacementGuards.HubAccess());
        _engine.AddCommitGuard(new HubCommitGuard(_engine.Schema));
        _engine.AddCommitObserver(new ReplicaSignaller(this));

        _listener = new TcpListener(IPAddress.Loopback, cluster.NodeListenPort);
        _listener.Start();
        NodeListenPort = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);

        var sweep = TimeSpan.FromMilliseconds(Math.Max(250, cluster.FailureTimeoutMs / 2));
        _sweepTimer = _time.CreateTimer(_ => SweepDeadNodes(), null, sweep, sweep);
        LogHubStarted(_logger, NodeListenPort);
    }

    /// <summary>Mints the internal identity assertion the gateway attaches to upstream sessions.</summary>
    public string MintAssertion(Identity identity, bool isGuest, bool isSqlOwner, DateTimeOffset tokenExpiresAt, bool firesLifecycle)
    {
        var ttlCap = _time.GetUtcNow().AddSeconds(Cluster.AssertionTtlSeconds);
        var expires = tokenExpiresAt < ttlCap ? tokenExpiresAt : ttlCap;
        return InternalIdentityAssertion.Mint(Cluster.Secret, identity, isGuest, isSqlOwner, expires, firesLifecycle);
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
        var saga = new Saga(handoffId, player, from, to);
        _sagas[handoffId] = saga;
        try
        {
            var origin = _membership.GetAssignment(from);
            var originLink = origin?.NodeName is { } originNode ? LinkOf(originNode) : null;
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
                    new HandoffImport(handoffId, to.Value, destination.FencingToken, player.ToString(), rows),
                    ct).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The import never became durable; the player stays on the origin.
                if (originLink is not null && origin is not null)
                {
                    await originLink.RequestAsync(
                        "handoff-abort", new HandoffRelease(handoffId, from.Value, origin.FencingToken), ct).ConfigureAwait(false);
                }

                LogHandoffAborted(_logger, handoffId, player.ToString(), from.Value, to.Value);
                throw;
            }

            saga.Imported = true;
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
        }
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
                return new HeartbeatReply(AssignmentsDto(session.NodeName));
            case "replica-subscribe":
                var subscribe = body!.Value.Deserialize<ReplicaSubscribe>()!;
                StartReplicaPump(link, session, subscribe.FromLsn);
                return null;
            case "events-forward":
                var forward = body!.Value.Deserialize<EventsForward>()!;
                await _foreignEvents.DispatchAsync(session.NodeName, forward.ShardValue, forward, _stopped.Token).ConfigureAwait(false);
                return null;
            case "handoff-query":
                return await HandleHandoffQueryAsync(body!.Value.Deserialize<HandoffQuery>()!).ConfigureAwait(false);
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
        return new AuthReply(
            LinkProof.Compute(cluster.Secret, request.NodeNonce, "hub"),
            AssignmentsDto(request.NodeName),
            cluster.FailureTimeoutMs);
    }

    private async Task<HandoffQueryReply> HandleHandoffQueryAsync(HandoffQuery query)
    {
        // An in-flight saga answers "wait": the coordinator still intends to import, so the
        // recovering origin must not abort yet — aborting while the import lands would duplicate
        // the player.
        if (_sagas.TryGetValue(query.HandoffId, out var saga) && !saga.Imported)
            throw new InvalidOperationException("The handoff is still in flight; retry.");
        if (saga is { Imported: true })
            return new HandoffQueryReply(true);

        var destination = _membership.GetAssignment(new ShardKey(query.ToShard));
        if (destination?.NodeName is not { } owner || LinkOf(owner) is not { } link)
            throw new InvalidOperationException($"The destination shard's owner is unreachable; retry.");
        var reply = await link.RequestAsync("handoff-query-owner", query, _stopped.Token).ConfigureAwait(false);
        return reply!.Value.Deserialize<HandoffQueryReply>()!;
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
        session.ReplicaPump ??= Task.Run(async () =>
        {
            var cursor = fromLsn;
            var ct = _stopped.Token;
            while (!ct.IsCancellationRequested && link.IsAlive)
            {
                try
                {
                    var head = _engine.Log.HeadLsn;
                    if (cursor < head)
                    {
                        var lsns = new List<ulong>();
                        var records = new List<WireOp[]>();
                        foreach (var record in _engine.Log.ReadFrom(cursor + 1))
                        {
                            cursor = record.Lsn;
                            var replicated = ReplicatedOps(record);
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
                    return; // Link death ends the pump; the node re-subscribes on reconnect.
                }
            }
        });
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
    }

    public void Dispose()
    {
        _stopped.Cancel();
        _sweepTimer?.Dispose();
        _listener?.Stop();
        foreach (var link in _linksByNode.Values)
            link.Dispose();
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
}
