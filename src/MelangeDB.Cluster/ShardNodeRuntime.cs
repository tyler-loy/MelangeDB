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
    private readonly CancellationTokenSource _stopped = new();
    private volatile NodeLink? _link;
    private long _leaseValidUntilTicks;
    private Task? _connectLoop;
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

        _connectLoop = Task.Run(RunAsync);
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
                shardSetChanged = true;
                LogShardOpened(_logger, assignment.Shard.Value, NodeName, runtime.Engine.Log.HeadLsn);
                ResolvePendingHandoffs(runtime);
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
                runtime.Import(import.HandoffId, import.PlayerHex, 0, import.Rows);
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

            case "handoff-query-owner":
            {
                var query = body!.Value.Deserialize<HandoffQuery>()!;
                var runtime = TryGetShard(new ShardKey(query.ToShard))
                    ?? throw new InvalidOperationException($"This node does not own shard {query.ToShard}.");
                return Task.FromResult<object?>(new HandoffQueryReply(runtime.WasImported(query.HandoffId)));
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

    private static readonly Identity ReplicaCaller = Identity.Hash("melange/replica");

    /// <summary>
    /// Handoff recovery, origin side: every freeze in this shard's log with no release or abort
    /// re-froze its rows at open; this resolves each by asking the hub whether the destination's
    /// import became durable — release if it did, abort if it did not, retry while the answer is
    /// "in flight". Exactly one owner, whichever way it lands.
    /// </summary>
    private void ResolvePendingHandoffs(ShardRuntime runtime)
    {
        foreach (var pending in runtime.PendingFreezes)
        {
            var marker = pending;
            _ = Task.Run(async () =>
            {
                var ct = _stopped.Token;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        if (_link is not { } link)
                            throw new InvalidOperationException("No hub link yet.");
                        var reply = await link.RequestAsync(
                            "handoff-query", new HandoffQuery(marker.HandoffId, marker.ToShard), ct).ConfigureAwait(false);
                        if (reply!.Value.Deserialize<HandoffQueryReply>()!.Imported)
                            runtime.Release(marker.HandoffId);
                        else
                            runtime.Abort(marker.HandoffId);
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception)
                    {
                        try
                        {
                            await Task.Delay(300, ct).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                }
            });
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
