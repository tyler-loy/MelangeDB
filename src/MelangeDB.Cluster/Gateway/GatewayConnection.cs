using System.Net.WebSockets;
using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Cluster;

/// <summary>
/// One client socket at the gateway. The client sees a single MelangeDB endpoint speaking the
/// ordinary protocol; behind it the gateway holds the dual attachment — a permanent session to
/// the hub and a moving session to whichever node owns the client's current shard — and routes
/// each frame by content: reducer calls by the descriptor's execution site, subscriptions by the
/// table's placement. Upstream frames forward to the client verbatim (original bytes), so the
/// client cannot tell how many nodes exist.
///
/// Seamless handoff is this class's other half. As the player's handoff progresses
/// (<see cref="IPlayerHandoffObserver"/>), the connection: pre-opens a session to the approaching
/// destination; queues shard-routed calls once the transfer starts (invisible to the player —
/// the settled mid-handoff decision); and at the destination-authoritative moment mutes the
/// origin <em>synchronously</em> — before the saga requests the release, so the release's row
/// deletions never reach the client — then re-issues the client's shard subscriptions on the
/// destination. Re-subscribing under an already-used id re-scopes it: the destination's initial
/// set atomically replaces the client's row cache, and because that set includes the border band
/// around the player (the terrain and entities just behind them), the client observes no gap, no
/// missing terrain, and no disconnect.
/// </summary>
internal sealed partial class GatewayConnection : IPlayerHandoffObserver, IShardMoveObserver
{
    private const int MaxHeldCalls = 256;

    private readonly WebSocket _client;
    private readonly GatewayRuntime _gateway;
    private readonly SemaphoreSlim _clientSendLock = new(1, 1);
    private readonly SemaphoreSlim _attachmentLock = new(1, 1);
    private readonly Lock _stateLock = new();
    private readonly Dictionary<uint, bool> _subscriptionOnShard = [];
    private readonly Dictionary<uint, byte[]> _shardSubscribeFrames = [];
    private readonly Dictionary<ulong, Task<UpstreamSession>> _preopened = [];
    private readonly List<(byte[] Bytes, uint RequestId)> _heldCalls = [];
    private readonly CancellationTokenSource _closed = new();
    private AuthResult? _session;
    private UpstreamSession? _hub;
    private UpstreamSession? _shard;
    private volatile UpstreamSession? _muted;
    private ShardKey? _shardKey;
    private bool _queueShardCalls;
    private bool _handshaken;
    private IDisposable? _handoffRegistration;
    private IDisposable? _moveRegistration;

    /// <summary>Bumped whenever queueing starts or resolves, so a stale drain-queue timeout task disarms itself.</summary>
    private int _queueGeneration;

    public GatewayConnection(WebSocket client, GatewayRuntime gateway)
    {
        _client = client;
        _gateway = gateway;
    }

    public async Task RunAsync(CancellationToken requestAborted)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(requestAborted, _closed.Token);
        var ct = linked.Token;
        var buffer = new byte[64 * 1024];
        using var message = new MemoryStream();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                message.SetLength(0);
                while (true)
                {
                    var result = await _client.ReceiveAsync(buffer, ct).ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        if (_client.State == WebSocketState.CloseReceived)
                            await _client.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, ct).ConfigureAwait(false);
                        return;
                    }

                    message.Write(buffer, 0, result.Count);
                    if (message.Length > _gateway.Options.CurrentValue.Transport.MaxMessageBytes)
                        throw new WebSocketException(WebSocketError.InvalidMessageType);
                    if (result.EndOfMessage)
                        break;
                }

                var bytes = message.ToArray();
                await HandleFrameAsync(_gateway.Serializer.Deserialize(bytes), bytes, ct).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (exception is OperationCanceledException or WebSocketException or InvalidOperationException)
        {
            // Client went away or upstream handshake failed terminally; teardown below.
        }
        finally
        {
            _closed.Cancel();
            _handoffRegistration?.Dispose();
            _moveRegistration?.Dispose();
            if (_hub is { } hub)
                await hub.DisposeAsync().ConfigureAwait(false);
            if (_shard is { } shard)
                await shard.DisposeAsync().ConfigureAwait(false);
            if (_muted is { } muted && !ReferenceEquals(muted, _shard))
                await muted.DisposeAsync().ConfigureAwait(false);
            Task<UpstreamSession>[] preopened;
            lock (_stateLock)
            {
                preopened = [.. _preopened.Values];
                _preopened.Clear();
            }

            foreach (var pending in preopened)
                _ = pending.ContinueWith(static t => t.Result.DisposeAsync(), TaskContinuationOptions.OnlyOnRanToCompletion);
        }
    }

    private async Task HandleFrameAsync(Frame frame, byte[] bytes, CancellationToken ct)
    {
        if (!_handshaken)
        {
            if (frame is not HelloFrame hello)
                throw new InvalidOperationException("The first frame must be Hello.");
            await HandleHelloAsync(hello, ct).ConfigureAwait(false);
            return;
        }

        switch (frame)
        {
            case CallReducerFrame call:
                await RouteCallAsync(call, bytes, ct).ConfigureAwait(false);
                break;
            case SubscribeFrame subscribe:
                await RouteSubscribeAsync(subscribe, bytes, ct).ConfigureAwait(false);
                break;
            case UnsubscribeFrame unsubscribe:
                await RouteUnsubscribeAsync(unsubscribe, bytes, ct).ConfigureAwait(false);
                break;
            case PingFrame ping:
                await SendToClientAsync(new PongFrame(ping.Id), ct).ConfigureAwait(false);
                break;
            case PongFrame:
                break;
            case ResumeFrame:
                // Resume cursors count against one log; a gateway session spans several. The
                // client converges through the same full-resync path a rejected resume always
                // takes — part of the settled dual-attachment protocol decision.
                await SendToClientAsync(new ResumeResultFrame(
                    false, "The gateway does not resume sessions; re-establish subscriptions from fresh initial sets."), ct).ConfigureAwait(false);
                break;
            case ReauthenticateFrame reauthenticate:
                await HandleReauthenticateAsync(reauthenticate, ct).ConfigureAwait(false);
                break;
            default:
                throw new InvalidOperationException($"Frame {frame.Type} is not valid client-to-gateway.");
        }
    }

    private async Task HandleHelloAsync(HelloFrame hello, CancellationToken ct)
    {
        if (hello.MinVersion > MessagePackFrameSerializer.ProtocolVersion || hello.MaxVersion < MessagePackFrameSerializer.ProtocolVersion)
        {
            await SendToClientAsync(new ErrorFrame(
                MelangeErrorCodes.UnsupportedVersion,
                $"This gateway speaks protocol version {MessagePackFrameSerializer.ProtocolVersion}."), ct).ConfigureAwait(false);
            throw new InvalidOperationException("No common protocol version.");
        }

        switch (await _gateway.Authenticator.ValidateAsync(hello.Token).ConfigureAwait(false))
        {
            case AuthFailure failure:
                await SendToClientAsync(new ErrorFrame(MelangeErrorCodes.Unauthorized, failure.Reason), ct).ConfigureAwait(false);
                throw new InvalidOperationException("Gateway authentication failed.");
            case AuthResult { IsInternal: true }:
                await SendToClientAsync(new ErrorFrame(
                    MelangeErrorCodes.Unauthorized, "Internal identity assertions are not accepted at the gateway."), ct).ConfigureAwait(false);
                throw new InvalidOperationException("Assertion presented at the gateway.");
            case AuthResult session:
                _session = session;
                break;
        }

        // The permanent hub attachment: it fires the client's lifecycle reducers and carries
        // Global/Replicated traffic. Its Welcome is the one the client sees, so resume epochs and
        // head LSNs are the hub's.
        _hub = await ConnectUpstreamAsync(_gateway.HubSocketUri(), firesLifecycle: true, ct).ConfigureAwait(false);
        _handoffRegistration = _gateway.Hub.Handoffs.Register(_session!.Identity, this);
        _moveRegistration = _gateway.Hub.ShardMoves.Register(this);
        _handshaken = true;
        await SendToClientAsync(new WelcomeFrame(
            MessagePackFrameSerializer.ProtocolVersion,
            Guid.NewGuid(),
            _hub.Welcome.EpochId,
            _hub.Welcome.HeadLsn,
            _hub.Welcome.HttpProtocol,
            _session!.Identity), ct).ConfigureAwait(false);
    }

    private async Task<UpstreamSession> ConnectUpstreamAsync(Uri uri, bool firesLifecycle, CancellationToken ct)
    {
        var session = _session!;
        var assertion = _gateway.Hub.MintAssertion(
            session.Identity, session.IsGuest, session.IsSqlOwner, session.IsBulkOwner, session.TokenExpiresAt, firesLifecycle, session.IsBackupOwner);
        UpstreamSession? self = null;
        var connected = await UpstreamSession.ConnectAsync(
            uri,
            assertion,
            _gateway.Serializer,
            (frame, bytes) => ForwardUpstreamFrameAsync(self, frame, bytes),
            () => OnUpstreamClosed(self),
            _gateway.CountUpstreamSent,
            _gateway.CountUpstreamReceived,
            ct).ConfigureAwait(false);
        self = connected;
        return connected;
    }

    private void OnUpstreamClosed(UpstreamSession? source)
    {
        // Only a *current* attachment dying under a live client warrants the resync error; a
        // muted origin or a pre-opened destination closing is the machinery working as designed.
        if (_closed.IsCancellationRequested || source is null || ReferenceEquals(source, _muted))
            return;
        if (!ReferenceEquals(source, _hub) && !ReferenceEquals(source, _shard))
            return;
        _ = SendToClientAsync(new ErrorFrame(
            MelangeErrorCodes.OverflowResync,
            "An attachment behind the gateway was re-established; re-establish subscriptions."), CancellationToken.None);
    }

    private async Task ForwardUpstreamFrameAsync(UpstreamSession? source, Frame frame, byte[] bytes)
    {
        // Frames forward only from the current attachments: a muted origin's frames (the
        // release's deletions above all) and a pre-opened destination's frames never reach the
        // client. The swap is what promotes a session into this set.
        if (source is null || ReferenceEquals(source, _muted))
            return;
        if (!ReferenceEquals(source, _hub) && !ReferenceEquals(source, _shard))
            return;
        switch (frame)
        {
            case ReauthenticateResultFrame:
                return; // The gateway answers re-auth itself.
            default:
                await SendRawToClientAsync(bytes, _closed.Token).ConfigureAwait(false);
                break;
        }
    }

    private async Task RouteCallAsync(CallReducerFrame call, byte[] bytes, CancellationToken ct)
    {
        var toShard = false;
        try
        {
            var descriptor = _gateway.Reducers.Get(call.Reducer);
            toShard = descriptor.ExecutionSite == ReducerSite.Shard;
        }
        catch (UnknownReducerException)
        {
            // Unknown reducer: let the hub answer, so the error shape is the server's own.
        }

        if (toShard)
        {
            lock (_stateLock)
            {
                if (_queueShardCalls)
                {
                    // Mid-handoff: hold the call and replay it on the destination once it is
                    // authoritative (or back on the origin if the transfer aborts). Invisible to
                    // the player — the settled decision — at the cost of the transfer window's
                    // latency on these calls. The cap bounds a wedged transfer's memory.
                    if (_heldCalls.Count < MaxHeldCalls)
                    {
                        _heldCalls.Add((bytes, call.RequestId));
                        return;
                    }

                    LogHandoffQueueOverflow(_gateway.Logger, MaxHeldCalls);
                    _ = SendToClientAsync(new ReducerResultFrame(
                        call.RequestId, false, 0, MelangeErrorCodes.Internal,
                        "The transfer in progress has queued too many calls; retry.")
                    { Channel = MelangeChannels.Calls }, ct);
                    return;
                }
            }
        }

        try
        {
            var target = toShard
                ? await EnsureShardUpstreamAsync(ct).ConfigureAwait(false)
                : _hub!;
            await target.SendRawAsync(bytes, ct).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            await SendToClientAsync(new ReducerResultFrame(
                call.RequestId, false, 0, MelangeErrorCodes.Internal, exception.Message)
            { Channel = MelangeChannels.Calls }, ct).ConfigureAwait(false);
        }
    }

    private async Task RouteSubscribeAsync(SubscribeFrame subscribe, byte[] bytes, CancellationToken ct)
    {
        var toShard = false;
        try
        {
            var query = SqlSubsetParser.Parse(subscribe.Query, subscribe.Parameters);
            if (_gateway.Schema.TryGetByName(query.Table, out var table))
                toShard = table.Placement is Placement.Partitioned or Placement.Local;
        }
        catch (SqlParseException)
        {
            // Unparseable: the hub produces the protocol's own parse error.
        }

        try
        {
            var target = toShard ? await EnsureShardUpstreamAsync(ct).ConfigureAwait(false) : _hub!;
            lock (_stateLock)
            {
                _subscriptionOnShard[subscribe.SubscriptionId] = toShard;
                if (toShard)
                    _shardSubscribeFrames[subscribe.SubscriptionId] = bytes; // Replayed at every swap.
                else
                    _shardSubscribeFrames.Remove(subscribe.SubscriptionId);
            }

            await target.SendRawAsync(bytes, ct).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            await SendToClientAsync(new ErrorFrame(
                MelangeErrorCodes.Internal, exception.Message, SubscriptionId: subscribe.SubscriptionId), ct).ConfigureAwait(false);
        }
    }

    private async Task RouteUnsubscribeAsync(UnsubscribeFrame unsubscribe, byte[] bytes, CancellationToken ct)
    {
        bool onShard;
        lock (_stateLock)
        {
            onShard = _subscriptionOnShard.GetValueOrDefault(unsubscribe.SubscriptionId);
            _subscriptionOnShard.Remove(unsubscribe.SubscriptionId);
            _shardSubscribeFrames.Remove(unsubscribe.SubscriptionId);
        }

        var target = onShard ? _shard : _hub;
        if (target is { IsAlive: true })
            await target.SendRawAsync(bytes, ct).ConfigureAwait(false);
        else
            await SendToClientAsync(new UnsubscribedFrame(unsubscribe.SubscriptionId), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The moving shard attachment: the strategy names the session's current shard from hub
    /// state, and the gateway follows it — a changed answer (the player took a portal) drops the
    /// old node session and opens one to the new owner. Seamless handoff swaps this attachment
    /// through <see cref="OnDestinationAuthoritative"/> instead, so a walking transition never
    /// passes through here with a stale map. The client never learns which node either way.
    /// </summary>
    private async Task<UpstreamSession> EnsureShardUpstreamAsync(CancellationToken ct)
    {
        var strategy = _gateway.Strategy
            ?? throw new InvalidOperationException("No IShardStrategy is registered; shard-executed traffic cannot be routed.");
        var session = _session!;
        var shard = strategy.ShardForSession(new SessionContext(
            session.Identity, session.IsGuest, _gateway.Hub.Engine.CommittedView));

        await _attachmentLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_shard is { IsAlive: true } current && _shardKey == shard)
                return current;

            if (_shard is { } stale)
            {
                await stale.DisposeAsync().ConfigureAwait(false);
                _shard = null;
            }

            var connected = await ConnectShardAsync(shard, ct).ConfigureAwait(false);
            _shard = connected;
            _shardKey = shard;
            return connected;
        }
        finally
        {
            _attachmentLock.Release();
        }
    }

    /// <summary>
    /// A freshly created (or freshly reassigned) shard is opened by its owner on the owner's
    /// next heartbeat; retry across that window rather than surfacing a transient 404.
    /// </summary>
    private async Task<UpstreamSession> ConnectShardAsync(ShardKey shard, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var (_, publicAddress) = _gateway.Hub.ResolveShard(shard);
                return await ConnectUpstreamAsync(_gateway.ShardSocketUri(publicAddress, shard), firesLifecycle: false, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception) when (attempt < 20)
            {
                await Task.Delay(250, ct).ConfigureAwait(false);
            }
        }
    }

    // ---- IPlayerHandoffObserver: the saga drives these; see HandoffNotifier for the ordering contract. ----

    /// <summary>The player entered the border band: open the destination session now, so the swap later is instant.</summary>
    public void OnApproach(ShardKey from, ShardKey to)
    {
        if (!_handshaken || _closed.IsCancellationRequested)
            return;
        lock (_stateLock)
        {
            if (_shardKey == to || _preopened.ContainsKey(to.Value))
                return;
            _preopened[to.Value] = Task.Run(async () =>
            {
                var session = await ConnectShardAsync(to, _closed.Token).ConfigureAwait(false);
                LogPreopened(_gateway.Logger, to.Value);
                return session;
            });
        }
    }

    /// <summary>The transfer started: hold shard-routed calls; they land wherever the saga concludes.</summary>
    public void OnStarted(ShardKey from, ShardKey to)
    {
        lock (_stateLock)
        {
            _queueShardCalls = true;
        }
    }

    /// <summary>
    /// The destination owns the player. Synchronously mute the origin — the saga sends the
    /// release (and its row deletions) only after this returns — then swap asynchronously:
    /// promote the pre-opened session (or connect now), replay the shard subscriptions so the
    /// destination's initial sets atomically replace the client's caches, and flush the held
    /// calls in order.
    /// </summary>
    public void OnDestinationAuthoritative(ShardKey from, ShardKey to)
    {
        lock (_stateLock)
        {
            if (_shardKey == from && _shard is { } origin)
                _muted = origin;
        }

        _ = Task.Run(() => SwapToAsync(to));
    }

    /// <summary>Closure: on success the swap already ran; on abort, release the held calls back to the origin.</summary>
    public void OnClosed(ShardKey from, ShardKey to, bool success)
    {
        if (!success)
            ReleaseHeldCalls();
    }

    /// <summary>
    /// Stops queueing and flushes the held calls to whatever node currently owns the session's
    /// shard — the origin after an aborted transfer or a failed drain, the destination after a
    /// drain the swap missed. <see cref="EnsureShardUpstreamAsync"/> re-resolves ownership per
    /// call, so this is correct in every one of those endings.
    /// </summary>
    private void ReleaseHeldCalls()
    {
        _ = Task.Run(async () =>
        {
            List<(byte[] Bytes, uint RequestId)> held;
            lock (_stateLock)
            {
                _muted = null;
                _queueShardCalls = false;
                _queueGeneration++;
                held = [.. _heldCalls];
                _heldCalls.Clear();
            }

            foreach (var (bytes, requestId) in held)
            {
                try
                {
                    var target = await EnsureShardUpstreamAsync(_closed.Token).ConfigureAwait(false);
                    await target.SendRawAsync(bytes, _closed.Token).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    await SendToClientAsync(new ReducerResultFrame(
                        requestId, false, 0, MelangeErrorCodes.Internal, exception.Message)
                    { Channel = MelangeChannels.Calls }, _closed.Token).ConfigureAwait(false);
                }
            }
        });
    }

    // ---- IShardMoveObserver: a planned drain of a whole shard; see ShardMoveNotifier for ordering. ----

    /// <summary>
    /// A drain of this connection's shard started. Mute the origin attachment first — its
    /// transport is about to close under the quiesce, and that closure must read as machinery,
    /// never as a client-visible resync — then queue the shard's calls, bounded by
    /// Cluster:DrainQueueTimeoutMs so a wedged drain answers callers with a retryable error
    /// instead of holding them forever.
    /// </summary>
    public void OnMoveStarted(ShardKey shard)
    {
        int generation;
        lock (_stateLock)
        {
            if (_shardKey != shard)
                return;
            if (_shard is { } origin)
                _muted = origin;
            _queueShardCalls = true;
            generation = ++_queueGeneration;
        }

        var timeoutMs = Math.Max(1, _gateway.Options.CurrentValue.Cluster.DrainQueueTimeoutMs);
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(timeoutMs, _closed.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            bool wedged;
            lock (_stateLock)
            {
                wedged = _queueShardCalls && _queueGeneration == generation;
            }

            if (wedged)
            {
                LogDrainQueueTimedOut(_gateway.Logger, shard.Value, timeoutMs);
                ReleaseHeldCalls();
            }
        });
    }

    /// <summary>The destination owns the drained shard: reconnect there, re-scope, flush — the forced swap.</summary>
    public void OnMoved(ShardKey shard)
    {
        bool affected;
        lock (_stateLock)
        {
            affected = _shardKey == shard;
        }

        if (affected)
            _ = Task.Run(() => SwapToAsync(shard, force: true));
    }

    /// <summary>The drain failed; the origin keeps the shard. Flush the queue back to it.</summary>
    public void OnMoveFailed(ShardKey shard)
    {
        bool affected;
        lock (_stateLock)
        {
            affected = _shardKey == shard;
        }

        if (affected)
            ReleaseHeldCalls();
    }

    private async Task SwapToAsync(ShardKey to, bool force = false)
    {
        try
        {
            await _attachmentLock.WaitAsync(_closed.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        UpstreamSession? retired = null;
        try
        {
            // "Already there" short-circuits a duplicate handoff notification — but never a
            // drain's swap, where the shard key is unchanged by design and the whole point is
            // reconnecting the same shard on its new node.
            if (!force && _shardKey == to && _shard is { IsAlive: true })
                return;

            Task<UpstreamSession>? preopened;
            lock (_stateLock)
            {
                _preopened.Remove(to.Value, out preopened);
            }

            var session = preopened is not null
                ? await preopened.ConfigureAwait(false)
                : await ConnectShardAsync(to, _closed.Token).ConfigureAwait(false);

            byte[][] subscribes;
            List<(byte[] Bytes, uint RequestId)> held;
            lock (_stateLock)
            {
                retired = _shard;
                _shard = session;
                _shardKey = to;
                subscribes = [.. _shardSubscribeFrames.Values];
                held = [.. _heldCalls];
                _heldCalls.Clear();
                _queueShardCalls = false;
                _queueGeneration++;
            }

            // Re-scope every shard subscription on the destination: the fresh initial set
            // (band included) atomically replaces the client's cache — no gap, no missing
            // terrain, no disconnect.
            foreach (var frame in subscribes)
                await session.SendRawAsync(frame, _closed.Token).ConfigureAwait(false);
            foreach (var (bytes, _) in held)
                await session.SendRawAsync(bytes, _closed.Token).ConfigureAwait(false);
            LogSwapCompleted(_gateway.Logger, to.Value, subscribes.Length, held.Count);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            LogSwapFailed(_gateway.Logger, to.Value, exception.Message);
        }
        finally
        {
            _attachmentLock.Release();
            _muted = null;
            if (retired is not null)
                await retired.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task HandleReauthenticateAsync(ReauthenticateFrame reauthenticate, CancellationToken ct)
    {
        switch (await _gateway.Authenticator.ValidateAsync(reauthenticate.Token).ConfigureAwait(false))
        {
            case AuthFailure failure:
                await SendToClientAsync(new ReauthenticateResultFrame(false, failure.Reason), ct).ConfigureAwait(false);
                return;
            case AuthResult next:
                if (next.Identity != _session!.Identity)
                {
                    await SendToClientAsync(new ErrorFrame(
                        MelangeErrorCodes.IdentityChanged,
                        "Reauthenticate presented a token for a different identity; reconnect."), ct).ConfigureAwait(false);
                    throw new InvalidOperationException("Reauthentication with a different identity.");
                }

                _session = next;
                if (_hub is { IsAlive: true } hub)
                {
                    await hub.SendFrameAsync(new ReauthenticateFrame(_gateway.Hub.MintAssertion(
                        next.Identity, next.IsGuest, next.IsSqlOwner, next.IsBulkOwner, next.TokenExpiresAt, firesLifecycle: true, next.IsBackupOwner)), ct).ConfigureAwait(false);
                }

                if (_shard is { IsAlive: true } shard)
                {
                    await shard.SendFrameAsync(new ReauthenticateFrame(_gateway.Hub.MintAssertion(
                        next.Identity, next.IsGuest, next.IsSqlOwner, next.IsBulkOwner, next.TokenExpiresAt, firesLifecycle: false, next.IsBackupOwner)), ct).ConfigureAwait(false);
                }

                await SendToClientAsync(new ReauthenticateResultFrame(true, null), ct).ConfigureAwait(false);
                return;
        }
    }

    private Task SendToClientAsync(Frame frame, CancellationToken ct) =>
        SendRawToClientAsync(_gateway.Serializer.Serialize(frame), ct);

    private async Task SendRawToClientAsync(byte[] bytes, CancellationToken ct)
    {
        await _clientSendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_client.State == WebSocketState.Open)
                await _client.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is WebSocketException or ObjectDisposedException)
        {
            // The client is gone; the read loop observes it and tears the connection down.
        }
        finally
        {
            _clientSendLock.Release();
        }
    }

    [LoggerMessage(EventId = 1721, EventName = "GatewayPreopened", Level = LogLevel.Debug,
        Message = "Gateway pre-opened a session to shard {Shard} for an approaching player; the eventual swap will be instant.")]
    private static partial void LogPreopened(ILogger logger, ulong shard);

    [LoggerMessage(EventId = 1718, EventName = "GatewaySwapCompleted", Level = LogLevel.Debug,
        Message = "Gateway swapped a client's shard attachment to shard {Shard}: {Subscriptions} subscription(s) re-scoped, {HeldCalls} held call(s) flushed. The client observed nothing.")]
    private static partial void LogSwapCompleted(ILogger logger, ulong shard, int subscriptions, int heldCalls);

    [LoggerMessage(EventId = 1719, EventName = "GatewaySwapFailed", Level = LogLevel.Warning,
        Message = "Gateway could not swap a client to shard {Shard} ({Reason}); the client converges through the ordinary resync path on its next call.")]
    private static partial void LogSwapFailed(ILogger logger, ulong shard, string reason);

    [LoggerMessage(EventId = 1720, EventName = "GatewayHandoffQueueOverflow", Level = LogLevel.Warning,
        Message = "A client queued more than {Cap} reducer calls during one transfer; further calls are refused with a retryable error until the transfer resolves.")]
    private static partial void LogHandoffQueueOverflow(ILogger logger, int cap);

    [LoggerMessage(EventId = 1730, EventName = "GatewayDrainQueueTimedOut", Level = LogLevel.Warning,
        Message = "A client's calls stayed queued past Cluster:DrainQueueTimeoutMs ({TimeoutMs}ms) during the drain of shard {Shard} — the drain is wedged. " +
            "The queue was flushed to the shard's current owner; calls that cannot be delivered answer with a retryable error.")]
    private static partial void LogDrainQueueTimedOut(ILogger logger, ulong shard, int timeoutMs);
}
