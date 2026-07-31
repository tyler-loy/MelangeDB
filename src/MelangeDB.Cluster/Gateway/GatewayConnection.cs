using System.Net.WebSockets;
using MelangeDB.Protocol;
using MelangeDB.Server;

namespace MelangeDB.Cluster;

/// <summary>
/// One client socket at the gateway. The client sees a single MelangeDB endpoint speaking the
/// ordinary protocol; behind it the gateway holds the dual attachment — a permanent session to
/// the hub and a moving session to whichever node owns the client's current shard — and routes
/// each frame by content: reducer calls by the descriptor's execution site, subscriptions by the
/// table's placement. Upstream frames forward to the client verbatim (original bytes), so the
/// client cannot tell how many nodes exist, which is the acceptance criterion.
/// </summary>
internal sealed class GatewayConnection
{
    private readonly WebSocket _client;
    private readonly GatewayRuntime _gateway;
    private readonly SemaphoreSlim _clientSendLock = new(1, 1);
    private readonly Dictionary<uint, bool> _subscriptionOnShard = [];
    private readonly CancellationTokenSource _closed = new();
    private AuthResult? _session;
    private UpstreamSession? _hub;
    private UpstreamSession? _shard;
    private ShardKey? _shardKey;
    private bool _handshaken;

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
            if (_hub is { } hub)
                await hub.DisposeAsync().ConfigureAwait(false);
            if (_shard is { } shard)
                await shard.DisposeAsync().ConfigureAwait(false);
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
        _handshaken = true;
        await SendToClientAsync(new WelcomeFrame(
            MessagePackFrameSerializer.ProtocolVersion,
            Guid.NewGuid(),
            _hub.Welcome.EpochId,
            _hub.Welcome.HeadLsn,
            _hub.Welcome.HttpProtocol), ct).ConfigureAwait(false);
    }

    private async Task<UpstreamSession> ConnectUpstreamAsync(Uri uri, bool firesLifecycle, CancellationToken ct)
    {
        var session = _session!;
        var assertion = _gateway.Hub.MintAssertion(
            session.Identity, session.IsGuest, session.IsSqlOwner, session.TokenExpiresAt, firesLifecycle);
        return await UpstreamSession.ConnectAsync(
            uri,
            assertion,
            _gateway.Serializer,
            (frame, bytes) => ForwardUpstreamFrameAsync(frame, bytes),
            OnUpstreamClosed,
            _gateway.CountUpstreamSent,
            _gateway.CountUpstreamReceived,
            ct).ConfigureAwait(false);
    }

    private void OnUpstreamClosed()
    {
        // If the client is still here, one of its attachments died under it (a node kill or a
        // shard reassignment). The client converges by re-establishing subscriptions — the same
        // path backpressure resync uses.
        if (_closed.IsCancellationRequested)
            return;
        _ = SendToClientAsync(new ErrorFrame(
            MelangeErrorCodes.OverflowResync,
            "An attachment behind the gateway was re-established; re-establish subscriptions."), CancellationToken.None);
    }

    private async Task ForwardUpstreamFrameAsync(Frame frame, byte[] bytes)
    {
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
        catch (ArgumentException)
        {
            // Unknown reducer: let the hub answer, so the error shape is the server's own.
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
            _subscriptionOnShard[subscribe.SubscriptionId] = toShard;
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
        var onShard = _subscriptionOnShard.GetValueOrDefault(unsubscribe.SubscriptionId);
        _subscriptionOnShard.Remove(unsubscribe.SubscriptionId);
        var target = onShard ? _shard : _hub;
        if (target is { IsAlive: true })
            await target.SendRawAsync(bytes, ct).ConfigureAwait(false);
        else
            await SendToClientAsync(new UnsubscribedFrame(unsubscribe.SubscriptionId), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The moving shard attachment: the strategy names the session's current shard from hub
    /// state, and the gateway follows it — a changed answer (the player took a portal) drops the
    /// old node session and opens one to the new owner. The client never learns which node.
    /// </summary>
    private async Task<UpstreamSession> EnsureShardUpstreamAsync(CancellationToken ct)
    {
        var strategy = _gateway.Strategy
            ?? throw new InvalidOperationException("No IShardStrategy is registered; shard-executed traffic cannot be routed.");
        var session = _session!;
        var shard = strategy.ShardForSession(new SessionContext(
            session.Identity, session.IsGuest, _gateway.Hub.Engine.CommittedView));

        if (_shard is { IsAlive: true } current && _shardKey == shard)
            return current;

        if (_shard is { } stale)
        {
            await stale.DisposeAsync().ConfigureAwait(false);
            _shard = null;
        }

        var (_, publicAddress) = _gateway.Hub.ResolveShard(shard);
        _shard = await ConnectUpstreamAsync(_gateway.ShardSocketUri(publicAddress, shard), firesLifecycle: false, ct)
            .ConfigureAwait(false);
        _shardKey = shard;
        return _shard;
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
                        next.Identity, next.IsGuest, next.IsSqlOwner, next.TokenExpiresAt, firesLifecycle: true)), ct).ConfigureAwait(false);
                }

                if (_shard is { IsAlive: true } shard)
                {
                    await shard.SendFrameAsync(new ReauthenticateFrame(_gateway.Hub.MintAssertion(
                        next.Identity, next.IsGuest, next.IsSqlOwner, next.TokenExpiresAt, firesLifecycle: false)), ct).ConfigureAwait(false);
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
}
