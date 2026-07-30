using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Server;

/// <summary>
/// One client socket: the versioned handshake, the frame read loop, the lane-based sender that
/// interleaves bulk initial sets with interactive traffic, the heartbeat, and the per-connection
/// subscription set. Every connection authenticates — by upgrade-request header, by connect
/// ticket, or by a token in Hello — and is bound to that one identity for its lifetime:
/// <c>Reauthenticate</c> refreshes the token but a different identity closes the connection,
/// because every delta already sent was filtered under the current identity's policies. Frames
/// from the client are processed strictly in order; outbound ordering is guaranteed only within a
/// channel, which is exactly what the protocol promises.
/// </summary>
internal sealed class MelangeSocketConnection : IDeltaSink
{
    private readonly WebSocket _socket;
    private readonly MelangeTransport _transport;
    private readonly string _httpProtocol;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly IMelangeSerializer _serializer;

    private readonly ConcurrentQueue<byte[]> _priority = new();
    private readonly ConcurrentQueue<byte[]> _delta = new();
    private readonly List<BulkStream> _bulk = [];
    private readonly SemaphoreSlim _sendSignal = new(0);
    private readonly CancellationTokenSource _closed = new();
    private readonly Dictionary<uint, ServerSubscription> _subscriptions = [];
    private readonly List<TransactionUpdateFrame> _resumeBuffer = [];

    private AuthResult? _session;
    private PolicyContext? _policyContext;
    private bool _slotReserved;
    private long _bufferedDeltaBytes;
    private bool _resumeBuffering;
    private bool _resyncPending;
    private long _lastReceivedTicks;
    private uint _nextPingId;
    private ITimer? _heartbeat;
    private bool _handshaken;
    private volatile bool _senderBusy;
    private int _terminated;

    public MelangeSocketConnection(
        WebSocket socket,
        MelangeTransport transport,
        string httpProtocol,
        AuthResult? preAuthenticated = null,
        bool slotReserved = false)
    {
        _socket = socket;
        _transport = transport;
        _httpProtocol = httpProtocol;
        _time = transport.Time;
        _logger = transport.Logger;
        _serializer = transport.Serializer;
        ConnectionId = ConnectionId.New();
        _session = preAuthenticated;
        _slotReserved = slotReserved;
        if (preAuthenticated is not null)
            _policyContext = new PolicyContext(preAuthenticated.Identity, preAuthenticated.IsGuest, transport.Engine.CommittedView);
        _lastReceivedTicks = _time.GetUtcNow().UtcTicks;
    }

    public ConnectionId ConnectionId { get; }

    /// <summary>The connection's identity — <see cref="Identity.None"/> until authenticated.</summary>
    public Identity Caller => _session?.Identity ?? Identity.None;

    public async Task RunAsync(CancellationToken requestAborted)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(requestAborted, _closed.Token, _transport.Stopping);
        var sender = Task.Run(() => SendLoopAsync(linked.Token), CancellationToken.None);
        StartHeartbeat();
        var protocolFault = false;
        try
        {
            await ReadLoopAsync(linked.Token).ConfigureAwait(false);
        }
        catch (MelangeProtocolException)
        {
            // Garbage on the wire ends the connection — but the explanatory error frame already
            // queued on the priority lane must reach the client first, or the failure is mute.
            protocolFault = true;
        }
        catch (Exception exception) when (exception is OperationCanceledException or WebSocketException)
        {
            // An aborted socket or a heartbeat kill; nothing left to say to anyone.
        }
        finally
        {
            if (protocolFault)
                await DrainPriorityLaneAsync().ConfigureAwait(false);
            _heartbeat?.Dispose();
            await _closed.CancelAsync().ConfigureAwait(false);
            UnregisterAllSubscriptions();
            if (_slotReserved)
            {
                _transport.ReleaseConnectionSlot(Caller);
                _slotReserved = false;
            }

            _transport.OnConnectionClosed(this);
            try
            {
                await sender.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            if (protocolFault && _socket.State == WebSocketState.Open)
            {
                try
                {
                    using var closeTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                    await _socket.CloseAsync(WebSocketCloseStatus.PolicyViolation, "protocol error", closeTimeout.Token).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is OperationCanceledException or WebSocketException or ObjectDisposedException)
                {
                }
            }
        }
    }

    /// <summary>
    /// Closes this connection from outside the read loop — the terminate-sessions path. The error
    /// frame is queued first so the close is not mute; returns false if already closing.
    /// </summary>
    public bool Terminate()
    {
        if (Interlocked.Exchange(ref _terminated, 1) != 0 || _closed.IsCancellationRequested)
            return false;
        EnqueuePriority(new ErrorFrame(
            MelangeErrorCodes.Unauthorized,
            "This identity's sessions were terminated by the server."));
        _ = CloseAfterPriorityDrainAsync();
        return true;
    }

    /// <summary>
    /// Ends the connection <em>after</em> the queued explanation reaches the peer: an immediate
    /// abort would RST the socket and discard the client's unread buffer, making the close mute.
    /// The close frame flushes behind the error frame, the read loop then observes the peer's
    /// answer (or its drop), and a real-time backstop ends things for a peer that never reacts.
    /// </summary>
    private async Task CloseAfterPriorityDrainAsync()
    {
        await DrainPriorityLaneAsync().ConfigureAwait(false);
        try
        {
            if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await _socket.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, "session closed", timeout.Token).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (exception is OperationCanceledException or WebSocketException or ObjectDisposedException or InvalidOperationException)
        {
        }

        await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        if (!_closed.IsCancellationRequested)
            await _closed.CancelAsync().ConfigureAwait(false);
    }

    /// <summary>Waits briefly for the priority lane — error frames included — to hit the socket.</summary>
    private async Task DrainPriorityLaneAsync()
    {
        for (var i = 0; i < 400 && (!_priority.IsEmpty || _senderBusy); i++)
            await Task.Delay(5).ConfigureAwait(false);
    }

    /// <summary>Queues one transaction's deltas. Called under the engine's write lock, in LSN order.</summary>
    public void EnqueueDelta(TransactionUpdateFrame frame)
    {
        if (_resyncPending)
            return;
        if (_resumeBuffering)
        {
            _resumeBuffer.Add(frame);
            return;
        }

        var bytes = _serializer.Serialize(frame);
        var buffered = Interlocked.Add(ref _bufferedDeltaBytes, bytes.Length);
        var limits = _transport.Options.Subscriptions;
        if (buffered > limits.MaxBufferedBytes && limits.BackpressurePolicy != BackpressurePolicy.Buffer)
        {
            ApplyBackpressure(limits.BackpressurePolicy);
            return;
        }

        _delta.Enqueue(bytes);
        _sendSignal.Release();
    }

    private void ApplyBackpressure(BackpressurePolicy policy)
    {
        if (policy == BackpressurePolicy.Disconnect)
        {
            _socket.Abort();
            return;
        }

        // DropAndResync: the client on a too-slow link loses its delta stream, not its connection.
        // Queued deltas are discarded, the server forgets its subscriptions, and one small error
        // frame tells the client to re-establish them — the same convergence path a rejected
        // Resume uses. Unregistration is deferred off the fan-out iteration.
        _resyncPending = true;
        while (_delta.TryDequeue(out _))
        {
        }

        _resumeBuffer.Clear();
        Interlocked.Exchange(ref _bufferedDeltaBytes, 0);
        EnqueuePriority(new ErrorFrame(
            MelangeErrorCodes.OverflowResync,
            "The connection fell behind its delta stream (Subscriptions:MaxBufferedBytes); re-establish subscriptions.")
        { Channel = MelangeChannels.Control });
        _ = Task.Run(() =>
        {
            _transport.Engine.ReadConsistent(_ =>
            {
                UnregisterAllSubscriptionsUnderLock();
                _resyncPending = false;
            });
        });
    }

    private void EnqueuePriority(Frame frame)
    {
        _priority.Enqueue(_serializer.Serialize(frame));
        _sendSignal.Release();
    }

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        using var message = new MemoryStream();
        while (!ct.IsCancellationRequested)
        {
            message.SetLength(0);
            while (true)
            {
                var result = await _socket.ReceiveAsync(buffer, ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    if (_socket.State == WebSocketState.CloseReceived)
                    {
                        await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, ct).ConfigureAwait(false);
                    }

                    return;
                }

                message.Write(buffer, 0, result.Count);
                if (message.Length > _transport.Options.Transport.MaxMessageBytes)
                {
                    EnqueuePriority(new ErrorFrame(
                        MelangeErrorCodes.MessageTooLarge,
                        $"Inbound frame exceeds Transport:MaxMessageBytes ({_transport.Options.Transport.MaxMessageBytes})."));
                    throw new MelangeProtocolException("Inbound frame too large.");
                }

                if (result.EndOfMessage)
                    break;
            }

            _lastReceivedTicks = _time.GetUtcNow().UtcTicks;
            var frame = _serializer.Deserialize(message.GetBuffer().AsSpan(0, (int)message.Length));
            await HandleFrameAsync(frame).ConfigureAwait(false);
        }
    }

    private async Task HandleFrameAsync(Frame frame)
    {
        if (!_handshaken)
        {
            if (frame is not HelloFrame hello)
                throw new MelangeProtocolException("The first frame must be Hello.");
            await HandleHelloAsync(hello).ConfigureAwait(false);
            return;
        }

        switch (frame)
        {
            case HelloFrame:
                throw new MelangeProtocolException("Hello may only be sent once.");
            case CallReducerFrame call:
                HandleCallReducer(call);
                break;
            case SubscribeFrame subscribe:
                HandleSubscribe(subscribe);
                break;
            case UnsubscribeFrame unsubscribe:
                HandleUnsubscribe(unsubscribe);
                break;
            case ResumeFrame resume:
                HandleResume(resume);
                break;
            case ReauthenticateFrame reauthenticate:
                await HandleReauthenticateAsync(reauthenticate).ConfigureAwait(false);
                break;
            case PingFrame ping:
                EnqueuePriority(new PongFrame(ping.Id));
                break;
            case PongFrame:
                break;
            default:
                throw new MelangeProtocolException($"Frame {frame.Type} is not valid client-to-server.");
        }
    }

    private async Task HandleHelloAsync(HelloFrame hello)
    {
        if (hello.MinVersion > MessagePackFrameSerializer.ProtocolVersion || hello.MaxVersion < MessagePackFrameSerializer.ProtocolVersion)
        {
            EnqueuePriority(new ErrorFrame(
                MelangeErrorCodes.UnsupportedVersion,
                $"This server speaks protocol version {MessagePackFrameSerializer.ProtocolVersion}; the client offered {hello.MinVersion}-{hello.MaxVersion}."));
            throw new MelangeProtocolException("No common protocol version.");
        }

        if (_session is null)
        {
            // Header and ticket connections arrive pre-authenticated; everyone else presents a
            // token here. No token, no connection — the IdP is the gate.
            switch (await _transport.Authenticator.ValidateAsync(hello.Token).ConfigureAwait(false))
            {
                case AuthFailure failure:
                    EnqueuePriority(new ErrorFrame(MelangeErrorCodes.Unauthorized, failure.Reason));
                    throw new MelangeProtocolException("Handshake authentication failed.");
                case AuthResult session:
                    if (_transport.Sessions.IsRevoked(session.Identity))
                    {
                        EnqueuePriority(new ErrorFrame(MelangeErrorCodes.Unauthorized, "This identity is revoked."));
                        throw new MelangeProtocolException("Revoked identity.");
                    }

                    if (!_transport.TryReserveConnectionSlot(session.Identity))
                    {
                        EnqueuePriority(new ErrorFrame(
                            MelangeErrorCodes.ConnectionCap,
                            $"This identity already holds Auth:MaxConnectionsPerIdentity ({_transport.Options.Auth.MaxConnectionsPerIdentity}) connections."));
                        throw new MelangeProtocolException("Connection cap exceeded.");
                    }

                    _slotReserved = true;
                    _session = session;
                    _policyContext = new PolicyContext(session.Identity, session.IsGuest, _transport.Engine.CommittedView);
                    break;
            }
        }

        _handshaken = true;
        EnqueuePriority(new WelcomeFrame(
            MessagePackFrameSerializer.ProtocolVersion,
            ConnectionId.Value,
            _transport.Engine.Log.EpochId,
            _transport.Engine.Log.HeadLsn,
            _httpProtocol));
    }

    /// <summary>
    /// The invariant that makes re-auth safe: a fresh token may extend the session but must never
    /// change its identity. Every initial set and delta on this connection was filtered under the
    /// current identity's policies, so an in-place switch would deliver A's rows to B — the
    /// connection closes instead, and the client reconnects as whoever it now is.
    /// </summary>
    private async Task HandleReauthenticateAsync(ReauthenticateFrame reauthenticate)
    {
        switch (await _transport.Authenticator.ValidateAsync(reauthenticate.Token).ConfigureAwait(false))
        {
            case AuthFailure failure:
                // An invalid refresh is not fatal by itself: the session lives until the current
                // token's expiry plus the grace window, and the client may try again.
                EnqueuePriority(new ReauthenticateResultFrame(false, failure.Reason));
                return;
            case AuthResult next:
                if (next.Identity != Caller)
                {
                    EnqueuePriority(new ErrorFrame(
                        MelangeErrorCodes.IdentityChanged,
                        "Reauthenticate presented a token for a different identity; a connection is bound to one identity for its lifetime. Reconnect."));
                    throw new MelangeProtocolException("Reauthentication with a different identity.");
                }

                if (_transport.Sessions.IsRevoked(next.Identity))
                {
                    EnqueuePriority(new ReauthenticateResultFrame(false, "This identity is revoked."));
                    throw new MelangeProtocolException("Reauthentication for a revoked identity.");
                }

                _session = next;

                // Same identity, possibly new claims (guest conversion). The context is replaced
                // under the write lock so no fan-out observes a half-swapped caller.
                var context = new PolicyContext(next.Identity, next.IsGuest, _transport.Engine.CommittedView);
                _transport.Engine.ReadConsistent(_ =>
                {
                    _policyContext = context;
                    foreach (var subscription in _subscriptions.Values)
                        subscription.Context = context;
                });
                EnqueuePriority(new ReauthenticateResultFrame(true, null));
                break;
        }
    }

    private void HandleCallReducer(CallReducerFrame call)
    {
        var parentContext = default(ActivityContext);
        if (call.TraceParent is { } traceparent)
            ActivityContext.TryParse(traceparent, null, isRemote: true, out parentContext);

        ulong lsn;
        try
        {
            lsn = _transport.Reducers.Call(
                call.Reducer,
                Caller,
                ConnectionId,
                call.Arguments,
                parentContext,
                CallSource.Client(_session?.IsGuest ?? false));
        }
        catch (Exception exception)
        {
            var (code, message) = exception switch
            {
                ReducerArgumentException => (MelangeErrorCodes.InvalidArguments, exception.Message),
                RejectedException => (MelangeErrorCodes.Rejected, exception.Message),
                RateLimitedException => (MelangeErrorCodes.RateLimited, exception.Message),
                ReducerDeniedException => (MelangeErrorCodes.Denied, exception.Message),
                ArgumentException => (MelangeErrorCodes.UnknownReducer, $"No reducer named '{call.Reducer}' is registered."),
                _ => (MelangeErrorCodes.Internal, "The reducer failed; see the server logs."),
            };
            if (code == MelangeErrorCodes.Internal)
                LogMessages.ReducerCallFailed(_logger, call.Reducer, exception);
            EnqueuePriority(new ReducerResultFrame(call.RequestId, false, 0, code, message) { Channel = MelangeChannels.Calls });
            return;
        }

        EnqueuePriority(new ReducerResultFrame(call.RequestId, true, lsn, null, null) { Channel = MelangeChannels.Calls });
    }

    private void HandleSubscribe(SubscribeFrame subscribe)
    {
        try
        {
            var query = SqlSubsetParser.Parse(subscribe.Query, subscribe.Parameters);
            var limits = _transport.Options.Subscriptions;
            if (_subscriptions.TryGetValue(subscribe.SubscriptionId, out var existing))
            {
                // Re-scope: the moving-range pattern. The diff rides the data channel under the
                // engine lock, so it cannot interleave incorrectly with commit deltas. It is
                // synthetic rather than commit-tied, so it carries LSN 0, which a client applies
                // unconditionally instead of judging against its anchor.
                _transport.Engine.ReadConsistent(_ =>
                {
                    var ops = _transport.Subscriptions.Rescope(existing, query, limits);
                    if (ops.Count > 0)
                        EnqueueDelta(new TransactionUpdateFrame(0, [new SubscriptionUpdate(existing.Id, ops)]) { Channel = MelangeChannels.Data });
                });
                return;
            }

            if (_subscriptions.Count >= limits.MaxPerConnection)
            {
                throw new SubscriptionRejectedException(
                    MelangeErrorCodes.TooManySubscriptions,
                    $"This connection already holds Subscriptions:MaxPerConnection ({limits.MaxPerConnection}) subscriptions.");
            }

            using var activity = _transport.Telemetry?.StartInitialSet(query.Table);
            var (subscription, initialSet) = _transport.Engine.ReadConsistent(head =>
            {
                var registered = _transport.Subscriptions.Register(
                    this, subscribe.SubscriptionId, query, limits, head, computeInitialSet: true, _transport.Policies, _policyContext);
                _subscriptions[subscribe.SubscriptionId] = registered.Subscription;
                return registered;
            });
            ServerTelemetry.CompleteInitialSet(activity, initialSet.Rows.Count, initialSet.Bytes);
            lock (_bulk)
            {
                _bulk.Add(new BulkStream(subscription, initialSet));
            }

            _sendSignal.Release();
        }
        catch (SqlParseException parse)
        {
            _transport.Telemetry?.RecordRejected(MelangeErrorCodes.ParseError);
            EnqueuePriority(new ErrorFrame(MelangeErrorCodes.ParseError, parse.Message, SubscriptionId: subscribe.SubscriptionId));
        }
        catch (SubscriptionRejectedException rejected)
        {
            _transport.Telemetry?.RecordRejected(rejected.Code);
            EnqueuePriority(new ErrorFrame(rejected.Code, rejected.Message, SubscriptionId: subscribe.SubscriptionId));
        }
    }

    private void HandleUnsubscribe(UnsubscribeFrame unsubscribe)
    {
        if (_subscriptions.TryGetValue(unsubscribe.SubscriptionId, out var subscription))
        {
            _transport.Engine.ReadConsistent(_ =>
            {
                _transport.Subscriptions.Unregister(subscription);
                _subscriptions.Remove(unsubscribe.SubscriptionId);
            });
            lock (_bulk)
            {
                _bulk.RemoveAll(stream => stream.Subscription.Id == unsubscribe.SubscriptionId);
            }
        }

        EnqueuePriority(new UnsubscribedFrame(unsubscribe.SubscriptionId));
    }

    private void HandleResume(ResumeFrame resume)
    {
        if (_subscriptions.Count > 0)
        {
            EnqueuePriority(new ResumeResultFrame(false, "Resume must precede any subscription on the connection."));
            return;
        }

        var engine = _transport.Engine;
        if (resume.EpochId != engine.Log.EpochId)
        {
            // A stale or unknown epoch is an explicit failure: the cursor counts against a log
            // this server does not have, so the only correct answer is a full resync.
            EnqueuePriority(new ResumeResultFrame(false, "Unknown or stale log epoch; a full resync is required."));
            return;
        }

        if (resume.LastAckedLsn > engine.Log.HeadLsn)
        {
            EnqueuePriority(new ResumeResultFrame(false, "The resume cursor is beyond this log's head; a full resync is required."));
            return;
        }

        if (resume.LastAckedLsn < engine.Log.HeadLsn)
        {
            var oldestMissed = engine.Log.ReadFrom(resume.LastAckedLsn + 1).FirstOrDefault();
            var window = TimeSpan.FromSeconds(_transport.Options.Resume.RetentionWindowSeconds);
            if (oldestMissed is not null
                && _time.GetUtcNow() - oldestMissed.Timestamp.ToDateTimeOffset() > window)
            {
                EnqueuePriority(new ResumeResultFrame(
                    false,
                    $"The gap is older than Resume:RetentionWindowSeconds ({_transport.Options.Resume.RetentionWindowSeconds}s); a full resync is required."));
                return;
            }
        }

        ulong resumeHead = 0;
        var registered = new List<ServerSubscription>(resume.Subscriptions.Count);
        try
        {
            engine.ReadConsistent(head =>
            {
                resumeHead = head;
                _resumeBuffering = true;
                foreach (var request in resume.Subscriptions)
                {
                    var query = SqlSubsetParser.Parse(request.Query, request.Parameters);
                    var (subscription, _) = _transport.Subscriptions.Register(
                        this, request.SubscriptionId, query, _transport.Options.Subscriptions, head,
                        computeInitialSet: false, _transport.Policies, _policyContext);
                    _subscriptions[request.SubscriptionId] = subscription;
                    registered.Add(subscription);
                }
            });
        }
        catch (Exception exception) when (exception is SqlParseException or SubscriptionRejectedException)
        {
            engine.ReadConsistent(_ =>
            {
                UnregisterAllSubscriptionsUnderLock();
                _resumeBuffering = false;
                _resumeBuffer.Clear();
            });
            EnqueuePriority(new ResumeResultFrame(false, $"A resumed subscription failed validation: {exception.Message}"));
            return;
        }

        EnqueuePriority(new ResumeResultFrame(true, null));

        // Serve the gap from the log, then release live deltas buffered while replaying. The
        // buffered frames all carry LSNs above resumeHead, so the data channel stays in LSN order.
        foreach (var record in engine.Log.ReadFrom(resume.LastAckedLsn + 1))
        {
            if (record.Lsn > resumeHead)
                break;
            var updates = SubscriptionEngine.ComputeReplayUpdates(registered, record);
            if (updates.Count == 0)
                continue;
            var bytes = _serializer.Serialize(new TransactionUpdateFrame(record.Lsn, updates) { Channel = MelangeChannels.Data });
            Interlocked.Add(ref _bufferedDeltaBytes, bytes.Length);
            _delta.Enqueue(bytes);
            _sendSignal.Release();
        }

        engine.ReadConsistent(_ =>
        {
            foreach (var frame in _resumeBuffer)
            {
                var bytes = _serializer.Serialize(frame);
                Interlocked.Add(ref _bufferedDeltaBytes, bytes.Length);
                _delta.Enqueue(bytes);
            }

            _resumeBuffer.Clear();
            _resumeBuffering = false;
        });
        _sendSignal.Release();
    }

    private void StartHeartbeat()
    {
        var interval = TimeSpan.FromMilliseconds(_transport.Options.Transport.HeartbeatIntervalMs);
        _heartbeat = _time.CreateTimer(_ => HeartbeatTick(), null, interval, Timeout.InfiniteTimeSpan);
    }

    private void HeartbeatTick()
    {
        try
        {
            var options = _transport.Options;
            if (TokenExpiryExceeded(options.Auth))
            {
                // The token expired and the grace window passed with no successful re-auth: a
                // revoked or expired credential must not keep working indefinitely.
                EnqueuePriority(new ErrorFrame(
                    MelangeErrorCodes.TokenExpired,
                    $"The session token expired and Auth:ReauthGraceSeconds ({options.Auth.ReauthGraceSeconds}s) passed without Reauthenticate."));
                _ = CloseAfterPriorityDrainAsync();
                return;
            }

            var silence = _time.GetUtcNow() - new DateTimeOffset(Interlocked.Read(ref _lastReceivedTicks), TimeSpan.Zero);
            if (silence > TimeSpan.FromMilliseconds(options.Transport.HeartbeatTimeoutMs))
            {
                // No close frame arrived and nothing else did either: an ungraceful drop. Abort so
                // the read loop observes the death instead of waiting forever.
                LogMessages.HeartbeatTimeout(_logger, ConnectionId.ToString(), options.Transport.HeartbeatTimeoutMs);
                _socket.Abort();
                return;
            }

            EnqueuePriority(new PingFrame(_nextPingId++));
            _heartbeat?.Change(TimeSpan.FromMilliseconds(options.Transport.HeartbeatIntervalMs), Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private bool TokenExpiryExceeded(AuthOptions options) =>
        _session is { TokenExpiresAt: var expires }
        && expires != DateTimeOffset.MaxValue
        && _time.GetUtcNow() > expires.AddSeconds(options.ReauthGraceSeconds);

    private async Task SendLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await _sendSignal.WaitAsync(ct).ConfigureAwait(false);
            _senderBusy = true;
            try
            {
                while (TryDequeueNext(out var bytes))
                {
                    await _socket.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                _senderBusy = false;
            }
        }
    }

    /// <summary>
    /// Lane priority: control and call results first, then one committed delta, then one bulk
    /// chunk — so a 30MB initial set never delays a reducer response by more than one chunk.
    /// </summary>
    private bool TryDequeueNext(out byte[] bytes)
    {
        if (_priority.TryDequeue(out bytes!))
            return true;
        if (_delta.TryDequeue(out bytes!))
        {
            Interlocked.Add(ref _bufferedDeltaBytes, -bytes.Length);
            return true;
        }

        return TryBuildBulkChunk(out bytes!);
    }

    private bool TryBuildBulkChunk(out byte[]? bytes)
    {
        BulkStream? stream;
        lock (_bulk)
        {
            stream = _bulk.Count > 0 ? _bulk[0] : null;
        }

        if (stream is null)
        {
            bytes = null;
            return false;
        }

        var chunkBudget = _transport.Options.Transport.MaxInitialSetChunkBytes;
        var frame = stream.NextChunk(chunkBudget);
        bytes = _serializer.Serialize(frame);
        if (frame.IsLast)
        {
            lock (_bulk)
            {
                _bulk.Remove(stream);
            }
        }
        else
        {
            _sendSignal.Release();
        }

        return true;
    }

    private void UnregisterAllSubscriptions() =>
        _transport.Engine.ReadConsistent(_ => UnregisterAllSubscriptionsUnderLock());

    private void UnregisterAllSubscriptionsUnderLock()
    {
        foreach (var subscription in _subscriptions.Values)
            _transport.Subscriptions.Unregister(subscription);
        _subscriptions.Clear();
        lock (_bulk)
        {
            _bulk.Clear();
        }
    }

    /// <summary>One subscription's initial set being streamed as chunks on its bulk channel.</summary>
    private sealed class BulkStream(ServerSubscription subscription, InitialSet initialSet)
    {
        private int _index;
        private uint _chunk;

        public ServerSubscription Subscription { get; } = subscription;

        public SubscriptionAppliedFrame NextChunk(int chunkBudget)
        {
            var rows = new List<WireRow>();
            long budget = 0;
            while (_index < initialSet.Rows.Count && (budget == 0 || budget < chunkBudget))
            {
                var (key, row) = initialSet.Rows[_index];

                // Per-row column sets were mask-evaluated under the write lock at the anchor;
                // static wire columns already exclude [ServerOnly].
                var columns = initialSet.RowColumns is { } perRow ? perRow[_index] : Subscription.StaticWireColumns;
                _index++;
                rows.Add(new WireRow(key.ToArray(), RowWire.ToColumns(Subscription.Schema, row.Span, columns)));
                budget += key.Length + row.Length;
            }

            return new SubscriptionAppliedFrame(
                Subscription.Id,
                initialSet.AnchorLsn,
                _chunk++,
                IsLast: _index >= initialSet.Rows.Count,
                rows)
            { Channel = MelangeChannels.BulkFor(Subscription.Id) };
        }
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, Exception?> ReducerCallFailedMessage =
            LoggerMessage.Define<string>(
                LogLevel.Error,
                new EventId(1204, "ReducerCallFailed"),
                "Reducer '{Reducer}' threw an unexpected exception during a transport call.");

        public static void ReducerCallFailed(ILogger logger, string reducer, Exception failure) =>
            ReducerCallFailedMessage(logger, reducer, failure);

        private static readonly Action<ILogger, string, int, Exception?> HeartbeatTimeoutMessage =
            LoggerMessage.Define<string, int>(
                LogLevel.Information,
                new EventId(1203, "HeartbeatTimeout"),
                "Connection {ConnectionId} exceeded Transport:HeartbeatTimeoutMs ({TimeoutMs}ms) of silence; aborting the socket.");

        public static void HeartbeatTimeout(ILogger logger, string connectionId, int timeoutMs) =>
            HeartbeatTimeoutMessage(logger, connectionId, timeoutMs, null);
    }
}
