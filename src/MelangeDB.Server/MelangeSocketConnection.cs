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
/// subscription set. Frames from the client are processed strictly in order; outbound ordering is
/// guaranteed only within a channel, which is exactly what the protocol promises.
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

    private long _bufferedDeltaBytes;
    private bool _resumeBuffering;
    private bool _resyncPending;
    private long _lastReceivedTicks;
    private uint _nextPingId;
    private ITimer? _heartbeat;
    private bool _handshaken;

    public MelangeSocketConnection(WebSocket socket, MelangeTransport transport, string httpProtocol)
    {
        _socket = socket;
        _transport = transport;
        _httpProtocol = httpProtocol;
        _time = transport.Time;
        _logger = transport.Logger;
        _serializer = transport.Serializer;
        ConnectionId = ConnectionId.New();
        Caller = Identity.Hash("melange-anonymous");
        _lastReceivedTicks = _time.GetUtcNow().UtcTicks;
    }

    public ConnectionId ConnectionId { get; }

    /// <summary>The connection's identity. Stubbed from the Hello token until phase 04's validation.</summary>
    public Identity Caller { get; private set; }

    public async Task RunAsync(CancellationToken requestAborted)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(requestAborted, _closed.Token);
        var sender = Task.Run(() => SendLoopAsync(linked.Token), CancellationToken.None);
        StartHeartbeat();
        try
        {
            await ReadLoopAsync(linked.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is OperationCanceledException or WebSocketException or MelangeProtocolException)
        {
            // An aborted socket, a heartbeat kill, or garbage on the wire all end the same way.
        }
        finally
        {
            _heartbeat?.Dispose();
            await _closed.CancelAsync().ConfigureAwait(false);
            UnregisterAllSubscriptions();
            _transport.OnConnectionClosed(this);
            try
            {
                await sender.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
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
            HandleFrame(frame);
        }
    }

    private void HandleFrame(Frame frame)
    {
        if (!_handshaken)
        {
            if (frame is not HelloFrame hello)
                throw new MelangeProtocolException("The first frame must be Hello.");
            HandleHello(hello);
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
            case ReauthenticateFrame:
                // Phase 04 owns validation. The invariant it will enforce: a re-auth may refresh
                // a token but never change the connection's identity.
                EnqueuePriority(new ReauthenticateResultFrame(true, null));
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

    private void HandleHello(HelloFrame hello)
    {
        if (hello.MinVersion > MessagePackFrameSerializer.ProtocolVersion || hello.MaxVersion < MessagePackFrameSerializer.ProtocolVersion)
        {
            EnqueuePriority(new ErrorFrame(
                MelangeErrorCodes.UnsupportedVersion,
                $"This server speaks protocol version {MessagePackFrameSerializer.ProtocolVersion}; the client offered {hello.MinVersion}-{hello.MaxVersion}."));
            throw new MelangeProtocolException("No common protocol version.");
        }

        if (!string.IsNullOrEmpty(hello.Token))
            Caller = StubIdentity.FromToken(hello.Token);
        _handshaken = true;
        EnqueuePriority(new WelcomeFrame(
            MessagePackFrameSerializer.ProtocolVersion,
            ConnectionId.Value,
            _transport.Engine.Log.EpochId,
            _transport.Engine.Log.HeadLsn,
            _httpProtocol));
    }

    private void HandleCallReducer(CallReducerFrame call)
    {
        var parentContext = default(ActivityContext);
        if (call.TraceParent is { } traceparent)
            ActivityContext.TryParse(traceparent, null, isRemote: true, out parentContext);

        ulong lsn;
        try
        {
            lsn = _transport.Reducers.Call(call.Reducer, Caller, ConnectionId, call.Arguments, parentContext);
        }
        catch (Exception exception)
        {
            var (code, message) = exception switch
            {
                ReducerArgumentException => (MelangeErrorCodes.InvalidArguments, exception.Message),
                RejectedException => (MelangeErrorCodes.Rejected, exception.Message),
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
                // engine lock, so it cannot interleave incorrectly with commit deltas.
                _transport.Engine.ReadConsistent(head =>
                {
                    var ops = _transport.Subscriptions.Rescope(existing, query, limits);
                    if (ops.Count > 0)
                        EnqueueDelta(new TransactionUpdateFrame(head, [new SubscriptionUpdate(existing.Id, ops)]) { Channel = MelangeChannels.Data });
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
                var registered = _transport.Subscriptions.Register(this, subscribe.SubscriptionId, query, limits, head, computeInitialSet: true);
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
                        this, request.SubscriptionId, query, _transport.Options.Subscriptions, head, computeInitialSet: false);
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
            var options = _transport.Options.Transport;
            var silence = _time.GetUtcNow() - new DateTimeOffset(Interlocked.Read(ref _lastReceivedTicks), TimeSpan.Zero);
            if (silence > TimeSpan.FromMilliseconds(options.HeartbeatTimeoutMs))
            {
                // No close frame arrived and nothing else did either: an ungraceful drop. Abort so
                // the read loop observes the death instead of waiting forever.
                LogMessages.HeartbeatTimeout(_logger, ConnectionId.ToString(), options.HeartbeatTimeoutMs);
                _socket.Abort();
                return;
            }

            EnqueuePriority(new PingFrame(_nextPingId++));
            _heartbeat?.Change(TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs), Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private async Task SendLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await _sendSignal.WaitAsync(ct).ConfigureAwait(false);
            while (TryDequeueNext(out var bytes))
            {
                await _socket.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
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
                var (key, row) = initialSet.Rows[_index++];
                rows.Add(new WireRow(key.ToArray(), RowWire.ToColumns(Subscription.Schema, row.Span, Subscription.Projection)));
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
