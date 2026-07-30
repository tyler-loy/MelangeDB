using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>
/// The MelangeDB C# client: connects over a websocket (HTTP/1.1 or HTTP/2), calls reducers and
/// awaits their results, and maintains per-subscription row caches advanced by live deltas. On
/// reconnect it resumes from its last acknowledged LSN — naming the log epoch, so a cursor can
/// never be applied against the wrong log — and falls back to full re-establishment when the
/// server says the gap cannot be served.
/// </summary>
public sealed class MelangeClient : IAsyncDisposable
{
    private static readonly HttpMessageInvoker SharedInvoker = new(new SocketsHttpHandler());

    private readonly MelangeClientOptions _options;
    private readonly IMelangeSerializer _serializer = new MessagePackFrameSerializer();
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<ReducerResultFrame>> _pendingCalls = new();
    private readonly ConcurrentDictionary<uint, TaskCompletionSource> _pendingUnsubscribes = new();
    private readonly ConcurrentDictionary<uint, MelangeSubscription> _subscriptions = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private ClientWebSocket? _socket;
    private Task? _receiveLoop;
    private TaskCompletionSource<WelcomeFrame>? _pendingWelcome;
    private TaskCompletionSource<ResumeResultFrame>? _pendingResume;
    private uint _nextRequestId;
    private uint _nextSubscriptionId;
    private long _bytesReceived;
    private ulong _lastAckedLsn;

    public MelangeClient(MelangeClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <summary>The HTTP protocol the server reports for this connection, e.g. <c>HTTP/2</c>.</summary>
    public string? NegotiatedHttpProtocol { get; private set; }

    /// <summary>The negotiated protocol version.</summary>
    public int ProtocolVersion { get; private set; }

    /// <summary>The server's connection id for this socket.</summary>
    public Guid ConnectionId { get; private set; }

    /// <summary>The commit log epoch this client's resume cursor counts against.</summary>
    public Guid LogEpochId { get; private set; }

    /// <summary>The last LSN this client fully applied — the resume cursor.</summary>
    public ulong LastAckedLsn => Interlocked.Read(ref _lastAckedLsn);

    /// <summary>Total websocket payload bytes received. The resume-versus-refetch saving is measured here.</summary>
    public long BytesReceived => Interlocked.Read(ref _bytesReceived);

    public bool IsConnected => _socket is { State: WebSocketState.Open };

    /// <summary>Connection-scoped errors, including the server's demand for an overflow resync.</summary>
    public event Action<ErrorFrame>? OnError;

    /// <summary>Fires when the socket dies, gracefully or not.</summary>
    public event Action? OnDisconnected;

    /// <summary>Dials, performs the versioned handshake, and starts the receive loop.</summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (IsConnected)
            throw new InvalidOperationException("Already connected.");
        await DialAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Invokes a reducer and awaits its result, returning the commit LSN (0 when nothing was
    /// written). Failures throw <see cref="MelangeCallException"/> with the server's error code.
    /// </summary>
    public async Task<ulong> CallReducerAsync(string reducer, object?[]? arguments = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(reducer);
        var requestId = Interlocked.Increment(ref _nextRequestId);
        var pending = new TaskCompletionSource<ReducerResultFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingCalls[requestId] = pending;
        try
        {
            await SendAsync(new CallReducerFrame(
                requestId,
                reducer,
                ReducerArgs.Encode(arguments ?? []),
                Activity.Current?.Id)
            { Channel = MelangeChannels.Calls }, cancellationToken).ConfigureAwait(false);
            var result = await pending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!result.Ok)
                throw new MelangeCallException(result.ErrorCode ?? MelangeErrorCodes.Internal, result.Message ?? "The call failed.");
            return result.Lsn;
        }
        finally
        {
            _pendingCalls.TryRemove(requestId, out _);
        }
    }

    /// <summary>
    /// Subscribes to a query and awaits the full initial set. The returned subscription's cache
    /// then stays current as deltas arrive.
    /// </summary>
    public async Task<MelangeSubscription> SubscribeAsync(
        string query,
        IReadOnlyDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var id = Interlocked.Increment(ref _nextSubscriptionId);
        var subscription = new MelangeSubscription(id, query, parameters);
        _subscriptions[id] = subscription;
        await SendAsync(new SubscribeFrame(id, query, parameters) { Channel = MelangeChannels.Data }, cancellationToken).ConfigureAwait(false);
        try
        {
            await subscription.Applied.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _subscriptions.TryRemove(id, out _);
            throw;
        }

        return subscription;
    }

    /// <summary>
    /// Re-scopes a subscription's predicate parameters in place — the terrain-streaming pattern.
    /// The server answers with inserts for newly visible rows and deletes for newly invisible
    /// ones on the data channel; there is no explicit acknowledgement.
    /// </summary>
    public async Task RescopeAsync(
        MelangeSubscription subscription,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        subscription.Parameters = parameters;
        await SendAsync(new SubscribeFrame(subscription.Id, subscription.Query, parameters) { Channel = MelangeChannels.Data }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes a subscription and awaits the server's acknowledgement.</summary>
    public async Task UnsubscribeAsync(MelangeSubscription subscription, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingUnsubscribes[subscription.Id] = pending;
        await SendAsync(new UnsubscribeFrame(subscription.Id) { Channel = MelangeChannels.Data }, cancellationToken).ConfigureAwait(false);
        await pending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        _subscriptions.TryRemove(subscription.Id, out _);
    }

    /// <summary>
    /// Reconnects after a drop. Attempts <c>Resume</c> from the last acked LSN against the known
    /// log epoch; returns true when the server served the gap (no initial sets were refetched),
    /// false when it demanded — and this method performed — a full re-establishment.
    /// </summary>
    public async Task<bool> ReconnectAsync(CancellationToken cancellationToken = default)
    {
        var previousEpoch = LogEpochId;
        await DialAsync(cancellationToken).ConfigureAwait(false);

        var liveSubscriptions = _subscriptions.Values.Where(s => s.IsLive).ToList();
        if (liveSubscriptions.Count > 0 && LogEpochId == previousEpoch)
        {
            var pending = new TaskCompletionSource<ResumeResultFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingResume = pending;
            await SendAsync(new ResumeFrame(
                previousEpoch,
                LastAckedLsn,
                [.. liveSubscriptions.Select(s => new ResumeSubscription(s.Id, s.Query, s.Parameters))]), cancellationToken).ConfigureAwait(false);
            var result = await pending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (result.Accepted)
            {
                await ReestablishAsync(_subscriptions.Values.Where(s => !s.IsLive), cancellationToken).ConfigureAwait(false);
                return true;
            }
        }

        // Full resync: the server said the gap cannot be served (or this is a different log
        // incarnation entirely). Every cache reloads from a fresh initial set.
        Interlocked.Exchange(ref _lastAckedLsn, 0);
        foreach (var subscription in _subscriptions.Values)
            subscription.ResetForResync();
        await ReestablishAsync(_subscriptions.Values, cancellationToken).ConfigureAwait(false);
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        if (_socket is { } socket)
        {
            try
            {
                if (socket.State == WebSocketState.Open)
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, timeout.Token).ConfigureAwait(false);
                }
            }
            catch (Exception exception) when (exception is OperationCanceledException or WebSocketException)
            {
            }

            socket.Dispose();
        }

        if (_receiveLoop is { } loop)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is OperationCanceledException or WebSocketException)
            {
            }
        }
    }

    /// <summary>Abandons the socket without a close frame — an ungraceful drop, for testing heartbeats.</summary>
    public void Abort() => _socket?.Abort();

    private async Task DialAsync(CancellationToken cancellationToken)
    {
        var socket = new ClientWebSocket();
        socket.Options.HttpVersion = _options.HttpVersion;
        socket.Options.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        if (_options.CompressionEnabled)
            socket.Options.DangerousDeflateOptions = new WebSocketDeflateOptions();
        await socket.ConnectAsync(_options.Uri, SharedInvoker, cancellationToken).ConfigureAwait(false);

        _socket = socket;
        var welcomePending = new TaskCompletionSource<WelcomeFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingWelcome = welcomePending;
        _receiveLoop = Task.Run(() => ReceiveLoopAsync(socket), CancellationToken.None);
        await SendAsync(new HelloFrame(
            MessagePackFrameSerializer.ProtocolVersion,
            MessagePackFrameSerializer.ProtocolVersion,
            _options.Token), cancellationToken).ConfigureAwait(false);
        var welcome = await welcomePending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        ProtocolVersion = welcome.Version;
        ConnectionId = welcome.ConnectionId;
        LogEpochId = welcome.EpochId;
        NegotiatedHttpProtocol = welcome.HttpProtocol;
    }

    private async Task ReestablishAsync(IEnumerable<MelangeSubscription> subscriptions, CancellationToken cancellationToken)
    {
        foreach (var subscription in subscriptions)
        {
            subscription.ResetForResync();
            await SendAsync(new SubscribeFrame(subscription.Id, subscription.Query, subscription.Parameters) { Channel = MelangeChannels.Data }, cancellationToken).ConfigureAwait(false);
            await subscription.Applied.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task SendAsync(Frame frame, CancellationToken cancellationToken)
    {
        var socket = _socket ?? throw new InvalidOperationException("Not connected.");
        var bytes = _serializer.Serialize(frame);
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await socket.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task ReceiveLoopAsync(ClientWebSocket socket)
    {
        var buffer = new byte[64 * 1024];
        using var message = new MemoryStream();
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                message.SetLength(0);
                WebSocketReceiveResult result;
                do
                {
                    result = await socket.ReceiveAsync(buffer, CancellationToken.None).ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                        return;
                    message.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                Interlocked.Add(ref _bytesReceived, message.Length);
                var frame = _serializer.Deserialize(message.GetBuffer().AsSpan(0, (int)message.Length));
                _options.FrameInspector?.Invoke(frame, (int)message.Length);
                HandleFrame(frame);
            }
        }
        catch (Exception exception) when (exception is WebSocketException or OperationCanceledException or ObjectDisposedException)
        {
        }
        finally
        {
            FailPending();
            OnDisconnected?.Invoke();
        }
    }

    private void HandleFrame(Frame frame)
    {
        switch (frame)
        {
            case WelcomeFrame welcome:
                _pendingWelcome?.TrySetResult(welcome);
                break;
            case ReducerResultFrame result:
                if (_pendingCalls.TryRemove(result.RequestId, out var call))
                    call.TrySetResult(result);
                break;
            case SubscriptionAppliedFrame chunk:
                if (_subscriptions.TryGetValue(chunk.SubscriptionId, out var applied))
                {
                    applied.AcceptInitialChunk(chunk);

                    // The initial set is consistent at its anchor, and every delta frame at or
                    // below the anchor was already received in order on this connection — so the
                    // attachment cursor may advance, and a resume never replays the anchor's past.
                    if (chunk.IsLast)
                        InterlockedMax(ref _lastAckedLsn, chunk.AnchorLsn);
                }

                break;
            case TransactionUpdateFrame update:
                foreach (var group in update.Updates)
                {
                    if (_subscriptions.TryGetValue(group.SubscriptionId, out var subscription))
                        subscription.Apply(update.Lsn, group.Ops);
                }

                // The whole frame is applied or retained; the cursor may advance.
                InterlockedMax(ref _lastAckedLsn, update.Lsn);
                break;
            case UnsubscribedFrame unsubscribed:
                if (_pendingUnsubscribes.TryRemove(unsubscribed.SubscriptionId, out var pendingUnsubscribe))
                    pendingUnsubscribe.TrySetResult();
                break;
            case ResumeResultFrame resumeResult:
                _pendingResume?.TrySetResult(resumeResult);
                break;
            case PingFrame ping:
                _ = SendPongAsync(ping.Id);
                break;
            case PongFrame:
                break;
            case ReauthenticateResultFrame:
                break;
            case ErrorFrame error:
                HandleError(error);
                break;
        }
    }

    private void HandleError(ErrorFrame error)
    {
        if (error.SubscriptionId != 0 && _subscriptions.TryGetValue(error.SubscriptionId, out var subscription))
        {
            subscription.FailSubscribe(error.Code, error.Message);
            return;
        }

        if (error.RequestId != 0 && _pendingCalls.TryRemove(error.RequestId, out var call))
        {
            call.TrySetException(new MelangeCallException(error.Code, error.Message));
            return;
        }

        _pendingWelcome?.TrySetException(new MelangeCallException(error.Code, error.Message));
        OnError?.Invoke(error);
    }

    private async Task SendPongAsync(uint id)
    {
        try
        {
            await SendAsync(new PongFrame(id), CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is InvalidOperationException or WebSocketException or ObjectDisposedException)
        {
        }
    }

    private void FailPending()
    {
        foreach (var (id, pending) in _pendingCalls)
        {
            if (_pendingCalls.TryRemove(id, out _))
                pending.TrySetException(new MelangeCallException(MelangeErrorCodes.Internal, "The connection dropped before the result arrived."));
        }

        _pendingWelcome?.TrySetException(new MelangeCallException(MelangeErrorCodes.Internal, "The connection dropped during the handshake."));
        _pendingResume?.TrySetException(new MelangeCallException(MelangeErrorCodes.Internal, "The connection dropped during resume."));
    }

    private static void InterlockedMax(ref ulong location, ulong value)
    {
        var current = Interlocked.Read(ref location);
        while (value > current)
        {
            var previous = Interlocked.CompareExchange(ref location, value, current);
            if (previous == current)
                return;
            current = previous;
        }
    }
}
