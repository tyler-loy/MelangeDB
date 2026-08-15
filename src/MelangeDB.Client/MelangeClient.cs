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
/// server says the gap cannot be served. Under <see cref="DispatchMode.Immediate"/> (the
/// default) frames apply and events fire on the receive loop as they arrive; under
/// <see cref="DispatchMode.Manual"/> whole data frames queue in arrival order and
/// <see cref="FrameTick"/> applies them on the caller's own thread — the game-loop mode.
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
    private TaskCompletionSource<ReauthenticateResultFrame>? _pendingReauthenticate;
    private uint _nextRequestId;
    private uint _nextSubscriptionId;
    private long _bytesReceived;
    private ulong _lastAckedLsn;

    // The Manual-dispatch pump. Entries are whole frames (or queued lifecycle events) in
    // arrival order, each tagged with the era (dial generation) it arrived on; a full resync
    // raises the floor so entries from the dead era can never apply against the reset caches.
    // The lock serializes entry application against the resync clear — never held while the
    // receive loop enqueues, so the socket is never blocked by a slow tick.
    private readonly Lock _dispatchLock = new();
    private readonly ConcurrentQueue<DispatchEntry> _dispatchQueue = new();
    private int _dispatchCount;
    private ErrorFrame? _overflowError;
    private int _overflowEra;
    private int _era;
    private int _eraFloor;
    private int _ticking;

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

    /// <summary>
    /// The identity this connection authenticated as, as the server derived it during the
    /// handshake. This is the value that distinguishes "my rows" from everyone else's in a
    /// subscription-fed cache — read it from here rather than re-deriving it from the token,
    /// because the derivation belongs to the server alone and a second implementation of it is
    /// the one disagreement the contract cannot survive. <see cref="Identity.None"/> until the
    /// first successful connect; re-auth can never change it (an identity change closes the
    /// connection instead).
    /// </summary>
    public Identity Identity { get; private set; }

    /// <summary>The commit log epoch this client's resume cursor counts against.</summary>
    public Guid LogEpochId { get; private set; }

    /// <summary>
    /// The last LSN this client applied or retained — the resume cursor. Under Manual dispatch
    /// it advances at receive time: a queued frame is retained in-process, the same precedent
    /// as the rescope buffer one layer down, so the cursor may run ahead of what handlers have
    /// been told.
    /// </summary>
    public ulong LastAckedLsn => Interlocked.Read(ref _lastAckedLsn);

    /// <summary>Total websocket payload bytes received. The resume-versus-refetch saving is measured here.</summary>
    public long BytesReceived => Interlocked.Read(ref _bytesReceived);

    public bool IsConnected => _socket is { State: WebSocketState.Open };

    /// <summary>
    /// Connection-scoped errors, including the server's demand for an overflow resync. Under
    /// Manual dispatch the invocation joins the frame queue and fires from <see cref="FrameTick"/>
    /// after every frame received before it — except the client's own dispatch-overflow error,
    /// which jumps to the head of the queue so the next tick learns the connection died before
    /// spending its budget on the retained backlog.
    /// </summary>
    public event Action<ErrorFrame>? OnError;

    /// <summary>
    /// Fires when the current socket dies, gracefully or not. A socket a reconnect has already
    /// replaced tears down silently — its outage is over, and announcing it would be a lie.
    /// Under Manual dispatch the invocation joins the frame queue and fires from
    /// <see cref="FrameTick"/> after every frame received before it.
    /// </summary>
    public event Action? OnDisconnected;

    /// <summary>
    /// Test seam, consumed once by the next receive loop to die: awaited at the top of that
    /// loop's teardown, before any teardown effect. Lets a test hold a dying loop across a
    /// reconnect to pin the era-scoping schedule.
    /// </summary>
    internal Func<Task>? ReceiveTeardownGate;

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
    /// then stays current as deltas arrive. Under Manual dispatch the initial set applies only
    /// inside <see cref="FrameTick"/>, so this completes only while something ticks — await it,
    /// never block the ticking thread on it.
    /// </summary>
    public Task<MelangeSubscription> SubscribeAsync(
        string query,
        IReadOnlyDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default) =>
        SubscribeAsync(query, parameters, sink: null, cancellationToken);

    /// <summary>
    /// The typed-cache entry point: the sink is attached before the Subscribe frame leaves, so
    /// the initial set, every delta, and every future rescope reach the typed cache with no
    /// window where a row could slip past unobserved.
    /// </summary>
    internal async Task<MelangeSubscription> SubscribeAsync(
        string query,
        IReadOnlyDictionary<string, object?>? parameters,
        ISubscriptionSink? sink,
        CancellationToken cancellationToken)
    {
        var id = Interlocked.Increment(ref _nextSubscriptionId);
        var subscription = new MelangeSubscription(id, query, parameters, sink);
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
    /// false when it demanded — and this method performed — a full re-establishment. Under
    /// Manual dispatch, re-established initial sets apply only inside <see cref="FrameTick"/>,
    /// so this completes only while something ticks — await it, never block the ticking thread
    /// on it. The resume-accepted path keeps the frame queue (frames buffered before the drop
    /// apply before the resumed gap, in order); the full-resync path clears it, because a frame
    /// from the dead era must never apply against the reset caches.
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
        // incarnation entirely). Every cache reloads from a fresh initial set, so anything the
        // pump still holds from the dead era is invalid against them — clear it, and raise the
        // era floor so a straggler a dying receive loop enqueues late can never apply either.
        Interlocked.Exchange(ref _lastAckedLsn, 0);
        ClearDispatchQueue();
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
        var token = _options.Token ?? await _options.TokenStore.LoadAsync(cancellationToken).ConfigureAwait(false);
        var uri = _options.Uri;
        string? helloToken = token;
        if (_options.UseTicket)
        {
            // The header-less path: exchange the JWT for a single-use ticket over HTTP, then put
            // only the near-worthless ticket on the socket URL — never the token itself.
            var ticket = await MintTicketAsync(token, cancellationToken).ConfigureAwait(false);
            uri = new UriBuilder(uri) { Query = $"ticket={Uri.EscapeDataString(ticket)}" }.Uri;
            helloToken = null;
        }

        var socket = new ClientWebSocket();
        socket.Options.HttpVersion = _options.HttpVersion;
        socket.Options.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        if (_options.CompressionEnabled)
            socket.Options.DangerousDeflateOptions = new WebSocketDeflateOptions();
        await socket.ConnectAsync(uri, SharedInvoker, cancellationToken).ConfigureAwait(false);

        // Anything still pending belongs to the era this dial replaces and can never complete
        // now; fail it here, deterministically, because the old receive loop's own teardown is
        // era-scoped and will decline to touch state that no longer belongs to it.
        FailPending();
        _socket = socket;
        var welcomePending = new TaskCompletionSource<WelcomeFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingWelcome = welcomePending;
        var era = Interlocked.Increment(ref _era);
        _receiveLoop = Task.Run(() => ReceiveLoopAsync(socket, era), CancellationToken.None);
        await SendAsync(new HelloFrame(
            MessagePackFrameSerializer.ProtocolVersion,
            MessagePackFrameSerializer.ProtocolVersion,
            helloToken), cancellationToken).ConfigureAwait(false);
        var welcome = await welcomePending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        ProtocolVersion = welcome.Version;
        ConnectionId = welcome.ConnectionId;
        LogEpochId = welcome.EpochId;
        NegotiatedHttpProtocol = welcome.HttpProtocol;
        Identity = welcome.Identity;
        if (token is not null)
            await _options.TokenStore.SaveAsync(token, cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> MintTicketAsync(string? token, CancellationToken cancellationToken)
    {
        var endpoint = _options.TicketUri ?? new UriBuilder(_options.Uri)
        {
            Scheme = _options.Uri.Scheme switch
            {
                "wss" => "https",
                "ws" => "http",
                var scheme => scheme,
            },
            Path = _options.Uri.AbsolutePath.TrimEnd('/') + "/ticket",
        }.Uri;

        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint);
        if (token is not null)
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
        using var response = await SharedInvoker.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new MelangeCallException(MelangeErrorCodes.Unauthorized, $"The ticket endpoint answered {(int)response.StatusCode}: {body}");
        using var json = System.Text.Json.JsonDocument.Parse(body);
        return json.RootElement.GetProperty("ticket").GetString()
            ?? throw new MelangeCallException(MelangeErrorCodes.Internal, "The ticket endpoint returned no ticket.");
    }

    /// <summary>
    /// Presents a fresh token before the current one's expiry (plus the server's grace window).
    /// The server accepts only a token resolving to the <em>same</em> identity — an identity
    /// switch closes the connection instead, by design. On success the token is persisted to the
    /// <see cref="MelangeClientOptions.TokenStore"/>; a guest conversion is exactly this call with
    /// the account-linked token.
    /// </summary>
    public async Task ReauthenticateAsync(string token, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        var pending = new TaskCompletionSource<ReauthenticateResultFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingReauthenticate = pending;
        await SendAsync(new ReauthenticateFrame(token), cancellationToken).ConfigureAwait(false);
        var result = await pending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (!result.Accepted)
            throw new MelangeCallException(MelangeErrorCodes.Unauthorized, result.Message ?? "Re-authentication was rejected.");
        _options.Token = token;
        await _options.TokenStore.SaveAsync(token, cancellationToken).ConfigureAwait(false);
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

    private async Task ReceiveLoopAsync(ClientWebSocket socket, int era)
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
                HandleFrame(frame, socket, era);
            }
        }
        catch (Exception exception) when (exception is WebSocketException or OperationCanceledException or ObjectDisposedException)
        {
        }
        finally
        {
            if (Interlocked.Exchange(ref ReceiveTeardownGate, null) is { } gate)
                await gate().ConfigureAwait(false);

            // Teardown is scoped to this socket's era. A reconnect may already have installed a
            // fresh socket and a fresh pending handshake; a stale loop dying late (an Abort whose
            // death processing lost the scheduling race to ReconnectAsync) must fail neither —
            // its own era's pendings were already failed by the dial that replaced it, and firing
            // OnDisconnected after a successful reconnect would announce an outage that is over.
            if (ReferenceEquals(_socket, socket))
            {
                FailPending();

                // The event half of the teardown defers with the frames it trails: under Manual
                // dispatch it joins the queue, so handlers hear about the drop only after every
                // frame that arrived before it. Failing the pendings above stays immediate —
                // those complete awaited Tasks and must never wait for a tick.
                if (_options.Dispatch == DispatchMode.Manual)
                    EnqueueEvent(new DispatchEntry(era, EntryKind.Disconnected, null));
                else
                    OnDisconnected?.Invoke();
            }
        }
    }

    private void HandleFrame(Frame frame, ClientWebSocket socket, int era)
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

            // The frame-tick seam. These two cases are the data channel — everything that
            // mutates a cache or raises a row event flows through them — so Manual dispatch
            // defers exactly here, whole frames in arrival order, and FrameTick runs the same
            // apply machinery on the caller's thread. Control-plane frames (every other case)
            // stay immediate: they complete awaited Tasks and answer heartbeats.
            case SubscriptionAppliedFrame or TransactionUpdateFrame:
                if (_options.Dispatch == DispatchMode.Manual)
                    EnqueueDataFrame(frame, socket, era);
                else
                    ApplyDataFrame(frame);
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
            case ReauthenticateResultFrame reauthenticated:
                _pendingReauthenticate?.TrySetResult(reauthenticated);
                break;
            case ErrorFrame error:
                HandleError(error, era);
                break;
        }
    }

    private void HandleError(ErrorFrame error, int era)
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

        // The event invocation defers with the frames it arrived among; failing the pending
        // handshake above stays immediate, because it completes an awaited Task.
        if (_options.Dispatch == DispatchMode.Manual)
            EnqueueEvent(new DispatchEntry(era, EntryKind.Error, error));
        else
            OnError?.Invoke(error);
    }

    /// <summary>
    /// Manual dispatch only: applies queued whole frames and fires queued lifecycle events on
    /// the calling thread, in arrival order, and returns how many were applied. A frame is one
    /// whole commit (or one initial-set chunk), so a budgeted tick never observes half a
    /// transaction — the flip side is that a completed rescope's reconcile is one indivisible
    /// frame and can be big. The pump is single-consumer by contract: a concurrent call throws,
    /// and calling this at all under <see cref="DispatchMode.Immediate"/> throws — a
    /// misconfiguration should be loud, not silently idle.
    /// </summary>
    /// <param name="maxFrames">The most entries this tick may apply; must be positive.</param>
    public int FrameTick(int maxFrames = int.MaxValue)
    {
        if (_options.Dispatch != DispatchMode.Manual)
            throw new InvalidOperationException("FrameTick requires DispatchMode.Manual — under Immediate dispatch frames apply on the receive loop as they arrive.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxFrames);
        if (Interlocked.CompareExchange(ref _ticking, 1, 0) != 0)
            throw new InvalidOperationException("A FrameTick is already draining — the pump is single-consumer by contract.");
        try
        {
            var applied = 0;
            while (applied < maxFrames && TickOne())
                applied++;
            return applied;
        }
        finally
        {
            Volatile.Write(ref _ticking, 0);
        }
    }

    /// <summary>
    /// Applies the next live entry, skipping entries a full resync marked dead, and reports
    /// whether anything applied. Application holds the dispatch lock so a concurrent resync's
    /// clear-and-reset can never interleave with half an entry.
    /// </summary>
    private bool TickOne()
    {
        while (true)
        {
            lock (_dispatchLock)
            {
                // The synthesized overflow error outranks the backlog: the app must learn its
                // connection died before it spends tick budget chewing through retained frames.
                if (_overflowError is { } overflow)
                {
                    _overflowError = null;
                    if (_overflowEra < _eraFloor)
                        continue;
                    OnError?.Invoke(overflow);
                    return true;
                }

                if (!_dispatchQueue.TryDequeue(out var entry))
                    return false;
                Interlocked.Decrement(ref _dispatchCount);

                // An entry from before a full resync must never touch the reset caches.
                if (entry.Era < _eraFloor)
                    continue;

                switch (entry.Kind)
                {
                    case EntryKind.Data:
                        ApplyDataFrame(entry.Frame!);
                        break;
                    case EntryKind.Error:
                        OnError?.Invoke((ErrorFrame)entry.Frame!);
                        break;
                    case EntryKind.Disconnected:
                        OnDisconnected?.Invoke();
                        break;
                }

                return true;
            }
        }
    }

    /// <summary>The one apply path both dispatch modes share — the pump moves where it runs, never what it does.</summary>
    private void ApplyDataFrame(Frame frame)
    {
        switch (frame)
        {
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
        }
    }

    private void EnqueueDataFrame(Frame frame, ClientWebSocket socket, int era)
    {
        if (Volatile.Read(ref _dispatchCount) >= _options.DispatchQueueLimit)
        {
            // Overflow: never drop silently (the cache would diverge without a trace), never
            // block the receive loop (a blocked loop stops answering pings and the server
            // convicts the client illegibly). Fail the connection loudly instead — the error
            // takes the head of the queue, the socket is aborted, and because the cursor never
            // advanced past the dropped frame, the ordinary reconnect's resume replays it.
            lock (_dispatchLock)
            {
                if (_overflowError is null)
                {
                    _overflowError = new ErrorFrame(
                        MelangeErrorCodes.DispatchOverflow,
                        $"The dispatch queue reached DispatchQueueLimit ({_options.DispatchQueueLimit}) without a FrameTick; the connection was aborted. Reconnect to recover — the resume cursor stopped at the last retained frame.");
                    _overflowEra = era;
                }
            }

            socket.Abort();
            return;
        }

        Interlocked.Increment(ref _dispatchCount);
        _dispatchQueue.Enqueue(new DispatchEntry(era, EntryKind.Data, frame));

        // The cursor advances at receive time: a queued frame is retained in-process, the same
        // "applied or retained" contract the rescope buffer relies on one layer down. The same
        // conditions as the apply path — an initial set acks only when its last chunk names an
        // anchor for a subscription this client still holds.
        switch (frame)
        {
            case SubscriptionAppliedFrame { IsLast: true } chunk when _subscriptions.ContainsKey(chunk.SubscriptionId):
                InterlockedMax(ref _lastAckedLsn, chunk.AnchorLsn);
                break;
            case TransactionUpdateFrame update:
                InterlockedMax(ref _lastAckedLsn, update.Lsn);
                break;
        }
    }

    /// <summary>
    /// Queues a lifecycle event behind every frame received before it. Exempt from the queue
    /// limit: these arrive once per connection death, not as a stream, and the disconnect that
    /// trails an overflow abort must itself be deliverable.
    /// </summary>
    private void EnqueueEvent(DispatchEntry entry)
    {
        Interlocked.Increment(ref _dispatchCount);
        _dispatchQueue.Enqueue(entry);
    }

    /// <summary>
    /// The full-resync half of the cursor reset: drops everything queued and raises the era
    /// floor to the just-dialed socket's era, so even an entry a dying receive loop enqueues
    /// after this point can never apply against the caches the resync is about to rebuild.
    /// </summary>
    private void ClearDispatchQueue()
    {
        lock (_dispatchLock)
        {
            _eraFloor = Volatile.Read(ref _era);
            _overflowError = null;
            while (_dispatchQueue.TryDequeue(out _))
                Interlocked.Decrement(ref _dispatchCount);
        }
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
        _pendingReauthenticate?.TrySetException(new MelangeCallException(
            MelangeErrorCodes.Internal,
            "The connection dropped before the re-authentication result arrived — a token for a different identity closes the connection by design."));
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

    private enum EntryKind
    {
        Data,
        Error,
        Disconnected,
    }

    /// <summary>
    /// One queued dispatch: a whole data frame, a connection-scoped error, or a disconnect —
    /// tagged with the era (dial generation) it arrived on, which is what lets a full resync
    /// declare everything before it dead without racing the queue.
    /// </summary>
    private readonly record struct DispatchEntry(int Era, EntryKind Kind, Frame? Frame);
}
