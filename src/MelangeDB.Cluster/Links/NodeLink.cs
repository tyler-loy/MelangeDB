using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text.Json;
using MelangeDB.Server;

namespace MelangeDB.Cluster;

/// <summary>
/// A node-link request that failed. <see cref="IsPeerError"/> distinguishes the two failure
/// shapes a saga must treat differently: the peer <em>replied</em> with an error (it definitively
/// did not perform the request) versus a timeout or link death (the peer may or may not have —
/// the request's effect is unknown, and only reconciliation can say).
/// </summary>
public sealed class NodeLinkException(string message, bool isPeerError = false, string? code = null) : InvalidOperationException(message)
{
    /// <summary>True when the peer answered with an error reply — the request definitively did not happen.</summary>
    public bool IsPeerError { get; } = isPeerError;

    /// <summary>
    /// The error code of a reducer call that failed on the peer (<c>denied</c>,
    /// <c>rate_limited</c>, <c>outcome_unknown</c>, ...), or null for any other failure. An
    /// <c>outcome_unknown</c> reply is the one peer error whose request may have committed.
    /// </summary>
    public string? Code { get; } = code;
}

/// <summary>
/// Per-node counters over node-link traffic, by message type — counts and payload bytes. The
/// "zero cross-node traffic" acceptance test counts these — network calls, not code inspection —
/// so every frame that crosses a link increments here, heartbeats included (tests exclude them by
/// type name). The byte counters are the border-band bandwidth measure: sum the
/// <c>border-apply</c>/<c>border-reset-part-apply</c> types on a node to see what its neighbours'
/// edges cost it.
/// </summary>
public sealed class ClusterMetrics
{
    private readonly ConcurrentDictionary<string, long> _sent = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _received = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _sentBytes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _receivedBytes = new(StringComparer.Ordinal);

    internal void RecordSent(string type, int bytes)
    {
        _sent.AddOrUpdate(type, 1, static (_, count) => count + 1);
        _sentBytes.AddOrUpdate(type, bytes, (_, total) => total + bytes);
    }

    internal void RecordReceived(string type, int bytes)
    {
        _received.AddOrUpdate(type, 1, static (_, count) => count + 1);
        _receivedBytes.AddOrUpdate(type, bytes, (_, total) => total + bytes);
    }

    public IReadOnlyDictionary<string, long> SentByType => _sent;

    public IReadOnlyDictionary<string, long> ReceivedByType => _received;

    /// <summary>Payload bytes sent per message type.</summary>
    public IReadOnlyDictionary<string, long> SentBytesByType => _sentBytes;

    /// <summary>Payload bytes received per message type.</summary>
    public IReadOnlyDictionary<string, long> ReceivedBytesByType => _receivedBytes;

    /// <summary>Total messages sent, excluding the given types (typically the heartbeat pair).</summary>
    public long TotalSentExcept(params string[] exceptTypes) =>
        _sent.Where(pair => !exceptTypes.Contains(pair.Key, StringComparer.Ordinal)).Sum(static pair => pair.Value);

    private long _handoffsStarted;
    private long _handoffsCompleted;
    private long _handoffsAborted;
    private long _handoffsUnresolved;
    private long _handoffsRateLimited;
    private long _handoffsInFlight;

    /// <summary>Transfer sagas this hub started (explicit and boundary-triggered alike).</summary>
    public long HandoffsStarted => Interlocked.Read(ref _handoffsStarted);

    /// <summary>Sagas whose destination became authoritative.</summary>
    public long HandoffsCompleted => Interlocked.Read(ref _handoffsCompleted);

    /// <summary>Sagas that aborted definitively: the entity stayed on its origin.</summary>
    public long HandoffsAborted => Interlocked.Read(ref _handoffsAborted);

    /// <summary>Sagas whose import fate was unknowable when the coordinator gave up; a reconciler resolves each later.</summary>
    public long HandoffsUnresolved => Interlocked.Read(ref _handoffsUnresolved);

    /// <summary>Boundary-triggered requests suppressed by Cluster:HandoffMinIntervalMs — hysteresis working.</summary>
    public long HandoffsRateLimited => Interlocked.Read(ref _handoffsRateLimited);

    /// <summary>Sagas currently in flight on this hub.</summary>
    public long HandoffsInFlight => Interlocked.Read(ref _handoffsInFlight);

    internal void HandoffStarted()
    {
        Interlocked.Increment(ref _handoffsStarted);
        Interlocked.Increment(ref _handoffsInFlight);
    }

    internal void HandoffEnded(bool completed, bool aborted, bool unresolved)
    {
        Interlocked.Decrement(ref _handoffsInFlight);
        if (completed)
            Interlocked.Increment(ref _handoffsCompleted);
        if (aborted)
            Interlocked.Increment(ref _handoffsAborted);
        if (unresolved)
            Interlocked.Increment(ref _handoffsUnresolved);
    }

    internal void HandoffRateLimited() => Interlocked.Increment(ref _handoffsRateLimited);

    private long _handoffRequestsReceived;

    /// <summary>Boundary-triggered transfer requests the hub received, dropped or not.</summary>
    public long HandoffRequestsReceived => Interlocked.Read(ref _handoffRequestsReceived);

    internal void HandoffRequestReceived() => Interlocked.Increment(ref _handoffRequestsReceived);

    internal void HandoffResolvedRemotely(bool released)
    {
        if (released)
            Interlocked.Increment(ref _handoffsCompleted);
        else
            Interlocked.Increment(ref _handoffsAborted);
    }

    private long _drainsStarted;
    private long _drainsCompleted;
    private long _drainsFailed;

    /// <summary>Planned shard drains this hub started.</summary>
    public long DrainsStarted => Interlocked.Read(ref _drainsStarted);

    /// <summary>Drains whose destination took ownership and the gateways swapped.</summary>
    public long DrainsCompleted => Interlocked.Read(ref _drainsCompleted);

    /// <summary>Drains that failed; the origin kept (or reopens) the shard.</summary>
    public long DrainsFailed => Interlocked.Read(ref _drainsFailed);

    internal void DrainStarted() => Interlocked.Increment(ref _drainsStarted);

    internal void DrainEnded(bool completed)
    {
        if (completed)
            Interlocked.Increment(ref _drainsCompleted);
        else
            Interlocked.Increment(ref _drainsFailed);
    }

    private long _provisionsRequested;
    private long _provisionsFulfilled;
    private long _provisionsExpired;
    private long _decommissionsRequested;

    /// <summary>Provision tickets this hub obtained from its node provisioner.</summary>
    public long ProvisionsRequested => Interlocked.Read(ref _provisionsRequested);

    /// <summary>Tickets whose named node joined membership before the ticket expired.</summary>
    public long ProvisionsFulfilled => Interlocked.Read(ref _provisionsFulfilled);

    /// <summary>Tickets whose named node never arrived within Cluster:ProvisionTicketTimeoutMs.</summary>
    public long ProvisionsExpired => Interlocked.Read(ref _provisionsExpired);

    /// <summary>DecommissionAsync calls this hub issued (scale-in and late arrivals alike).</summary>
    public long DecommissionsRequested => Interlocked.Read(ref _decommissionsRequested);

    internal void ProvisionRequested() => Interlocked.Increment(ref _provisionsRequested);

    internal void ProvisionFulfilled() => Interlocked.Increment(ref _provisionsFulfilled);

    internal void ProvisionExpired() => Interlocked.Increment(ref _provisionsExpired);

    internal void DecommissionRequested() => Interlocked.Increment(ref _decommissionsRequested);

    private long _nodesDeclaredDead;
    private long _leaseLapses;

    /// <summary>
    /// Hub: the gap between consecutive heartbeats from one node, as the hub received them — the
    /// quantity <c>Cluster:FailureTimeoutMs</c> is compared against. A gap longer than the timeout
    /// is a death declaration; the distribution says how close a healthy cluster comes to one.
    /// </summary>
    public LivenessGaps HeartbeatGaps { get; } = new();

    /// <summary>
    /// Shard node: the time from sending one acknowledged heartbeat to the reply to the next,
    /// which is how long the node's lease had to outlast. The lease lapses when this exceeds nine
    /// tenths of the hub's failure timeout.
    /// </summary>
    public LivenessGaps LeaseRenewalGaps { get; } = new();

    /// <summary>Hub: nodes this hub declared dead, by timeout or (with fast failover) by link loss.</summary>
    public long NodesDeclaredDead => Interlocked.Read(ref _nodesDeclaredDead);

    /// <summary>Shard node: times this node's lease lapsed and it parked its shards.</summary>
    public long LeaseLapses => Interlocked.Read(ref _leaseLapses);

    internal void NodeDeclaredDead() => Interlocked.Increment(ref _nodesDeclaredDead);

    internal void LeaseLapsed() => Interlocked.Increment(ref _leaseLapses);

    // ---- Handoff timing (phase 31): where a transfer's time goes under load. ----

    /// <summary>Hub: the freeze request to the origin, sent to answered.</summary>
    public StepDurations HandoffFreeze { get; } = new();

    /// <summary>Hub: the import request to the destination, sent to durably answered.</summary>
    public StepDurations HandoffImport { get; } = new();

    /// <summary>Hub: the destination-authoritative notification (transfer listeners, gateway mutes).</summary>
    public StepDurations HandoffNotify { get; } = new();

    /// <summary>Hub: the release request to the origin, sent to answered.</summary>
    public StepDurations HandoffRelease { get; } = new();

    /// <summary>Hub: a completed saga, start to release answered.</summary>
    public StepDurations HandoffSaga { get; } = new();

    /// <summary>
    /// Hub (gateway): how long a client connection held its shard-bound calls for a transfer, from
    /// the saga's start to the swap flushing them on the destination.
    /// </summary>
    public StepDurations GatewayHold { get; } = new();

    /// <summary>Hub (gateway): calls held per completed swap: the burst the destination receives at once.</summary>
    public StepDurations GatewayHeldCalls { get; } = new();

    /// <summary>Hub (gateway): a swap's wait for the connection's attachment lock.</summary>
    public StepDurations GatewaySwapLockWait { get; } = new();

    /// <summary>Hub (gateway): a swap's wait for its destination session, pre-opened or connected then.</summary>
    public StepDurations GatewaySwapConnect { get; } = new();

    private long _gatewaySwapsPreopened;
    private long _gatewaySwapsConnected;

    /// <summary>Hub (gateway): swaps that used a session pre-opened on approach.</summary>
    public long GatewaySwapsPreopened => Interlocked.Read(ref _gatewaySwapsPreopened);

    /// <summary>Hub (gateway): swaps that had to connect to the destination then (no pre-open, or it failed).</summary>
    public long GatewaySwapsConnected => Interlocked.Read(ref _gatewaySwapsConnected);

    internal void GatewaySwapSession(bool preopened) =>
        Interlocked.Increment(ref preopened ? ref _gatewaySwapsPreopened : ref _gatewaySwapsConnected);

    /// <summary>
    /// Shard node: how long a crossing signal (an entity past a boundary) waited in its shard's
    /// boundary monitor queue before the monitor looked at it.
    /// </summary>
    public StepDurations BoundaryCrossingWait { get; } = new();

    private long _boundarySignalsDropped;
    private long _boundaryCrossingsDropped;

    /// <summary>Shard node: boundary signals the monitor queue dropped, full, to make room.</summary>
    public long BoundarySignalsDropped => Interlocked.Read(ref _boundarySignalsDropped);

    /// <summary>Shard node: of those, crossing signals (handoff triggers) rather than approaches.</summary>
    public long BoundaryCrossingsDropped => Interlocked.Read(ref _boundaryCrossingsDropped);

    internal void BoundarySignalDropped(bool crossing)
    {
        Interlocked.Increment(ref _boundarySignalsDropped);
        if (crossing)
            Interlocked.Increment(ref _boundaryCrossingsDropped);
    }
}

/// <summary>
/// A running record of one step's durations (or, for <see cref="ClusterMetrics.GatewayHeldCalls"/>,
/// counts): how many, their total, the largest, and how many exceeded each of a fixed set of thresholds.
/// </summary>
public sealed class StepDurations
{
    /// <summary>The thresholds <see cref="Over"/> counts against.</summary>
    public static IReadOnlyList<int> Thresholds { get; } = [10, 50, 100, 250, 500, 1_000, 2_000, 5_000];

    private readonly long[] _over = new long[Thresholds.Count];
    private long _count;
    private long _totalMicros;
    private long _maxMicros;

    public long Count => Interlocked.Read(ref _count);

    public double TotalMs => Interlocked.Read(ref _totalMicros) / 1000d;

    public double MaxMs => Interlocked.Read(ref _maxMicros) / 1000d;

    /// <summary>Records longer than <paramref name="threshold"/>, which must be one of <see cref="Thresholds"/>.</summary>
    public long Over(int threshold)
    {
        for (var i = 0; i < Thresholds.Count; i++)
        {
            if (Thresholds[i] == threshold)
                return Interlocked.Read(ref _over[i]);
        }

        throw new ArgumentOutOfRangeException(nameof(threshold), threshold, "Not one of StepDurations.Thresholds.");
    }

    internal void Record(TimeSpan duration) => Record(duration.TotalMilliseconds);

    internal void Record(double value)
    {
        Interlocked.Increment(ref _count);
        var micros = (long)(value * 1000);
        Interlocked.Add(ref _totalMicros, micros);
        long current;
        do
        {
            current = Interlocked.Read(ref _maxMicros);
            if (micros <= current)
                break;
        }
        while (Interlocked.CompareExchange(ref _maxMicros, micros, current) != current);

        for (var i = 0; i < Thresholds.Count && value > Thresholds[i]; i++)
            Interlocked.Increment(ref _over[i]);
    }
}

/// <summary>
/// A running record of liveness gaps (heartbeat intervals as observed): how many, the longest, and
/// how many exceeded each of a fixed set of thresholds. The thresholds bracket the failure timeouts
/// worth considering, so one run at a long timeout answers "how many false deaths would a shorter
/// one have caused?" for all of them at once.
/// </summary>
public sealed class LivenessGaps
{
    /// <summary>The thresholds <see cref="Over"/> counts against, in milliseconds.</summary>
    public static IReadOnlyList<int> ThresholdsMs { get; } = [500, 750, 1_000, 1_500, 2_000, 2_500, 3_000, 5_000, 10_000];

    private readonly long[] _over = new long[ThresholdsMs.Count];
    private long _count;
    private long _maxMicros;

    /// <summary>Gaps recorded.</summary>
    public long Count => Interlocked.Read(ref _count);

    /// <summary>The longest gap recorded, in milliseconds.</summary>
    public double MaxMs => Interlocked.Read(ref _maxMicros) / 1000d;

    /// <summary>Gaps longer than <paramref name="thresholdMs"/>, which must be one of <see cref="ThresholdsMs"/>.</summary>
    public long Over(int thresholdMs)
    {
        for (var i = 0; i < ThresholdsMs.Count; i++)
        {
            if (ThresholdsMs[i] == thresholdMs)
                return Interlocked.Read(ref _over[i]);
        }

        throw new ArgumentOutOfRangeException(nameof(thresholdMs), thresholdMs, "Not one of LivenessGaps.ThresholdsMs.");
    }

    internal void Record(TimeSpan gap)
    {
        Interlocked.Increment(ref _count);
        var micros = (long)(gap.TotalMilliseconds * 1000);
        long current;
        do
        {
            current = Interlocked.Read(ref _maxMicros);
            if (micros <= current)
                break;
        }
        while (Interlocked.CompareExchange(ref _maxMicros, micros, current) != current);

        for (var i = 0; i < ThresholdsMs.Count && gap.TotalMilliseconds > ThresholdsMs[i]; i++)
            Interlocked.Increment(ref _over[i]);
    }
}

/// <summary>
/// One authenticated TCP connection between a shard node and the hub: length-prefixed frames,
/// JSON or JSON with a binary attachment for row data (<see cref="LinkCodec"/>), duplex
/// request/response with correlation ids, and notifications. The link is
/// transport only — message semantics (auth, heartbeats, replication, events, handoff) live in
/// the hub and shard runtimes, which install a handler. Every frame is counted in
/// <see cref="ClusterMetrics"/>; serialization is real, which is the point of running multi-node
/// tests in one process.
/// <para>
/// The reader is a dedicated thread doing blocking reads, not a thread-pool loop: replies, and the
/// one request type answered inline (<see cref="InlineType"/>, the liveness ping), must not wait for
/// a pool thread. Under load the pool is what starves first, and a heartbeat that waits for it can
/// arrive seconds late from a healthy node (phase 30's liveness soak). Every other request is still
/// dispatched to the pool, so a slow handler cannot stall the link.
/// </para>
/// </summary>
internal sealed class NodeLink : IDisposable
{
    /// <summary>A received frame's envelope. <see cref="Body"/> is the raw JSON; the frame's attachment rides beside it.</summary>
    private sealed record WireFrame(long Id, long Re, string Type, JsonElement? Body);

    /// <summary>
    /// Encoded frames are built in buffers from this pool rather than the shared one. Its arrays top
    /// out at 1 MiB, four to a size, so whatever it holds between sends is bounded (about 8 MiB for
    /// the whole process); a bigger frame allocates and is collected. The shared pool caches an
    /// array per size per thread, and node-link sends happen on any pool thread: with frames of
    /// hundreds of KiB it held over 300 MiB nothing accounted for.
    /// </summary>
    private static readonly ArrayPool<byte> FrameBuffers = ArrayPool<byte>.Create(1024 * 1024, 4);

    /// <summary>A JSON buffer this thread keeps between frames only while it stays this small.</summary>
    private const int KeptJsonBufferBytes = 64 * 1024;

    // The envelope's JSON is written into a per-thread buffer and writer, as JsonSerializer's own
    // SerializeToUtf8Bytes does, so a heartbeat allocates neither. A frame whose JSON outgrows
    // KeptJsonBufferBytes (a JSON link carrying rows) drops the buffer rather than pin it.
    [ThreadStatic]
    private static ArrayBufferWriter<byte>? t_jsonBuffer;

    [ThreadStatic]
    private static Utf8JsonWriter? t_jsonWriter;

    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly ClusterMetrics _metrics;
    private readonly Lock _sendLock = new();
    private readonly ConcurrentDictionary<long, TaskCompletionSource<LinkBody?>> _pending = new();
    private readonly CancellationTokenSource _closed = new();
    private long _nextId;

    public NodeLink(TcpClient client, ClusterMetrics metrics)
    {
        _client = client;
        _client.NoDelay = true;

        // Sends block (see Send), so a peer that stops reading must not hold the sender forever:
        // a write that cannot drain within the link's request timeout kills the link, the same
        // verdict a request would reach.
        _client.SendTimeout = 15_000;
        _stream = client.GetStream();
        _metrics = metrics;
    }

    /// <summary>Set by the owner before <see cref="Start"/>: answers inbound requests and notifications.</summary>
    public Func<NodeLink, string, LinkBody?, Task<object?>>? Handler { get; set; }

    /// <summary>
    /// Whether this end sends binary frames. Off until the peer has said, at authentication, that it
    /// reads them (<see cref="LinkCodec.BinaryFormat"/>); a reader accepts both kinds regardless, so
    /// either end may switch at any point after that.
    /// </summary>
    public bool SendsBinary
    {
        get => Volatile.Read(ref _sendsBinary);
        set => Volatile.Write(ref _sendsBinary, value);
    }

    private bool _sendsBinary;

    /// <summary>
    /// The one request type answered on the reader thread itself by <see cref="InlineHandler"/>,
    /// with its reply written synchronously: never queued behind the thread pool. For liveness only;
    /// the handler must be fast and must not block. Only one end of a link may set it: the reply
    /// can wait for the send lock behind a large frame, which is safe only while the peer's reader
    /// is free to drain that frame, and a peer answering inline too could be waiting on us.
    /// </summary>
    public string? InlineType { get; set; }

    /// <summary>Answers <see cref="InlineType"/> requests on the reader thread; throwing sends an error reply.</summary>
    public Func<NodeLink, LinkBody?, object?>? InlineHandler { get; set; }

    /// <summary>Cancelled when the link dies.</summary>
    public CancellationToken ClosedToken => _closed.Token;

    /// <summary>Fires once when the read loop ends — link death, however it happened.</summary>
    public event Action<NodeLink>? Closed;

    /// <summary>Owner-attached state: the hub tags links with the authenticated node's session.</summary>
    public object? Tag { get; set; }

    public bool IsAlive => !_closed.IsCancellationRequested && _client.Connected;

    public void Start() =>
        new Thread(ReadLoop) { IsBackground = true, Name = "MelangeDB node link reader" }.Start();

    /// <summary>
    /// Sends a request and blocks the calling thread for the reply: for a dedicated thread that must
    /// not depend on the thread pool (the liveness pinger). The reader completes the reply directly,
    /// so nothing on this path waits for a pool thread.
    /// </summary>
    /// <exception cref="NodeLinkException">An error reply, a timeout, or the link closing.</exception>
    public LinkBody? Request(string type, object? body, int timeoutMs)
    {
        var id = Interlocked.Increment(ref _nextId);
        var tcs = new TaskCompletionSource<LinkBody?>();
        _pending[id] = tcs;
        try
        {
            Send(id, 0, type, body);
            try
            {
                if (!tcs.Task.Wait(timeoutMs, _closed.Token))
                    throw new NodeLinkException($"Node-link request '{type}' timed out.");
            }
            catch (OperationCanceledException)
            {
                throw new NodeLinkException("The node link closed.");
            }
            catch (AggregateException exception) when (exception.InnerException is NodeLinkException inner)
            {
                throw inner;
            }

            return tcs.Task.Result;
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    /// <summary>Sends a request and awaits the peer's response body; throws <see cref="NodeLinkException"/> on an error reply.</summary>
    public async Task<LinkBody?> RequestAsync(string type, object? body, CancellationToken ct = default, int timeoutMs = 15_000)
    {
        var id = Interlocked.Increment(ref _nextId);
        var tcs = new TaskCompletionSource<LinkBody?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = tcs;
        try
        {
            await SendAsync(id, 0, type, body, ct).ConfigureAwait(false);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct, _closed.Token);
            timeout.CancelAfter(timeoutMs);
            await using var _ = timeout.Token.Register(() => tcs.TrySetException(
                new NodeLinkException($"Node-link request '{type}' timed out or the link closed.")));
            return await tcs.Task.ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    /// <summary>Sends a one-way notification.</summary>
    public Task NotifyAsync(string type, object? body, CancellationToken ct = default) =>
        SendAsync(0, 0, type, body, ct);

    /// <summary>
    /// The largest frame payload a link accepts. A reader that receives a larger one cannot trust
    /// the stream and drops the link, so a sender refuses to write one: the request fails on its
    /// own, and the link, with every other shard's traffic and the node's lease, survives. A
    /// message that can grow with a shard's data (a border reset) is sent in parts instead.
    /// </summary>
    internal const int MaxFramePayloadBytes = 64 * 1024 * 1024;

    /// <summary>
    /// Encodes a frame in one pass: the envelope and body written straight into a JSON writer (with
    /// row bytes routed to an attachment on a binary link), then the length prefix, JSON and
    /// attachment copied once into a pooled buffer. The caller returns the buffer to
    /// <see cref="FrameBuffers"/> once it is written.
    /// </summary>
    private (byte[] Buffer, int FrameLength, int PayloadLength) Encode(long id, long re, string type, object? body)
    {
        var attachment = SendsBinary ? new AttachmentWriter() : null;
        var json = t_jsonBuffer ??= new ArrayBufferWriter<byte>(1024);
        json.ResetWrittenCount();
        var writer = t_jsonWriter ??= new Utf8JsonWriter(json);
        writer.Reset(json);
        try
        {
            writer.WriteStartObject();
            writer.WriteNumber("Id", id);
            writer.WriteNumber("Re", re);
            writer.WriteString("Type", type);
            writer.WritePropertyName("Body");
            switch (body)
            {
                case null:
                    writer.WriteNullValue();
                    break;
                case JsonElement element:
                    element.WriteTo(writer);
                    break;
                case LinkBody { HasAttachment: false } received:
                    received.Json.WriteTo(writer);
                    break;
                case LinkBody:
                    throw new NodeLinkException($"A '{type}' body read from a binary frame cannot be forwarded as is; decode it into its message first.");
                default:
                    LinkCodec.Write(writer, body, attachment);
                    break;
            }

            writer.WriteEndObject();
            writer.Flush();
        }
        catch
        {
            t_jsonBuffer = null;
            t_jsonWriter = null;
            throw;
        }

        // The locals keep this frame's buffer; the thread keeps it for the next only if it is small.
        if (json.Capacity > KeptJsonBufferBytes)
        {
            t_jsonBuffer = null;
            t_jsonWriter = null;
        }

        var payloadLength = attachment is null
            ? json.WrittenCount
            : LinkCodec.BinaryHeaderBytes + json.WrittenCount + attachment.Length;
        if (payloadLength > MaxFramePayloadBytes)
        {
            throw new NodeLinkException(
                $"A '{type}' frame of {payloadLength} bytes exceeds the node link's {MaxFramePayloadBytes}-byte limit; it was not sent.");
        }

        var frameLength = 4 + payloadLength;
        var buffer = FrameBuffers.Rent(frameLength);
        var span = buffer.AsSpan(0, frameLength);
        BinaryPrimitives.WriteInt32LittleEndian(span, payloadLength);
        span = span[4..];
        if (attachment is not null)
        {
            span[0] = LinkCodec.BinaryMarker;
            BinaryPrimitives.WriteInt32LittleEndian(span[1..], json.WrittenCount);
            span = span[LinkCodec.BinaryHeaderBytes..];
        }

        json.WrittenSpan.CopyTo(span);
        attachment?.CopyTo(span[json.WrittenCount..]);
        return (buffer, frameLength, payloadLength);
    }

    /// <summary>
    /// Every send is a synchronous write under a plain lock, held only while the frame is copied
    /// into the socket. An async send under a <see cref="SemaphoreSlim"/> handed the lock, on each
    /// release, to a queued waiter whose continuation then waited for a thread-pool thread, so under
    /// load a liveness ping sat behind that queue for up to 1.5 s while no write held the lock for
    /// more than 23 ms (phase 30's soak). Waiters on a plain lock are threads already running.
    /// </summary>
    private void Send(long id, long re, string type, object? body)
    {
        var (buffer, frameLength, payloadLength) = Encode(id, re, type, body);
        try
        {
            _closed.Token.ThrowIfCancellationRequested();
            lock (_sendLock)
            {
                try
                {
                    _stream.Write(buffer, 0, frameLength);
                }
                catch (IOException)
                {
                    Dispose();
                    throw;
                }
            }
        }
        finally
        {
            FrameBuffers.Return(buffer);
        }

        _metrics.RecordSent(type, payloadLength);
    }

    private Task SendAsync(long id, long re, string type, object? body, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            Send(id, re, type, body);
            return Task.CompletedTask;
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    private void ReadLoop()
    {
        var ct = _closed.Token;
        try
        {
            var header = new byte[4];
            while (!ct.IsCancellationRequested)
            {
                _stream.ReadExactly(header);
                var length = BinaryPrimitives.ReadInt32LittleEndian(header);
                if (length is <= 0 or > MaxFramePayloadBytes)
                    throw new InvalidDataException($"Node-link frame length {length} is out of range.");
                var payload = new byte[length];
                _stream.ReadExactly(payload);
                var (frame, body) = Decode(payload);
                _metrics.RecordReceived(frame.Type, payload.Length);
                if (frame.Re != 0)
                {
                    if (_pending.TryGetValue(frame.Re, out var tcs))
                    {
                        if (frame.Type == "error")
                        {
                            tcs.TrySetException(new NodeLinkException(
                                frame.Body?.GetProperty("Message").GetString() ?? "error",
                                isPeerError: true,
                                frame.Body is { } error && error.TryGetProperty("Code", out var code) && code.ValueKind == JsonValueKind.String
                                    ? code.GetString()
                                    : null));
                        }
                        else
                        {
                            tcs.TrySetResult(body);
                        }
                    }

                    continue;
                }

                if (frame.Id != 0 && frame.Type == InlineType && InlineHandler is { } inline)
                {
                    AnswerInline(frame, body, inline);
                    continue;
                }

                // Handled off the read loop so a slow handler cannot stall unrelated traffic;
                // responses re-serialize through SendAsync like everything else.
                _ = Task.Run(() => DispatchAsync(frame, body), CancellationToken.None);
            }
        }
        catch (Exception)
        {
            // Link death: an aborted socket, a malformed frame, or disposal. Pending requests
            // fail via their timeout registration when the closed token fires below.
        }
        finally
        {
            _closed.Cancel();
            foreach (var pending in _pending.Values)
                pending.TrySetException(new NodeLinkException("The node link closed."));
            Closed?.Invoke(this);
        }
    }

    /// <summary>
    /// Splits a received payload into its envelope and body. A JSON frame is the envelope itself; a
    /// binary one is the marker, the JSON's length, the envelope, and the attachment its body's row
    /// bytes point into. The body keeps the payload array alive until the handler has decoded it.
    /// </summary>
    private static (WireFrame Frame, LinkBody? Body) Decode(byte[] payload)
    {
        WireFrame? frame;
        LinkBody? body;
        if (payload[0] == LinkCodec.BinaryMarker)
        {
            if (payload.Length < LinkCodec.BinaryHeaderBytes)
                throw new InvalidDataException("Node-link binary frame is shorter than its header.");
            var jsonLength = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(1));
            if (jsonLength <= 0 || jsonLength > payload.Length - LinkCodec.BinaryHeaderBytes)
                throw new InvalidDataException($"Node-link binary frame JSON length {jsonLength} is out of range.");
            frame = JsonSerializer.Deserialize<WireFrame>(payload.AsSpan(LinkCodec.BinaryHeaderBytes, jsonLength));
            body = frame?.Body is { } json ? new LinkBody(json, payload, LinkCodec.BinaryHeaderBytes + jsonLength) : null;
        }
        else
        {
            frame = JsonSerializer.Deserialize<WireFrame>(payload);
            body = frame?.Body is { } json ? new LinkBody(json) : null;
        }

        return (frame ?? throw new InvalidDataException("Node-link frame deserialized to null."), body);
    }

    private void AnswerInline(WireFrame frame, LinkBody? body, Func<NodeLink, LinkBody?, object?> handler)
    {
        object? reply;
        string type;
        try
        {
            reply = handler(this, body);
            type = frame.Type + "-ok";
        }
        catch (Exception exception)
        {
            reply = new { Message = exception.Message, Code = ReducerCallErrors.CodeOf(exception) };
            type = "error";
        }

        Send(0, frame.Id, type, reply);
    }

    private async Task DispatchAsync(WireFrame frame, LinkBody? body)
    {
        object? result = null;
        string? error = null;
        string? code = null;
        try
        {
            if (Handler is { } handler)
                result = await handler(this, frame.Type, body).ConfigureAwait(false);
            else
                error = "No handler installed.";
        }
        catch (Exception exception)
        {
            error = exception.Message;
            code = ReducerCallErrors.CodeOf(exception);
        }

        if (frame.Id == 0)
            return;
        try
        {
            await (error is null
                ? SendAsync(0, frame.Id, frame.Type + "-ok", result, _closed.Token)
                : SendAsync(0, frame.Id, "error", new { Message = error, Code = code }, _closed.Token)).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The link died before the response could be written; the peer times out.
        }
    }

    public void Dispose()
    {
        _closed.Cancel();
        _client.Dispose();
    }
}
