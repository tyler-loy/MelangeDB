using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text.Json;

namespace MelangeDB.Cluster;

/// <summary>
/// A node-link request that failed. <see cref="IsPeerError"/> distinguishes the two failure
/// shapes a saga must treat differently: the peer <em>replied</em> with an error (it definitively
/// did not perform the request) versus a timeout or link death (the peer may or may not have —
/// the request's effect is unknown, and only reconciliation can say).
/// </summary>
public sealed class NodeLinkException(string message, bool isPeerError = false) : InvalidOperationException(message)
{
    /// <summary>True when the peer answered with an error reply — the request definitively did not happen.</summary>
    public bool IsPeerError { get; } = isPeerError;
}

/// <summary>
/// Per-node counters over node-link traffic, by message type — counts and payload bytes. The
/// "zero cross-node traffic" acceptance test counts these — network calls, not code inspection —
/// so every frame that crosses a link increments here, heartbeats included (tests exclude them by
/// type name). The byte counters are the border-band bandwidth measure: sum the
/// <c>border-apply</c>/<c>border-reset-apply</c> types on a node to see what its neighbours'
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
/// One authenticated TCP connection between a shard node and the hub: length-prefixed JSON
/// frames, duplex request/response with correlation ids, and notifications. The link is
/// transport only — message semantics (auth, heartbeats, replication, events, handoff) live in
/// the hub and shard runtimes, which install a handler. Every frame is counted in
/// <see cref="ClusterMetrics"/>; serialization is real, which is the point of running multi-node
/// tests in one process.
/// </summary>
internal sealed class NodeLink : IDisposable
{
    private sealed record WireFrame(long Id, long Re, string Type, JsonElement? Body);

    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly ClusterMetrics _metrics;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly ConcurrentDictionary<long, TaskCompletionSource<JsonElement?>> _pending = new();
    private readonly CancellationTokenSource _closed = new();
    private long _nextId;
    private Task? _readLoop;

    public NodeLink(TcpClient client, ClusterMetrics metrics)
    {
        _client = client;
        _client.NoDelay = true;
        _stream = client.GetStream();
        _metrics = metrics;
    }

    /// <summary>Set by the owner before <see cref="Start"/>: answers inbound requests and notifications.</summary>
    public Func<NodeLink, string, JsonElement?, Task<object?>>? Handler { get; set; }

    /// <summary>Fires once when the read loop ends — link death, however it happened.</summary>
    public event Action<NodeLink>? Closed;

    /// <summary>Owner-attached state: the hub tags links with the authenticated node's session.</summary>
    public object? Tag { get; set; }

    public bool IsAlive => !_closed.IsCancellationRequested && _client.Connected;

    public void Start() => _readLoop = Task.Run(ReadLoopAsync);

    /// <summary>Sends a request and awaits the peer's response body; throws <see cref="NodeLinkException"/> on an error reply.</summary>
    public async Task<JsonElement?> RequestAsync(string type, object? body, CancellationToken ct = default, int timeoutMs = 15_000)
    {
        var id = Interlocked.Increment(ref _nextId);
        var tcs = new TaskCompletionSource<JsonElement?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = tcs;
        try
        {
            await SendAsync(new WireFrame(id, 0, type, ToElement(body)), ct).ConfigureAwait(false);
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
        SendAsync(new WireFrame(0, 0, type, ToElement(body)), ct);

    private static JsonElement? ToElement(object? body) =>
        body is null ? null : JsonSerializer.SerializeToElement(body, body.GetType());

    private async Task SendAsync(WireFrame frame, CancellationToken ct)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(frame);
        var buffer = new byte[4 + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, payload.Length);
        payload.CopyTo(buffer, 4);
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _stream.WriteAsync(buffer, ct).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }

        _metrics.RecordSent(frame.Type, payload.Length);
    }

    private async Task ReadLoopAsync()
    {
        var ct = _closed.Token;
        try
        {
            var header = new byte[4];
            while (!ct.IsCancellationRequested)
            {
                await _stream.ReadExactlyAsync(header, ct).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadInt32LittleEndian(header);
                if (length is <= 0 or > 64 * 1024 * 1024)
                    throw new InvalidDataException($"Node-link frame length {length} is out of range.");
                var payload = new byte[length];
                await _stream.ReadExactlyAsync(payload, ct).ConfigureAwait(false);
                var frame = JsonSerializer.Deserialize<WireFrame>(payload)
                    ?? throw new InvalidDataException("Node-link frame deserialized to null.");
                _metrics.RecordReceived(frame.Type, payload.Length);
                if (frame.Re != 0)
                {
                    if (_pending.TryGetValue(frame.Re, out var tcs))
                    {
                        if (frame.Type == "error")
                        {
                            tcs.TrySetException(new NodeLinkException(
                                frame.Body?.GetProperty("Message").GetString() ?? "error", isPeerError: true));
                        }
                        else
                        {
                            tcs.TrySetResult(frame.Body);
                        }
                    }

                    continue;
                }

                // Handled off the read loop so a slow handler cannot stall unrelated traffic;
                // responses re-serialize through SendAsync like everything else.
                _ = Task.Run(() => DispatchAsync(frame), CancellationToken.None);
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

    private async Task DispatchAsync(WireFrame frame)
    {
        object? result = null;
        string? error = null;
        try
        {
            if (Handler is { } handler)
                result = await handler(this, frame.Type, frame.Body).ConfigureAwait(false);
            else
                error = "No handler installed.";
        }
        catch (Exception exception)
        {
            error = exception.Message;
        }

        if (frame.Id == 0)
            return;
        try
        {
            await SendAsync(
                error is null
                    ? new WireFrame(0, frame.Id, frame.Type + "-ok", ToElement(result))
                    : new WireFrame(0, frame.Id, "error", ToElement(new { Message = error })),
                _closed.Token).ConfigureAwait(false);
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
