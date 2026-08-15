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
