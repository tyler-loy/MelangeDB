using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text.Json;

namespace MelangeDB.Cluster;

/// <summary>A node-link request answered with an error by the peer.</summary>
public sealed class NodeLinkException(string message) : InvalidOperationException(message);

/// <summary>
/// Per-node counters over node-link traffic, by message type. The "zero cross-node traffic"
/// acceptance test counts these — network calls, not code inspection — so every frame that
/// crosses a link increments here, heartbeats included (tests exclude them by type name).
/// </summary>
public sealed class ClusterMetrics
{
    private readonly ConcurrentDictionary<string, long> _sent = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _received = new(StringComparer.Ordinal);

    internal void RecordSent(string type) => _sent.AddOrUpdate(type, 1, static (_, count) => count + 1);

    internal void RecordReceived(string type) => _received.AddOrUpdate(type, 1, static (_, count) => count + 1);

    public IReadOnlyDictionary<string, long> SentByType => _sent;

    public IReadOnlyDictionary<string, long> ReceivedByType => _received;

    /// <summary>Total messages sent, excluding the given types (typically the heartbeat pair).</summary>
    public long TotalSentExcept(params string[] exceptTypes) =>
        _sent.Where(pair => !exceptTypes.Contains(pair.Key, StringComparer.Ordinal)).Sum(static pair => pair.Value);
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

        _metrics.RecordSent(frame.Type);
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
                _metrics.RecordReceived(frame.Type);
                if (frame.Re != 0)
                {
                    if (_pending.TryGetValue(frame.Re, out var tcs))
                    {
                        if (frame.Type == "error")
                            tcs.TrySetException(new NodeLinkException(frame.Body?.GetProperty("Message").GetString() ?? "error"));
                        else
                            tcs.TrySetResult(frame.Body);
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
