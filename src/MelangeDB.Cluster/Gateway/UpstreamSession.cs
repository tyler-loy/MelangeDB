using System.Net.WebSockets;
using MelangeDB.Protocol;

namespace MelangeDB.Cluster;

/// <summary>
/// One gateway-held websocket session to a node's melange endpoint, authenticated with a
/// hub-minted internal identity assertion. The pump answers the node's heartbeat pings itself and
/// hands every other frame — with its original bytes, so forwarding never re-serializes — to the
/// owner. One client maps to one upstream session per attached node: the permanent hub attachment
/// plus the moving shard attachment.
/// <para>
/// Only the frames the gateway acts on are decoded (<see cref="Decoded"/>); everything else, every
/// delta above all, reaches the owner as bytes with a null frame. Decoding each relayed delta,
/// into a fresh 64 KiB receive buffer per message, was most of a loaded gateway's allocation:
/// about a quarter of the serve process's time went to GC pauses at 400 subscribers (road-to-0.4
/// phase 31).
/// </para>
/// </summary>
internal sealed class UpstreamSession : IAsyncDisposable
{
    private readonly WebSocket _socket;
    private readonly IMelangeSerializer _serializer;
    private readonly Func<Frame?, byte[], Task> _onFrame;
    private readonly Action _onClosed;
    private readonly Action _countSent;
    private readonly Action _countReceived;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly CancellationTokenSource _closed = new();

    // One reader at a time (the handshake, then the pump), so one buffer serves every receive.
    private readonly byte[] _receiveBuffer = new byte[16 * 1024];
    private readonly MemoryStream _fragments = new();
    private Task? _pump;

    /// <summary>
    /// The frames the gateway reads rather than relays: the handshake's answer, heartbeats the
    /// pump answers itself, and the re-authentication result the gateway answers for the client.
    /// </summary>
    private static bool Decoded(FrameType type) => type is FrameType.Welcome or FrameType.Error or FrameType.Ping
        or FrameType.Pong or FrameType.ReauthenticateResult;

    private UpstreamSession(
        WebSocket socket,
        IMelangeSerializer serializer,
        Func<Frame?, byte[], Task> onFrame,
        Action onClosed,
        Action countSent,
        Action countReceived)
    {
        _socket = socket;
        _serializer = serializer;
        _onFrame = onFrame;
        _onClosed = onClosed;
        _countSent = countSent;
        _countReceived = countReceived;
    }

    public WelcomeFrame Welcome { get; private set; } = null!;

    public bool IsAlive => !_closed.IsCancellationRequested && _socket.State == WebSocketState.Open;

    /// <summary>
    /// Connects, presents the assertion in Hello, and awaits the node's Welcome — all inside
    /// <paramref name="timeout"/>. Without a bound, a connect to a node whose *machine* died (no
    /// refusal ever comes back) waited out the operating system's TCP connect timeout, about 21 s on
    /// Windows and about two minutes on stock Linux, while the gateway held the client's attachment
    /// lock, so every shard-bound call that client made waited behind it (#200).
    /// </summary>
    /// <exception cref="TimeoutException">The node did not complete the handshake within <paramref name="timeout"/>.</exception>
    public static async Task<UpstreamSession> ConnectAsync(
        Func<CancellationToken, Task<WebSocket>> open,
        string target,
        string assertion,
        IMelangeSerializer serializer,
        Func<Frame?, byte[], Task> onFrame,
        Action onClosed,
        Action countSent,
        Action countReceived,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        WebSocket? socket = null;
        try
        {
            socket = await open(bounded.Token).ConfigureAwait(false);
            var session = new UpstreamSession(socket, serializer, onFrame, onClosed, countSent, countReceived);
            await session.SendFrameAsync(new HelloFrame(
                MessagePackFrameSerializer.ProtocolVersion,
                MessagePackFrameSerializer.ProtocolVersion,
                assertion), bounded.Token).ConfigureAwait(false);
            session.Welcome = await session.ReadUntilWelcomeAsync(bounded.Token).ConfigureAwait(false);
            session._pump = Task.Run(() => session.PumpAsync());
            return session;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            socket?.Abort();
            socket?.Dispose();
            throw new TimeoutException(
                $"{target} did not complete the upstream handshake within {timeout.TotalMilliseconds:F0} ms.");
        }
        catch
        {
            socket?.Abort();
            socket?.Dispose();
            throw;
        }
    }

    /// <summary>A websocket of its own to <paramref name="uri"/>: the per-session upstream.</summary>
    public static Func<CancellationToken, Task<WebSocket>> Dial(Uri uri) => async ct =>
    {
        var socket = new ClientWebSocket();
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);
        try
        {
            await socket.ConnectAsync(uri, ct).ConfigureAwait(false);
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    };

    public async Task SendFrameAsync(Frame frame, CancellationToken ct = default)
    {
        var bytes = _serializer.Serialize(frame);
        await SendRawAsync(bytes, ct).ConfigureAwait(false);
    }

    /// <summary>Forwards already-serialized bytes — the client's frame, verbatim.</summary>
    public async Task SendRawAsync(byte[] bytes, CancellationToken ct = default)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _socket.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }

        _countSent();
    }

    private async Task<WelcomeFrame> ReadUntilWelcomeAsync(CancellationToken ct)
    {
        while (true)
        {
            var (frame, _) = await ReceiveAsync(ct).ConfigureAwait(false);
            switch (frame)
            {
                case WelcomeFrame welcome:
                    return welcome;
                case ErrorFrame error:
                    throw new InvalidOperationException($"Upstream handshake failed: {error.Code}: {error.Message}");
            }
        }
    }

    /// <summary>
    /// The next message's bytes, and its frame when the gateway needs it decoded; null otherwise.
    /// A serializer other than MessagePack cannot be peeked, so its frames are all decoded.
    /// </summary>
    private async Task<(Frame? Frame, byte[] Bytes)> ReceiveAsync(CancellationToken ct)
    {
        byte[] bytes;
        var result = await _socket.ReceiveAsync(_receiveBuffer, ct).ConfigureAwait(false);
        if (result.MessageType == WebSocketMessageType.Close)
            throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely);
        if (result.EndOfMessage)
        {
            bytes = _receiveBuffer.AsSpan(0, result.Count).ToArray();
        }
        else
        {
            _fragments.SetLength(0);
            _fragments.Write(_receiveBuffer, 0, result.Count);
            do
            {
                result = await _socket.ReceiveAsync(_receiveBuffer, ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                    throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely);
                _fragments.Write(_receiveBuffer, 0, result.Count);
            }
            while (!result.EndOfMessage);
            bytes = _fragments.ToArray();
        }

        _countReceived();
        var relayOnly = _serializer is MessagePackFrameSerializer
            && MessagePackFrameSerializer.TryPeekType(bytes, out var type)
            && !Decoded(type);
        return (relayOnly ? null : _serializer.Deserialize(bytes), bytes);
    }

    private async Task PumpAsync()
    {
        var ct = _closed.Token;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var (frame, bytes) = await ReceiveAsync(ct).ConfigureAwait(false);
                switch (frame)
                {
                    case PingFrame ping:
                        await SendFrameAsync(new PongFrame(ping.Id), ct).ConfigureAwait(false);
                        break;
                    case PongFrame or WelcomeFrame:
                        break;
                    default:
                        await _onFrame(frame, bytes).ConfigureAwait(false);
                        break;
                }
            }
        }
        catch (Exception)
        {
            // Upstream death — socket abort, node kill, or our own disposal.
        }
        finally
        {
            _onClosed();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _closed.CancelAsync().ConfigureAwait(false);
        _socket.Dispose();
        _fragments.Dispose();
        if (_pump is { } pump)
        {
            try
            {
                await pump.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
    }
}
