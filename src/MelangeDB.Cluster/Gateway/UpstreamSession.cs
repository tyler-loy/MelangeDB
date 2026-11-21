using System.Net.WebSockets;
using MelangeDB.Protocol;

namespace MelangeDB.Cluster;

/// <summary>
/// One gateway-held websocket session to a node's melange endpoint, authenticated with a
/// hub-minted internal identity assertion. The pump answers the node's heartbeat pings itself and
/// hands every other frame — with its original bytes, so forwarding never re-serializes — to the
/// owner. One client maps to one upstream session per attached node: the permanent hub attachment
/// plus the moving shard attachment.
/// </summary>
internal sealed class UpstreamSession : IAsyncDisposable
{
    private readonly ClientWebSocket _socket;
    private readonly IMelangeSerializer _serializer;
    private readonly Func<Frame, byte[], Task> _onFrame;
    private readonly Action _onClosed;
    private readonly Action _countSent;
    private readonly Action _countReceived;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly CancellationTokenSource _closed = new();
    private Task? _pump;

    private UpstreamSession(
        ClientWebSocket socket,
        IMelangeSerializer serializer,
        Func<Frame, byte[], Task> onFrame,
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
        Uri uri,
        string assertion,
        IMelangeSerializer serializer,
        Func<Frame, byte[], Task> onFrame,
        Action onClosed,
        Action countSent,
        Action countReceived,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        var socket = new ClientWebSocket();
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);
        try
        {
            await socket.ConnectAsync(uri, bounded.Token).ConfigureAwait(false);
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
            socket.Abort();
            socket.Dispose();
            throw new TimeoutException(
                $"The node at {uri.Authority} did not complete the upstream handshake within {timeout.TotalMilliseconds:F0} ms.");
        }
        catch
        {
            socket.Abort();
            socket.Dispose();
            throw;
        }
    }

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

    private async Task<(Frame Frame, byte[] Bytes)> ReceiveAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        using var message = new MemoryStream();
        while (true)
        {
            var result = await _socket.ReceiveAsync(buffer, ct).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
                throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely);
            message.Write(buffer, 0, result.Count);
            if (result.EndOfMessage)
                break;
        }

        _countReceived();
        var bytes = message.ToArray();
        return (_serializer.Deserialize(bytes), bytes);
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
