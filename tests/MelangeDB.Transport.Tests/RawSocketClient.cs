using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A frame-level websocket client with no behaviour of its own — no ping replies, no cache, no
/// reads it wasn't asked for. The tool for testing what the server does with a client that
/// misbehaves: goes silent, stops reading, or never says goodbye.
/// </summary>
internal sealed class RawSocketClient : IAsyncDisposable
{
    private readonly ClientWebSocket _socket = new();
    private readonly IMelangeSerializer _serializer = new MessagePackFrameSerializer();
    private readonly byte[] _buffer = new byte[512 * 1024];

    public WebSocketState State => _socket.State;

    public async Task<WelcomeFrame> ConnectAsync(Uri uri, CancellationToken ct)
    {
        await _socket.ConnectAsync(uri, ct);
        await SendAsync(new HelloFrame(1, 1, null), ct);
        return Assert.IsType<WelcomeFrame>(await ReceiveAsync(ct));
    }

    public Task SendAsync(Frame frame, CancellationToken ct) =>
        _socket.SendAsync(_serializer.Serialize(frame), WebSocketMessageType.Binary, endOfMessage: true, ct);

    /// <summary>Receives one frame; throws on close or abort.</summary>
    public async Task<Frame> ReceiveAsync(CancellationToken ct)
    {
        using var message = new MemoryStream();
        while (true)
        {
            var result = await _socket.ReceiveAsync(_buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close)
                throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely);
            message.Write(_buffer, 0, result.Count);
            if (result.EndOfMessage)
                return _serializer.Deserialize(message.GetBuffer().AsSpan(0, (int)message.Length));
        }
    }

    /// <summary>Receives frames until one matches, returning it. Non-matching frames are discarded.</summary>
    public async Task<T> ReceiveUntilAsync<T>(CancellationToken ct, Func<T, bool>? match = null)
        where T : Frame
    {
        while (true)
        {
            if (await ReceiveAsync(ct) is T typed && (match is null || match(typed)))
                return typed;
        }
    }

    public void Abort() => _socket.Abort();

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return ValueTask.CompletedTask;
    }
}
