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

    /// <summary>Sets a handshake request header — the <c>Authorization: Bearer</c> auth path.</summary>
    public void SetRequestHeader(string name, string value) => _socket.Options.SetRequestHeader(name, value);

    /// <summary>
    /// Connects and handshakes. <paramref name="token"/> rides the Hello frame; the default is the
    /// shared test token, and null sends Hello with no token at all — the browser-style cases pass
    /// a ticket on the URL or a header instead.
    /// </summary>
    public async Task<WelcomeFrame> ConnectAsync(Uri uri, CancellationToken ct, string? token = "default")
    {
        await _socket.ConnectAsync(uri, ct);
        await SendAsync(
            new HelloFrame(
                MessagePackFrameSerializer.ProtocolVersion,
                MessagePackFrameSerializer.ProtocolVersion,
                token == "default" ? TestTokens.Default : token),
            ct);
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
