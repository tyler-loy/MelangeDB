using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The shared upstream link (road-to-0.4 phase 31): many sessions over one websocket, each one a
/// <see cref="WebSocket"/> with the contract the gateway and the node rely on. Runs over a real
/// websocket pair on loopback TCP.
/// </summary>
public sealed class MuxLinkTests : IAsyncLifetime
{
    private readonly ConcurrentQueue<(MuxSocket Socket, ulong Shard)> _accepted = new();
    private readonly SemaphoreSlim _acceptedSignal = new(0);
    private Func<MuxSocket, ulong, Task> _accept;
    private MuxLink _gateway = null!;
    private MuxLink _node = null!;

    public MuxLinkTests()
    {
        _accept = (socket, shard) =>
        {
            _accepted.Enqueue((socket, shard));
            _acceptedSignal.Release();
            return Task.CompletedTask;
        };
    }

    public async ValueTask InitializeAsync()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var client = new TcpClient();
        var connect = client.ConnectAsync(IPAddress.Loopback, port);
        var server = await listener.AcceptTcpClientAsync();
        await connect;
        var gatewaySocket = WebSocket.CreateFromStream(client.GetStream(), isServer: false, subProtocol: null, keepAliveInterval: TimeSpan.Zero);
        var nodeSocket = WebSocket.CreateFromStream(server.GetStream(), isServer: true, subProtocol: null, keepAliveInterval: TimeSpan.Zero);
        _gateway = MuxLink.Client(gatewaySocket);
        _node = MuxLink.Server(nodeSocket, (socket, shard) => _accept(socket, shard));
        _gateway.Start();
        _node.Start();
    }

    public async ValueTask DisposeAsync()
    {
        await _gateway.DisposeAsync();
        await _node.DisposeAsync();
    }

    private async Task<(MuxSocket Gateway, MuxSocket Node)> OpenAsync(ulong shard = 7)
    {
        var gateway = _gateway.Open(shard);
        Assert.True(await _acceptedSignal.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), "the node accepted the session");
        Assert.True(_accepted.TryDequeue(out var node));
        Assert.Equal(shard, node.Shard);
        return (gateway, node.Socket);
    }

    private static async Task<byte[]> ReceiveMessageAsync(WebSocket socket, int bufferSize = 4096)
    {
        var buffer = new byte[bufferSize];
        using var message = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.Equal(WebSocketMessageType.Binary, result.MessageType);
            message.Write(buffer, 0, result.Count);
        }
        while (!result.EndOfMessage);
        return message.ToArray();
    }

    private static byte[] Pattern(int length, byte seed)
    {
        var bytes = new byte[length];
        for (var i = 0; i < length; i++)
            bytes[i] = unchecked((byte)(seed + i * 31));
        return bytes;
    }

    [Fact]
    public async Task Messages_arrive_whole_and_in_order_both_ways_across_chunks_and_small_reads()
    {
        var (gateway, node) = await OpenAsync();

        // Larger than a chunk and than the window, so it is cut and needs credit to finish.
        var large = Pattern(MuxLink.Window * 3 + 123, 1);
        var sends = Task.Run(async () =>
        {
            await gateway.SendAsync(Pattern(10, 2), WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);
            await gateway.SendAsync(large, WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);
            await gateway.SendAsync(Array.Empty<byte>(), WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);

        Assert.Equal(Pattern(10, 2), await ReceiveMessageAsync(node, bufferSize: 3));
        Assert.Equal(large, await ReceiveMessageAsync(node, bufferSize: 5000));
        Assert.Empty(await ReceiveMessageAsync(node));
        await sends.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        await node.SendAsync(Pattern(70_000, 9), WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);
        Assert.Equal(Pattern(70_000, 9), await ReceiveMessageAsync(gateway));
    }

    [Fact]
    public async Task Sessions_interleave_without_mixing()
    {
        var sessions = new List<(MuxSocket Gateway, MuxSocket Node)>();
        for (var i = 0; i < 8; i++)
            sessions.Add(await OpenAsync((ulong)i));

        // Each session sends more than its window, so the sends finish only as the reads below
        // hand credit back: start them, read, then wait for them.
        var sends = Task.WhenAll(sessions.Select((pair, i) => Task.Run(async () =>
        {
            for (var m = 0; m < 20; m++)
                await pair.Node.SendAsync(Pattern(20_000 + m, (byte)(i * 20 + m)), WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken)));

        for (var i = 0; i < sessions.Count; i++)
        {
            for (var m = 0; m < 20; m++)
                Assert.Equal(Pattern(20_000 + m, (byte)(i * 20 + m)), await ReceiveMessageAsync(sessions[i].Gateway));
        }

        await sends.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_reader_that_stops_blocks_only_its_own_sender()
    {
        var slow = await OpenAsync(1);
        var fast = await OpenAsync(2);

        // Nobody reads the slow session: its sender gets one window through, then waits.
        var stuck = Task.Run(async () =>
        {
            for (var i = 0; i < 40; i++)
                await slow.Node.SendAsync(Pattern(MuxLink.MaxChunk, 3), WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);
        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.False(stuck.IsCompleted, "a sender past its window must wait for credit");

        // The other session on the same link is not held behind it.
        await fast.Node.SendAsync(Pattern(1000, 4), WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(Pattern(1000, 4), await ReceiveMessageAsync(fast.Gateway));

        // Reading frees it.
        for (var i = 0; i < 40; i++)
            Assert.Equal(Pattern(MuxLink.MaxChunk, 3), await ReceiveMessageAsync(slow.Gateway));
        await stuck.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_close_handshake_ends_closed_on_both_sides_and_leaves_the_link()
    {
        var (gateway, node) = await OpenAsync();

        await gateway.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "done", TestContext.Current.CancellationToken);
        Assert.Equal(WebSocketState.CloseSent, gateway.State);
        var result = await node.ReceiveAsync(new byte[16], TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(WebSocketMessageType.Close, result.MessageType);
        Assert.Equal(WebSocketCloseStatus.NormalClosure, result.CloseStatus);
        Assert.Equal("done", result.CloseStatusDescription);
        Assert.Equal(WebSocketState.CloseReceived, node.State);

        await node.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, TestContext.Current.CancellationToken);
        Assert.Equal(WebSocketState.Closed, node.State);
        var echo = await gateway.ReceiveAsync(new byte[16], TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(WebSocketMessageType.Close, echo.MessageType);
        Assert.Equal(WebSocketState.Closed, gateway.State);

        await WaitForAsync(() => _gateway.SessionCount == 0 && _node.SessionCount == 0);
        Assert.True(_gateway.IsAlive);
    }

    [Fact]
    public async Task An_abort_reads_as_a_dropped_connection_on_the_other_side()
    {
        var (gateway, node) = await OpenAsync();
        var pending = node.ReceiveAsync(new byte[16], TestContext.Current.CancellationToken);

        gateway.Abort();

        var dropped = await Assert.ThrowsAsync<WebSocketException>(() => pending.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(WebSocketError.ConnectionClosedPrematurely, dropped.WebSocketErrorCode);
        Assert.Equal(WebSocketState.Aborted, node.State);
        await Assert.ThrowsAsync<WebSocketException>(() =>
            node.SendAsync(new byte[1], WebSocketMessageType.Binary, true, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task The_links_death_drops_every_session_on_it()
    {
        var first = await OpenAsync(1);
        var second = await OpenAsync(2);
        var waiting = first.Gateway.ReceiveAsync(new byte[16], TestContext.Current.CancellationToken);

        await _node.DisposeAsync();

        var dropped = await Assert.ThrowsAsync<WebSocketException>(() => waiting.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(WebSocketError.ConnectionClosedPrematurely, dropped.WebSocketErrorCode);
        await WaitForAsync(() => !_gateway.IsAlive && second.Gateway.State == WebSocketState.Aborted);
        await _gateway.Completion.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        // A session opened on a dead link is dead at once, not hung.
        var late = _gateway.Open(3);
        Assert.Equal(WebSocketState.Aborted, late.State);
    }

    [Fact]
    public async Task A_session_the_node_refuses_reads_as_a_close_with_its_reason()
    {
        _accept = static (socket, _) => socket.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, "not the owner", CancellationToken.None);

        var gateway = _gateway.Open(99);
        var result = await gateway.ReceiveAsync(new byte[16], TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(WebSocketMessageType.Close, result.MessageType);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, result.CloseStatus);
        Assert.Equal("not the owner", result.CloseStatusDescription);
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition never held");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }
    }
}
