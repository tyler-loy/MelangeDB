using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>The versioned handshake and the connection-level protocol rules around it.</summary>
public class HandshakeTests
{
    [Fact]
    public async Task The_handshake_negotiates_the_current_version_and_reports_the_epoch_and_head()
    {
        await using var host = await TransportTestHost.StartAsync();
        var head = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var raw = new RawSocketClient();
        var welcome = await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        Assert.Equal(MessagePackFrameSerializer.ProtocolVersion, welcome.Version);
        Assert.Equal(host.Engine.Log.EpochId, welcome.EpochId);
        Assert.Equal(head, welcome.HeadLsn);
        Assert.NotEqual(Guid.Empty, welcome.ConnectionId);
    }

    [Fact]
    public async Task A_client_offering_no_common_version_is_told_so_and_dropped()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var serializer = new MessagePackFrameSerializer();
        await socket.SendAsync(
            serializer.Serialize(new HelloFrame(7, 9, null)),
            WebSocketMessageType.Binary,
            endOfMessage: true,
            TestContext.Current.CancellationToken);

        var buffer = new byte[4096];
        var result = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
        var error = Assert.IsType<ErrorFrame>(serializer.Deserialize(buffer.AsSpan(0, result.Count)));
        Assert.Equal(MelangeErrorCodes.UnsupportedVersion, error.Code);
        Assert.Contains("7-9", error.Message);
        socket.Dispose();
    }

    [Fact]
    public async Task A_protocol_v1_client_is_turned_away_at_the_handshake_not_at_the_first_row()
    {
        // Protocol v2 is a hard break: rows are schema-ordered bytes where v1 sent named maps, and
        // there is no v1 encoder left to fall back to. A v1 peer must therefore be refused here,
        // with a version error naming what it offered — not accepted and then failed on the first
        // row it cannot read, which is the failure mode a silent break produces.
        await using var host = await TransportTestHost.StartAsync();
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var serializer = new MessagePackFrameSerializer();
        await socket.SendAsync(
            serializer.Serialize(new HelloFrame(1, 1, TestTokens.Default)),
            WebSocketMessageType.Binary,
            endOfMessage: true,
            TestContext.Current.CancellationToken);

        var buffer = new byte[4096];
        var result = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
        var error = Assert.IsType<ErrorFrame>(serializer.Deserialize(buffer.AsSpan(0, result.Count)));
        Assert.Equal(MelangeErrorCodes.UnsupportedVersion, error.Code);
        Assert.Contains("1-1", error.Message);
        socket.Dispose();
    }

    [Fact]
    public async Task Any_frame_before_Hello_is_a_protocol_error()
    {
        await using var host = await TransportTestHost.StartAsync();
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var serializer = new MessagePackFrameSerializer();
        await socket.SendAsync(
            serializer.Serialize(new PingFrame(1)),
            WebSocketMessageType.Binary,
            endOfMessage: true,
            TestContext.Current.CancellationToken);

        // The server treats it as garbage and drops the connection.
        var buffer = new byte[4096];
        await Assert.ThrowsAnyAsync<WebSocketException>(async () =>
        {
            while (true)
            {
                var result = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
                if (result.MessageType == WebSocketMessageType.Close)
                    throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely);
            }
        });
        socket.Dispose();
    }

    [Fact]
    public async Task Reauthenticate_with_an_invalid_token_is_refused_without_dropping_the_session()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new ReauthenticateFrame("not-a-jwt"), TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReauthenticateResultFrame>(TestContext.Current.CancellationToken);
        Assert.False(result.Accepted);

        // The session lives on under its current token; the client may retry before the grace runs out.
        await raw.SendAsync(new PingFrame(9), TestContext.Current.CancellationToken);
        var pong = await raw.ReceiveUntilAsync<PongFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(9u, pong.Id);
    }

    [Fact]
    public async Task An_oversized_inbound_frame_is_rejected_with_the_limit_named()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:MaxMessageBytes"] = "1024",
        });
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(
            new CallReducerFrame(1, "Noop", new byte[8192], null) { Channel = MelangeChannels.Calls },
            TestContext.Current.CancellationToken);
        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.MessageTooLarge, error.Code);
        Assert.Contains("MaxMessageBytes", error.Message);
    }

    [Fact]
    public async Task A_client_requesting_permessage_deflate_works_end_to_end()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 1L, new byte[2048]);

        await using var client = host.CreateClient(o => o.CompressionEnabled = true);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, subscription.Count);
        var lsn = await client.CallReducerAsync("SetChunk", [2L, 2L, new byte[2048]], TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delta over a compressed socket");
        Assert.Equal(2, subscription.Count);
    }
}
