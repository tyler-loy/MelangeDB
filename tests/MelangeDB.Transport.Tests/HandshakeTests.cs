using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>The versioned handshake and the connection-level protocol rules around it.</summary>
public class HandshakeTests
{
    [Fact]
    public async Task The_handshake_negotiates_version_1_and_reports_the_epoch_and_head()
    {
        await using var host = await TransportTestHost.StartAsync();
        var head = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var raw = new RawSocketClient();
        var welcome = await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        Assert.Equal(1, welcome.Version);
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
    public async Task Reauthenticate_is_acknowledged_as_a_frame_with_semantics_deferred_to_phase_04()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new ReauthenticateFrame("a-fresh-token"), TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReauthenticateResultFrame>(TestContext.Current.CancellationToken);
        Assert.True(result.Accepted);
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
