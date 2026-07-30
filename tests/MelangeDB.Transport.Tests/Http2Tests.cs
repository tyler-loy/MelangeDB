using System.Net;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// WebSockets over HTTP/2 arrive as RFC 8441 extended CONNECT; a GET-only mapping makes them
/// silently fall back to HTTP/1.1. The server reports the protocol it actually negotiated in the
/// Welcome frame — asserted here, since "it still worked" is exactly how the silent fallback hides.
/// </summary>
public class Http2Tests
{
    [Fact]
    public async Task A_client_connects_over_http2_extended_connect_and_the_negotiated_version_is_asserted()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient(o =>
        {
            o.Uri = host.WsUriHttp2;
            o.HttpVersion = HttpVersion.Version20;
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // The server-side HttpContext.Request.Protocol — for a websocket this can only be an
        // extended CONNECT stream, HTTP/2 has no other way to carry one.
        Assert.Equal("HTTP/2", client.NegotiatedHttpProtocol);

        // And the protocol is fully functional over it: call, initial set, delta.
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, subscription.Count);
        var lsn = await client.CallReducerAsync("SetChunk", [2L, 2L, new byte[] { 2 }], TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delta over HTTP/2");
        Assert.Equal(2, subscription.Count);
    }

    [Fact]
    public async Task A_client_connects_over_http11_and_the_negotiated_version_is_asserted()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        Assert.Equal("HTTP/1.1", client.NegotiatedHttpProtocol);
        await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Two_http2_clients_multiplex_and_work_independently()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var first = host.CreateClient(o =>
        {
            o.Uri = host.WsUriHttp2;
            o.HttpVersion = HttpVersion.Version20;
        });
        await using var second = host.CreateClient(o =>
        {
            o.Uri = host.WsUriHttp2;
            o.HttpVersion = HttpVersion.Version20;
        });
        await first.ConnectAsync(TestContext.Current.CancellationToken);
        await second.ConnectAsync(TestContext.Current.CancellationToken);
        Assert.NotEqual(first.ConnectionId, second.ConnectionId);

        var sub1 = await first.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        var sub2 = await second.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        var lsn = host.Call("SetChunk", 9L, 9L, new byte[] { 9 });
        await TransportTestHost.WaitUntilAsync(
            () => first.LastAckedLsn >= lsn && second.LastAckedLsn >= lsn, "both clients to receive the delta");
        Assert.Equal(1, sub1.Count);
        Assert.Equal(1, sub2.Count);
    }
}
