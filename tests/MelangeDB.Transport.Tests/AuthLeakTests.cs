using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The adversarial half of authentication, written before the happy paths on purpose — testing
/// only the happy path here is how the insecure variant ships. A connection is bound to one
/// identity for its lifetime, an expired credential must stop working, no token means no
/// connection, and a ticket spends exactly once.
/// </summary>
public class AuthLeakTests
{
    [Fact]
    public async Task Reauthenticate_with_a_token_for_a_different_identity_closes_the_connection()
    {
        // THE leak test: every delta already sent on this connection was filtered under identity
        // A's policies. If the server switched to B in place, A's rows would be sitting in B's
        // hands — so the only correct answer is a close, never a swap.
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        await raw.SendAsync(
            new ReauthenticateFrame(TestTokens.For("somebody-else")),
            TestContext.Current.CancellationToken);

        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.IdentityChanged, error.Code);
        await Assert.ThrowsAnyAsync<WebSocketException>(async () =>
        {
            while (true)
                await raw.ReceiveAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task A_session_whose_token_expires_without_reauth_is_closed_after_the_grace_window()
    {
        // The other half of re-auth safety: ignoring expiry after the handshake would mean a
        // revoked or expired credential keeps working indefinitely.
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Auth:ReauthGraceSeconds"] = "30",
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = "10000000",
        }, manualTime: true);
        var time = host.Time!;

        await using var raw = new RawSocketClient();
        var token = TestTokens.For("expiring", expires: time.GetUtcNow().AddSeconds(60));
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, token);

        // Expiry (60s) plus grace (30s) plus one heartbeat tick to notice.
        time.Advance(TimeSpan.FromSeconds(60 + 30 + 16));

        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.TokenExpired, error.Code);
        Assert.Contains("ReauthGraceSeconds", error.Message);
        await Assert.ThrowsAnyAsync<WebSocketException>(async () =>
        {
            while (true)
                await raw.ReceiveAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task A_client_with_no_token_is_rejected_the_idp_is_the_gate()
    {
        await using var host = await TransportTestHost.StartAsync();
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var serializer = new MessagePackFrameSerializer();
        await socket.SendAsync(
            serializer.Serialize(new HelloFrame(
                MessagePackFrameSerializer.ProtocolVersion,
                MessagePackFrameSerializer.ProtocolVersion,
                null)),
            WebSocketMessageType.Binary,
            endOfMessage: true,
            TestContext.Current.CancellationToken);

        var buffer = new byte[4096];
        var result = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
        var error = Assert.IsType<ErrorFrame>(serializer.Deserialize(buffer.AsSpan(0, result.Count)));
        Assert.Equal(MelangeErrorCodes.Unauthorized, error.Code);
        socket.Dispose();
    }

    [Fact]
    public async Task A_garbage_token_is_rejected_at_the_handshake()
    {
        await using var host = await TransportTestHost.StartAsync();
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var serializer = new MessagePackFrameSerializer();
        await socket.SendAsync(
            serializer.Serialize(new HelloFrame(
                MessagePackFrameSerializer.ProtocolVersion,
                MessagePackFrameSerializer.ProtocolVersion,
                "not-a-jwt")),
            WebSocketMessageType.Binary,
            endOfMessage: true,
            TestContext.Current.CancellationToken);

        var buffer = new byte[4096];
        var result = await socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
        var error = Assert.IsType<ErrorFrame>(serializer.Deserialize(buffer.AsSpan(0, result.Count)));
        Assert.Equal(MelangeErrorCodes.Unauthorized, error.Code);
        socket.Dispose();
    }

    [Fact]
    public async Task A_ticket_is_rejected_on_second_use()
    {
        await using var host = await TransportTestHost.StartAsync();
        using var http = host.CreateHttp();
        var minted = await http.PostAsync("/melange/ticket", null, TestContext.Current.CancellationToken);
        var ticket = System.Text.Json.JsonDocument.Parse(
            await minted.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement.GetProperty("ticket").GetString();
        var uri = new Uri(host.WsUri + "?ticket=" + ticket);

        await using var first = new RawSocketClient();
        await first.ConnectAsync(uri, TestContext.Current.CancellationToken, token: null);

        // The replay: same ticket, second socket. Rejected at the upgrade, before any accept.
        await using var second = new RawSocketClient();
        await Assert.ThrowsAnyAsync<WebSocketException>(
            () => second.ConnectAsync(uri, TestContext.Current.CancellationToken, token: null));
    }

    [Fact]
    public async Task A_ticket_is_rejected_after_its_ttl()
    {
        await using var host = await TransportTestHost.StartAsync(manualTime: true);
        using var http = host.CreateHttp();
        var minted = await http.PostAsync("/melange/ticket", null, TestContext.Current.CancellationToken);
        var ticket = System.Text.Json.JsonDocument.Parse(
            await minted.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement.GetProperty("ticket").GetString();

        host.Time!.Advance(TimeSpan.FromSeconds(31));

        await using var raw = new RawSocketClient();
        await Assert.ThrowsAnyAsync<WebSocketException>(
            () => raw.ConnectAsync(new Uri(host.WsUri + "?ticket=" + ticket), TestContext.Current.CancellationToken, token: null));
    }
}
