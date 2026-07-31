using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Authentication's working half: stable identities from IdP-issued tokens (guests included),
/// every presentation path (Hello token, header, ticket), re-auth inside the grace window, guest
/// conversion with subscriptions intact, revocation, and the connection cap. The adversarial half
/// lives in <see cref="AuthLeakTests"/> and was written first.
/// </summary>
public class AuthTests
{
    [Fact]
    public async Task A_guest_token_resolves_to_a_stable_identity_across_reconnects()
    {
        await using var host = await TransportTestHost.StartAsync();
        var guestToken = TestTokens.For("guest-7", role: "guest");

        await using (var client = host.CreateClient(o => o.Token = guestToken))
        {
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            await client.CallReducerAsync("Spawn", ["Wanderer", 1], TestContext.Current.CancellationToken);
        }

        Assert.Equal([TestTokens.IdentityOf("guest-7")], PlayerIdentities(host));

        // A fresh socket, the same token: the same identity, so Move finds the spawned row.
        await using var reconnected = host.CreateClient(o => o.Token = guestToken);
        await reconnected.ConnectAsync(TestContext.Current.CancellationToken);
        await reconnected.CallReducerAsync("Move", [4.5f], TestContext.Current.CancellationToken);
        Assert.Equal([TestTokens.IdentityOf("guest-7")], PlayerIdentities(host));
    }

    [Fact]
    public async Task An_identity_survives_a_server_restart()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using (var client = host.CreateClient())
        {
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            await client.CallReducerAsync("Spawn", ["Restarter", 1], TestContext.Current.CancellationToken);
        }

        await host.RestartAsync();

        // Same token, new process: Move succeeds only if the identity hash came out the same.
        await using var after = host.CreateClient();
        await after.ConnectAsync(TestContext.Current.CancellationToken);
        await after.CallReducerAsync("Move", [9f], TestContext.Current.CancellationToken);
        Assert.Equal([TransportTestHost.Caller], PlayerIdentities(host));
    }

    [Fact]
    public async Task The_same_subject_from_different_issuers_is_two_identities()
    {
        // The collision test for hashing issuer AND subject: hashing the subject alone would let
        // one token source impersonate another's users wholesale.
        await using var host = await TransportTestHost.StartAsync();

        await using (var first = host.CreateClient(o => o.Token = TestTokens.For("alice")))
        {
            await first.ConnectAsync(TestContext.Current.CancellationToken);
            await first.CallReducerAsync("Spawn", ["Alice1", 1], TestContext.Current.CancellationToken);
        }

        await using (var second = host.CreateClient(o => o.Token = TestTokens.For("alice", issuer: TestTokens.SecondIssuer)))
        {
            await second.ConnectAsync(TestContext.Current.CancellationToken);
            await second.CallReducerAsync("Spawn", ["Alice2", 1], TestContext.Current.CancellationToken);
        }

        var identities = PlayerIdentities(host);
        Assert.Equal(2, identities.Count);
        Assert.Contains(TestTokens.IdentityOf("alice"), identities);
        Assert.Contains(TestTokens.IdentityOf("alice", TestTokens.SecondIssuer), identities);
    }

    [Fact]
    public async Task A_headerless_client_authenticates_via_the_ticket_flow()
    {
        // The browser case: no ability to set handshake headers, so the JWT goes over plain HTTP
        // to the ticket endpoint and only the single-use ticket rides the socket URL.
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o => o.UseTicket = true);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Spawn", ["BrowserKid", 2], TestContext.Current.CancellationToken);
        Assert.Equal([TransportTestHost.Caller], PlayerIdentities(host));
    }

    [Fact]
    public async Task A_bearer_header_on_the_upgrade_request_authenticates()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        raw.SetRequestHeader("Authorization", "Bearer " + TestTokens.Default);
        var welcome = await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, token: null);
        Assert.Equal(1, welcome.Version);
        await raw.SendAsync(new PingFrame(3), TestContext.Current.CancellationToken);
        Assert.Equal(3u, (await raw.ReceiveUntilAsync<PongFrame>(TestContext.Current.CancellationToken)).Id);
    }

    [Fact]
    public async Task Reauthenticating_within_the_grace_window_keeps_the_session_alive()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Auth:ReauthGraceSeconds"] = "30",
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = "10000000",
        }, manualTime: true);
        var time = host.Time!;

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(
            host.WsUri,
            TestContext.Current.CancellationToken,
            TestTokens.For("renewer", expires: time.GetUtcNow().AddSeconds(60)));

        // Past expiry, inside the grace window: the session is alive and may refresh.
        time.Advance(TimeSpan.FromSeconds(75));
        await raw.SendAsync(
            new ReauthenticateFrame(TestTokens.For("renewer", expires: time.GetUtcNow().AddHours(1))),
            TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReauthenticateResultFrame>(TestContext.Current.CancellationToken);
        Assert.True(result.Accepted);

        // Far past the old token's deadline: the refreshed session survives.
        time.Advance(TimeSpan.FromSeconds(120));
        await raw.SendAsync(new PingFrame(11), TestContext.Current.CancellationToken);
        Assert.Equal(11u, (await raw.ReceiveUntilAsync<PongFrame>(TestContext.Current.CancellationToken)).Id);
    }

    [Fact]
    public async Task Guest_conversion_is_a_reauth_with_the_same_subject_and_the_session_continues()
    {
        // Account linking is the IdP's job: it preserved the subject, so the linked token resolves
        // to the SAME identity, nothing merges because nothing moved, and the live session picks
        // the new token up mid-flight with its subscriptions intact.
        await using var host = await TransportTestHost.StartAsync();
        var guestToken = TestTokens.For("player-9", role: "guest");
        var linkedToken = TestTokens.For("player-9");

        await using var client = host.CreateClient(o => o.Token = guestToken);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Spawn", ["Guesty", 1], TestContext.Current.CancellationToken);
        var chunks = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);

        await client.ReauthenticateAsync(linkedToken, TestContext.Current.CancellationToken);

        // Same identity: the guest's spawned row is still this session's row.
        await client.CallReducerAsync("Move", [2.5f], TestContext.Current.CancellationToken);
        Assert.Equal([TestTokens.IdentityOf("player-9")], PlayerIdentities(host));

        // The subscription survived the swap: a server-side write still arrives as a delta.
        var lsn = host.Call("SetChunk", 42L, 1L, new byte[] { 9 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delta after guest conversion");
        Assert.Equal(1, chunks.Count);
    }

    [Fact]
    public async Task Revoking_an_identity_terminates_live_sessions_and_blocks_new_ones()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var dropped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.OnDisconnected += () => dropped.TrySetResult();

        var closed = host.Sessions.Revoke(TransportTestHost.Caller);
        Assert.Equal(1, closed);
        await dropped.Task.WaitAsync(TestTime.Dilated(TimeSpan.FromSeconds(15)), TestContext.Current.CancellationToken);

        // No restart happened, and the same valid token no longer connects.
        await using var again = host.CreateClient();
        await Assert.ThrowsAnyAsync<Exception>(() => again.ConnectAsync(TestContext.Current.CancellationToken));

        // Reinstating lets the identity back in.
        host.Sessions.Reinstate(TransportTestHost.Caller);
        await using var reinstated = host.CreateClient();
        await reinstated.ConnectAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task One_identity_cannot_exceed_the_connection_cap()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Auth:MaxConnectionsPerIdentity"] = "2",
        });

        await using var first = new RawSocketClient();
        await first.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await using var second = new RawSocketClient();
        await second.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        // The third socket for the same identity is refused with the cap named.
        var refused = new System.Net.WebSockets.ClientWebSocket();
        await refused.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var serializer = new MessagePackFrameSerializer();
        await refused.SendAsync(
            serializer.Serialize(new HelloFrame(1, 1, TestTokens.Default)),
            System.Net.WebSockets.WebSocketMessageType.Binary,
            endOfMessage: true,
            TestContext.Current.CancellationToken);
        var buffer = new byte[4096];
        var result = await refused.ReceiveAsync(buffer, TestContext.Current.CancellationToken);
        var error = Assert.IsType<ErrorFrame>(serializer.Deserialize(buffer.AsSpan(0, result.Count)));
        Assert.Equal(MelangeErrorCodes.ConnectionCap, error.Code);
        refused.Dispose();

        // A different identity is unaffected by this one's cap.
        await using var other = new RawSocketClient();
        await other.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("someone-else"));
    }

    private static IReadOnlyList<Identity> PlayerIdentities(TransportTestHost host)
    {
        var schema = host.Engine.Schema.Get(typeof(PlayerState));
        return host.Engine.ReadConsistent(_ => host.Engine.HotStore.Scan(schema.Id)
            .Select(pair => ((PlayerState)Core.RowSerializer.Deserialize(schema, pair.Value.ToArray())).Id)
            .ToList());
    }
}
