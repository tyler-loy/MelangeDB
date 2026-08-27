using MelangeDB.Client;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Issue #152: reducers could reach no claim from the caller's token but the four whose names are
/// configuration, so an IdP putting an account id beside the subject had nowhere to put it. Capture
/// is an allow-list — <c>Auth:CaptureClaims</c> — and the point of these tests is that it behaves
/// the same however the token was presented, because a mechanism that works for a desktop client
/// and silently does nothing for a browser one is worse than not having it.
/// </summary>
public class CallerClaimsTests
{
    private static Dictionary<string, string?> Capturing(params string[] claims)
    {
        var settings = new Dictionary<string, string?>();
        for (var i = 0; i < claims.Length; i++)
            settings[$"MelangeDb:Auth:CaptureClaims:{i}"] = claims[i];
        return settings;
    }

    private static string TokenWith(string subject, params (string Type, object Value)[] claims) =>
        TestTokens.For(subject, extraClaims: claims.ToDictionary(c => c.Type, c => c.Value));

    [Fact]
    public async Task A_captured_claim_reaches_a_reducer_the_client_called()
    {
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        await using var client = host.CreateClient(o =>
            o.Token = TokenWith(TestTokens.DefaultSubject, ("account_id", "acct-42")));
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        Assert.Equal("acct-42", host.Claims.Last("call")["account_id"]);
    }

    [Fact]
    public async Task Nothing_is_captured_until_a_host_names_it()
    {
        // The token carries the claim; the configuration does not ask for it. Capture is opt-in so
        // a token's size never becomes a per-connection cost nobody declared.
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient(o =>
            o.Token = TokenWith(TestTokens.DefaultSubject, ("account_id", "acct-42")));
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        var seen = host.Claims.Last("call");
        Assert.Equal(0, seen.Count);
        Assert.Null(seen["account_id"]);
    }

    [Fact]
    public async Task An_unlisted_claim_is_dropped_even_though_the_token_carried_it()
    {
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        await using var client = host.CreateClient(o => o.Token = TokenWith(
            TestTokens.DefaultSubject, ("account_id", "acct-42"), ("secret_note", "not-for-reducers")));
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        var seen = host.Claims.Last("call");
        Assert.Equal("acct-42", seen["account_id"]);
        Assert.Null(seen["secret_note"]);
        Assert.Equal(["account_id"], seen.Types);
    }

    [Fact]
    public async Task ClientConnected_sees_the_claims_because_that_is_where_a_session_is_first_learned_of()
    {
        // The issue's own requirement: the account-to-character link is written when the session
        // begins, not when the player first happens to call something.
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        await using var client = host.CreateClient(o =>
            o.Token = TokenWith(TestTokens.DefaultSubject, ("account_id", "acct-7")));
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await TransportTestHost.WaitUntilAsync(() => host.Claims.Saw("connect"), "ClientConnected to fire");
        Assert.Equal("acct-7", host.Claims.Last("connect")["account_id"]);
    }

    [Fact]
    public async Task The_ticket_path_captures_the_same_claims_as_the_header_path()
    {
        // The browser path, and the one the issue called out: the token never reaches middleware,
        // so anything that only worked for header-presented tokens would silently do nothing here.
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        await using var client = host.CreateClient(o =>
        {
            o.UseTicket = true;
            o.Token = TokenWith(TestTokens.DefaultSubject, ("account_id", "acct-browser"));
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        Assert.Equal("acct-browser", host.Claims.Last("call")["account_id"]);
    }

    [Fact]
    public async Task The_http_one_shot_path_captures_them_too()
    {
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        using var http = host.CreateHttp(TokenWith(TestTokens.DefaultSubject, ("account_id", "acct-http")));

        var response = await http.PostAsync(
            "/melange/call/RecordClaims",
            new StringContent("[]", System.Text.Encoding.UTF8, "application/json"),
            TestContext.Current.CancellationToken);
        Assert.True(response.IsSuccessStatusCode);

        Assert.Equal("acct-http", host.Claims.Last("call")["account_id"]);
    }

    [Fact]
    public async Task A_repeated_claim_keeps_every_value()
    {
        // Roles and groups routinely repeat. The first value answers the indexer — what a
        // single-valued claim like an account id wants — and GetValues answers all of them.
        await using var host = await TransportTestHost.StartAsync(Capturing("groups"));
        await using var client = host.CreateClient(o => o.Token = TestTokens.For(
            TestTokens.DefaultSubject,
            extraClaims: new Dictionary<string, object> { ["groups"] = new[] { "moderators", "beta" } }));
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        var seen = host.Claims.Last("call");
        Assert.Equal("moderators", seen["groups"]);
        Assert.Equal(["moderators", "beta"], seen.GetValues("groups"));
    }

    [Fact]
    public async Task A_scheduled_fire_has_no_caller_token_and_so_no_claims()
    {
        // Empty rather than absent-shaped: there is no token behind a timer, and a reducer that
        // reads a claim there is asking a question with no answer.
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        host.Reducers.Call("RecordClaims", TransportTestHost.Caller);

        Assert.Equal(0, host.Claims.Last("call").Count);
    }

    [Fact]
    public async Task Claims_survive_a_reconnect_because_they_are_read_from_the_token_each_time()
    {
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        await using var client = host.CreateClient(o =>
            o.Token = TokenWith(TestTokens.DefaultSubject, ("account_id", "acct-99")));
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        client.Abort();
        await client.ReconnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("RecordClaims", null, TestContext.Current.CancellationToken);

        Assert.Equal("acct-99", host.Claims.Last("call")["account_id"]);
    }

    [Fact]
    public async Task A_client_cannot_forge_a_claim_by_putting_one_in_its_own_token_unsigned()
    {
        // The claim is only trusted because the token validated. An unsigned or wrongly-signed
        // token does not authenticate at all, so there is no session to carry claims from.
        await using var host = await TransportTestHost.StartAsync(Capturing("account_id"));
        await using var client = host.CreateClient(o => o.Token = "not.a.valid.token");

        await Assert.ThrowsAnyAsync<Exception>(() => client.ConnectAsync(TestContext.Current.CancellationToken));
        Assert.False(host.Claims.Saw("call"));
    }
}
