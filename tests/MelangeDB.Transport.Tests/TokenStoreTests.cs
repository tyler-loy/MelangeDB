using MelangeDB.Client;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The client's pluggable token store: for a guest, the token IS the character, so losing it is
/// losing the character. The file store round-trips durably, the client falls back to the store
/// when no token is configured, and an accepted re-auth persists the fresh token.
/// </summary>
public class TokenStoreTests
{
    [Fact]
    public async Task The_file_store_round_trips_and_overwrites_atomically()
    {
        var path = Path.Combine(Directory.CreateTempSubdirectory("melange-tokens-").FullName, "token.txt");
        var store = new FileTokenStore(path);
        Assert.Null(await store.LoadAsync(TestContext.Current.CancellationToken));

        await store.SaveAsync("first-token", TestContext.Current.CancellationToken);
        Assert.Equal("first-token", await store.LoadAsync(TestContext.Current.CancellationToken));

        await store.SaveAsync("second-token", TestContext.Current.CancellationToken);
        Assert.Equal("second-token", await store.LoadAsync(TestContext.Current.CancellationToken));

        // A fresh store over the same path sees the persisted token — the across-runs contract.
        Assert.Equal("second-token", await new FileTokenStore(path).LoadAsync(TestContext.Current.CancellationToken));
        Assert.False(File.Exists(path + ".tmp"));
    }

    [Fact]
    public async Task A_client_with_no_configured_token_connects_from_its_store()
    {
        await using var host = await TransportTestHost.StartAsync();
        var store = new InMemoryTokenStore();
        await store.SaveAsync(TestTokens.For("stored-guest", role: "guest"), TestContext.Current.CancellationToken);

        await using var client = host.CreateClient(o =>
        {
            o.Token = null;
            o.TokenStore = store;
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task An_accepted_reauth_persists_the_fresh_token_to_the_store()
    {
        await using var host = await TransportTestHost.StartAsync();
        var path = Path.Combine(Directory.CreateTempSubdirectory("melange-tokens-").FullName, "token.txt");
        var store = new FileTokenStore(path);
        var guestToken = TestTokens.For("converting", role: "guest");
        var linkedToken = TestTokens.For("converting");

        await using var client = host.CreateClient(o =>
        {
            o.Token = guestToken;
            o.TokenStore = store;
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        Assert.Equal(guestToken, await store.LoadAsync(TestContext.Current.CancellationToken));

        await client.ReauthenticateAsync(linkedToken, TestContext.Current.CancellationToken);
        Assert.Equal(linkedToken, await store.LoadAsync(TestContext.Current.CancellationToken));
    }
}
