using MelangeDB.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Row policies, asserted at the WIRE level — frames inspected, not the client API — because an
/// API-level test can pass while the protocol leaks, and in a full-loot game a leaked inventory
/// row is wallhack-grade intel. Policies union; a policy may read a private table; a private table
/// stays invisible no matter what.
/// </summary>
public class RowPolicyTests
{
    private static readonly Action<IServiceCollection> InventoryPolicies = services =>
    {
        services.AddSingleton<IRowPolicy<InventoryItem>, InventoryVisibility>();
        services.AddSingleton<IRowPolicy<InventoryItem>, AdminSeesAllInventory>();
    };

    [Fact]
    public async Task A_player_never_receives_another_players_pack_rows_on_the_wire()
    {
        await using var host = await TransportTestHost.StartAsync(services: InventoryPolicies);
        var alice = TestTokens.IdentityOf("alice");
        var bob = TestTokens.IdentityOf("bob");
        host.Call("GiveItem", alice, 0, "alice-sword");
        host.Call("GiveItem", bob, 0, "bob-shield");
        host.Call("GiveItem", bob, 1, "chest-apple");

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("alice"));
        var initial = await InitialSetAsync(raw, 1, "SELECT * FROM InventoryItem");

        // The initial set: own pack plus the world container — and bob's shield never crossed the wire.
        Assert.Equal(["alice-sword", "chest-apple"], ItemNames(initial));

        // The delta path: a write into bob's pack produces NO frame for alice. The marker write
        // that follows is the proof — the first update frame to arrive carries only the marker.
        host.Call("GiveItem", bob, 0, "bob-dagger");
        var marker = host.Call("GiveItem", alice, 0, "alice-cloak");
        var update = await raw.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(marker, update.Lsn);
        var names = update.Updates.SelectMany(u => u.Ops).Select(op => op.Columns!["ItemName"]).ToList();
        Assert.Equal(["alice-cloak"], names);
    }

    [Fact]
    public async Task Both_players_see_the_shared_world_container_the_union_case()
    {
        await using var host = await TransportTestHost.StartAsync(services: InventoryPolicies);
        var alice = TestTokens.IdentityOf("alice");
        host.Call("GiveItem", alice, 0, "alice-sword");
        host.Call("GiveItem", alice, 1, "cart-ore");

        await using var aliceSocket = new RawSocketClient();
        await aliceSocket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("alice"));
        await using var bobSocket = new RawSocketClient();
        await bobSocket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("bob"));

        // Union, not intersection: alice sees her own reason AND the world reason; bob sees only
        // the world reason — but he does see it, which intersection semantics could not express.
        Assert.Equal(
            ["alice-sword", "cart-ore"],
            ItemNames(await InitialSetAsync(aliceSocket, 1, "SELECT * FROM InventoryItem")));
        Assert.Equal(
            ["cart-ore"],
            ItemNames(await InitialSetAsync(bobSocket, 1, "SELECT * FROM InventoryItem")));
    }

    [Fact]
    public async Task An_admin_policy_reading_a_private_table_gives_admins_everything_and_leaves_non_admins_untouched()
    {
        // The SpacetimeDB footgun, shown fixed: its RLS rule referencing a private table fails to
        // evaluate for ordinary clients and kills their ENTIRE subscription (gray screen, no
        // spawn). Here the policy is in-process code — for an admin it is a lookup that returns
        // true; for everyone else it returns false and their subscription is simply filtered.
        await using var host = await TransportTestHost.StartAsync(services: InventoryPolicies);
        var alice = TestTokens.IdentityOf("alice");
        var bob = TestTokens.IdentityOf("bob");
        host.Call("AddAdmin", TestTokens.IdentityOf("admin-1"));
        host.Call("GiveItem", alice, 0, "alice-sword");
        host.Call("GiveItem", bob, 0, "bob-shield");

        await using var admin = new RawSocketClient();
        await admin.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("admin-1"));
        Assert.Equal(
            ["alice-sword", "bob-shield"],
            ItemNames(await InitialSetAsync(admin, 1, "SELECT * FROM InventoryItem")));

        // The non-admin's subscription is UNAFFECTED: registered, filtered, and live.
        await using var aliceSocket = new RawSocketClient();
        await aliceSocket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("alice"));
        Assert.Equal(
            ["alice-sword"],
            ItemNames(await InitialSetAsync(aliceSocket, 1, "SELECT * FROM InventoryItem")));
        var lsn = host.Call("GiveItem", alice, 0, "alice-cloak");
        var update = await aliceSocket.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(lsn, update.Lsn);
    }

    [Fact]
    public async Task A_row_becoming_invisible_emits_a_delete_to_that_client_and_nothing_to_others()
    {
        await using var host = await TransportTestHost.StartAsync(services: InventoryPolicies);
        var alice = TestTokens.IdentityOf("alice");
        var bob = TestTokens.IdentityOf("bob");
        host.Call("GiveItem", alice, 0, "tradable");

        await using var aliceSocket = new RawSocketClient();
        await aliceSocket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("alice"));
        var aliceInitial = await InitialSetAsync(aliceSocket, 1, "SELECT * FROM InventoryItem");
        var itemKey = Assert.Single(aliceInitial).Key;
        var itemId = Convert.ToUInt64(Assert.Single(aliceInitial).Columns["Id"], System.Globalization.CultureInfo.InvariantCulture);

        await using var caroSocket = new RawSocketClient();
        await caroSocket.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("caro"));
        Assert.Empty(await InitialSetAsync(caroSocket, 1, "SELECT * FROM InventoryItem"));

        // The trade: the row moves to bob. For alice that row just became invisible — a delete on
        // the wire, or her cache would keep showing an item she no longer holds.
        var traded = host.Call("MoveItem", itemId, bob, 0);
        var toAlice = await aliceSocket.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(traded, toAlice.Lsn);
        var op = Assert.Single(Assert.Single(toAlice.Updates).Ops);
        Assert.Equal(RowOpKind.Delete, op.Kind);
        Assert.Equal(itemKey, op.Key);
        Assert.Null(op.Columns);

        // Caro (neither owner) got nothing for the trade: her first frame is the later marker.
        var marker = host.Call("GiveItem", TestTokens.IdentityOf("caro"), 0, "caro-marker");
        var toCaro = await caroSocket.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(marker, toCaro.Lsn);
    }

    [Fact]
    public async Task A_policy_can_treat_guests_differently_via_the_context()
    {
        // A guest is an ordinary identity whose token carries Auth:GuestRole — and that fact
        // reaches policies, so "members only" is one predicate rather than a second auth system.
        await using var host = await TransportTestHost.StartAsync(services: services =>
            services.AddSingleton<IRowPolicy<Chunk>, MembersOnlyChunks>());
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var guest = new RawSocketClient();
        await guest.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("visitor", role: "guest"));
        Assert.Empty(await InitialSetAsync(guest, 1, "SELECT * FROM Chunk"));

        await using var member = new RawSocketClient();
        await member.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("visitor-upgraded"));
        Assert.Single(await InitialSetAsync(member, 1, "SELECT * FROM Chunk"));
    }

    private sealed class MembersOnlyChunks : IRowPolicy<Chunk>
    {
        public bool IsVisibleTo(in Chunk row, PolicyContext ctx) => !ctx.IsGuest;
    }

    [Fact]
    public async Task No_policy_can_make_a_private_table_visible()
    {
        await using var host = await TransportTestHost.StartAsync(services: services =>
            services.AddSingleton<IRowPolicy<SecretTable>, RevealEverything>());
        host.Call("AddSecret", 1UL, "classified");

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM SecretTable", null), TestContext.Current.CancellationToken);
        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);

        // Same code as an unknown table: the error must not confirm the table exists.
        Assert.Equal(MelangeErrorCodes.UnknownTable, error.Code);
    }

    private sealed class RevealEverything : IRowPolicy<SecretTable>
    {
        public bool IsVisibleTo(in SecretTable row, PolicyContext ctx) => true;
    }

    private static string[] ItemNames(IReadOnlyList<WireRow> rows) =>
        [.. rows.Select(r => (string)r.Columns["ItemName"]!).OrderBy(n => n, StringComparer.Ordinal)];

    internal static async Task<List<WireRow>> InitialSetAsync(RawSocketClient raw, uint id, string query)
    {
        await raw.SendAsync(new SubscribeFrame(id, query, null), TestContext.Current.CancellationToken);
        var rows = new List<WireRow>();
        while (true)
        {
            var chunk = await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(
                TestContext.Current.CancellationToken, f => f.SubscriptionId == id);
            rows.AddRange(chunk.Rows);
            if (chunk.IsLast)
                return rows;
        }
    }
}
