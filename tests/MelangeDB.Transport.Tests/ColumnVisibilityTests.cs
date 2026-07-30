using MelangeDB.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Column visibility, asserted by inspecting frames: <c>[ServerOnly]</c> columns never reach any
/// client (admin included), asking for one is an error rather than a null, and dynamic column
/// masks are correct in both directions and update the client when they change mid-subscription.
/// Rows union, columns intersect.
/// </summary>
public class ColumnVisibilityTests
{
    [Fact]
    public async Task A_ServerOnly_column_never_appears_on_the_wire_for_anyone_admin_included()
    {
        await using var host = await TransportTestHost.StartAsync(services: services =>
        {
            services.AddSingleton<IRowPolicy<InventoryItem>, AdminSeesAllInventory>();
        });
        host.Call("AddAdmin", TestTokens.IdentityOf("admin-1"));
        host.Call("SpawnCreature", 10f, 777UL);

        // The admin gets every ROW policies allow — and still not one [ServerOnly] COLUMN. If
        // NextThinkAt ever reaches a frame, a cheater knows exactly when the AI thinks next.
        await using var admin = new RawSocketClient();
        await admin.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("admin-1"));
        var initial = await RowPolicyTests.InitialSetAsync(admin, 1, "SELECT * FROM Creature");
        var row = Assert.Single(initial);
        Assert.Equal(["Id", "X"], row.Columns.Keys.OrderBy(k => k, StringComparer.Ordinal));

        // The delta path hides them too.
        var creatureId = Convert.ToUInt64(row.Columns["Id"], System.Globalization.CultureInfo.InvariantCulture);
        host.Call("MoveCreature", creatureId, 11f);
        var update = await admin.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        var op = Assert.Single(Assert.Single(update.Updates).Ops);
        Assert.Equal(["Id", "X"], op.Columns!.Keys.OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public async Task A_change_to_only_ServerOnly_columns_emits_no_frame_at_all()
    {
        // The timing oracle: an update frame with unchanged visible columns would still tell a
        // client "the creature just decided something." Silence is part of the guarantee.
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SpawnCreature", 10f, 777UL);

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        var initial = await RowPolicyTests.InitialSetAsync(raw, 1, "SELECT * FROM Creature");
        var creatureId = Convert.ToUInt64(Assert.Single(initial).Columns["Id"], System.Globalization.CultureInfo.InvariantCulture);

        host.Call("NudgeCreatureThink", creatureId, 888UL);
        var marker = host.Call("SetChunk", 1L, 1L, new byte[] { 1 });
        _ = marker;

        // Nothing arrives for the nudge; prove it with a second, visible creature write.
        var visible = host.Call("MoveCreature", creatureId, 12f);
        var update = await raw.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(visible, update.Lsn);
    }

    [Fact]
    public async Task Explicitly_requesting_a_ServerOnly_column_is_an_error_not_a_null()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        await raw.SendAsync(new SubscribeFrame(1, "SELECT Id, NextThinkAt FROM Creature", null), TestContext.Current.CancellationToken);
        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.ServerOnlyColumn, error.Code);

        // A predicate on one leaks values through membership; that is an error too.
        await raw.SendAsync(
            new SubscribeFrame(2, "SELECT * FROM Creature WHERE NextThinkAt = :t", new Dictionary<string, object?> { ["t"] = 1UL }),
            TestContext.Current.CancellationToken);
        var predicateError = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.ServerOnlyColumn, predicateError.Code);
    }

    [Fact]
    public async Task A_column_mask_is_correct_in_both_directions_and_updates_the_client_when_it_changes()
    {
        await using var host = await TransportTestHost.StartAsync(services: services =>
            services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>());

        // Bob spawns in the hideout; alice spawns in the open.
        await using var bobClient = host.CreateClient(o => o.Token = TestTokens.For("bob"));
        await bobClient.ConnectAsync(TestContext.Current.CancellationToken);
        await bobClient.CallReducerAsync("Spawn", ["Bob", HideoutHidesPosition.HideoutRoom], TestContext.Current.CancellationToken);

        await using var alice = new RawSocketClient();
        await alice.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("alice"));
        await using var bob = new RawSocketClient();
        await bob.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, TestTokens.For("bob"));

        // Direction one: alice cannot see a hidden player's position; the row itself is visible.
        var toAlice = Assert.Single(await RowPolicyTests.InitialSetAsync(alice, 1, "SELECT * FROM PlayerState"));
        Assert.False(toAlice.Columns.ContainsKey("X"), "another player's position leaked through the hideout mask");
        Assert.Equal("Bob", toAlice.Columns["Name"]);

        // Direction two: the owner always sees their own row whole.
        var toBob = Assert.Single(await RowPolicyTests.InitialSetAsync(bob, 1, "SELECT * FROM PlayerState"));
        Assert.True(toBob.Columns.ContainsKey("X"), "the owner's own position was masked");

        // The mask changes mid-subscription: bob leaves the hideout, and alice's client is
        // updated with the newly visible column — no resubscribe required for row-driven masks.
        await bobClient.CallReducerAsync("Spawn", ["Bob", 1], TestContext.Current.CancellationToken);
        var update = await alice.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        var op = Assert.Single(Assert.Single(update.Updates).Ops);
        Assert.Equal(RowOpKind.Update, op.Kind);
        Assert.True(op.Columns!.ContainsKey("X"), "leaving the hideout must reveal the position");

        // And back again: re-entering hides it, also as a live update.
        await bobClient.CallReducerAsync("Spawn", ["Bob", HideoutHidesPosition.HideoutRoom], TestContext.Current.CancellationToken);
        var hiddenAgain = await alice.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        var hiddenOp = Assert.Single(Assert.Single(hiddenAgain.Updates).Ops);
        Assert.False(hiddenOp.Columns!.ContainsKey("X"), "re-entering the hideout must hide the position");
    }
}
