using MelangeDB.Client;
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
        var initial = await admin.InitialSetAsync(1, "SELECT * FROM Creature");
        var row = Assert.Single(initial.Rows);

        // The descriptor is the guarantee now: a [ServerOnly] column that never appears in it
        // cannot appear in any row, because the rows are positional against it.
        Assert.Equal(["Id", "X"], initial.ColumnNames);
        Assert.Equal(["Id", "X"], initial.Columns(row).Keys.OrderBy(k => k, StringComparer.Ordinal));

        // The delta path hides them too.
        var creatureId = Convert.ToUInt64(initial.Columns(row)["Id"], System.Globalization.CultureInfo.InvariantCulture);
        host.Call("MoveCreature", creatureId, 11f);
        var update = await admin.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        var op = Assert.Single(Assert.Single(update.Updates).Ops);
        Assert.Equal(["Id", "X"], initial.Columns(op).Keys.OrderBy(k => k, StringComparer.Ordinal));
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
        var initial = await raw.InitialSetAsync(1, "SELECT * FROM Creature");
        var creatureId = Convert.ToUInt64(
            initial.Columns(Assert.Single(initial.Rows))["Id"],
            System.Globalization.CultureInfo.InvariantCulture);

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
        var aliceInitial = await alice.InitialSetAsync(1, "SELECT * FROM PlayerState");
        var toAlice = aliceInitial.Columns(Assert.Single(aliceInitial.Rows));
        Assert.False(toAlice.ContainsKey("X"), "another player's position leaked through the hideout mask");
        Assert.Equal("Bob", toAlice["Name"]);

        // The descriptor still names every column — a column policy narrows the ROW, per row, and
        // says so with a mask. That is the distinction the mask exists to draw: [ServerOnly] is a
        // shape, a hideout is a state.
        Assert.Contains("X", aliceInitial.ColumnNames);

        // Direction two: the owner always sees their own row whole.
        var bobInitial = await bob.InitialSetAsync(1, "SELECT * FROM PlayerState");
        var toBob = bobInitial.Columns(Assert.Single(bobInitial.Rows));
        Assert.True(toBob.ContainsKey("X"), "the owner's own position was masked");

        // The mask changes mid-subscription: bob leaves the hideout, and alice's client is
        // updated with the newly visible column — no resubscribe required for row-driven masks.
        await bobClient.CallReducerAsync("Spawn", ["Bob", 1], TestContext.Current.CancellationToken);
        var update = await alice.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        var op = Assert.Single(Assert.Single(update.Updates).Ops);
        Assert.Equal(RowOpKind.Update, op.Kind);
        Assert.True(aliceInitial.Columns(op).ContainsKey("X"), "leaving the hideout must reveal the position");

        // And back again: re-entering hides it, also as a live update.
        await bobClient.CallReducerAsync("Spawn", ["Bob", HideoutHidesPosition.HideoutRoom], TestContext.Current.CancellationToken);
        var hiddenAgain = await alice.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken);
        var hiddenOp = Assert.Single(Assert.Single(hiddenAgain.Updates).Ops);
        Assert.False(aliceInitial.Columns(hiddenOp).ContainsKey("X"), "re-entering the hideout must hide the position");
    }

    [Fact]
    public async Task A_masked_row_reaching_a_typed_cache_fails_loudly_rather_than_filling_a_default()
    {
        // A generated row struct has a field for every column, so a row that arrived without one
        // cannot be built honestly — and building it with X = 0 would hand the game a position for
        // a player whose position is deliberately hidden. Under protocol v2 that is not a
        // judgement call the client can duck: the row bytes simply do not contain the column, and
        // reading them positionally as if they did would decode the next column into it.
        await using var host = await TransportTestHost.StartAsync(services: services =>
            services.AddSingleton<IColumnPolicy<PlayerState>, HideoutHidesPosition>());

        await using var bobClient = host.CreateClient(o => o.Token = TestTokens.For("bob"));
        await bobClient.ConnectAsync(TestContext.Current.CancellationToken);
        await bobClient.CallReducerAsync("Spawn", ["Bob", HideoutHidesPosition.HideoutRoom], TestContext.Current.CancellationToken);

        await using var aliceClient = host.CreateClient(o => o.Token = TestTokens.For("alice"));
        await aliceClient.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeDB.Types.MelangeConnection(aliceClient);

        var failure = await Assert.ThrowsAsync<MelangeSubscriptionException>(
            () => conn.Db.PlayerState.SubscribeAllAsync(TestContext.Current.CancellationToken));

        Assert.Contains("untyped", failure.Message);
    }
}
