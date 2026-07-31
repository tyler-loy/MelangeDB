using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Kills mid-transfer, on clusters these tests own. The saga's invariant: whichever step dies,
/// recovery lands on exactly one owner — both halves appended their markers to their own logs
/// before acknowledging, so the answer is always in a log somewhere. Failure timeouts are long so
/// reassignment never races the recovery under test.
/// </summary>
public class HandoffRecoveryTests
{
    [Fact]
    public async Task Killing_the_destination_before_its_import_aborts_and_the_player_stays_on_the_origin()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var player = TestTokens.IdentityOf("doomed-traveller");
        var originOwner = await cluster.EnsureShardOwnedAsync(1);
        var destinationOwner = await cluster.EnsureShardOwnedAsync(2);
        Assert.NotEqual(originOwner.Name, destinationOwner.Name); // Least-loaded assignment splits them.
        var origin = originOwner.Runtime.TryGetShard(new ShardKey(1))!;
        origin.ReducerHost.Call("GrantGold", player, 1u, 500);

        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step == "import")
                await cluster.StopNodeAsync(destinationOwner.Name);
        };
        await Assert.ThrowsAnyAsync<Exception>(() => cluster.Coordinator.TransferPlayerAsync(
            player, new ShardKey(1), new ShardKey(2), TestContext.Current.CancellationToken));
        cluster.Hub.HandoffStepHook = null;

        // The coordinator saw a dead link, not an error reply — the destination MAY hold the
        // import (an ack lost in transit looks identical), so aborting blind could mint two
        // owners. The freeze deliberately stays: writable nowhere until the truth is known.
        var frozen = Assert.ThrowsAny<Exception>(() => origin.ReducerHost.Call("GrantGold", player, 1u, 1));
        Assert.Contains("frozen mid-handoff", frozen.Message);

        // The revived destination's log has no import; the origin's reconciler learns that and
        // aborts — the player is unfrozen, intact, and writable exactly where it was.
        await cluster.StartNodeAsync(destinationOwner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => origin.PendingFreezes.Count == 0,
            "the origin's reconciler resolved the unknowable failure to an abort");
        var kept = origin.Engine.CommittedView.Find<PlayerState>(player);
        Assert.NotNull(kept);
        Assert.Equal(500, kept!.Value.Gold);
        origin.ReducerHost.Call("GrantGold", player, 1u, 1);

        var destination = cluster.Node(destinationOwner.Name).Runtime.TryGetShard(new ShardKey(2))!;
        Assert.Null(destination.Engine.CommittedView.Find<PlayerState>(player));
    }

    [Fact]
    public async Task Killing_the_origin_after_the_import_recovers_to_exactly_one_owner_the_destination()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var player = TestTokens.IdentityOf("orphaned-traveller");
        var originOwner = await cluster.EnsureShardOwnedAsync(1);
        var destinationOwner = await cluster.EnsureShardOwnedAsync(2);
        var origin = originOwner.Runtime.TryGetShard(new ShardKey(1))!;
        origin.ReducerHost.Call("GrantGold", player, 1u, 750);

        // The destination's import is durable; the origin dies before it can release.
        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step == "release")
                await cluster.StopNodeAsync(originOwner.Name);
        };
        await Assert.ThrowsAnyAsync<Exception>(() => cluster.Coordinator.TransferPlayerAsync(
            player, new ShardKey(1), new ShardKey(2), TestContext.Current.CancellationToken));
        cluster.Hub.HandoffStepHook = null;

        var destination = cluster.Node(destinationOwner.Name).Runtime.TryGetShard(new ShardKey(2))!;
        Assert.NotNull(destination.Engine.CommittedView.Find<PlayerState>(player));

        // The revived origin replays its log, finds the unreleased freeze, asks whether the
        // import happened — it did — and completes its half: the rows leave the origin.
        await cluster.StartNodeAsync(originOwner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(originOwner.Name).Runtime.TryGetShard(new ShardKey(1)) is { } reopened
                && reopened.Engine.CommittedView.Find<PlayerState>(player) is null,
            "the recovered origin released the transferred player");

        var moved = destination.Engine.CommittedView.Find<PlayerState>(player);
        Assert.NotNull(moved);
        Assert.Equal(750, moved!.Value.Gold);
        Assert.Equal(2u, moved.Value.InstanceId);
    }
}
