using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The planned drain (road-to-0.2 phase 13): moving a live shard between nodes while both are up.
/// Each test stands up its own cluster — drains mutate topology, and a shared fixture's other
/// tests must not observe a world mid-move.
/// </summary>
public sealed class DrainTests
{
    [Fact]
    public async Task A_drain_moves_a_live_shard_and_its_connected_client_observes_a_pause_not_a_disconnect()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2);
        var origin = await fixture.EnsureShardOwnedAsync(70);
        var destination = fixture.Nodes.Single(n => n.Name != origin.Name);

        await using var client = fixture.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SetLocation", [70u], TestContext.Current.CancellationToken);
        for (var i = 0; i < 3; i++)
            await client.CallReducerAsync("SpawnMob", [70u, 100 + i], TestContext.Current.CancellationToken);
        var mobs = await client.SubscribeAsync(
            "SELECT * FROM Mob WHERE InstanceId = :i",
            new Dictionary<string, object?> { ["i"] = 70u },
            TestContext.Current.CancellationToken);
        Assert.Equal(3, mobs.Count);

        // A call issued mid-drain — after the origin is quiesced, before the destination owns —
        // must queue at the gateway and execute on the destination, in order, with a real result.
        Task<ulong>? midDrainCall = null;
        fixture.Hub.DrainStepHook = step =>
        {
            if (step == "reassign")
                midDrainCall = client.CallReducerAsync("SpawnMob", [70u, 999], CancellationToken.None);
            return Task.CompletedTask;
        };
        try
        {
            await fixture.Coordinator.DrainShardAsync(
                new ShardKey(70), destination.Name, TestContext.Current.CancellationToken);
        }
        finally
        {
            fixture.Hub.DrainStepHook = null;
        }

        // Ownership moved: membership names the destination, the destination serves the shard,
        // and the origin no longer holds it.
        var assignment = fixture.Hub.Membership.GetAssignment(new ShardKey(70))!;
        Assert.Equal(destination.Name, assignment.NodeName);
        Assert.NotNull(destination.Runtime.TryGetShard(new ShardKey(70)));
        Assert.Null(origin.Runtime.TryGetShard(new ShardKey(70)));

        // The queued call completed against the destination — a commit, not an error.
        Assert.NotNull(midDrainCall);
        Assert.True(await midDrainCall! > 0);

        // The client's cache re-scoped to the destination's state: the original rows survived the
        // move byte-for-byte (recovery from the shard's own log) plus the mid-drain spawn.
        await ClusterFixture.WaitUntilAsync(() => mobs.Count == 4, "the client's cache converged on the destination");

        // Same socket, still live: post-drain traffic flows without any reconnect.
        await client.CallReducerAsync("SpawnMob", [70u, 5], TestContext.Current.CancellationToken);
        await ClusterFixture.WaitUntilAsync(() => mobs.Count == 5, "post-drain deltas reach the client");
    }

    [Fact]
    public async Task A_drain_without_a_destination_picks_the_least_loaded_node_and_the_data_survives()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2);
        var origin = await fixture.EnsureShardOwnedAsync(71);
        for (var i = 0; i < 5; i++)
        {
            await fixture.Coordinator.ExecuteOnShardAsync(
                new ShardKey(71), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [71u, i],
                TestContext.Current.CancellationToken);
        }

        await fixture.Coordinator.DrainShardAsync(new ShardKey(71), ct: TestContext.Current.CancellationToken);

        var assignment = fixture.Hub.Membership.GetAssignment(new ShardKey(71))!;
        Assert.NotEqual(origin.Name, assignment.NodeName);
        var moved = fixture.Node(assignment.NodeName!).Runtime.TryGetShard(new ShardKey(71));
        Assert.NotNull(moved);
        Assert.Equal(5, moved!.Engine.CommittedView.Count<Mob>());

        // The moved shard takes writes under its new term.
        var lsn = await fixture.Coordinator.ExecuteOnShardAsync(
            new ShardKey(71), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [71u, 5],
            TestContext.Current.CancellationToken);
        Assert.True(lsn > 0);
        Assert.Equal(6, moved.Engine.CommittedView.Count<Mob>());
    }

    [Fact]
    public async Task A_drain_with_an_undrainable_target_is_refused_and_the_shard_keeps_serving()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2);
        var origin = await fixture.EnsureShardOwnedAsync(72);

        // To its own owner: refused rather than silently done.
        var toSelf = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Coordinator.DrainShardAsync(new ShardKey(72), origin.Name, TestContext.Current.CancellationToken));
        Assert.Contains("refused", toSelf.Message);

        // To a node that does not exist: a drain must never assign to a corpse.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Coordinator.DrainShardAsync(new ShardKey(72), "node-x", TestContext.Current.CancellationToken));

        // A shard that was never created has nothing to drain.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Coordinator.DrainShardAsync(new ShardKey(999), ct: TestContext.Current.CancellationToken));

        // Unmoved and unharmed.
        Assert.Equal(origin.Name, fixture.Hub.Membership.GetAssignment(new ShardKey(72))!.NodeName);
        var lsn = await fixture.Coordinator.ExecuteOnShardAsync(
            new ShardKey(72), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [72u, 1],
            TestContext.Current.CancellationToken);
        Assert.True(lsn > 0);
    }

    [Fact]
    public async Task A_drain_that_fails_after_quiesce_hands_the_shard_back_to_the_origin_intact()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2);
        var origin = await fixture.EnsureShardOwnedAsync(73);
        await fixture.Coordinator.ExecuteOnShardAsync(
            new ShardKey(73), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [73u, 42],
            TestContext.Current.CancellationToken);

        // Fault injected between quiesce and reassign — the exact window where the shard is
        // closed on the origin but membership still names it.
        fixture.Hub.DrainStepHook = step =>
            step == "reassign" ? throw new InvalidOperationException("injected fault") : Task.CompletedTask;
        try
        {
            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                fixture.Coordinator.DrainShardAsync(new ShardKey(73), ct: TestContext.Current.CancellationToken));
            Assert.Equal("injected fault", failure.Message);
        }
        finally
        {
            fixture.Hub.DrainStepHook = null;
        }

        // The abort handed the shard back: ownership never moved, the origin reopens it (within a
        // heartbeat), and the row survived the round trip through quiesce and recovery.
        Assert.Equal(origin.Name, fixture.Hub.Membership.GetAssignment(new ShardKey(73))!.NodeName);
        await ClusterFixture.WaitUntilAsync(
            () => origin.Runtime.TryGetShard(new ShardKey(73)) is { } reopened
                && reopened.Engine.CommittedView.Count<Mob>() == 1,
            "the origin reopened the shard with its data intact");

        var lsn = await fixture.Coordinator.ExecuteOnShardAsync(
            new ShardKey(73), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [73u, 2],
            TestContext.Current.CancellationToken);
        Assert.True(lsn > 0);
        Assert.Equal(1, fixture.Hub.Metrics.DrainsFailed);
    }
}
