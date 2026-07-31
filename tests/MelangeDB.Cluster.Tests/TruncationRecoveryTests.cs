using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Log truncation versus the cluster's log-recovered state — the phase 08 silent-gap bug class,
/// hunted in its phase 09 habitats. Handoff markers pin truncation while their saga is
/// unresolved, so a snapshot mid-transfer can never erase the fact that a player is frozen
/// (origin) or was imported (destination); a settled saga releases the pin. The replica stream
/// bootstraps instead of silently skipping a truncated gap.
/// </summary>
public class TruncationRecoveryTests
{
    [Fact]
    public async Task A_freeze_marker_survives_the_origins_own_snapshot_and_recovery_still_resolves_the_transfer()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var player = TestTokens.IdentityOf("snapshotted-traveller");
        var originOwner = await cluster.EnsureShardOwnedAsync(1);
        var destinationOwner = await cluster.EnsureShardOwnedAsync(2);
        var origin = originOwner.Runtime.TryGetShard(new ShardKey(1))!;

        // Padding before the freeze, so truncation has something to actually remove.
        for (var i = 0; i < 3; i++)
            origin.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 1u, 10 + i);
        origin.ReducerHost.Call("GrantGold", player, 1u, 500);

        Exception? hookFailure = null;
        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step != "release")
                return;
            try
            {
                // Mid-saga: freeze appended, import durable on the destination, release not yet
                // sent. The origin snapshots its own shard — the exact moment a truncated freeze
                // marker would silently unfreeze a mid-transfer player.
                await Task.Delay(800); // Let the event forwarder's cursor reach the head; it floors truncation too.
                origin.Engine.TakeSnapshot();
                Assert.True(origin.Engine.Log.BaseLsn > 0, "truncation should have removed the padding");
                var tail = origin.Engine.Log.ReadFrom(origin.Engine.Log.BaseLsn + 1)
                    .Select(static record => $"{record.Lsn}:{record.ReducerName}").ToList();
                Assert.True(
                    tail.Any(static entry => entry.EndsWith("melange/handoff-freeze", StringComparison.Ordinal)),
                    $"freeze marker missing; base={origin.Engine.Log.BaseLsn} pending={origin.PendingFreezes.Count} tail=[{string.Join(", ", tail)}]");

                // And then the origin dies before the release arrives.
                await cluster.StopNodeAsync(originOwner.Name);
            }
            catch (Exception exception)
            {
                hookFailure = exception;
                throw;
            }
        };
        await Assert.ThrowsAnyAsync<Exception>(() => cluster.Coordinator.TransferPlayerAsync(
            player, new ShardKey(1), new ShardKey(2), TestContext.Current.CancellationToken));
        cluster.Hub.HandoffStepHook = null;
        Assert.Null(hookFailure);

        // The revived origin recovers the pinned freeze from its truncated log, learns the
        // destination imported, and releases: exactly one owner.
        await cluster.StartNodeAsync(originOwner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(originOwner.Name).Runtime.TryGetShard(new ShardKey(1)) is { } reopened
                && reopened.Engine.CommittedView.Find<PlayerState>(player) is null,
            "the recovered origin found its freeze marker and released the transferred player");
        var moved = cluster.Node(destinationOwner.Name).Runtime.TryGetShard(new ShardKey(2))!
            .Engine.CommittedView.Find<PlayerState>(player);
        Assert.NotNull(moved);
        Assert.Equal(500, moved!.Value.Gold);
    }

    [Fact]
    public async Task An_import_marker_survives_the_destinations_snapshot_so_a_restarted_destination_never_denies_its_import()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var player = TestTokens.IdentityOf("denied-traveller");
        var originOwner = await cluster.EnsureShardOwnedAsync(1);
        var destinationOwner = await cluster.EnsureShardOwnedAsync(2);
        var origin = originOwner.Runtime.TryGetShard(new ShardKey(1))!;
        var destination = destinationOwner.Runtime.TryGetShard(new ShardKey(2))!;
        origin.ReducerHost.Call("GrantGold", player, 1u, 750);
        for (var i = 0; i < 3; i++)
            destination.ReducerHost.Call("SpawnMob", ClusterFixture.Caller, 2u, 20 + i); // Truncation fodder.

        // Import lands; the origin dies before release, so the import stays unsettled — and its
        // marker must survive the destination's own snapshot, or a restarted destination would
        // answer "never imported" while holding the rows: two owners.
        cluster.Hub.HandoffStepHook = async step =>
        {
            if (step == "release")
                await cluster.StopNodeAsync(originOwner.Name);
        };
        await Assert.ThrowsAnyAsync<Exception>(() => cluster.Coordinator.TransferPlayerAsync(
            player, new ShardKey(1), new ShardKey(2), TestContext.Current.CancellationToken));
        cluster.Hub.HandoffStepHook = null;

        await Task.Delay(800, TestContext.Current.CancellationToken); // Event forwarder cursor catch-up.
        destination.Engine.TakeSnapshot();
        Assert.True(destination.Engine.Log.BaseLsn > 0, "truncation should have removed the padding");
        Assert.Contains(
            destination.Engine.Log.ReadFrom(destination.Engine.Log.BaseLsn + 1),
            static record => record.ReducerName == "melange/handoff-import");

        // Bounce the destination: its recovery must re-derive the import from the pinned marker.
        await cluster.StopNodeAsync(destinationOwner.Name);
        await cluster.StartNodeAsync(destinationOwner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(destinationOwner.Name).Runtime.TryGetShard(new ShardKey(2)) is not null,
            "the destination reopened its shard");
        var reopened = cluster.Node(destinationOwner.Name).Runtime.TryGetShard(new ShardKey(2))!;
        Assert.True(reopened.WasImported(reopened.UnsettledImports.Single().HandoffId));

        // The revived origin asks, gets the truthful "imported", and releases: one owner.
        await cluster.StartNodeAsync(originOwner.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(originOwner.Name).Runtime.TryGetShard(new ShardKey(1)) is { } reopenedOrigin
                && reopenedOrigin.Engine.CommittedView.Find<PlayerState>(player) is null,
            "the recovered origin released against the destination's surviving import marker");
        Assert.NotNull(reopened.Engine.CommittedView.Find<PlayerState>(player));
    }

    [Fact]
    public async Task A_settled_import_stops_pinning_truncation()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var player = TestTokens.IdentityOf("settled-traveller");
        var originOwner = await cluster.EnsureShardOwnedAsync(1);
        var destinationOwner = await cluster.EnsureShardOwnedAsync(2);
        originOwner.Runtime.TryGetShard(new ShardKey(1))!.ReducerHost.Call("GrantGold", player, 1u, 5);
        await cluster.Coordinator.TransferPlayerAsync(
            player, new ShardKey(1), new ShardKey(2), TestContext.Current.CancellationToken);

        // The origin released, so the destination's reconciler settles the import within a sweep;
        // once settled, the marker is truncatable — the pin is bounded by the saga, not history.
        var destination = destinationOwner.Runtime.TryGetShard(new ShardKey(2))!;
        await ClusterFixture.WaitUntilAsync(
            () => destination.UnsettledImports.Count == 0,
            "the destination's reconciler settled the completed import");

        await Task.Delay(800, TestContext.Current.CancellationToken); // Event forwarder cursor catch-up.
        destination.Engine.TakeSnapshot();
        Assert.DoesNotContain(
            destination.Engine.Log.ReadFrom(destination.Engine.Log.BaseLsn + 1),
            static record => record.ReducerName == "melange/handoff-import");
    }

    [Fact]
    public async Task A_node_returning_after_hub_truncation_bootstraps_the_replica_stream_and_converges_exactly()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000);
        var node = cluster.Nodes[0];
        await cluster.EnsureShardOwnedAsync(1);
        var itemDef = cluster.HubEngine.Schema.Get(typeof(ItemDef));

        cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 1L, "alpha");
        cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 2L, "beta");
        await ClusterFixture.WaitUntilAsync(
            () => node.Runtime.TryGetShard(new ShardKey(1))!.Engine.CommittedView.Count<ItemDef>() == 2,
            "the node converged before the outage");

        // The node goes dark; the hub updates, deletes, inserts — then snapshots and truncates
        // past everything the node ever saw. The gap's records are gone forever.
        var headBeforeGap = cluster.HubEngine.Log.HeadLsn;
        await cluster.StopNodeAsync(node.Name);
        cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 1L, "alpha-2");
        cluster.HubReducers.Call("DeleteItemDef", ClusterFixture.Caller, 2L);
        cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 3L, "gamma");
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                cluster.HubEngine.TakeSnapshot();
                return cluster.HubEngine.Log.BaseLsn > headBeforeGap;
            },
            "the hub truncated past the node's replica cursor");

        // The returning node cannot be served from the log; it must get the full-state reset —
        // including the deletion, which a pure upsert bootstrap would have resurrected.
        await cluster.StartNodeAsync(node.Name);
        var hubRows = Rows(cluster.HubEngine, itemDef.Id);
        await ClusterFixture.WaitUntilAsync(
            () => node.App is not null
                && node.Runtime.TryGetShard(new ShardKey(1)) is { } shard
                && Rows(shard.Engine, itemDef.Id).SequenceEqual(hubRows),
            "the bootstrapped node converged byte-identically, deletion included");

        var reopened = node.Runtime.TryGetShard(new ShardKey(1))!;
        Assert.Null(reopened.Engine.CommittedView.Find<ItemDef>(2L));
        Assert.Equal("alpha-2", reopened.Engine.CommittedView.Find<ItemDef>(1L)!.Value.Name);
        Assert.True(node.Metrics.ReceivedByType.GetValueOrDefault("replica-reset") >= 1,
            "the stream must have been bootstrapped, not silently resumed past the gap");
    }

    private static List<string> Rows(Core.MelangeEngine engine, TableId table) =>
        [.. engine.HotStore.Scan(table).Select(static pair => $"{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}")];
}
