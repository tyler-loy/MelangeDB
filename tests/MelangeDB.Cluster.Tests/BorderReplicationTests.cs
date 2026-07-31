using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Interest-driven read-only replication over the spatial strategy: each shard node holds its
/// neighbours' border bands, serves them, and can never mutate them — the one-writer-many-readers
/// property of Partitioned, actually exercised. Blocks are 4x4 chunks; the band is 2 deep.
/// </summary>
public class BorderReplicationTests
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value; // Chunks x 0..3.
    private static readonly ulong BlockB = SpatialShardStrategy.ShardOfBlock(1, 0).Value; // Chunks x 4..7.

    [Fact]
    public async Task Border_band_entities_are_visible_on_the_neighbouring_node_and_not_mutable_there()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardB = (await cluster.EnsureShardOwnedAsync(BlockB)).Runtime.TryGetShard(new ShardKey(BlockB))!;

        // A critter in B's edge chunk (4,2) is inside A's border band (depth 2).
        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(4, 2));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is not null,
            "the border copy reached the neighbouring node");
        Assert.True(shardA.BorrowedRowCount >= 1);

        // Visible is not writable: the copy is read-only, asserted at every commit — a violated
        // read-only invariant would be silent state divergence between the copy and its owner.
        var refusal = Assert.Throws<BorderReadOnlyException>(
            () => shardA.ReducerHost.Call("ShoveCritter", ClusterFixture.Caller, 1UL, Chunks.Id(3, 2)));
        Assert.Contains("read-only border-band copy", refusal.Message);
        Assert.Contains($"shard:{BlockB}", refusal.Message);

        // The owner keeps writing it, and its writes flow to the observer's copy.
        shardB.ReducerHost.Call("ShoveCritter", ClusterFixture.Caller, 1UL, Chunks.Id(5, 2));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1))?.ChunkId == Chunks.Id(5, 2),
            "the owner's update reached the observer's copy");
    }

    [Fact]
    public async Task A_row_leaving_the_band_is_retracted_from_the_observer_and_a_delete_propagates()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardB = (await cluster.EnsureShardOwnedAsync(BlockB)).Runtime.TryGetShard(new ShardKey(BlockB))!;

        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(4, 2));
        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 2UL, Chunks.Id(5, 3));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Count<Critter>() == 2,
            "both border copies reached the observer");

        // Critter 1 wanders deeper into B, out of A's band: the observer must stop seeing it —
        // the publisher turns the out-of-scope update into a retraction.
        shardB.ReducerHost.Call("ShoveCritter", ClusterFixture.Caller, 1UL, Chunks.Id(7, 2));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is null,
            "the out-of-band row was retracted from the observer");

        // Critter 2 dies: the delete propagates.
        shardB.ReducerHost.Call("DespawnCritter", ClusterFixture.Caller, 2UL);
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(2)) is null,
            "the delete propagated to the observer");
        await ClusterFixture.WaitUntilAsync(
            () => shardA.BorrowedRowCount == 0,
            "no borrowed rows remain registered");
    }

    [Fact]
    public async Task The_read_only_registry_survives_the_observers_own_snapshot_and_restart()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var ownerA = await cluster.EnsureShardOwnedAsync(BlockA);
        var shardB = (await cluster.EnsureShardOwnedAsync(BlockB)).Runtime.TryGetShard(new ShardKey(BlockB))!;
        var shardA = ownerA.Runtime.TryGetShard(new ShardKey(BlockA))!;

        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 7UL, Chunks.Id(4, 1));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(7)) is not null,
            "the border copy reached the observer");

        // The observer snapshots and truncates its own log: the border records that built the
        // borrowed registry are gone, but the rows survive in the snapshot. Without the sidecar,
        // a restarted observer would answer "not borrowed" and let the copy be mutated silently.
        await Task.Delay(800, TestContext.Current.CancellationToken); // Event-forwarder cursor catch-up frees truncation.
        shardA.Engine.TakeSnapshot();
        await cluster.StopNodeAsync(ownerA.Name);
        await cluster.StartNodeAsync(ownerA.Name);
        ShardRuntime? reopened = null;
        await ClusterFixture.WaitUntilAsync(
            () => (reopened = cluster.Node(ownerA.Name).Runtime.TryGetShard(new ShardKey(BlockA))) is not null,
            "the observer reopened its shard");

        Assert.NotNull(reopened!.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(7)));
        Assert.True(reopened.BorrowedRowCount >= 1, "the borrowed registry must survive snapshot+truncate+restart");
        Assert.Throws<BorderReadOnlyException>(
            () => reopened.ReducerHost.Call("ShoveCritter", ClusterFixture.Caller, 7UL, Chunks.Id(3, 1)));
    }

    [Fact]
    public async Task An_observer_whose_cursor_the_owner_truncated_past_is_reset_deletions_included()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var ownerA = await cluster.EnsureShardOwnedAsync(BlockA);
        var ownerB = await cluster.EnsureShardOwnedAsync(BlockB);
        Assert.NotEqual(ownerA.Name, ownerB.Name);
        var shardA = ownerA.Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardB = ownerB.Runtime.TryGetShard(new ShardKey(BlockB))!;

        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(4, 1));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is not null,
            "the first border copy arrived");

        // The observer goes dark; the owner deletes critter 1, spawns critter 2, then truncates
        // past everything the observer ever saw. The gap's records are gone forever.
        await cluster.StopNodeAsync(ownerA.Name);
        shardB.ReducerHost.Call("DespawnCritter", ClusterFixture.Caller, 1UL);
        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 2UL, Chunks.Id(4, 2));
        var headBefore = shardB.Engine.Log.HeadLsn;
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                shardB.Engine.TakeSnapshot();
                return shardB.Engine.Log.BaseLsn >= headBefore;
            },
            "the owner truncated past the observer's border cursor");

        // The returning observer cannot be served from the log; it must get the full band reset —
        // including the deletion, which a silently resumed stream would never deliver.
        await cluster.StartNodeAsync(ownerA.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(ownerA.Name).Runtime.TryGetShard(new ShardKey(BlockA)) is { } reopened
                && reopened.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(2)) is not null
                && reopened.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is null,
            "the reset observer converged, deletion included");
        Assert.True(
            cluster.Node(ownerA.Name).Metrics.ReceivedByType.GetValueOrDefault("border-reset-apply") >= 1,
            "the stream must have been reset, not silently resumed past the gap");
    }

    [Fact]
    public async Task Border_bandwidth_is_measured_as_bytes_on_the_link_not_inferred()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var observer = await cluster.EnsureShardOwnedAsync(BlockA);
        var shardB = (await cluster.EnsureShardOwnedAsync(BlockB)).Runtime.TryGetShard(new ShardKey(BlockB))!;
        var shardA = observer.Runtime.TryGetShard(new ShardKey(BlockA))!;

        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(4, 2));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is not null,
            "the border copy arrived");

        var applied = observer.Metrics.ReceivedBytesByType.GetValueOrDefault("border-apply")
            + observer.Metrics.ReceivedBytesByType.GetValueOrDefault("border-reset-apply");
        Assert.True(applied > 0, "border-band bandwidth must be observable as received bytes by type");
    }
}
