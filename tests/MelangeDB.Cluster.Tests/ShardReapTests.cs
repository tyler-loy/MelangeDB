using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The one cluster operation that destroys durable state, so the tests that matter most are the
/// refusals: a reap that goes ahead when it should not have cannot be undone.
/// </summary>
public class ShardReapTests
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value;
    private static readonly ulong BlockB = SpatialShardStrategy.ShardOfBlock(1, 0).Value;

    [Fact]
    public async Task An_empty_shard_is_removed_and_its_key_can_be_visited_again_as_a_new_shard()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var before = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA))!;
        var directory = owner.Runtime.TryGetShard(new ShardKey(BlockA))!.Directory;
        Assert.True(Directory.Exists(directory));

        Assert.True(await cluster.Hub.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));

        Assert.Null(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));
        await ClusterFixture.WaitUntilAsync(
            () => !Directory.Exists(directory),
            "the shard's directory went away (Windows can hold a deleted directory visible briefly)");
        Assert.Null(owner.Runtime.TryGetShard(new ShardKey(BlockA)));

        // The key is not reserved: arriving there again is a new shard, and it must not inherit
        // the retired originator — ids minted under the old one may still be alive elsewhere.
        var after = cluster.Hub.Membership.EnsureShard(new ShardKey(BlockA), DateTimeOffset.UnixEpoch);
        Assert.NotEqual(before.Originator, after.Originator);
    }

    [Fact]
    public async Task A_shard_that_still_owns_rows_is_refused_and_left_untouched()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var shard = owner.Runtime.TryGetShard(new ShardKey(BlockA))!;
        shard.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(0, 0));
        var directory = shard.Directory;

        Assert.False(await cluster.Hub.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));

        // Refused means untouched, not partially done: the assignment, the engine, and the data
        // are all still there, and the critter is still readable.
        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));
        Assert.True(Directory.Exists(directory));
        var live = owner.Runtime.TryGetShard(new ShardKey(BlockA));
        Assert.NotNull(live);
        Assert.NotNull(live!.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)));
    }

    [Fact]
    public async Task A_shard_holding_only_a_neighbours_borrowed_band_is_still_reapable()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var ownerA = await cluster.EnsureShardOwnedAsync(BlockA);
        var shardB = (await cluster.EnsureShardOwnedAsync(BlockB)).Runtime.TryGetShard(new ShardKey(BlockB))!;

        // B's critter lands in A's border band, so A holds a copy it does not own. Borrowed rows
        // are rebuilt by a band reset, so they must not keep A alive.
        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(4, 2));
        await ClusterFixture.WaitUntilAsync(
            () => ownerA.Runtime.TryGetShard(new ShardKey(BlockA))?.BorrowedRowCount >= 1,
            "A borrowed B's critter");

        Assert.True(await cluster.Hub.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));

        // B is untouched: it owns the critter, and losing an observer is not losing data.
        Assert.NotNull(shardB.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)));
    }

    [Fact]
    public async Task An_empty_shard_whose_log_is_pinned_is_refused_until_the_pin_is_released()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var shard = owner.Runtime.TryGetShard(new ShardKey(BlockA))!;
        var directory = shard.Directory;

        // The shard is empty, but a backup is streaming its log. Emptiness is not the only
        // question: the records are still owed to someone.
        var pin = shard.Engine.PinTruncation();
        Assert.False(await cluster.Hub.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));
        Assert.True(Directory.Exists(directory));
        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));

        // Released, the same shard reaps — the refusal was about the pin and nothing else.
        pin.Dispose();
        Assert.True(await cluster.Hub.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));
        await ClusterFixture.WaitUntilAsync(
            () => !Directory.Exists(directory),
            "the shard's directory went away (Windows can hold a deleted directory visible briefly)");
    }

    [Fact]
    public async Task Reaping_a_shard_that_was_never_created_is_refused_before_anything_happens()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => cluster.Hub.ReapShardAsync(new ShardKey(BlockB), TestContext.Current.CancellationToken));
    }
}
