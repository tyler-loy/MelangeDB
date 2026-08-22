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

        Assert.True(await cluster.Coordinator.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));

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

        Assert.False(await cluster.Coordinator.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));

        // Refused means untouched, not partially done: the assignment, the engine, and the data
        // are all still there, and the critter is still readable.
        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));
        Assert.True(Directory.Exists(directory));
        var live = owner.Runtime.TryGetShard(new ShardKey(BlockA));
        Assert.NotNull(live);
        Assert.NotNull(live!.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)));
    }

    /// <summary>
    /// The refusal above passed for the wrong reason, and only by timing. Emptiness used to be read
    /// off <c>SampleLoad</c>, whose authoritative row count is throttled to ten seconds on purpose —
    /// so a shard the heartbeat had already sampled as empty stayed "empty" to a reap for the whole
    /// of the next ten seconds, no matter what it took in between. The existing test only avoided
    /// that by squeezing its write and its reap into one 150 ms heartbeat gap; a heartbeat landing
    /// first would have deleted a shard holding a critter.
    ///
    /// <para>Here the stale sample is forced rather than raced: sample while empty, then write,
    /// then reap. The reap has to count the row itself.</para>
    /// </summary>
    [Fact]
    public async Task A_shard_that_took_rows_since_its_last_load_sample_is_still_refused()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var shard = owner.Runtime.TryGetShard(new ShardKey(BlockA))!;
        var directory = shard.Directory;

        // The sample the reap must not believe: taken while the shard genuinely holds nothing, and
        // cached for the next ten seconds.
        Assert.Equal(0, shard.SampleLoad().AuthoritativeRows);
        shard.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(0, 0));
        Assert.Equal(0, shard.SampleLoad().AuthoritativeRows);

        Assert.False(await cluster.Coordinator.ReapShardAsync(
            new ShardKey(BlockA), TestContext.Current.CancellationToken));

        Assert.True(Directory.Exists(directory));
        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));
        Assert.NotNull(owner.Runtime.TryGetShard(new ShardKey(BlockA))!
            .Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)));
    }

    /// <summary>
    /// The other half of that: counting freshly is not enough on its own, because any count is
    /// stale the moment the lock is released. A reducer call resolves its shard under the node's
    /// shard-set lock and commits with it released, so between "no rows" and the delete an
    /// acknowledged write can land and be destroyed with the directory. The count and the refusal
    /// of further writes are therefore one decision in one hold of the engine write lock — driven
    /// directly here, because a test cannot reliably schedule itself into that window.
    /// </summary>
    [Fact]
    public async Task Sealing_an_empty_shard_refuses_the_writes_that_would_have_been_destroyed()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var shard = owner.Runtime.TryGetShard(new ShardKey(BlockA))!;

        Assert.Equal(0, shard.SealIfEmpty());

        var refused = Assert.ThrowsAny<Exception>(
            () => shard.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(0, 0)));
        Assert.Contains("reaped", refused.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Null(shard.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)));

        // A seal is not a close. A reap that goes on to refuse must leave the shard as it found it.
        shard.Unseal();
        shard.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(0, 0));
        Assert.NotNull(shard.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)));

        // And the shard is no longer empty: the next attempt reads the row rather than the
        // decision that preceded it.
        Assert.Equal(1, shard.SealIfEmpty());
    }

    /// <summary>
    /// The lost-reply window, which the old ordering could not survive. The owner used to delete
    /// the directory and then reply, with the hub removing the membership row last; a reply lost
    /// there left a row naming a shard whose data was gone, and <c>ApplyAssignments</c> would open
    /// a fresh empty engine under the very originator the reap retired — re-minting ids that
    /// transfers have already carried to other shards.
    ///
    /// <para>The fix inverts the order, so what is asserted here is the invariant that inversion
    /// buys: after the owner's half, nothing is destroyed. The shard is closed and the membership
    /// row is untouched, which is precisely what "the reap did not happen" has to mean.</para>
    /// </summary>
    [Fact]
    public async Task Nothing_is_destroyed_until_the_membership_row_is_gone()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var before = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA))!;
        var directory = owner.Runtime.TryGetShard(new ShardKey(BlockA))!.Directory;

        // The owner's half alone — as if the hub never received the reply.
        Assert.True(owner.Runtime.ReapShard(new ShardReap(BlockA, before.FencingToken)).Reaped);

        Assert.True(Directory.Exists(directory));
        Assert.Null(owner.Runtime.TryGetShard(new ShardKey(BlockA)));
        var after = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA));
        Assert.NotNull(after);
        Assert.Equal(before.Originator, after!.Originator);

        // The mark holds the door shut meanwhile: heartbeats keep arriving carrying an assignment
        // that still names this node, and none of them may reopen the shard under the hub.
        await Task.Delay(500, TestContext.Current.CancellationToken);
        Assert.Null(owner.Runtime.TryGetShard(new ShardKey(BlockA)));

        // Repeating the owner's half is answered rather than redone — the hub retries after a lost
        // reply, and there is nothing left here to inspect or refuse.
        Assert.True(owner.Runtime.ReapShard(new ShardReap(BlockA, before.FencingToken)).Reaped);
        Assert.True(Directory.Exists(directory));

        // Only the second half destroys anything, and the hub sends it only once the row is gone.
        cluster.Hub.Membership.RemoveShard(new ShardKey(BlockA));
        Assert.True(owner.Runtime.DeleteReapedShard(new ShardReapDelete(BlockA)).Reaped);
        await ClusterFixture.WaitUntilAsync(
            () => !Directory.Exists(directory),
            "the shard's directory went away (Windows can hold a deleted directory visible briefly)");
    }

    /// <summary>
    /// A drain and a reap both decide where the shard ends up, so they cannot overlap. The mark is
    /// read before the shard is resolved, so a quiesced shard answers "mid-drain" rather than the
    /// "this node does not own it" that a closed engine would otherwise produce.
    /// </summary>
    [Fact]
    public async Task A_shard_mid_drain_is_refused()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var owner = await cluster.EnsureShardOwnedAsync(BlockA);
        var assignment = cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA))!;
        var directory = owner.Runtime.TryGetShard(new ShardKey(BlockA))!.Directory;

        owner.Runtime.QuiesceShard(new ShardDrain(BlockA, assignment.FencingToken));

        var refusal = owner.Runtime.ReapShard(new ShardReap(BlockA, assignment.FencingToken));
        Assert.False(refusal.Reaped);
        Assert.Contains("mid-drain", refusal.Refusal!, StringComparison.Ordinal);
        Assert.True(Directory.Exists(directory));
        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));
    }

    /// <summary>
    /// A reaped shard never reports again, and freshness only governs whether a sample is believed
    /// — <c>Snapshot()</c> and the observable gauges publish the last one regardless of its age.
    /// Left alone, a deployment that reaps a world a day accumulates one dead series a day.
    /// </summary>
    [Fact]
    public async Task A_reaped_shard_leaves_the_cluster_load_view()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        await cluster.EnsureShardOwnedAsync(BlockA);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Hub.Load.Snapshot().Any(l => l.Shard.Value == BlockA),
            "the hub sampled the shard at least once");

        Assert.True(await cluster.Coordinator.ReapShardAsync(
            new ShardKey(BlockA), TestContext.Current.CancellationToken));

        Assert.DoesNotContain(cluster.Hub.Load.Snapshot(), l => l.Shard.Value == BlockA);
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

        Assert.True(await cluster.Coordinator.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));

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
        Assert.False(await cluster.Coordinator.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));
        Assert.True(Directory.Exists(directory));
        Assert.NotNull(cluster.Hub.Membership.GetAssignment(new ShardKey(BlockA)));

        // Released, the same shard reaps — the refusal was about the pin and nothing else.
        pin.Dispose();
        Assert.True(await cluster.Coordinator.ReapShardAsync(new ShardKey(BlockA), TestContext.Current.CancellationToken));
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
            () => cluster.Coordinator.ReapShardAsync(new ShardKey(BlockB), TestContext.Current.CancellationToken));
    }
}
