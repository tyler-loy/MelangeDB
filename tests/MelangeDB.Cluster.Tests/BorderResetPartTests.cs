using System.Net;
using System.Net.Sockets;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// A border reset grows with the owner's band: a million-row shard's band against a side
/// neighbour measured 152-170 MiB, over the node link's 64 MiB frame limit, and the receiving
/// reader dropped the link (taking the node's lease, and with fast failover its shards, with it).
/// Resets now travel in parts the observer stages and applies whole, and a link refuses to send a
/// frame its peer would refuse to read.
/// </summary>
public class BorderResetPartTests
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value; // Chunks x 0..3.
    private static readonly ulong BlockB = SpatialShardStrategy.ShardOfBlock(1, 0).Value; // Chunks x 4..7.

    private static WireOp Op(int keyBytes, int rowBytes) => new((byte)RowOpKind.Insert, 1, new byte[keyBytes], new byte[rowBytes]);

    [Fact]
    public void A_reset_splits_into_parts_under_the_budget_with_every_row_once_in_order()
    {
        var tables = new[]
        {
            new ReplicaTableSnapshot(1, [.. Enumerable.Range(0, 50).Select(i => Op(8, 100 + i))]),
            new ReplicaTableSnapshot(2, []),
            new ReplicaTableSnapshot(3, [.. Enumerable.Range(0, 30).Select(_ => Op(8, 300))]),
        };

        var parts = BorderPublisher.SplitReset(tables, partBytes: 2_000);
        Assert.True(parts.Count > 5);
        foreach (var part in parts)
            Assert.True(part.Sum(t => t.Rows.Sum(r => ((r.Key.Length + r.Row!.Length) * 4L / 3) + 64)) <= 2_000);

        // Concatenated, the parts are the reset: same tables, same rows, same order.
        var flattened = parts.SelectMany(p => p).SelectMany(t => t.Rows.Select(r => (t.Table, r))).ToList();
        var original = tables.SelectMany(t => t.Rows.Select(r => (t.Table, r))).ToList();
        Assert.Equal(original.Count, flattened.Count);
        Assert.True(original.Zip(flattened).All(pair => pair.First.Table == pair.Second.Table && ReferenceEquals(pair.First.r, pair.Second.r)));

        // An empty band is still one part (it carries the deletions and the cursor), and a row
        // larger than the budget travels alone rather than being refused.
        Assert.Single(BorderPublisher.SplitReset([new ReplicaTableSnapshot(1, [])], 2_000));
        var oversized = BorderPublisher.SplitReset([new ReplicaTableSnapshot(1, [Op(8, 10), Op(8, 50_000), Op(8, 10)])], 2_000);
        Assert.Equal([1, 1, 1], oversized.Select(p => p.Sum(t => t.Rows.Length)));
    }

    [Fact]
    public async Task A_reset_in_many_parts_converges_the_observer_deletions_included()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        foreach (var node in cluster.Nodes)
            node.Runtime.BorderResetPartBytes = 1; // One row per part.
        var ownerA = await cluster.EnsureShardOwnedAsync(BlockA);
        var ownerB = await cluster.EnsureShardOwnedAsync(BlockB);
        Assert.NotEqual(ownerA.Name, ownerB.Name);
        var shardA = ownerA.Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardB = ownerB.Runtime.TryGetShard(new ShardKey(BlockB))!;

        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 1UL, Chunks.Id(4, 1));
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is not null,
            "the first border copy arrived");

        // The observer goes dark; the owner deletes critter 1, spawns five more in the band, and
        // truncates past everything the observer saw, so only a reset can bring it back.
        await cluster.StopNodeAsync(ownerA.Name);
        shardB.ReducerHost.Call("DespawnCritter", ClusterFixture.Caller, 1UL);
        for (var seed = 2UL; seed <= 6; seed++)
            shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, seed, Chunks.Id(4, (int)(seed % 4)));
        var headBefore = shardB.Engine.Log.HeadLsn;
        await ClusterFixture.WaitUntilAsync(
            () =>
            {
                shardB.Engine.TakeSnapshot();
                return shardB.Engine.Log.BaseLsn >= headBefore;
            },
            "the owner truncated past the observer's border cursor");

        await cluster.StartNodeAsync(ownerA.Name);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.Node(ownerA.Name).Runtime.TryGetShard(new ShardKey(BlockA)) is { } reopened
                && Enumerable.Range(2, 5).All(seed => reopened.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId((ulong)seed)) is not null)
                && reopened.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(1)) is null,
            "the observer converged from a reset in parts, deletion included");
        Assert.True(
            cluster.Node(ownerA.Name).Metrics.ReceivedByType.GetValueOrDefault("border-reset-part-apply") >= 5,
            "the reset must have arrived in parts");
    }

    [Fact]
    public async Task Nothing_applies_before_the_final_part_and_a_part_out_of_sequence_is_refused()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var ownerA = await cluster.EnsureShardOwnedAsync(BlockA);
        await cluster.EnsureShardOwnedAsync(BlockB);
        var shardA = ownerA.Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardB = cluster.Nodes.Select(n => n.Runtime.TryGetShard(new ShardKey(BlockB))).Single(s => s is not null)!;
        shardB.ReducerHost.Call("SpawnCritter", ClusterFixture.Caller, 7UL, Chunks.Id(4, 1));
        var table = shardB.Engine.Schema.Get(typeof(Critter));
        var (key, row) = shardB.Engine.HotStore.Scan(table.Id).Single();
        var critter = new ReplicaTableSnapshot(table.Id.Value, [new WireOp((byte)RowOpKind.Insert, table.Id.Value, key.ToArray(), row.ToArray())]);
        BorderReset Reset(ReplicaTableSnapshot[] tables) =>
            new(BlockB, BlockA, shardB.Engine.Log.EpochId.ToString(), shardB.Engine.Log.HeadLsn, tables);
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(7)) is not null,
            "the ordinary border stream delivered the critter");

        // A reset whose first part is empty would, applied alone, delete the borrowed critter.
        shardA.ApplyBorderResetPart(new BorderResetPart("r1", 0, Final: false, Reset([])));
        Assert.NotNull(shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(7)));

        // Skipping part 1 is refused and discards the staging...
        Assert.Throws<InvalidOperationException>(() => shardA.ApplyBorderResetPart(new BorderResetPart("r1", 2, Final: true, Reset([critter]))));

        // ...so the rest of that reset can never complete it, nor can another reset's part.
        Assert.Throws<InvalidOperationException>(() => shardA.ApplyBorderResetPart(new BorderResetPart("r1", 1, Final: true, Reset([critter]))));
        Assert.Throws<InvalidOperationException>(() => shardA.ApplyBorderResetPart(new BorderResetPart("r2", 1, Final: true, Reset([critter]))));
        Assert.NotNull(shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(7)));

        // A whole reset in two parts applies on the final one.
        shardA.ApplyBorderResetPart(new BorderResetPart("r3", 0, Final: false, Reset([])));
        shardA.ApplyBorderResetPart(new BorderResetPart("r3", 1, Final: true, Reset([critter])));
        Assert.NotNull(shardA.Engine.CommittedView.Find<Critter>(SpatialReducers.CritterId(7)));
        Assert.Equal(1, shardA.BorrowedRowCount);
    }

    [Fact]
    public async Task A_link_refuses_to_send_a_frame_its_peer_would_drop_and_stays_up()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, TestContext.Current.CancellationToken);
        using var accepted = await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken);
        listener.Stop();

        using var sender = new NodeLink(client, new ClusterMetrics());
        using var receiver = new NodeLink(accepted, new ClusterMetrics());
        receiver.Handler = (_, _, body) => Task.FromResult<object?>(body);
        sender.Start();
        receiver.Start();

        // Base64 makes 49 MiB of bytes a frame over 64 MiB.
        var oversized = new byte[49 * 1024 * 1024];
        var refused = await Assert.ThrowsAsync<NodeLinkException>(() => sender.RequestAsync("echo", oversized, TestContext.Current.CancellationToken));
        Assert.Contains("exceeds the node link's", refused.Message);

        var echoed = await sender.RequestAsync("echo", new[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
        Assert.Equal(3, echoed!.Value.GetArrayLength());
        Assert.True(sender.IsAlive && receiver.IsAlive);
    }
}
