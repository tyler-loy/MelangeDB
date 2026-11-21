using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// A shard that joins a node while that node's Replicated stream is already running. The stream
/// runs from the node's low-water mark, which is past the newcomer's cursor (zero), so a slice
/// arriving before the next re-subscribe carries records the newcomer is not ready for. Applying it
/// used to advance the newcomer's cursor over every earlier record, a permanent hole in its
/// Replicated tables. Slices now say what they follow (<see cref="ReplicaBatch.PrevLsn"/>), and a
/// shard that is behind waits for the re-subscribe that its own publication triggers.
/// </summary>
public class ReplicationGapTests
{
    [Fact]
    public async Task A_shard_opened_mid_stream_still_receives_every_earlier_replicated_record()
    {
        // A slow heartbeat holds the window open: the re-subscribe that a newly published shard
        // triggers waits for the next beat, and the write below lands inside that wait.
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 2_000, failureTimeoutMs: 20_000);
        var itemDef = cluster.HubEngine.Schema.Get(typeof(ItemDef));

        await cluster.EnsureShardOwnedAsync(1);
        cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 1L, "before");
        var first = cluster.ShardOf(1).Engine;
        await ClusterFixture.WaitUntilAsync(() => Rows(first, itemDef.Id).Count == 1, "the existing shard has the first record");

        await cluster.EnsureShardOwnedAsync(2);
        cluster.HubReducers.Call("UpsertItemDef", ClusterFixture.Caller, 2L, "after");

        var hubRows = Rows(cluster.HubEngine, itemDef.Id);
        Assert.Equal(2, hubRows.Count);
        var newcomer = cluster.ShardOf(2).Engine;
        await ClusterFixture.WaitUntilAsync(
            () => Rows(newcomer, itemDef.Id).SequenceEqual(hubRows),
            "the shard opened mid-stream converged on both records, the one before it joined included");
    }

    private static List<string> Rows(MelangeEngine engine, TableId table) =>
        engine.ReadConsistent(_ =>
            engine.HotStore.Scan(table).Select(static pair => $"{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}").ToList());
}
