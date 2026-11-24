using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// A shard whose engine closed itself to writes (its hot store failed to apply a committed record,
/// or its commit log was poisoned) would refuse every write while its node, heartbeating normally,
/// kept the lease. The node closes it and reopens it on the next heartbeat, rebuilding its state
/// from the shard's log, as a restart would, scoped to the one shard.
/// </summary>
public sealed class ShardEngineFailureTests
{
    [Fact]
    public async Task A_shard_whose_engine_closed_itself_is_reopened_from_its_log()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 1);
        var key = new ShardKey(72);
        var node = await fixture.EnsureShardOwnedAsync(72);
        for (var i = 0; i < 5; i++)
        {
            await fixture.Coordinator.ExecuteOnShardAsync(
                key, nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [72u, i], TestContext.Current.CancellationToken);
        }

        var failed = node.Runtime.TryGetShard(key)!;
        failed.Engine.FailProjectionForTest(new IOException("injected: the hot store's disk failed"));

        await ClusterFixture.WaitUntilAsync(
            () => node.Runtime.TryGetShard(key) is { } current && !ReferenceEquals(current, failed) && current.Engine.ProjectionFailure is null,
            "the node reopened the failed shard");

        var reopened = node.Runtime.TryGetShard(key)!;
        Assert.Equal(5, reopened.Engine.CommittedView.Count<Mob>());
        await fixture.Coordinator.ExecuteOnShardAsync(
            key, nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [72u, 5], TestContext.Current.CancellationToken);
        Assert.Equal(6, node.Runtime.TryGetShard(key)!.Engine.CommittedView.Count<Mob>());
    }
}
