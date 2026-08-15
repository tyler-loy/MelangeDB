using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

public sealed class LoadSignalTests
{
    /// <summary>
    /// The phase 13 load signal end to end: shard nodes sample each owned engine's write-lock
    /// busy fraction per heartbeat, the samples ride the heartbeat body, and the hub's load view
    /// reports every shard under its current owner. Utilization is asserted nonzero under a
    /// sustained pump — commits are flowing, so the busy fraction of some interval must be
    /// positive — and never above one, because it is a fraction of wall time by construction.
    /// </summary>
    [Fact]
    public async Task Heartbeats_carry_per_shard_load_into_the_hubs_load_view()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 2);
        var owner = await fixture.EnsureShardOwnedAsync(7);

        using var pump = new CancellationTokenSource();
        var pumping = Task.Run(async () =>
        {
            while (!pump.IsCancellationRequested)
            {
                await fixture.Coordinator.ExecuteOnShardAsync(
                    new ShardKey(7), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [7u, 10], pump.Token);
            }
        }, TestContext.Current.CancellationToken);

        try
        {
            await ClusterFixture.WaitUntilAsync(
                () => fixture.Coordinator.LoadView() is var view
                    && view.SingleOrDefault(static load => load.Shard.Value == 7) is { } load
                    && load.NodeName == owner.Name
                    && load.HeadLsn > 0
                    && load.Utilization is > 0 and <= 1,
                "the load view reports shard 7 under its owner with nonzero utilization");
        }
        finally
        {
            pump.Cancel();
            try
            {
                await pumping;
            }
            catch (Exception)
            {
                // The pump was cancelled mid-call; its last request may have died with the fixture.
            }
        }
    }

    /// <summary>
    /// An idle shard's signal decays back toward zero once the pump stops — the view reflects
    /// current load, not a high-water mark. Asserted loosely (below half) rather than at zero,
    /// because heartbeats and background sweeps keep the lock mildly warm.
    /// </summary>
    [Fact]
    public async Task An_idle_shards_utilization_decays()
    {
        await using var fixture = await ClusterFixture.StartAsync(shardNodes: 1);
        await fixture.EnsureShardOwnedAsync(3);
        for (var i = 0; i < 25; i++)
        {
            await fixture.Coordinator.ExecuteOnShardAsync(
                new ShardKey(3), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [3u, 10],
                TestContext.Current.CancellationToken);
        }

        await ClusterFixture.WaitUntilAsync(
            () => fixture.Coordinator.LoadView().SingleOrDefault(static load => load.Shard.Value == 3) is { } load
                && load.Utilization < 0.5,
            "the idle shard's utilization decays");
    }
}
