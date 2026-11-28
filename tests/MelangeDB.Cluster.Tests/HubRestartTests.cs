using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// A whole cluster stopped and started again on its kept data (#314). The hub must not re-mint a
/// fencing token below a term that already wrote a shard's log, or every node refuses the shard,
/// and must not hand a shard another shard's originator, or the two mint the same ids.
/// </summary>
public sealed class HubRestartTests
{
    [Fact]
    public async Task A_moved_shard_reopens_after_a_restart_that_forgot_its_term()
    {
        var root = Directory.CreateTempSubdirectory("melange-restart-").FullName;
        static void InMemory(IServiceCollection services) =>
            services.AddSingleton<IMembershipStore, InMemoryMembershipStore>();

        await using (var first = await ClusterFixture.StartAsync(shardNodes: 2, dataRoot: root, configureHub: InMemory))
        {
            first.KeepData = true;
            var origin = await first.EnsureShardOwnedAsync(90);
            await first.Coordinator.ExecuteOnShardAsync(new ShardKey(90), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [90u, 1], TestContext.Current.CancellationToken);
            await first.Coordinator.DrainShardAsync(new ShardKey(90), ct: TestContext.Current.CancellationToken);
            await ClusterFixture.WaitUntilAsync(
                () => first.Nodes.Single(n => n.Name != origin.Name).Runtime.TryGetShard(new ShardKey(90)) is not null,
                "shard 90 opened on its new owner");
            Assert.Equal(2, first.Hub.Membership.GetAssignment(new ShardKey(90))!.FencingToken);
            await first.Coordinator.ExecuteOnShardAsync(new ShardKey(90), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [90u, 2], TestContext.Current.CancellationToken);
        }

        // The in-memory store starts over at term 1, below the log's term 2. The owner's refusal
        // reaches the hub, which raises the token past it, and the shard opens with no manual step.
        await using var second = await ClusterFixture.StartAsync(shardNodes: 2, dataRoot: root, configureHub: InMemory);
        var owner = await second.EnsureShardOwnedAsync(90);
        Assert.Equal(3, second.Hub.Membership.GetAssignment(new ShardKey(90))!.FencingToken);
        var engine = owner.Runtime.TryGetShard(new ShardKey(90))!.Engine;
        Assert.Equal(2, engine.CommittedView.Count<Mob>());
        await second.Coordinator.ExecuteOnShardAsync(new ShardKey(90), nameof(ClusterReducers.SpawnMob), ClusterFixture.Caller, [90u, 3], TestContext.Current.CancellationToken);
        Assert.Equal(3, engine.CommittedView.Count<Mob>());
    }

    [Fact]
    public async Task The_default_store_remembers_terms_and_originators_across_a_restart()
    {
        var root = Directory.CreateTempSubdirectory("melange-restart-").FullName;
        ShardAssignment moved;
        ShardAssignment stayed;
        await using (var first = await ClusterFixture.StartAsync(shardNodes: 2, dataRoot: root))
        {
            first.KeepData = true;
            Assert.IsType<FileMembershipStore>(first.Hub.Membership);
            await first.EnsureShardOwnedAsync(91);
            await first.EnsureShardOwnedAsync(92);
            await first.Coordinator.DrainShardAsync(new ShardKey(91), ct: TestContext.Current.CancellationToken);
            moved = first.Hub.Membership.GetAssignment(new ShardKey(91))!;
            stayed = first.Hub.Membership.GetAssignment(new ShardKey(92))!;
            Assert.Equal(2, moved.FencingToken);
        }

        // Asked for in the other order: an in-memory store would swap the two originators.
        await using var second = await ClusterFixture.StartAsync(shardNodes: 2, dataRoot: root);
        await second.EnsureShardOwnedAsync(92);
        await second.EnsureShardOwnedAsync(91);
        var movedAfter = second.Hub.Membership.GetAssignment(new ShardKey(91))!;
        var stayedAfter = second.Hub.Membership.GetAssignment(new ShardKey(92))!;
        Assert.Equal(moved.Originator, movedAfter.Originator);
        Assert.Equal(stayed.Originator, stayedAfter.Originator);
        Assert.Equal(moved.FencingToken, movedAfter.FencingToken);
    }
}
