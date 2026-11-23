using MelangeDB.Core;
using MelangeDB.Server;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The hub's route for a client reducer call named by shard (road-to-0.4 phase 29's cluster half,
/// used by the MCP endpoint). The call must reach the shard's node as the client call it is: the
/// node applies the reducer's policy and the caller's claims, and a failure there comes back as
/// the exception it raised, not as a generic link error.
/// </summary>
public sealed class RoutedCallTests
{
    private const ulong Shard = 61;

    [Fact]
    public async Task A_routed_call_commits_on_the_shards_node()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        await cluster.EnsureShardOwnedAsync(Shard);
        var router = RouterOf(cluster);

        Assert.True(router.RoutesByShard);
        var lsn = await router.CallAsync(
            Shard, nameof(RoutedCallReducers.SpawnMobAsMember), ClusterFixture.Caller,
            ReducerArguments.Encode([61u]), CallSource.Client(), CallerClaims.Empty, TestContext.Current.CancellationToken);

        Assert.Equal(lsn, cluster.ShardOf(Shard).Engine.Log.HeadLsn);
        Assert.Single(cluster.ShardOf(Shard).Engine.HotStore.Scan(TableId.FromName("Mob")));
    }

    [Fact]
    public async Task The_callers_guest_flag_reaches_the_reducer_policy()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        await cluster.EnsureShardOwnedAsync(Shard);

        var denied = await Assert.ThrowsAsync<ReducerDeniedException>(() => RouterOf(cluster).CallAsync(
            Shard, nameof(RoutedCallReducers.SpawnMobAsMember), ClusterFixture.Caller,
            ReducerArguments.Encode([61u]), CallSource.Client(callerIsGuest: true), CallerClaims.Empty, TestContext.Current.CancellationToken));
        Assert.Contains(nameof(RoutedCallReducers.SpawnMobAsMember), denied.Message);
    }

    [Fact]
    public async Task The_callers_claims_reach_the_reducer()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        await cluster.EnsureShardOwnedAsync(Shard);
        var router = RouterOf(cluster);
        var arguments = ReducerArguments.Encode([61u, "tier"]);

        await router.CallAsync(
            Shard, nameof(RoutedCallReducers.SpawnMobWithClaim), ClusterFixture.Caller, arguments, CallSource.Client(),
            CallerClaims.From([new("tier", "gold")]), TestContext.Current.CancellationToken);

        var rejected = await Assert.ThrowsAsync<RejectedException>(() => router.CallAsync(
            Shard, nameof(RoutedCallReducers.SpawnMobWithClaim), ClusterFixture.Caller, arguments, CallSource.Client(),
            CallerClaims.Empty, TestContext.Current.CancellationToken));
        Assert.Contains("no 'tier' claim", rejected.Message);
    }

    [Fact]
    public async Task Failures_on_the_node_come_back_as_their_own_exceptions()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        await cluster.EnsureShardOwnedAsync(Shard);
        var router = RouterOf(cluster);

        await Assert.ThrowsAsync<UnknownReducerException>(() => router.CallAsync(
            Shard, "NoSuchReducer", ClusterFixture.Caller, ReducerArguments.Encode([]), CallSource.Client(),
            CallerClaims.Empty, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ReducerArgumentException>(() => router.CallAsync(
            Shard, nameof(RoutedCallReducers.SpawnMobAsMember), ClusterFixture.Caller, ReducerArguments.Encode(["not a number"]),
            CallSource.Client(), CallerClaims.Empty, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task A_call_naming_a_shard_that_does_not_exist_is_refused_without_creating_it()
    {
        await using var cluster = await ClusterFixture.StartAsync();
        var router = RouterOf(cluster);

        var refused = await Assert.ThrowsAsync<ArgumentException>(() => router.CallAsync(
            9_999, nameof(RoutedCallReducers.SpawnMobAsMember), ClusterFixture.Caller, ReducerArguments.Encode([1u]),
            CallSource.Client(), CallerClaims.Empty, TestContext.Current.CancellationToken));
        Assert.Equal("shard", refused.ParamName);
        Assert.Null(cluster.Hub.Membership.GetAssignment(new ShardKey(9_999)));
    }

    private static IShardCallRouter RouterOf(ClusterFixture cluster) =>
        cluster.HubApp.Services.GetRequiredService<IShardCallRouter>();
}
