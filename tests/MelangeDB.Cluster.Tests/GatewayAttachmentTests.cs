using System.Collections.Concurrent;
using MelangeDB.Core;
using MelangeDB.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The gateway's attachment to shard nodes at its edges: shard keys across the whole ulong range,
/// and a handoff swap that cannot complete.
/// </summary>
public class GatewayAttachmentTests
{
    /// <summary>
    /// SpatialShardStrategy gives every block with a negative x coordinate a key with the top bit
    /// set. The shard endpoint's route was constrained to <c>long</c>, so those keys 404'd on every
    /// node: no player could be served west of x = 0, and the gateway's pre-opens to such a
    /// neighbour retried every 250 ms for the whole attach budget, enough connections under load to
    /// exhaust the ephemeral port range.
    /// </summary>
    [Fact]
    public async Task A_shard_whose_key_has_the_top_bit_set_is_served_by_its_node()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1);
        var shard = SpatialShardStrategy.ShardOfBlock(-1, 0);
        Assert.True(shard.Value > long.MaxValue);
        await cluster.EnsureShardOwnedAsync(shard.Value);

        var gateway = new GatewayRuntime(cluster.HubApp.Services, cluster.Hub);
        var (_, address) = cluster.Hub.ResolveShard(shard);
        var assertion = cluster.Hub.MintAssertion(
            ClusterFixture.Caller, isGuest: false, isSqlOwner: false, isBulkOwner: false,
            DateTimeOffset.UtcNow.AddMinutes(5), firesLifecycle: false);

        // Both upstream shapes: a socket of its own, and a session on the node's shared link.
        Func<CancellationToken, Task<System.Net.WebSockets.WebSocket>>[] openers =
        [
            UpstreamSession.Dial(gateway.ShardSocketUri(address, shard)),
            token => gateway.Links.OpenAsync(address, shard, token),
        ];
        foreach (var open in openers)
        {
            await using var session = await UpstreamSession.ConnectAsync(
                open,
                "the shard",
                assertion,
                gateway.Serializer,
                static (_, _) => Task.CompletedTask,
                static () => { },
                static () => { },
                static () => { },
                TestTime.Dilated(TimeSpan.FromSeconds(10)),
                TestContext.Current.CancellationToken);
            Assert.True(session.IsAlive);
        }

        await gateway.Links.DisposeAsync();
    }

    /// <summary>
    /// A swap whose destination never answers used to leave the connection holding calls: nothing
    /// else ends a transfer for the gateway, so every later call was held, then refused at the
    /// 256-call cap, for as long as the client stayed connected. Now the held calls go to the
    /// session's current owner and the client is told to re-establish its subscriptions.
    /// </summary>
    [Fact]
    public async Task A_swap_that_cannot_connect_releases_the_held_calls_instead_of_holding_every_call_after_it()
    {
        await using var cluster = await ClusterFixture.StartAsync(shardNodes: 1, heartbeatMs: 150, failureTimeoutMs: 1_000);
        var node = cluster.Nodes.Single();
        var unservable = new ShardKey(99);
        node.Runtime.BeforeOpen = shard =>
        {
            if (shard == unservable)
                throw new IOException("simulated: the destination never opens");
        };
        await cluster.EnsureShardOwnedAsync(1);

        await using var client = cluster.CreateClient(TestTokens.For("stranded"));
        var player = TestTokens.IdentityOf("stranded");
        var errors = new ConcurrentQueue<string>();
        client.OnError += error => errors.Enqueue(error.Code);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SetLocation", [1u], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SpawnMob", [1u, 5], TestContext.Current.CancellationToken);

        // A transfer to a shard whose node will never serve it: the saga reaches the swap, and the
        // swap's connect fails once the attach budget runs out.
        var from = new ShardKey(1);
        cluster.Hub.Handoffs.NotifyStarted(player, from, unservable);
        var held = client.CallReducerAsync("SpawnMob", [1u, 6], TestContext.Current.CancellationToken);
        cluster.Hub.Handoffs.NotifyDestinationAuthoritative(player, from, unservable);
        cluster.Hub.Handoffs.NotifyClosed(player, from, unservable, success: true);

        var deadline = TestTime.Dilated(TimeSpan.FromSeconds(30));
        await held.WaitAsync(deadline, TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SpawnMob", [1u, 7], TestContext.Current.CancellationToken)
            .WaitAsync(deadline, TestContext.Current.CancellationToken);

        Assert.Equal(3, cluster.ShardOf(1).Engine.CommittedView.Count<Mob>());
        await ClusterFixture.WaitUntilAsync(
            () => errors.Contains(MelangeErrorCodes.OverflowResync),
            "the client was told to re-establish its subscriptions");
    }
}
