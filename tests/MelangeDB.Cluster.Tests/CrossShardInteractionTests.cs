using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The cross-shard interaction ladder, in the settled priority order: co-locate and transact
/// locally (the norm spatial locality buys — asserted as zero per-transaction cross-node
/// messages); ownership transfer for entities that cross (SeamlessMigrationTests); and the saga
/// over the event bus for the rare genuine remote case — eventually consistent, compensated,
/// explicitly not ACID.
/// </summary>
public class CrossShardInteractionTests
{
    private static readonly ulong BlockA = SpatialShardStrategy.ShardOfBlock(0, 0).Value;
    private static readonly ulong BlockD = SpatialShardStrategy.ShardOfBlock(1, 1).Value; // Chunks 4..7 x 4..7.

    [Fact]
    public async Task Co_located_players_trade_in_one_local_transaction_with_zero_per_transaction_cross_node_messages()
    {
        // Band 1 (margin 0) so a 4x4 block has genuinely interior chunks; the traders stand on one.
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 200, failureTimeoutMs: 60_000, spatial: true,
            extraSettings: new Dictionary<string, string?>
            {
                ["MelangeDb:Cluster:BorderBandChunks"] = "1",
                ["MelangeDb:Cluster:HandoffMarginChunks"] = "0",
            });
        var alice = TestTokens.IdentityOf("alice");
        var bob = TestTokens.IdentityOf("bob");
        var shard = (await cluster.EnsureShardOwnedAsync(BlockD)).Runtime.TryGetShard(new ShardKey(BlockD))!;
        shard.ReducerHost.Call("Move", alice, Chunks.Id(6, 6));
        shard.ReducerHost.Call("Move", bob, Chunks.Id(6, 6));
        shard.ReducerHost.Call("EarnGold", alice, 100);
        await cluster.QuiesceAsync();

        // Steady-state control traffic (border subscribe refreshes) is not per-transaction work;
        // the assertion is that the trade itself generates no cross-node messages at all.
        long ControlFree() => cluster.TotalLinkMessages()
            - Sum("border-subscribe") - Sum("border-subscribe-ok")
            - Sum("border-subscribe-owner") - Sum("border-subscribe-owner-ok");
        long Sum(string type) => cluster.Hub.Metrics.SentByType.GetValueOrDefault(type)
            + cluster.Nodes.Where(static n => n.App is not null).Sum(n => n.Metrics.SentByType.GetValueOrDefault(type));

        var before = ControlFree();
        shard.ReducerHost.Call("TradeGold", alice, bob.ToString(), 40);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        Assert.Equal(before, ControlFree());
        Assert.Equal(60, shard.Engine.CommittedView.Find<Pack>(alice)!.Value.Gold);
        Assert.Equal(40, shard.Engine.CommittedView.Find<Pack>(bob)!.Value.Gold);
    }

    [Fact]
    public async Task A_gift_across_distant_shards_runs_as_a_saga_over_the_event_bus_and_delivers()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var sender = TestTokens.IdentityOf("gifter");
        var recipient = TestTokens.IdentityOf("giftee");
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        var shardD = (await cluster.EnsureShardOwnedAsync(BlockD)).Runtime.TryGetShard(new ShardKey(BlockD))!;
        shardA.ReducerHost.Call("Move", sender, Chunks.Id(1, 1));
        shardA.ReducerHost.Call("EarnGold", sender, 100);
        shardD.ReducerHost.Call("Move", recipient, Chunks.Id(6, 6));

        shardA.ReducerHost.Call("SendGift", sender, recipient.ToString(), BlockD, BlockA, 30);

        // The debit is immediate and local; the credit is eventually consistent — the honest
        // shape of a cross-shard saga, and the reason co-location is the first choice.
        Assert.Equal(70, shardA.Engine.CommittedView.Find<Pack>(sender)!.Value.Gold);
        await ClusterFixture.WaitUntilAsync(
            () => shardD.Engine.CommittedView.Find<Pack>(recipient)?.Gold == 30,
            "the saga credited the recipient on their shard");
        await ClusterFixture.WaitUntilAsync(
            () => cluster.HubReceipts.Items.Contains("Gift:delivered:30"),
            "the hub-side saga handler recorded the delivery");
    }

    [Fact]
    public async Task A_failed_credit_is_compensated_by_refunding_the_sender_not_papered_over()
    {
        await using var cluster = await ClusterFixture.StartAsync(
            shardNodes: 2, heartbeatMs: 150, failureTimeoutMs: 60_000, spatial: true);
        var sender = TestTokens.IdentityOf("hopeful-gifter");
        var ghost = TestTokens.IdentityOf("nobody-home");
        var shardA = (await cluster.EnsureShardOwnedAsync(BlockA)).Runtime.TryGetShard(new ShardKey(BlockA))!;
        await cluster.EnsureShardOwnedAsync(BlockD);
        shardA.ReducerHost.Call("Move", sender, Chunks.Id(1, 1));
        shardA.ReducerHost.Call("EarnGold", sender, 100);

        // The recipient has no rows on the target shard: the credit step rejects definitively,
        // and the compensating refund reverses the debit. Between debit and refund the gold is
        // simply gone from view — eventually consistent, explicitly not ACID, documented as such.
        shardA.ReducerHost.Call("SendGift", sender, ghost.ToString(), BlockD, BlockA, 30);
        await ClusterFixture.WaitUntilAsync(
            () => cluster.HubReceipts.Items.Contains("Gift:refunded:30"),
            "the saga compensated the failed credit");
        await ClusterFixture.WaitUntilAsync(
            () => shardA.Engine.CommittedView.Find<Pack>(sender)?.Gold == 100,
            "the sender's balance was restored by the refund");
    }
}
