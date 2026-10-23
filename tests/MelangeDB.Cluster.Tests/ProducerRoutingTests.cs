using MelangeDB.Core;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Producers through the gateway (road-to-0.3 phase 26): it speaks protocol version 3, and each
/// call runs where the reducer rules put it — a Global read on the hub, a Partitioned read on the
/// caller's shard — through the one socket, with nothing in any frame naming a node.
/// </summary>
[Collection("cluster")]
public class ProducerRoutingTests
{
    private readonly ClusterFixture _cluster;

    public ProducerRoutingTests(SharedClusterFixture shared) => _cluster = shared.Cluster;

    [Fact]
    public void Execution_sites_follow_the_tables_each_producer_reads()
    {
        var producers = new ProducerRegistry(new MelangeDB.Generated.MelangeModel().Producers());

        Assert.True(producers.TryGet("GlobalValue", out var global));
        Assert.Equal(ReducerSite.Hub, global.ExecutionSite);
        Assert.True(producers.TryGet("MobHps", out var partitioned));
        Assert.Equal(ReducerSite.Shard, partitioned.ExecutionSite);
    }

    [Fact]
    public async Task A_client_calls_a_hub_producer_and_a_shard_producer_through_the_gateway()
    {
        await _cluster.EnsureShardOwnedAsync(21);
        await using var client = _cluster.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, client.ProtocolVersion);

        await client.CallReducerAsync("SetLocation", [21u], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SpawnMob", [21u, 77], TestContext.Current.CancellationToken);
        await client.CallReducerAsync("SpawnMob", [21u, 33], TestContext.Current.CancellationToken);
        var hps = await client.CallProducerAsync("MobHps", [21u], TestContext.Current.CancellationToken);
        var reader = new ProducerResultReader(hps.Payload.Span);
        Assert.Equal(2, reader.BeginArray());
        Assert.Equal(33, reader.ReadInt32());
        Assert.Equal(77, reader.ReadInt32());
        reader.End();

        await client.CallReducerAsync("BumpGlobal", [3L], TestContext.Current.CancellationToken);
        var expected = _cluster.HubEngine.CommittedView.Find<GlobalCounter>(1L)!.Value.Value;
        var global = await client.CallProducerAsync("GlobalValue", null, TestContext.Current.CancellationToken);
        Assert.Equal(expected, new ProducerResultReader(global.Payload.Span).ReadInt64());
    }

    [Fact]
    public async Task An_unknown_producer_is_answered_by_the_hub_in_its_own_words()
    {
        await using var client = _cluster.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var failure = await Assert.ThrowsAsync<MelangeDB.Client.MelangeCallException>(() =>
            client.CallProducerAsync("NoSuchProducer", null, TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnknownProducer, failure.Code);
    }
}
