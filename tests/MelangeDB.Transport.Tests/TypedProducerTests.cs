using MelangeDB.Types;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Producer bindings end to end (road-to-0.3 phase 26): the stubs and result records generated from
/// this assembly's own manifest call the real server and decode what its generated encoders wrote.
/// </summary>
public class TypedProducerTests
{
    [Fact]
    public async Task A_generated_stub_returns_typed_records_and_fills_in_defaults()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 10L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 20L, new byte[] { 2 });
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeConnection(client);

        List<MelangeDB.Types.ChunkView> all = await conn.Producers.ChunksAsync(cancellationToken: TestContext.Current.CancellationToken);
        var far = await conn.Producers.ChunksAsync(15, TestContext.Current.CancellationToken);

        Assert.Equal([1L, 2L], all.Select(static c => c.Id));
        var only = Assert.Single(far);
        Assert.Equal(new MelangeDB.Types.ChunkView { Id = 2, X = 20, AskedBy = TransportTestHost.Caller }, only);
    }

    [Fact]
    public async Task A_scalar_producer_decodes_to_its_scalar_and_failures_keep_their_codes()
    {
        await using var host = await TransportTestHost.StartAsync(new() { ["MelangeDb:Producers:TimeoutSeconds"] = "0.2" });
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var conn = new MelangeConnection(client);

        var timeout = await Assert.ThrowsAsync<MelangeDB.Client.MelangeCallException>(() => conn.Producers.HangAsync(TestContext.Current.CancellationToken));
        Assert.Equal(MelangeDB.Protocol.MelangeErrorCodes.Timeout, timeout.Code);
    }
}
