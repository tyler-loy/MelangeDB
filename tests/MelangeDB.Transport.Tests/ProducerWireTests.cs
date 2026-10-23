using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Producers on the wire (road-to-0.3 phase 26): the <c>CallProducer</c>/<c>ProducerResult</c>
/// frames of protocol version 3, the version negotiation that keeps version-2 clients working, the
/// per-connection concurrency cap, cancellation on disconnect, and the HTTP form.
/// </summary>
public class ProducerWireTests
{
    [Fact]
    public async Task A_client_calls_a_producer_over_the_socket_and_decodes_the_result()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 10L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 20L, new byte[] { 2 });
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, client.ProtocolVersion);

        var reply = await client.CallProducerAsync("Chunks", [15L], TestContext.Current.CancellationToken);

        var reader = new ProducerResultReader(reply.Payload.Span);
        Assert.Equal(1, reader.BeginArray());
        Assert.Equal(3, reader.BeginArray());
        Assert.Equal(2L, reader.ReadInt64());
        Assert.Equal(20L, reader.ReadInt64());
        Assert.Equal(TransportTestHost.Caller, reader.ReadIdentity());
        reader.End();
        Assert.Equal(host.Engine.HotStore.AppliedLsn, reply.Lsn);
    }

    [Fact]
    public async Task Failures_carry_their_codes()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var unknown = await Assert.ThrowsAsync<MelangeCallException>(() => client.CallProducerAsync("Nope", null, TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.UnknownProducer, unknown.Code);
        var invalid = await Assert.ThrowsAsync<MelangeCallException>(() => client.CallProducerAsync("Chunks", ["text"], TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.InvalidArguments, invalid.Code);
    }

    [Fact]
    public async Task A_call_past_the_timeout_answers_timeout()
    {
        await using var host = await TransportTestHost.StartAsync(new() { ["MelangeDb:Producers:TimeoutSeconds"] = "0.2" });
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var timeout = await Assert.ThrowsAsync<MelangeCallException>(() => client.CallProducerAsync("Hang", null, TestContext.Current.CancellationToken));
        Assert.Equal(MelangeErrorCodes.Timeout, timeout.Code);
    }

    [Fact]
    public async Task A_version_2_client_is_still_served_and_may_not_send_a_producer_call()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        var welcome = await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken, maxVersion: 2);
        Assert.Equal(2, welcome.Version);

        await raw.SendAsync(new CallProducerFrame(1, "Chunks", ReducerArgs.Encode(), null) { Channel = MelangeChannels.Calls }, TestContext.Current.CancellationToken);
        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.Protocol, error.Code);
    }

    [Fact]
    public async Task A_connection_holds_at_most_its_cap_of_calls_and_a_disconnect_cancels_them()
    {
        await using var host = await TransportTestHost.StartAsync(new() { ["MelangeDb:Producers:MaxConcurrentPerConnection"] = "1" });
        var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        await raw.SendAsync(new CallProducerFrame(1, "Hang", ReducerArgs.Encode(), null) { Channel = MelangeChannels.Calls }, TestContext.Current.CancellationToken);
        await host.ProducerProbe.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await raw.SendAsync(new CallProducerFrame(2, "Chunks", ReducerArgs.Encode(), null) { Channel = MelangeChannels.Calls }, TestContext.Current.CancellationToken);
        var refused = await raw.ReceiveUntilAsync<ProducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(2u, refused.RequestId);
        Assert.Equal(MelangeErrorCodes.RateLimited, refused.ErrorCode);

        await raw.DisposeAsync();
        await host.ProducerProbe.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Over_HTTP_a_producer_answers_json_named_as_the_manifest_names_it()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 5L, 50L, new byte[] { 5 });
        using var http = host.CreateHttp();

        var response = await http.PostAsync("/melange/producers/Chunks", Json("[]"), TestContext.Current.CancellationToken);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);

        Assert.True(response.IsSuccessStatusCode, body.ToString());
        Assert.True(body.GetProperty("ok").GetBoolean());
        var chunk = Assert.Single(body.GetProperty("result").EnumerateArray());
        Assert.Equal(5, chunk.GetProperty("Id").GetInt64());
        Assert.Equal(50, chunk.GetProperty("X").GetInt64());
        Assert.Equal(TransportTestHost.Caller.ToString(), chunk.GetProperty("AskedBy").GetString());

        var unknown = await http.PostAsync("/melange/producers/Nope", Json("[]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, unknown.StatusCode);
        Assert.Equal(MelangeErrorCodes.UnknownProducer, (await unknown.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken)).GetProperty("error").GetString());
    }

    private static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");
}
