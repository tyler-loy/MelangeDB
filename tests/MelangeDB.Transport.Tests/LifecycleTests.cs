using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Lifecycle reducers: a session beginning is distinct from a query being run. ClientConnected
/// fires on the websocket handshake — never on HTTP one-shots — and ClientDisconnected fires on
/// graceful close and heartbeat-detected drops alike, each fire its own transaction.
/// </summary>
public class LifecycleTests
{
    [Fact]
    public async Task Handshake_fires_ClientConnected_and_graceful_close_fires_ClientDisconnected_each_as_its_own_transaction()
    {
        await using var host = await TransportTestHost.StartAsync();
        var events = host.SessionEvents;
        events.WriteRows = true;
        var head = host.Engine.Log.HeadLsn;

        await using (var client = host.CreateClient())
        {
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            await TransportTestHost.WaitUntilAsync(
                () => events.Count("connect", TransportTestHost.Caller) == 1,
                "ClientConnected to fire on handshake");
            Assert.Equal(0, events.Count("disconnect", TransportTestHost.Caller));
        }

        await TransportTestHost.WaitUntilAsync(
            () => events.Count("disconnect", TransportTestHost.Caller) == 1,
            "ClientDisconnected to fire on graceful close");

        // Two separate transactions, one commit record each — never a shared one.
        var records = host.Engine.Log.ReadFrom(head + 1).ToList();
        Assert.Equal(2, records.Count);
        Assert.Equal("OnConnect", records[0].ReducerName);
        Assert.Equal("OnDisconnect", records[1].ReducerName);
        Assert.All(records, record => Assert.Single(record.WriteSet));
    }

    [Fact]
    public async Task An_ungraceful_drop_fires_ClientDisconnected_through_the_heartbeat_timeout()
    {
        await using var host = await TransportTestHost.StartAsync(manualTime: true);
        var events = host.SessionEvents;

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(
            () => events.Count("connect", TransportTestHost.Caller) == 1,
            "ClientConnected to fire on handshake");

        // The client goes silent — no Pong, no close frame. Only the heartbeat can notice.
        var reader = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    await raw.ReceiveAsync(TestContext.Current.CancellationToken);
                }
            }
            catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
            {
            }
        }, TestContext.Current.CancellationToken);

        host.Time!.Advance(TimeSpan.FromSeconds(15));
        host.Time.Advance(TimeSpan.FromSeconds(15));
        Assert.Equal(0, events.Count("disconnect", TransportTestHost.Caller));

        host.Time.Advance(TimeSpan.FromSeconds(45));
        await TransportTestHost.WaitUntilAsync(
            () => events.Count("disconnect", TransportTestHost.Caller) == 1,
            "ClientDisconnected to fire on the heartbeat-detected drop");
        await reader;
    }

    [Fact]
    public async Task Http_one_shot_calls_sql_and_tickets_fire_no_lifecycle_reducers()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Sql:AdHocEnabled"] = "true",
        });
        var events = host.SessionEvents;
        using var http = host.CreateHttp();

        var call = await http.PostAsync("/melange/call/Noop", new StringContent("[]"), TestContext.Current.CancellationToken);
        Assert.True(call.IsSuccessStatusCode);
        var sql = await http.PostAsync(
            "/melange/sql",
            new StringContent("{\"query\": \"SELECT * FROM Chunk\"}"),
            TestContext.Current.CancellationToken);
        Assert.True(sql.IsSuccessStatusCode);
        var ticket = await http.PostAsync("/melange/ticket", new StringContent(string.Empty), TestContext.Current.CancellationToken);
        Assert.True(ticket.IsSuccessStatusCode);

        // An admin query is not a session: nothing fired, and no ghost rows can exist.
        Assert.Empty(events.Events);

        // Redeeming that ticket on a socket, though, is a session.
        var minted = System.Text.Json.JsonDocument.Parse(
            await ticket.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(
            new Uri(host.WsUri + "?ticket=" + minted.RootElement.GetProperty("ticket").GetString()),
            TestContext.Current.CancellationToken,
            token: null);
        await TransportTestHost.WaitUntilAsync(
            () => events.Count("connect", TransportTestHost.Caller) == 1,
            "ClientConnected to fire for the ticket-redeemed socket");
    }

    [Fact]
    public async Task A_socket_call_whose_body_throws_ArgumentException_is_a_failure_not_a_missing_reducer()
    {
        // Issue #98, on the socket path: its ArgumentException arm sat directly above the general
        // handler and swallowed every library-thrown argument fault from inside a reducer.
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        await raw.SendAsync(
            new CallReducerFrame(1, "ThrowArgumentFromBody", ReducerArgs.Encode([1u]), null), TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);

        Assert.False(result.Ok);
        Assert.Equal(MelangeErrorCodes.Internal, result.ErrorCode);
    }

    [Fact]
    public async Task A_client_calling_a_scheduled_reducer_is_told_unknown()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        await raw.SendAsync(new CallReducerFrame(1, "Respawn", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
        var result = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.False(result.Ok);
        Assert.Equal(MelangeErrorCodes.UnknownReducer, result.ErrorCode);

        // Byte-for-byte what a name that genuinely does not resolve answers: a difference of any
        // kind — including an exception's (Parameter '…') suffix — confirms this reducer exists.
        await raw.SendAsync(new CallReducerFrame(2, "NoSuchReducer", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
        var absent = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.UnknownReducer, absent.ErrorCode);
        Assert.Equal(
            absent.Message?.Replace("NoSuchReducer", "Respawn", StringComparison.Ordinal),
            result.Message);
        Assert.Equal("No reducer named 'Respawn' is registered.", result.Message);

        // Same answer over HTTP: the one-shot call path is a client origin too.
        using var http = host.CreateHttp();
        var response = await http.PostAsync("/melange/call/Respawn", new StringContent("[]"), TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task A_second_connection_of_the_same_identity_gets_its_own_lifecycle_pair()
    {
        await using var host = await TransportTestHost.StartAsync();
        var events = host.SessionEvents;

        await using var first = new RawSocketClient();
        var firstWelcome = await first.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await using var second = new RawSocketClient();
        var secondWelcome = await second.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        Assert.NotEqual(firstWelcome.ConnectionId, secondWelcome.ConnectionId);

        await TransportTestHost.WaitUntilAsync(
            () => events.Count("connect", TransportTestHost.Caller) == 2,
            "one ClientConnected per connection");

        first.Abort();
        await TransportTestHost.WaitUntilAsync(
            () => events.Count("disconnect", TransportTestHost.Caller) == 1,
            "the aborted connection's ClientDisconnected");
        Assert.Equal(2, events.Count("connect", TransportTestHost.Caller));

        // The ConnectionId distinguishes the pair: each disconnect matches its own connect.
        var connections = events.Events.Where(e => e.Kind == "connect").Select(e => e.Connection).ToList();
        var dropped = Assert.Single(events.Events, e => e.Kind == "disconnect").Connection;
        Assert.Contains(dropped, connections);
    }
}
