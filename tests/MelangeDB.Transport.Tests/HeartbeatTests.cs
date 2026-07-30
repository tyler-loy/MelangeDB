using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A closed socket is not the only way a client goes away. The server pings on the heartbeat
/// interval and aborts a connection whose silence exceeds the timeout — driven here entirely by a
/// hand-cranked clock, so nothing sleeps and nothing flakes.
/// </summary>
public class HeartbeatTests
{
    [Fact]
    public async Task An_ungraceful_drop_is_detected_within_the_configured_timeout()
    {
        await using var host = await TransportTestHost.StartAsync(manualTime: true);
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        // The client goes silent: it reads (so we can observe the server's verdict) but never
        // sends another frame — no Pong, no traffic, no close. From the server's side this is
        // indistinguishable from a dead process behind a live NAT entry.
        var pings = 0;
        var died = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reader = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    if (await raw.ReceiveAsync(TestContext.Current.CancellationToken) is PingFrame)
                        Interlocked.Increment(ref pings);
                }
            }
            catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
            {
                died.TrySetResult();
            }
        }, TestContext.Current.CancellationToken);

        // Two heartbeat intervals of silence: pinged, but still within the 45s timeout — alive.
        host.Time!.Advance(TimeSpan.FromSeconds(15));
        host.Time.Advance(TimeSpan.FromSeconds(15));
        await TransportTestHost.WaitUntilAsync(() => Volatile.Read(ref pings) >= 2, "heartbeat pings to arrive");
        Assert.False(died.Task.IsCompleted, "the connection must survive silence within the timeout");

        // Crossing Transport:HeartbeatTimeoutMs: the server gives up and aborts the socket.
        host.Time.Advance(TimeSpan.FromSeconds(45));
        await died.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        await reader;
    }

    [Fact]
    public async Task A_responsive_client_is_never_dropped()
    {
        await using var host = await TransportTestHost.StartAsync(manualTime: true);
        host.Call("SetChunk", 1L, 1L, new byte[] { 1 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);

        // MelangeClient answers pings, so hours of idle time change nothing.
        for (var i = 0; i < 10; i++)
        {
            host.Time!.Advance(TimeSpan.FromSeconds(30));

            // Let the ping/pong round-trip complete before advancing again, so the manual clock
            // cannot outrun the real sockets carrying the frames.
            var lsn = await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
            _ = lsn;
        }

        Assert.True(client.IsConnected);
        var head = host.Call("SetChunk", 2L, 2L, new byte[] { 2 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= head, "the delta after long idling");
        Assert.Equal(2, subscription.Count);
    }
}
