using System.Net.WebSockets;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// A client on a too-slow link during heavy write traffic. The send buffer is bounded
/// (Subscriptions:MaxBufferedBytes); what happens at the bound is the configured policy. The
/// default drops the delta stream and demands a resync — bounded memory, kept connection — and
/// the client converges through the same re-establishment path a rejected Resume uses.
/// </summary>
public class BackpressureTests
{
    private const int WriteCount = 60;

    // A flood that must trip the policy has to defeat the kernel's socket buffering first: on
    // Linux loopback the send buffer autotunes to ~4MB (tcp_wmem), and a flood that fits inside
    // it is drained by the sender without a byte ever queuing in the server — whether the policy
    // fired then depended on which of the producer and the sender the scheduler favoured, the
    // starvation flake this constant closes. 60 × 256KB ≈ 15MB clears any plausible absorption
    // with margin, so the policy's trigger follows from the flood's size alone.
    private const int FloodRowBytes = 256 * 1024;

    private static Dictionary<string, string?> Settings(string policy) => new()
    {
        ["MelangeDb:Subscriptions:BackpressurePolicy"] = policy,
        ["MelangeDb:Subscriptions:MaxBufferedBytes"] = "65536",
        ["MelangeDb:Validation:MaxCollectionLength"] = (2 * FloodRowBytes).ToString(),
        // The raw client deliberately stops reading during the flood, and it never pongs, so the
        // server's liveness clock runs from the subscribe frame on. On hardware slow enough to
        // stretch the flood past HeartbeatTimeoutMs, the heartbeat would abort the socket and
        // this suite would measure liveness, not backpressure — the DropAndResync and Buffer
        // policies keep the connection, and a liveness kill also false-passes Disconnect. The
        // default timeout dilates with MELANGE_TEST_TIME_SCALE; at scale 1 nothing changes.
        ["MelangeDb:Transport:HeartbeatTimeoutMs"] = (45_000 * TestTime.Scale).ToString(),
    };

    [Fact]
    public async Task DropAndResync_drops_the_delta_stream_and_the_client_reestablishes()
    {
        await using var host = await TransportTestHost.StartAsync(Settings("DropAndResync"));
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(TestContext.Current.CancellationToken, f => f.IsLast);

        // The client stops reading; ~15MB of deltas arrive against a 64KB buffer. The kernel
        // absorbs a few megabytes, the delta queue absorbs 64KB, and then the policy fires.
        for (var i = 0; i < WriteCount; i++)
            host.Call("SetChunk", (long)i, (long)i, new byte[FloodRowBytes]);

        // Resume reading: somewhere in the stream is the connection-scoped overflow error.
        var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
        Assert.Equal(MelangeErrorCodes.OverflowResync, error.Code);
        Assert.Equal(0u, error.SubscriptionId);

        // The server forgot this connection's subscriptions: re-subscribing with the same id
        // yields a fresh initial set (a live id would have re-scoped, which sends no initial set),
        // and the connection itself survived — that is the point of the policy.
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        var reapplied = await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(TestContext.Current.CancellationToken, f => f.IsLast);
        var total = reapplied.ChunkIndex + 1;
        Assert.True(total > 0, $"expected a fresh initial set, got {total} chunks");
    }

    /// <summary>
    /// The lost-resubscribe schedule, closed by making the drop synchronous. The deferred-cleanup
    /// shape it replaces lost a 1-in-3 race under CPU starvation: (1) the overflow fired on a
    /// committing thread and scheduled the forget-everything sweep as a thread-pool task; (2) the
    /// error frame reached the client and its re-subscribe arrived while the starved pool had not
    /// yet run the sweep; (3) HandleSubscribe found the stale registration and re-scoped — no
    /// initial set; (4) the sweep then unregistered the client's replacement, leaving a silently
    /// dead subscription and a test wedged until the liveness abort. The drop now completes under
    /// the engine lock before the error frame even exists, so no re-subscribe can be prompt
    /// enough to observe the half-dropped state; forcing the old schedule deterministically is
    /// impossible precisely because the async gap it needed is gone. Each cycle asserts the full
    /// contract the race broke: a fresh initial set and a live delta stream afterwards.
    /// </summary>
    [Fact]
    public async Task DropAndResync_a_prompt_resubscribe_always_gets_a_fresh_initial_set_and_live_deltas()
    {
        await using var host = await TransportTestHost.StartAsync(Settings("DropAndResync"));
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(TestContext.Current.CancellationToken, f => f.IsLast);

        for (var cycle = 0; cycle < 3; cycle++)
        {
            // The unread flood trips the policy; the connection survives with its subscriptions
            // forgotten, exactly as in the test above.
            for (var i = 0; i < WriteCount; i++)
                host.Call("SetChunk", (long)i, (long)(100 + cycle), new byte[FloodRowBytes]);
            var error = await raw.ReceiveUntilAsync<ErrorFrame>(TestContext.Current.CancellationToken);
            Assert.Equal(MelangeErrorCodes.OverflowResync, error.Code);

            // Re-establish immediately: the same id must always take the fresh path (initial
            // set), never the re-scope path a stale registration would offer.
            await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
            var reapplied = await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(TestContext.Current.CancellationToken, f => f.IsLast);
            Assert.True(reapplied.ChunkIndex + 1 > 0);

            // And the replacement must be live: the next commit's delta reaches the client.
            var lsn = host.Call("SetChunk", (long)(1000 + cycle), 0L, new byte[16]);
            var update = await raw.ReceiveUntilAsync<TransactionUpdateFrame>(TestContext.Current.CancellationToken, f => f.Lsn == lsn);
            Assert.Contains(update.Updates, u => u.SubscriptionId == 1);
        }
    }

    [Fact]
    public async Task Buffer_policy_keeps_buffering_and_eventually_delivers_everything()
    {
        await using var host = await TransportTestHost.StartAsync(Settings("Buffer"));
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(TestContext.Current.CancellationToken, f => f.IsLast);

        ulong head = 0;
        for (var i = 0; i < WriteCount; i++)
            head = host.Call("SetChunk", (long)i, (long)i, new byte[32 * 1024]);

        // Reading resumes; with Buffer the stream is complete — every LSN, in order, no error.
        var seen = new List<ulong>();
        while (true)
        {
            var frame = await raw.ReceiveAsync(TestContext.Current.CancellationToken);
            Assert.IsNotType<ErrorFrame>(frame);
            if (frame is TransactionUpdateFrame update)
            {
                seen.Add(update.Lsn);
                if (update.Lsn >= head)
                    break;
            }
        }

        Assert.Equal(WriteCount, seen.Count);
        Assert.Equal(seen.Order().ToList(), seen);
    }

    [Fact]
    public async Task Disconnect_policy_closes_the_connection_as_a_last_resort()
    {
        await using var host = await TransportTestHost.StartAsync(Settings("Disconnect"));
        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(TestContext.Current.CancellationToken, f => f.IsLast);

        for (var i = 0; i < WriteCount; i++)
            host.Call("SetChunk", (long)i, (long)i, new byte[FloodRowBytes]);

        // The stalled connection was aborted server-side; reading now can only end in death.
        await Assert.ThrowsAnyAsync<WebSocketException>(async () =>
        {
            while (true)
                await raw.ReceiveAsync(TestContext.Current.CancellationToken);
        });
    }
}
