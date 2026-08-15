using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The channel-tag constraint, observed on the wire: every frame carries its channel, ordering
/// holds within a channel, frames on different channels interleave — and because bulk initial sets
/// ride their own channel behind the interactive lanes, a large initial set cannot delay a
/// concurrent reducer response by more than one chunk.
/// </summary>
public class ChannelOrderingTests
{
    [Fact]
    public async Task Frames_carry_channel_tags_and_ordering_holds_within_each_channel()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            // Small chunks: the 400-row initial set becomes hundreds of bulk frames, so the
            // interactive frames' ability to overtake is observable rather than incidental.
            ["MelangeDb:Transport:MaxInitialSetChunkBytes"] = "2048",
        });
        host.Engine.BulkInsert(TransportTestHost.Caller, [.. Enumerable.Range(0, 3000).Select(i =>
            new Core.BulkRow("Chunk", new Dictionary<string, object?>
            {
                ["Id"] = (long)i,
                ["X"] = (long)(i % 16),
                ["Data"] = new byte[64],
            }))]);

        var frames = new List<Frame>();
        await using var client = host.CreateClient(o => o.FrameInspector = (frame, _) =>
        {
            lock (frames)
            {
                frames.Add(frame);
            }
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // Subscribe and, while the initial set is streaming, call a reducer; then produce deltas.
        var subscribing = client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
        var subscription = await subscribing;
        await client.CallReducerAsync("SetChunk", [5000L, 5L, new byte[] { 1 }], TestContext.Current.CancellationToken);
        var lsn = await client.CallReducerAsync("SetChunk", [5001L, 5L, new byte[] { 2 }], TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the deltas to drain");

        List<Frame> observed;
        lock (frames)
        {
            observed = [.. frames];
        }

        // Every frame carries its channel tag.
        Assert.All(observed.OfType<WelcomeFrame>(), f => Assert.Equal(MelangeChannels.Control, f.Channel));
        Assert.All(observed.OfType<ReducerResultFrame>(), f => Assert.Equal(MelangeChannels.Calls, f.Channel));
        Assert.All(observed.OfType<TransactionUpdateFrame>(), f => Assert.Equal(MelangeChannels.Data, f.Channel));
        var chunks = observed.OfType<SubscriptionAppliedFrame>().ToList();
        Assert.True(chunks.Count > 50, $"expected a many-chunk initial set, got {chunks.Count}");
        Assert.All(chunks, f => Assert.Equal(MelangeChannels.BulkFor(subscription.Id), f.Channel));

        // Ordering within the bulk channel: chunk indices strictly ascend.
        for (var i = 1; i < chunks.Count; i++)
            Assert.Equal(chunks[i - 1].ChunkIndex + 1, chunks[i].ChunkIndex);

        // The reducer results arrived; whether they overtook the bulk stream is not asserted here,
        // because nothing in this test holds the bulk channel open until the call is on the wire —
        // on a fast runner the whole set can drain first, and the interleave becomes a coin flip.
        // The cross-channel property lives in the next test, which forces the schedule.
        Assert.Contains(observed, f => f is ReducerResultFrame);

        // Ordering within the data channel: LSNs never regress.
        var updates = observed.OfType<TransactionUpdateFrame>().Where(f => f.Lsn != 0).ToList();
        for (var i = 1; i < updates.Count; i++)
            Assert.True(updates[i - 1].Lsn < updates[i].Lsn);
    }

    /// <summary>
    /// The head-of-line bound, with the schedule forced rather than raced: the subscribe and the
    /// call are sent before anything is read, so the ~16MB initial set wedges the sender against
    /// TCP and the call is guaranteed to arrive while bulk chunks are still pending. The response
    /// rides the calls channel past them; a sender that queued it behind the set would fail this
    /// every time, not just under unlucky scheduling.
    /// </summary>
    [Fact]
    public async Task A_large_initial_set_does_not_delay_a_concurrent_reducer_response()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Subscriptions:MaxBytesPerSubscription"] = "134217728",
            ["MelangeDb:Validation:MaxCollectionLength"] = "524288",
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = (45_000 * TestTime.Scale).ToString(),
        });

        // ~16MB: the unread set must overflow the kernel's socket buffering (Linux loopback
        // autotunes the send buffer to ~4MB), or the sender never wedges and the interleave below
        // is racy instead of forced.
        for (var i = 0; i < 64; i++)
            host.Call("SetChunk", (long)i, (long)i, new byte[256 * 1024]);

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        // Both frames are on the socket before the first read: the subscribe starts the stream,
        // the stream fills TCP and wedges, and the call arrives while chunks are still pending.
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.SendAsync(new CallReducerFrame(1, "Noop", ReducerArgs.Encode([]), null) { Channel = MelangeChannels.Calls }, TestContext.Current.CancellationToken);

        var sawResult = false;
        var chunks = 0;
        while (true)
        {
            var frame = await raw.ReceiveAsync(TestContext.Current.CancellationToken);
            if (frame is ReducerResultFrame)
                sawResult = true;
            if (frame is SubscriptionAppliedFrame chunk)
            {
                chunks++;
                if (chunk.IsLast)
                    break;
            }
        }

        Assert.True(chunks > 1, $"expected a multi-chunk initial set, got {chunks} chunk(s)");
        Assert.True(sawResult, "the reducer result should arrive while the initial set is still streaming");
    }

    /// <summary>
    /// The swap-window ordering rule: a freshly registered subscription's first initial-set chunk
    /// precedes any delta for that subscription on the wire. A gateway swap re-issues a
    /// subscription on the destination node under an id the client already holds live against the
    /// origin's log; a delta overtaking the replacement set's first chunk (deltas normally outrank
    /// bulk) would be judged against the origin's anchor — a different log — and silently dropped.
    /// That was the phase 10 walk losing a PlayerPos step under CPU starvation: the starved sender
    /// let the post-registration commit's delta reach the wire before the set that would have
    /// flipped the client to buffering. The schedule is forced without timing assumptions: an
    /// unread ~16MB initial set wedges the sender against TCP, and the second subscribe and the
    /// commit ride the same socket in order, so the delta is always enqueued while the fresh
    /// stream's first chunk is still pending.
    /// </summary>
    [Fact]
    public async Task A_fresh_subscriptions_first_chunk_precedes_its_deltas_on_the_wire()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            // Buffer: the wedged sender is the scenario, not the thing to punish.
            ["MelangeDb:Subscriptions:BackpressurePolicy"] = "Buffer",
            ["MelangeDb:Validation:MaxCollectionLength"] = "524288",
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = (45_000 * TestTime.Scale).ToString(),
        });
        host.Call("Spawn", "walker", 1);

        // ~16MB: the unread set must overflow the kernel's socket buffering (Linux loopback
        // autotunes the send buffer to ~4MB), or the sender never wedges and the race below is
        // vacuously unexercised.
        for (var i = 0; i < 64; i++)
            host.Call("SetChunk", (long)i, (long)i, new byte[256 * 1024]);

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        // Nothing is read until all three frames are sent: subscription 1's ~16MB set fills TCP
        // and wedges the sender mid-stream, subscription 2 registers while its first chunk is
        // still pending, and the Move commit fans out a subscription 2 delta strictly after the
        // registration (same-socket order).
        await raw.SendAsync(new SubscribeFrame(1, "SELECT * FROM Chunk", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.SendAsync(new SubscribeFrame(2, "SELECT * FROM PlayerState", null) { Channel = MelangeChannels.Data }, TestContext.Current.CancellationToken);
        await raw.SendAsync(new CallReducerFrame(1, "Move", ReducerArgs.Encode([1.5f]), null) { Channel = MelangeChannels.Calls }, TestContext.Current.CancellationToken);

        // Resume reading: the delta for subscription 2 must ride behind its first chunk, never
        // ahead of it. (On the pre-rule sender, the delta lane outranked bulk unconditionally and
        // the delta won the race whenever the sender was wedged — exactly this schedule.)
        var sawFirstChunk = false;
        while (true)
        {
            var frame = await raw.ReceiveAsync(TestContext.Current.CancellationToken);
            if (frame is SubscriptionAppliedFrame { SubscriptionId: 2 })
                sawFirstChunk = true;
            if (frame is TransactionUpdateFrame update && update.Updates.Any(u => u.SubscriptionId == 2))
            {
                Assert.True(sawFirstChunk, "a delta for subscription 2 reached the wire before its initial set's first chunk");
                break;
            }
        }
    }
}
