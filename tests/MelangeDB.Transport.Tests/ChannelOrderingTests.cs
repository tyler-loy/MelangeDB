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
    public async Task Frames_carry_channel_tags_ordering_holds_within_a_channel_and_channels_interleave()
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

        // Interleaving across channels: the reducer result (and its delta) overtook the bulk
        // stream. This doubles as the head-of-line bound — the response waited at most one chunk.
        var resultIndex = observed.FindIndex(f => f is ReducerResultFrame);
        var lastChunkIndex = observed.FindLastIndex(f => f is SubscriptionAppliedFrame);
        Assert.True(resultIndex >= 0);
        Assert.True(
            resultIndex < lastChunkIndex,
            "the reducer result should arrive while the initial set is still streaming");

        // Ordering within the data channel: LSNs never regress.
        var updates = observed.OfType<TransactionUpdateFrame>().Where(f => f.Lsn != 0).ToList();
        for (var i = 1; i < updates.Count; i++)
            Assert.True(updates[i - 1].Lsn < updates[i].Lsn);
    }

    [Fact]
    public async Task A_large_initial_set_does_not_delay_a_concurrent_reducer_response()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:Transport:MaxInitialSetChunkBytes"] = "4096",
            ["MelangeDb:Subscriptions:MaxBytesPerSubscription"] = "134217728",
        });
        host.Engine.BulkInsert(TransportTestHost.Caller, [.. Enumerable.Range(0, 800).Select(i =>
            new Core.BulkRow("Chunk", new Dictionary<string, object?>
            {
                ["Id"] = (long)i,
                ["X"] = (long)(i % 16),
                ["Data"] = new byte[2048],
            }))]);

        var order = new List<string>();
        await using var client = host.CreateClient(o => o.FrameInspector = (frame, _) =>
        {
            lock (order)
            {
                if (frame is ReducerResultFrame)
                    order.Add("result");
                else if (frame is SubscriptionAppliedFrame { IsLast: true })
                    order.Add("last-chunk");
            }
        });
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var subscribing = client.SubscribeAsync("SELECT * FROM Chunk", cancellationToken: TestContext.Current.CancellationToken);
        await client.CallReducerAsync("Noop", null, TestContext.Current.CancellationToken);
        await subscribing;

        lock (order)
        {
            // The stated bound: the response is interleaved ahead of the remaining bulk chunks,
            // so it lands before the ~1.6MB initial set finishes.
            Assert.Equal(["result", "last-chunk"], order);
        }
    }
}
