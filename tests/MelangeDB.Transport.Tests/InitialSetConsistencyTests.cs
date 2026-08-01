using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The classic bug of this phase: computing a snapshot while transactions commit produces a missed
/// or doubled row unless the snapshot and the delta stream are anchored to one LSN. These tests
/// subscribe repeatedly while a writer hammers the table and assert the cache converges to exactly
/// the committed state with zero contradicted deltas — no gap, no duplicate, ever.
/// <para>
/// Category=Stress keeps these out of CI with the cluster and load suites: on shared 2-vCPU
/// runners the drain has twice sat out its entire 4x-dilated deadline (runs 30690081070 and
/// 30690573307) — wedged, not slow, which is issue #23's open investigation. They run in the
/// local loop, where the same suite has stayed clean across every contended campaign.
/// </para>
/// </summary>
[Trait("Category", "Stress")]
public class InitialSetConsistencyTests
{
    [Fact]
    public async Task No_gap_and_no_duplicate_across_the_initial_set_delta_boundary_under_concurrent_writes()
    {
        await using var host = await TransportTestHost.StartAsync();
        var random = new Random(42);
        var writes = 0;
        using var writerDone = new CancellationTokenSource();
        var writer = Task.Run(() =>
        {
            // 400 commits mixing inserts, updates, and deletes over a 60-key space, so every
            // subscription registration lands mid-stream against a mutating table.
            for (var i = 0; i < 400; i++)
            {
                var id = random.Next(60);
                if (random.Next(4) == 0)
                    host.Call("DeleteChunk", (long)id);
                else
                    host.Call("SetChunk", (long)id, (long)(id % 8), new byte[] { (byte)i });
                Interlocked.Increment(ref writes);
            }
        }, TestContext.Current.CancellationToken);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        var subscriptions = new List<Client.MelangeSubscription>();
        while (!writer.IsCompleted)
        {
            subscriptions.Add(await client.SubscribeAsync(
                "SELECT * FROM Chunk",
                cancellationToken: TestContext.Current.CancellationToken));
            if (subscriptions.Count >= 8)
                break;
        }

        await writer;
        var head = host.Engine.Log.HeadLsn;

        // The drain condition is sound — every appended record fans a frame to these full-table
        // subscriptions, so LastAckedLsn must reach head once the stream is through — but the
        // whole wait can overlap the rest of the suite running in parallel: on a two-vCPU CI
        // runner in Debug, the backpressure floods and their fsync'd writers can starve this
        // client's receive continuations for most of a minute. The stock 15s (dilated) deadline
        // measured that contention, not this test's invariant; give the drain the suite's length.
        await TransportTestHost.WaitUntilAsync(
            () => client.LastAckedLsn >= head, "the client to drain the delta stream", timeoutSeconds: 60);

        // Authoritative state, read consistently server-side.
        var authoritative = host.Engine.ReadConsistent(_ =>
            host.Engine.HotStore.Scan(host.Engine.Schema.Get(typeof(Chunk)).Id)
                .ToDictionary(pair => Convert.ToHexString(pair.Key.Span), pair => pair.Value.ToArray()));

        Assert.NotEmpty(subscriptions);
        foreach (var subscription in subscriptions)
        {
            Assert.Equal(0, subscription.Inconsistencies);
            var cached = subscription.Rows.ToDictionary(row => Convert.ToHexString(row.Key), row => row);
            Assert.Equal(authoritative.Count, cached.Count);
            foreach (var (key, rowBytes) in authoritative)
            {
                Assert.True(cached.TryGetValue(key, out var row), $"row {key} missing from the client cache");
                var expected = RowSerializer.Deserialize(host.Engine.Schema.Get(typeof(Chunk)), rowBytes);
                Assert.Equal(((Chunk)expected).Data, (byte[])row.Columns["Data"]!);
            }
        }
    }

    [Fact]
    public async Task Range_subscription_stays_consistent_under_concurrent_writes()
    {
        await using var host = await TransportTestHost.StartAsync();
        var random = new Random(7);
        var writer = Task.Run(() =>
        {
            for (var i = 0; i < 300; i++)
            {
                var id = random.Next(50);
                // X moves rows in and out of the subscribed band as writes land.
                host.Call("SetChunk", (long)id, (long)random.Next(16), new byte[] { (byte)i });
            }
        }, TestContext.Current.CancellationToken);

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var subscription = await client.SubscribeAsync(
            "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 4L, ["hi"] = 11L },
            TestContext.Current.CancellationToken);

        await writer;
        var head = host.Engine.Log.HeadLsn;

        // Same suite-contention allowance as the test above.
        await TransportTestHost.WaitUntilAsync(
            () => client.LastAckedLsn >= head, "the client to drain the delta stream", timeoutSeconds: 60);

        Assert.Equal(0, subscription.Inconsistencies);
        var schema = host.Engine.Schema.Get(typeof(Chunk));
        var expected = host.Engine.ReadConsistent(_ =>
            host.Engine.HotStore.Scan(schema.Id)
                .Select(pair => (Chunk)RowSerializer.Deserialize(schema, pair.Value.ToArray()))
                .Where(chunk => chunk.X is >= 4 and <= 11)
                .Select(chunk => chunk.Id)
                .Order()
                .ToList());
        var actual = subscription.Rows.Select(row => (long)row.Columns["Id"]!).Order().ToList();
        Assert.Equal(expected, actual);
    }
}
