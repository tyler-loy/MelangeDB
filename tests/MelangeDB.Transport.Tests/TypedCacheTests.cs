using MelangeDB.Client;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The typed cache runtime against a real server — the merge semantics the plan pinned: the
/// engine computes deltas per subscription with no cross-subscription dedup on a connection, so
/// the client refcounts covering subscriptions per key and derives events from transitions.
/// A hand-written codec stands in for generated ones; the mechanics under test are the library's.
/// </summary>
public class TypedCacheTests
{
    /// <summary>What the client generator emits per table, written by hand for the runtime tests.</summary>
    private sealed class ChunkCodec : IClientRowCodec<Chunk>
    {
        public static readonly ChunkCodec Instance = new();

        public string TableName => "Chunk";

        public Chunk DecodeRow(IReadOnlyDictionary<string, object?> columns)
        {
            var row = default(Chunk);
            row.Id = ClientWireValues.ReadInt64(columns, "Id", "Chunk");
            row.X = ClientWireValues.ReadInt64(columns, "X", "Chunk");
            row.Data = ClientWireValues.ReadBytes(columns, "Data", "Chunk")!;
            return row;
        }

        public byte[] EncodePrimaryKey(in Chunk row) => KeyCodec.EncodeInt64(row.Id).ToArray();
    }

    private sealed class Recorder
    {
        private readonly Lock _lock = new();
        private readonly List<string> _events = [];

        public void Attach(ClientCache<Chunk> cache)
        {
            cache.OnInsert += row => Add($"+{row.Id}");
            cache.OnUpdate += (_, row) => Add($"~{row.Id}");
            cache.OnDelete += row => Add($"-{row.Id}");
        }

        public IReadOnlyList<string> Events
        {
            get
            {
                lock (_lock)
                {
                    return [.. _events];
                }
            }
        }

        public int Count(string prefix) => Events.Count(e => e.StartsWith(prefix, StringComparison.Ordinal));

        private void Add(string entry)
        {
            lock (_lock)
            {
                _events.Add(entry);
            }
        }
    }

    private static Task<TypedSubscription<Chunk>> SubscribeRangeAsync(ClientCacheRegistry registry, long low, long high) =>
        registry.SubscribeAsync(
            ChunkCodec.Instance,
            "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = low, ["hi"] = high },
            TestContext.Current.CancellationToken);

    [Fact]
    public async Task A_row_matching_two_subscriptions_is_one_cached_row_and_one_event_each_way()
    {
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var recorder = new Recorder();
        recorder.Attach(registry.GetOrAdd(ChunkCodec.Instance));

        await SubscribeRangeAsync(registry, 0, 10);
        await SubscribeRangeAsync(registry, 5, 15);

        // X = 7 matches both subscriptions: the server sends it once per subscription — that is
        // the measured dedup answer — and the merge must collapse the pair to one row, one event.
        // Waiting on the acked LSN guarantees the whole frame (both subscription groups) applied.
        var lsn = host.Call("SetChunk", 1L, 7L, new byte[] { 1 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the insert to arrive");
        Assert.Equal(1, recorder.Count("+"));
        Assert.Equal(1, registry.GetOrAdd(ChunkCodec.Instance).Count);

        // Same shape for the update: two identical copies arrive, one OnUpdate fires.
        lsn = host.Call("SetChunk", 1L, 7L, new byte[] { 2 });
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the update to arrive");
        Assert.Equal(1, recorder.Count("~"));

        // And the delete: both subscriptions lose the row, the cache deletes once, with the row.
        lsn = host.Call("DeleteChunk", 1L);
        await TransportTestHost.WaitUntilAsync(() => client.LastAckedLsn >= lsn, "the delete to arrive");
        Assert.Equal(1, recorder.Count("-"));
        Assert.Equal(0, registry.GetOrAdd(ChunkCodec.Instance).Count);
    }

    [Fact]
    public async Task Unsubscribe_removes_only_the_rows_no_other_subscription_covers()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 2L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 7L, new byte[] { 2 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var cache = registry.GetOrAdd(ChunkCodec.Instance);
        var recorder = new Recorder();
        recorder.Attach(cache);

        var narrow = await SubscribeRangeAsync(registry, 0, 10);
        await SubscribeRangeAsync(registry, 5, 15);
        Assert.Equal(2, cache.Count);

        await narrow.UnsubscribeAsync(TestContext.Current.CancellationToken);

        // The chunk at X=2 was the narrow subscription's alone — it leaves, with an event. The
        // chunk at X=7 is still covered by the wide subscription and must survive.
        Assert.Equal(["-1"], recorder.Events.Where(e => e.StartsWith('-')));
        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryFind(KeyCodec.EncodeInt64(2).ToArray(), out var survivor));
        Assert.Equal(7, survivor.X);
    }

    [Fact]
    public async Task Rescope_reconciles_by_diff_not_by_flush()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 2L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 7L, new byte[] { 2 });
        host.Call("SetChunk", 3L, 12L, new byte[] { 3 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var cache = registry.GetOrAdd(ChunkCodec.Instance);
        var recorder = new Recorder();

        var subscription = await SubscribeRangeAsync(registry, 0, 10);
        Assert.Equal(2, cache.Count);
        recorder.Attach(cache);

        // The window slides 0..10 → 5..15: chunk 1 (X=2) leaves, chunk 3 (X=12) arrives, and
        // chunk 2 (X=7) — the survivor — must produce no event at all. That silence is the
        // no-flush contract the terrain-streaming pattern depends on.
        await subscription.RescopeAsync(new Dictionary<string, object?> { ["lo"] = 5L, ["hi"] = 15L }, TestContext.Current.CancellationToken);
        await TransportTestHost.WaitUntilAsync(
            () => recorder.Count("-") >= 1 && recorder.Count("+") >= 1,
            "the rescope diff to arrive");

        Assert.Equal(["-1"], recorder.Events.Where(e => e.StartsWith('-')));
        Assert.Equal(["+3"], recorder.Events.Where(e => e.StartsWith('+')));
        Assert.Equal(0, recorder.Count("~"));
        Assert.Equal(2, cache.Count);
    }

    [Fact]
    public async Task A_key_only_delete_resolves_to_the_cached_row()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 9L, 3L, new byte[] { 42, 43 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var cache = registry.GetOrAdd(ChunkCodec.Instance);
        Chunk? deleted = null;
        cache.OnDelete += row => deleted = row;

        await registry.SubscribeAsync(ChunkCodec.Instance, "SELECT * FROM Chunk", null, TestContext.Current.CancellationToken);

        // The delete frame carries the encoded key and nothing else; the typed event must still
        // deliver the full row a consumer last saw — that is what the cache is for.
        host.Call("DeleteChunk", 9L);
        await TransportTestHost.WaitUntilAsync(() => deleted is not null, "the delete to arrive");
        Assert.Equal(9, deleted!.Value.Id);
        Assert.Equal(3, deleted.Value.X);
        Assert.Equal(new byte[] { 42, 43 }, deleted.Value.Data);
    }

    [Fact]
    public async Task A_full_resync_converges_the_cache_by_diff()
    {
        await using var host = await TransportTestHost.StartAsync();
        host.Call("SetChunk", 1L, 2L, new byte[] { 1 });
        host.Call("SetChunk", 2L, 7L, new byte[] { 2 });

        await using var client = host.CreateClient();
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        var registry = new ClientCacheRegistry(client);
        var cache = registry.GetOrAdd(ChunkCodec.Instance);
        await registry.SubscribeAsync(ChunkCodec.Instance, "SELECT * FROM Chunk", null, TestContext.Current.CancellationToken);
        Assert.Equal(2, cache.Count);

        var recorder = new Recorder();
        recorder.Attach(cache);

        // A fresh log is a different epoch: resume is refused, the subscription re-establishes
        // from a fresh initial set, and the world now holds one different chunk. The cache must
        // converge by diff — the two dead rows leave, the new one arrives, no flush in between.
        await host.RestartAsync(freshLog: true);
        host.Call("SetChunk", 5L, 4L, new byte[] { 9 });
        var resumed = await client.ReconnectAsync(TestContext.Current.CancellationToken);

        Assert.False(resumed);
        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryFind(KeyCodec.EncodeInt64(5).ToArray(), out _));
        Assert.Equal(2, recorder.Count("-"));
        Assert.Equal(1, recorder.Count("+"));
    }
}
