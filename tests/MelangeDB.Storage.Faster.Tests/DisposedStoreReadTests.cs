using System.Diagnostics;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// #199: what a long read over a paged store did to everything else, and what a read that
/// outlived its store reported. A cluster node's heartbeat samples <see cref="IHotStore.Statistics"/>;
/// it used to wait for the store lock, which a scan over a store larger than its buffer pool holds
/// across each disk read, and the unfair lock starved it long enough for the hub to fence a healthy
/// node. The shard was then released under the running read, the store disposed, and the read's
/// next rows came back "main record missing" / "out-of-line payload missing" — corruption, to
/// anyone reading the log, of a store whose files had simply been deleted.
/// </summary>
public class DisposedStoreReadTests(ITestOutputHelper output)
{
    private const int Rows = 100_000;

    private static StoreHarness Seeded()
    {
        var harness = new StoreHarness(StoreKind.Faster, tables: [typeof(TerrainBlob)]);
        for (var start = 0; start < Rows; start += 5_000)
        {
            var first = start;
            harness.Invoke("seed", ctx =>
            {
                for (var i = first; i < first + 5_000; i++)
                    ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = i % 16, Data = StoreContractTests.MakeBlob(i, 256) });
            });
        }

        return harness;
    }

    [Fact]
    public void A_read_that_outlives_its_store_is_told_the_store_is_gone_not_that_its_rows_are()
    {
        using var harness = Seeded();
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        using var view = ((IReadViewSource)harness.Engine.HotStore).OpenReadView();
        using var scan = view.Scan(table).GetEnumerator();
        for (var i = 0; i < 1_000; i++)
            Assert.True(scan.MoveNext());

        ((IDisposable)harness.Engine.HotStore).Dispose(); // The release, as the shard runtime performs it.

        Assert.Throws<ObjectDisposedException>(() => scan.MoveNext());
    }

    [Fact]
    public async Task Statistics_answer_promptly_while_a_scan_holds_the_store_busy()
    {
        using var harness = Seeded();
        var store = harness.Engine.HotStore;
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        var scanned = 0L;
        var scanner = Task.Run(() =>
        {
            for (var pass = 0; pass < 3; pass++)
            {
                foreach (var _ in store.Scan(table))
                    Interlocked.Increment(ref scanned);
            }
        }, TestContext.Current.CancellationToken);

        var worst = TimeSpan.Zero;
        var samples = 0;
        while (!scanner.IsCompleted)
        {
            var started = Stopwatch.GetTimestamp();
            store.Statistics();
            var took = Stopwatch.GetElapsedTime(started);
            if (took > worst)
                worst = took;
            samples++;
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        await scanner;
        output.WriteLine($"{samples} statistics reads during {Interlocked.Read(ref scanned)} scanned rows; worst {worst.TotalMilliseconds:F1} ms");
        Assert.Equal(3L * Rows, Interlocked.Read(ref scanned));
        Assert.True(samples > 10, "the scan finished before it could be measured against");
        Assert.True(worst < TimeSpan.FromMilliseconds(50), $"a statistics read waited {worst.TotalMilliseconds:F0} ms behind the scan");
    }
}
