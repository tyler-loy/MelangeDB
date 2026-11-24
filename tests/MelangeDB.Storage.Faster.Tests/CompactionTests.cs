using System.Diagnostics;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// #287: a paged table rewritten over and over keeps its log, and its disk, within a multiple of its
/// live data while the store runs. These run on MelangeLog whatever <c>MELANGE_TEST_PAGED_ENGINE</c>
/// says, since it is the engine that compacts.
/// </summary>
[Trait("Category", "Slow")]
public sealed class CompactionTests
{
    private const int Rows = 400;
    private const int BlobBytes = 16 * 1024;
    private const int Rounds = 20;

    // 8 MiB budget: 4 MiB a space, so 8 MiB segments; a round rewrites 6.25 MiB of blobs.
    private static readonly long SegmentBytes = 8L << 20;

    [Fact]
    public void Rewriting_a_paged_table_keeps_its_log_within_a_multiple_of_its_live_bytes()
    {
        using var harness = MelangeLogHarness(compaction: true);
        Rewrite(harness, Rounds);

        var store = harness.Engine.HotStore;
        var blob = WaitFor(store, "blob", log => log.LogBytes <= Bound(log));
        Assert.True(
            blob.LogBytes <= Bound(blob),
            $"blob log {blob.LogBytes >> 20} MiB against {blob.LiveBytes >> 20} MiB live, after {Rounds} rewrites of the table");
        Assert.True(blob.ReclaimedBytes > 0);
        Assert.True(DiskBytes(harness) <= Bound(blob) + 3 * SegmentBytes, $"disk {DiskBytes(harness) >> 20} MiB");

        // What an operator sees: every write accounted for, compaction's lock time, the files, the index.
        Assert.Equal((long)Rows * Rounds, blob.InPlaceWrites + blob.AppendedWrites);
        Assert.True(blob.CompactionSteps > 0);
        Assert.True(blob.CompactionLockTime > TimeSpan.Zero);
        Assert.Equal(DiskBytes(harness) / SegmentBytes, store.Statistics().Logs.Sum(static l => l.SegmentFiles));
        Assert.Equal(Rows, blob.IndexEntries);
        Assert.True(blob.IndexCapacity > blob.IndexEntries);
        Assert.Equal(0, blob.IntegrityFailures);
        Assert.True(blob.MemoryBytes is > 0 and <= 4L << 20);

        AssertRows(harness, Rounds);
        harness.Restart();
        AssertRows(harness, Rounds);
    }

    /// <summary>
    /// Live bytes are what compaction's trigger ratio divides by: each row counts once, as its main
    /// record plus its blob record, header and alignment included, and a freshly written log is all live.
    /// </summary>
    [Fact]
    public void Live_bytes_count_each_row_once()
    {
        using var harness = MelangeLogHarness(compaction: false);
        Rewrite(harness, 1);

        var logs = harness.Engine.HotStore.Statistics().Logs;
        var blob = logs.Single(l => l.Space == "blob");
        var main = logs.Single(l => l.Space == "main");
        Assert.InRange(blob.LiveBytes, (long)Rows * BlobBytes, (long)Rows * (BlobBytes + 64));
        Assert.Equal(main.LogBytes, main.LiveBytes);
        Assert.InRange(blob.LogBytes - blob.LiveBytes, 0, SegmentBytes);
    }

    [Fact]
    public void With_compaction_off_the_log_keeps_every_version()
    {
        using var harness = MelangeLogHarness(compaction: false);
        Rewrite(harness, 6);

        var blob = harness.Engine.HotStore.Statistics().Logs.Single(l => l.Space == "blob");
        Assert.Equal(0, blob.ReclaimedBytes);
        Assert.True(blob.LogBytes >= 6L * Rows * BlobBytes, $"blob log {blob.LogBytes >> 20} MiB");
        AssertRows(harness, 6);
    }

    [Fact]
    public void A_read_view_open_across_compaction_reads_the_rows_it_pinned()
    {
        using var harness = MelangeLogHarness(compaction: true);
        Rewrite(harness, 1);
        var store = harness.Engine.HotStore;
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;

        using var view = ((IReadViewSource)store).OpenReadView();
        Rewrite(harness, Rounds, firstRound: 1);
        WaitFor(store, "blob", log => log.ReclaimedBytes > 0);

        var pinned = view.Scan(table).ToDictionary(
            pair => ((TerrainBlob)RowSerializer.Deserialize(harness.Engine.Schema.Get(typeof(TerrainBlob)), pair.Value)).ChunkId,
            pair => ((TerrainBlob)RowSerializer.Deserialize(harness.Engine.Schema.Get(typeof(TerrainBlob)), pair.Value)).Data);
        Assert.Equal(Rows, pinned.Count);
        for (var i = 0; i < Rows; i++)
            Assert.Equal(Blob(i, 0), pinned[i]);
        AssertRows(harness, Rounds + 1);
    }

    [Fact]
    public void Promoting_a_paged_table_leaves_none_of_its_bytes_live()
    {
        using var harness = MelangeLogHarness(compaction: true);
        Rewrite(harness, 1);
        var store = harness.Engine.HotStore;
        Assert.True(store.Statistics().Logs.Single(l => l.Space == "blob").LiveBytes > 0);

        ((IResidencyControl)store).ApplyResidency("TerrainBlob", Residency.Resident);

        Assert.All(store.Statistics().Logs, static log => Assert.Equal(0, log.LiveBytes));
        AssertRows(harness, 1);
    }

    /// <summary>
    /// A trigger ratio of 10 would let the log reach ten times its live data. Over the log cap, or
    /// short of disk, compaction ignores it and compacts down to the floor ratio.
    /// </summary>
    [Theory]
    [InlineData("cap")]
    [InlineData("disk")]
    public void Pressure_compacts_past_a_lax_trigger_ratio(string pressure)
    {
        using var harness = MelangeLogHarness(compaction: true, options =>
        {
            options.HotStore.Compaction.TriggerRatio = 10;
            if (pressure == "cap")
                options.HotStore.Compaction.MaxLogBytes = 16L << 20;
            else
                options.HotStore.Compaction.MinFreeDiskBytes = long.MaxValue;
        });
        Rewrite(harness, Rounds);

        var blob = WaitFor(harness.Engine.HotStore, "blob", log => log.LogBytes <= Bound(log));
        Assert.True(blob.ReclaimedBytes > 0, "nothing was reclaimed");
        Assert.True(blob.LogBytes <= Bound(blob), $"blob log {blob.LogBytes >> 20} MiB against {blob.LiveBytes >> 20} MiB live");
        AssertRows(harness, Rounds);
    }

    [Fact]
    public void The_pread_read_mode_is_chosen_by_option()
    {
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options =>
            {
                options.Residency.Default = Residency.Paged;
                options.HotStore.MelangeLog.ReadMode = MelangeLogReadMode.Pread;
            },
            tables: [typeof(TerrainBlob)],
            provider: new FasterHotStoreProvider(PagedEngine.MelangeLog));
        Rewrite(harness, 3);
        AssertRows(harness, 3);
        Assert.True(harness.Engine.HotStore.Statistics().Tables.Single().PageFaults > 0);
    }

    private static StoreHarness MelangeLogHarness(bool compaction, Action<MelangeDbOptions>? configure = null) =>
        new(
            StoreKind.Faster,
            options =>
            {
                options.HotStore.Compaction.Enabled = compaction;
                options.Residency.Default = Residency.Paged;
                configure?.Invoke(options);
            },
            tables: [typeof(TerrainBlob)],
            provider: new FasterHotStoreProvider(new MelangeLogPagedKvFactory()));

    /// <summary>The trigger ratio over live bytes, plus the slack compaction never reclaims: the two segments of garbage it waits for, the ring, and the segment being filled.</summary>
    private static long Bound(HotStoreLogStatistics log) => (long)(2.0 * log.LiveBytes) + 4 * SegmentBytes;

    private static void Rewrite(StoreHarness harness, int rounds, int firstRound = 0)
    {
        for (var round = firstRound; round < firstRound + rounds; round++)
        {
            for (var start = 0; start < Rows; start += 50)
            {
                var version = round;
                var from = start;
                harness.Invoke("rewrite", ctx =>
                {
                    for (var i = from; i < from + 50; i++)
                    {
                        var row = new TerrainBlob { ChunkId = i, Region = i % 8, Data = Blob(i, version) };
                        if (version == 0)
                            ctx.Db.Insert(row);
                        else
                            ctx.Db.Update(row);
                    }
                });
            }
        }
    }

    private static void AssertRows(StoreHarness harness, int rounds)
    {
        Assert.Equal(Rows, harness.Engine.HotStore.Count(harness.Engine.Schema.Get(typeof(TerrainBlob)).Id));
        harness.Invoke("verify", ctx =>
        {
            for (var i = 0; i < Rows; i++)
                Assert.Equal(Blob(i, rounds - 1), ctx.Db.Find<TerrainBlob>((long)i)!.Value.Data);
        });
    }

    private static byte[] Blob(int id, int version) => StoreContractTests.MakeBlob(id * 1000 + version, BlobBytes);

    /// <summary>Compaction runs on its own thread: polls the space's statistics until <paramref name="done"/> or 30 s.</summary>
    private static HotStoreLogStatistics WaitFor(IHotStore store, string space, Func<HotStoreLogStatistics, bool> done)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            var log = store.Statistics().Logs.Single(l => l.Space == space);
            if (done(log) || clock.Elapsed > TimeSpan.FromSeconds(30))
                return log;
            Thread.Sleep(20);
        }
    }

    /// <summary>
    /// The segment files' bytes. Compaction runs on its own thread and deletes a segment it has
    /// emptied, so a file can vanish between being listed and being measured: it counts as zero,
    /// which is what compaction just made it.
    /// </summary>
    private static long DiskBytes(StoreHarness harness) =>
        new DirectoryInfo(harness.Options.HotStore.Path).EnumerateFiles("*.mlog*", SearchOption.AllDirectories).Sum(static f =>
        {
            try
            {
                return f.Length;
            }
            catch (FileNotFoundException)
            {
                return 0L;
            }
        });
}
