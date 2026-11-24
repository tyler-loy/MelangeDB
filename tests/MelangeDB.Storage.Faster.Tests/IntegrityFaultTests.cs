using System.Buffers.Binary;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// A MelangeLog record whose bytes went bad, in memory or on disk: a point read refuses it rather
/// than return wrong bytes, a scan skips it, the integrity counter counts it, compaction leaves its
/// segment in place rather than lose it, and a restart, rebuilding from the commit log, brings it back.
/// </summary>
public sealed class IntegrityFaultTests
{
    [Fact]
    public void A_corrupt_flushed_record_is_refused_counted_and_never_dropped_by_compaction()
    {
        var directory = Directory.CreateTempSubdirectory("melange-mlog-integrity-").FullName;
        using (var kv = new MelangeLogPagedKv(
                   Path.Combine(directory, "bad.mlog"), 12, 3, 14, 16, MelangeLogTuning.Default, 512))
        {
            // Key 3's first version lands in segment 0, then everything else is rewritten many
            // times over, growing each time so every rewrite appends: segment 0 is flushed and
            // nearly all garbage, compaction's first victim.
            for (var round = 0; round < 40; round++)
            {
                var value = new byte[200 + round * 8];
                BinaryPrimitives.WriteInt32LittleEndian(value, round);
                for (var id = 0; id < 50; id++)
                {
                    if (id == 3 && round > 0)
                        continue;
                    kv.Upsert(Key(id), value);
                }

                // From round 1, one key written once per round pins live data in the later
                // segments, more than key 3's, so none is dropped for free and segment 0, holding
                // only key 3, is the cheapest victim: compaction's first.
                if (round > 0)
                    kv.Upsert(Key(1000 + round), new byte[512]);
            }

            Assert.True(kv.CorruptForTest(Key(3)), "the record should have left memory");
            Assert.Throws<InvalidDataException>(() => kv.Read(Key(3), out _));
            Assert.Equal(1, kv.Statistics.IntegrityFailures);

            // Compaction reaches the segment, finds a live record it cannot verify, and gives the
            // segment up for good instead of dropping it: the record is still there to read (wrongly
            // checksummed, so refused) until a rebuild.
            var refused = 0;
            for (var step = 0; step < 10_000; step++)
            {
                kv.PrepareCompactionStep();
                try
                {
                    if (!kv.Compact(1.1, 4096).MoreWork)
                        break;
                }
                catch (InvalidDataException)
                {
                    refused++;
                }
            }

            Assert.True(refused == 1, $"refused {refused}; log {kv.Statistics.LogBytes} live {kv.Statistics.LiveBytes} head {kv.Addresses.Head}; " +
                string.Join(" ", kv.SegmentAccounting.Select(static (a, i) => $"{i}:{a.Live}{(a.Dropped ? "d" : "")}")));
            Assert.False(kv.SegmentAccounting[0].Dropped);
            Assert.Throws<InvalidDataException>(() => kv.Read(Key(3), out _));
            for (var id = 0; id < 50; id++)
            {
                if (id != 3)
                    Assert.Equal(39, BinaryPrimitives.ReadInt32LittleEndian(kv.Read(Key(id), out _)));
            }
        }

        Directory.Delete(directory, recursive: true);
    }

    [Fact]
    public void A_corrupt_row_in_the_store_is_refused_by_a_read_skipped_by_a_scan_and_rebuilt_on_restart()
    {
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.Default = Residency.Paged,
            tables: [typeof(Creature)],
            provider: new FasterHotStoreProvider(new MelangeLogPagedKvFactory()));
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 100; i++)
                ctx.Db.Insert(new Creature { ChunkId = i % 5, Name = $"c{i}", X = i });
        });
        var table = harness.Engine.Schema.Get(typeof(Creature)).Id;
        var store = (FasterHotStore)harness.Engine.HotStore;
        var victim = store.ScanKeys(table).First();

        store.CorruptPagedRowForTest(table, victim);

        Assert.Throws<InvalidDataException>(() => store.TryGetRow(table, victim, out _));
        Assert.Equal(99, store.Scan(table).Count());
        Assert.True(store.Statistics().Logs.Single(static l => l.Space == "main").IntegrityFailures >= 1);

        harness.Restart();
        Assert.Equal(100, harness.Engine.HotStore.Scan(table).Count());
        Assert.True(harness.Engine.HotStore.TryGetRow(table, victim, out _));
    }

    private static byte[] Key(int id)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteInt32BigEndian(key, 42);
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(4), id);
        return key;
    }
}
