using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// melangelog against a dictionary, under randomized upserts, deletes, reads and batched reads. Tiny
/// pages, ring and segments put every path under load: eviction, records spanning pages and segments,
/// in-place shrink and regrow (#137), and blind appends over flushed records, with the live-byte
/// accounting checked against the log at the end. The store suite runs the engine through the whole
/// store (MELANGE_TEST_PAGED_ENGINE=melangelog); this drives the key-value space on its own.
/// </summary>
public sealed class MelangeLogModelTests
{
    public static TheoryData<string, bool, bool, int> Modes => new()
    {
        { "Mmap", false, false, 3 },
        { "Mmap", false, true, 3 },
        { "Mmap", true, true, 3 },
        { "Pread", false, false, 3 },
        { "Pread", false, true, 3 },

        // A two-page ring: the tail page is the only mutable one, and every new page waits on the flusher.
        { "Mmap", false, true, 1 },
    };

    [Theory]
    [MemberData(nameof(Modes))]
    public void Every_read_returns_what_the_model_holds(string readMode, bool pointPread, bool compact, int ringPageBits)
    {
        var mode = Enum.Parse<MelangeLogReadMode>(readMode);
        const int keySpace = 600;
        const int ops = 20_000;
        var directory = Directory.CreateTempSubdirectory("melange-mlog-model-").FullName;
        var random = new Random(7);
        var model = new Dictionary<int, byte[]>();

        // 4 KiB pages, an 8-page ring (or 2), 16 KiB segments: a 20 KiB value spans pages and segments.
        using (var kv = new MelangeLogPagedKv(
                   Path.Combine(directory, "check.mlog"), 12, ringPageBits, 14, 16, MelangeLogTuning.Default with { ReadMode = mode }, 512, pointPread))
        {
            for (var n = 0; n < ops; n++)
            {
                // Compaction as hard as it goes: any garbage past 10% triggers it, a few KiB a step,
                // so segments are walked and dropped while every other path runs around them.
                if (compact && n % 25 == 0)
                {
                    kv.PrepareCompactionStep();
                    kv.Compact(1.1, 4096);
                    if (n % 1000 == 0)
                        AssertAccounting(kv);
                }

                var id = random.Next(keySpace);
                var key = Key(id);
                switch (random.Next(10))
                {
                    case < 5:
                        var value = Value(random, id, n);
                        kv.Upsert(key, value);
                        model[id] = value;
                        break;
                    case 5:
                        kv.Delete(key);
                        model.Remove(id);
                        break;
                    case 6:
                        var ids = new int[random.Next(1, 40)];
                        var keys = new byte[ids.Length][];
                        for (var i = 0; i < ids.Length; i++)
                            keys[i] = Key(ids[i] = random.Next(keySpace));
                        var values = new byte[]?[ids.Length];
                        _ = kv.ReadMany(keys, values);
                        for (var i = 0; i < ids.Length; i++)
                            Expect(model, ids[i], values[i], n);
                        break;
                    default:
                        Expect(model, id, kv.Read(key, out _), n);
                        break;
                }
            }

            for (var id = 0; id < keySpace; id++)
                Expect(model, id, kv.Read(Key(id), out _), ops);
            Assert.Equal(model.Count, kv.Addresses.IndexEntries);
            AssertAccounting(kv);

            // Every live byte is some current record's: delete them all and nothing is left counted.
            foreach (var id in model.Keys)
                kv.Delete(Key(id));
            AssertAccounting(kv);
            Assert.Equal(0, kv.Statistics.LiveBytes);
            Assert.All(kv.SegmentAccounting, static segment => Assert.Equal(0, segment.Live));

            // Nothing live: compaction drops every segment that has been flushed.
            if (compact)
            {
                while (kv.Compact(1.1, 1 << 20).MoreWork)
                {
                }

                var flushed = kv.Addresses.Head >> 14;
                Assert.All(kv.SegmentAccounting.Take((int)flushed), static segment => Assert.True(segment.Dropped));
                Assert.True(kv.Statistics.ReclaimedBytes > 0);
                Assert.True(kv.Statistics.RelocatedBytes > 0);
                Assert.True(kv.Statistics.LogBytes <= kv.Addresses.Tail - (flushed << 14));
            }
        }

        Assert.Empty(Directory.EnumerateFiles(directory));
        Directory.Delete(directory);
    }

    /// <summary>
    /// Two keys whose hashes collide (forced, with a 3-bit mask on both halves) are one slot. That is
    /// the design: at 128 bits a real collision does not happen. A read still compares the stored key,
    /// so the second key gets an error, never the first key's value.
    /// </summary>
    [Fact]
    public void A_forced_hash_collision_is_detected_not_returned()
    {
        var directory = Directory.CreateTempSubdirectory("melange-mlog-collision-").FullName;
        using (var kv = new MelangeLogPagedKv(
                   Path.Combine(directory, "check.mlog"), 12, 3, 14, 16, MelangeLogTuning.Default, 512, hashMaskForTest: 0x7UL))
        {
            kv.Upsert(Key(0), [1, 2, 3]);
            var other = Enumerable.Range(1, 10_000).First(id =>
            {
                try
                {
                    return kv.Read(Key(id), out _) is not null;
                }
                catch (InvalidDataException)
                {
                    return true;
                }
            });

            Assert.Throws<InvalidDataException>(() => kv.Read(Key(other), out _));
            Assert.Throws<InvalidDataException>(() => kv.Upsert(Key(other), [4]));
        }

        Directory.Delete(directory, recursive: true);
    }

    /// <summary>The segments' live counts add up to the space's, and none is negative.</summary>
    private static void AssertAccounting(MelangeLogPagedKv kv)
    {
        var segments = kv.SegmentAccounting;
        Assert.All(segments, static segment => Assert.True(segment.Live >= 0, $"segment live bytes {segment.Live}"));
        Assert.Equal(kv.Statistics.LiveBytes, segments.Sum(static segment => segment.Live));
        Assert.True(kv.Statistics.LiveBytes <= kv.Statistics.LogBytes);
    }

    private static void Expect(Dictionary<int, byte[]> model, int id, byte[]? actual, int op)
    {
        var expected = model.GetValueOrDefault(id);
        if (expected is null ? actual is not null : actual is null || !actual.AsSpan().SequenceEqual(expected))
        {
            Assert.Fail(
                $"op {op}: key {id} read {(actual is null ? "null" : actual.Length + " bytes")}, expected {(expected is null ? "null" : expected.Length + " bytes")}");
        }
    }

    private static byte[] Key(int id)
    {
        var key = new byte[4 + 1 + (id % 5)];
        BinaryPrimitives.WriteInt32BigEndian(key, 42);
        key[4] = (byte)id;
        for (var i = 5; i < key.Length; i++)
            key[i] = (byte)(id >> 8);
        return key;
    }

    /// <summary>Mostly small (in-place shrink and regrow), some page-sized, a few spanning several pages.</summary>
    private static byte[] Value(Random random, int id, int op)
    {
        var length = random.Next(20) switch
        {
            < 14 => random.Next(0, 200),
            < 19 => random.Next(200, 5000),
            _ => random.Next(5000, 20000),
        };
        var value = new byte[length];
        random.NextBytes(value);
        if (length >= 8)
            BinaryPrimitives.WriteInt64LittleEndian(value, ((long)id << 32) | (uint)op);
        return value;
    }
}
