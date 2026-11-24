using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// melangelog against a dictionary, under randomized upserts, deletes, reads and batched reads. Tiny
/// pages, ring and segments put every path under load: eviction, records spanning pages and segments,
/// in-place shrink and regrow (#137), blind appends over flushed records, and, with a 3-bit hash mask,
/// hash chains shared by many keys, tombstones included. The store suite runs the engine through the
/// whole store (MELANGE_TEST_PAGED_ENGINE=melangelog); this drives the key-value space on its own.
/// </summary>
public sealed class MelangeLogModelTests
{
    public static TheoryData<string, bool, ulong> Modes => new()
    {
        { "Mmap", false, ulong.MaxValue },
        { "Mmap", false, 0x7UL },
        { "Mmap", true, ulong.MaxValue },
        { "Mmap", true, 0x7UL },
        { "Pread", false, ulong.MaxValue },
        { "Pread", false, 0x7UL },
    };

    [Theory]
    [MemberData(nameof(Modes))]
    public void Every_read_returns_what_the_model_holds(string readMode, bool pointPread, ulong hashMask)
    {
        var mode = Enum.Parse<MelangeLogReadMode>(readMode);
        const int keySpace = 600;
        const int ops = 20_000;
        var directory = Directory.CreateTempSubdirectory("melange-mlog-model-").FullName;
        var random = new Random(7);
        var model = new Dictionary<int, byte[]>();

        // 4 KiB pages, an 8-page ring, 16 KiB segments: a 20 KiB value spans pages and segments.
        using (var kv = new MelangeLogPagedKv(
                   Path.Combine(directory, "check.mlog"), 12, 3, 14, 16, MelangeLogTuning.Default with { ReadMode = mode }, 512, pointPread, hashMask))
        {
            for (var n = 0; n < ops; n++)
            {
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
            if (hashMask == ulong.MaxValue)
                Assert.Equal(model.Count, kv.Addresses.IndexEntries);
        }

        Assert.Empty(Directory.EnumerateFiles(directory));
        Directory.Delete(directory);
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
