using System.Runtime.CompilerServices;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>Byte-wise ordering, exactly as the tree's plain order defines it.</summary>
internal sealed class BytewiseComparer : IComparer<byte[]>
{
    public static readonly BytewiseComparer Instance = new();

    public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
}

internal static class NativeTreeAssert
{
    /// <summary>
    /// Collects until the pool's native bytes reach zero. A version's finalizer frees its pages,
    /// and objects that only a finalizable object kept alive become collectable one GC later, so a
    /// few rounds may be needed — but every round must make progress or the test fails.
    /// </summary>
    public static void Reclaimed(NativePagePool pool)
    {
        for (var round = 0; round < 10 && pool.AllocatedBytes != 0; round++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.Equal(0, pool.AllocatedBytes);
        Assert.Equal(0, pool.PagesInUse);
        Assert.Equal(0, pool.BlobsInUse);
    }

    /// <summary>The tree holds exactly the model: count, validation, enumeration, point and range reads.</summary>
    public static void Matches(NativeTree tree, IReadOnlyList<KeyValuePair<byte[], byte[]>> model, Random random, int probes = 20)
    {
        Assert.Equal(model.Count, tree.Count);
        var stats = tree.Validate();
        Assert.Equal(model.Count, stats.Entries);

        var index = 0;
        foreach (var (key, value) in tree.Entries())
        {
            Assert.True(index < model.Count, "tree enumerates more entries than the model holds");
            Assert.Equal(model[index].Key, key);
            Assert.Equal(model[index].Value, value);
            index++;
        }

        Assert.Equal(model.Count, index);

        for (var i = 0; i < probes && model.Count > 0; i++)
        {
            var (key, value) = model[random.Next(model.Count)];
            Assert.True(tree.TryGet(key, out var found));
            Assert.Equal(value, found);
            Assert.True(tree.ContainsKey(key));
            Assert.True(tree.Contains(key, value));
        }

        for (var i = 0; i < probes; i++)
        {
            var low = RandomKey(random);
            var high = RandomKey(random);
            if (BytewiseComparer.Instance.Compare(low, high) > 0)
                (low, high) = (high, low);
            var expected = model.Where(p => BytewiseComparer.Instance.Compare(p.Key, low) >= 0 && BytewiseComparer.Instance.Compare(p.Key, high) <= 0)
                .Select(p => p.Key)
                .ToList();
            Assert.Equal(expected, tree.Keys(low, high).ToList());
            if (i % 4 == 0)
            {
                var exists = BinarySearch(model, low) >= 0;
                Assert.Equal(exists, tree.ContainsKey(low));
                Assert.Equal(exists, tree.TryGet(low, out _));
            }
        }
    }

    public static int BinarySearch(IReadOnlyList<KeyValuePair<byte[], byte[]>> model, byte[] key)
    {
        int lo = 0, hi = model.Count - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >>> 1;
            var c = BytewiseComparer.Instance.Compare(model[mid].Key, key);
            if (c == 0)
                return mid;
            if (c < 0)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return ~lo;
    }

    private static readonly byte[] Alphabet = [0, 1, 2, 0x7F, 0xFE, 0xFF];

    /// <summary>
    /// Short keys over a tiny alphabet: collisions, shared prefixes, keys that are prefixes of
    /// others, and the empty key all turn up often.
    /// </summary>
    public static byte[] RandomKey(Random random)
    {
        var key = new byte[random.Next(0, 9)];
        for (var i = 0; i < key.Length; i++)
            key[i] = Alphabet[random.Next(Alphabet.Length)];
        return key;
    }

    /// <summary>Mostly small values; some long enough to go out of line on small pages.</summary>
    public static byte[] RandomValue(Random random)
    {
        var length = random.Next(20) switch
        {
            < 16 => random.Next(0, 24),
            < 19 => random.Next(24, 200),
            _ => random.Next(200, 3000),
        };
        var value = new byte[length];
        random.NextBytes(value);
        return value;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public static void Run(Action action) => action();
}
