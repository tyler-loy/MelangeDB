using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// <see cref="NativeTree"/> against ordinary in-memory models. Small pages (256 and 512 bytes) force
/// deep trees, frequent splits and merges, and out-of-line parts at modest sizes, so the structural
/// paths run thousands of times per test; the default page size checks the common shape. Every test
/// owns its pool and ends by proving the pool returns to exactly zero native bytes once its versions
/// are unreachable.
/// </summary>
public sealed class NativeTreeTests
{
    public static TheoryData<int, int> PageSizesAndSeeds()
    {
        var data = new TheoryData<int, int>();
        foreach (var pageSize in new[] { 256, 512, NativePagePool.DefaultPageSize })
        {
            for (var seed = 1; seed <= 4; seed++)
                data.Add(pageSize, seed);
        }

        return data;
    }

    /// <summary>
    /// Random sets, replaces and removes in edits of 1 to 200 operations. After each commit the new
    /// version must equal the model; a sample of older versions is kept and re-verified against the
    /// model as it was when they were committed — which is what proves that later edits copied
    /// rather than wrote through shared pages.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageSizesAndSeeds))]
    public void Every_version_matches_the_model_it_was_committed_from(int pageSize, int seed)
    {
        var pool = new NativePagePool(pageSize);
        NativeTreeAssert.Run(() =>
        {
            var random = new Random(seed);
            var model = new SortedDictionary<byte[], byte[]>(BytewiseComparer.Instance);
            var tree = NativeTree.Empty(pool, NativeTreeOrder.Plain);
            var kept = new List<(NativeTree Tree, List<KeyValuePair<byte[], byte[]>> Snapshot)>();
            var commits = pageSize == NativePagePool.DefaultPageSize ? 120 : 160;
            for (var commit = 0; commit < commits; commit++)
            {
                var operations = random.Next(4) == 0 ? random.Next(1, 201) : random.Next(1, 6);
                var edit = tree.BeginEdit();
                for (var op = 0; op < operations; op++)
                {
                    var key = NativeTreeAssert.RandomKey(random);
                    // Remove less often than set early on, so the tree grows, then churns.
                    if (random.Next(10) < (model.Count < 300 ? 3 : 5))
                    {
                        Assert.Equal(model.Remove(key), edit.Remove(key));
                    }
                    else
                    {
                        var value = NativeTreeAssert.RandomValue(random);
                        var added = !model.ContainsKey(key);
                        model[key] = value;
                        Assert.Equal(added, edit.Set(key, value));
                    }

                    if (op % 50 == 49)
                    {
                        Assert.Equal(model.Count, edit.Count);
                        var probe = NativeTreeAssert.RandomKey(random);
                        Assert.Equal(model.TryGetValue(probe, out var expected), edit.TryGet(probe, out var actual));
                        Assert.Equal(expected, actual);
                        Assert.Equal(model.Count, edit.Validate().Entries);
                    }
                }

                tree = edit.Commit();
                var snapshot = model.ToList();
                NativeTreeAssert.Matches(tree, snapshot, random, probes: 8);

                if (random.Next(8) == 0)
                {
                    if (kept.Count == 6)
                        kept.RemoveAt(random.Next(kept.Count));
                    kept.Add((tree, snapshot));
                }

                if (commit % 10 == 0)
                {
                    foreach (var (old, oldSnapshot) in kept)
                        NativeTreeAssert.Matches(old, oldSnapshot, random, probes: 4);
                }
            }

            foreach (var (old, oldSnapshot) in kept)
                NativeTreeAssert.Matches(old, oldSnapshot, random, probes: 4);
        });

        NativeTreeAssert.Reclaimed(pool);
    }

    /// <summary>
    /// A pair tree orders exactly as <see cref="IndexEntry.CompareTo"/>, answers the same ranges as
    /// <see cref="SecondaryIndex"/>, and finds the first entry of a value by seeking (value, empty) —
    /// over data dense in empty parts, shared prefixes and values held by many keys.
    /// </summary>
    [Theory]
    [InlineData(256, 1)]
    [InlineData(256, 2)]
    [InlineData(512, 3)]
    [InlineData(NativePagePool.DefaultPageSize, 4)]
    public void Pair_tree_orders_and_seeks_like_the_secondary_index(int pageSize, int seed)
    {
        var pool = new NativePagePool(pageSize);
        NativeTreeAssert.Run(() =>
        {
            var random = new Random(seed);
            var model = new SortedSet<IndexEntry>();
            var index = SecondaryIndex.Empty;
            var tree = NativeTree.Empty(pool, NativeTreeOrder.Pair);
            for (var commit = 0; commit < 60; commit++)
            {
                var edit = tree.BeginEdit();
                var operations = random.Next(1, 120);
                for (var op = 0; op < operations; op++)
                {
                    var value = Short(random);
                    var key = Short(random);
                    var entry = new IndexEntry(new RowKey(value), new RowKey(key));
                    if (random.Next(10) < 3 && model.Count > 0)
                    {
                        // Remove something present most of the time.
                        if (random.Next(3) > 0)
                        {
                            entry = model.ElementAt(random.Next(model.Count));
                            value = entry.Value.ToArray();
                            key = entry.Key.ToArray();
                        }

                        Assert.Equal(model.Remove(entry), edit.Remove(value, key));
                        index = index.Remove(entry.Value, entry.Key);
                    }
                    else
                    {
                        Assert.Equal(model.Add(entry), edit.Set(value, key));
                        index = index.Add(entry.Value, entry.Key);
                    }
                }

                tree = edit.Commit();
                Assert.Equal(model.Count, tree.Count);
                tree.Validate();

                // Full order, pair by pair, is IndexEntry's order.
                var actual = tree.Scan(static (v, k) => new IndexEntry(new RowKey(v), new RowKey(k))).ToList();
                Assert.Equal(model.ToList(), actual);

                for (var probe = 0; probe < 20; probe++)
                {
                    var low = Short(random);
                    var high = Short(random);
                    if (low.AsSpan().SequenceCompareTo(high) > 0)
                        (low, high) = (high, low);
                    var expected = index.Range(new RowKey(low), new RowKey(high)).Select(k => k.ToArray()).ToList();
                    Assert.Equal(expected, tree.Values(low, high).ToList());

                    // Seek by value alone: the first entry holding it.
                    var first = model.FirstOrDefault(e => e.Value.Span.SequenceEqual(low));
                    var exists = model.Any(e => e.Value.Span.SequenceEqual(low));
                    Assert.Equal(exists, tree.ContainsKey(low));
                    Assert.Equal(exists, tree.TryGet(low, out var firstKey));
                    if (exists)
                        Assert.Equal(first.Key.ToArray(), firstKey);

                    // Resuming mid-value from an exact pair.
                    if (model.Count > 0)
                    {
                        var from = model.ElementAt(random.Next(model.Count));
                        var resumed = tree.RangeFrom(from.Value.ToArray(), from.Key.ToArray(), null, static (v, k) => new IndexEntry(new RowKey(v), new RowKey(k)));
                        Assert.Equal(model.GetViewBetween(from, model.Max).ToList(), resumed.ToList());
                        Assert.True(tree.Contains(from.Value.ToArray(), from.Key.ToArray()));
                    }
                }
            }
        });

        NativeTreeAssert.Reclaimed(pool);

        static byte[] Short(Random random)
        {
            var bytes = new byte[random.Next(0, 4)];
            for (var i = 0; i < bytes.Length; i++)
                bytes[i] = (byte)random.Next(3);
            return bytes;
        }
    }

    /// <summary>
    /// Keys of 4 KiB that share long prefixes (so separators are long too) and values up to 64 KiB,
    /// mixed with small entries, replaced with larger and smaller ones. Long parts live out of line
    /// and are shared, not copied, between versions.
    /// </summary>
    [Theory]
    [InlineData(512)]
    [InlineData(NativePagePool.DefaultPageSize)]
    public void Large_keys_and_values_go_out_of_line_and_survive_replacement(int pageSize)
    {
        var pool = new NativePagePool(pageSize);
        NativeTreeAssert.Run(() =>
        {
            var random = new Random(pageSize);
            var model = new SortedDictionary<byte[], byte[]>(BytewiseComparer.Instance);
            var tree = NativeTree.Empty(pool, NativeTreeOrder.Plain);
            var kept = new List<(NativeTree, List<KeyValuePair<byte[], byte[]>>)>();
            var prefix = new byte[4000];
            random.NextBytes(prefix);
            for (var commit = 0; commit < 40; commit++)
            {
                var edit = tree.BeginEdit();
                for (var op = 0; op < 10; op++)
                {
                    var key = random.Next(3) == 0 ? Small(random) : Large(random, prefix);
                    if (random.Next(6) == 0 && model.Count > 0)
                    {
                        key = model.Keys.ElementAt(random.Next(model.Count));
                        Assert.True(edit.Remove(key));
                        model.Remove(key);
                        continue;
                    }

                    if (random.Next(2) == 0 && model.Count > 0)
                        key = model.Keys.ElementAt(random.Next(model.Count)); // replace: grow or shrink
                    var value = new byte[random.Next(4) switch
                    {
                        0 => random.Next(0, 16),
                        1 => random.Next(16, 2000),
                        2 => random.Next(2000, 20000),
                        _ => 64 * 1024,
                    }];
                    random.NextBytes(value);
                    edit.Set(key, value);
                    model[key] = value;
                }

                tree = edit.Commit();
                var snapshot = model.ToList();
                NativeTreeAssert.Matches(tree, snapshot, random, probes: 5);
                Assert.True(tree.Validate().OutOfLineParts > 0);
                if (commit % 8 == 0)
                    kept.Add((tree, snapshot));
            }

            Assert.True(pool.BlobsInUse > 0);
            foreach (var (old, snapshot) in kept)
                NativeTreeAssert.Matches(old, snapshot, random, probes: 5);
        });

        NativeTreeAssert.Reclaimed(pool);

        static byte[] Small(Random random)
        {
            var key = new byte[random.Next(1, 12)];
            random.NextBytes(key);
            return key;
        }

        static byte[] Large(Random random, byte[] prefix)
        {
            var key = new byte[4096];
            prefix.CopyTo(key, 0);
            random.NextBytes(key.AsSpan(prefix.Length));
            return key;
        }
    }

    /// <summary>
    /// Insert many, delete most, insert again: merges and redistribution keep the page count in
    /// proportion to the entries and the depth from ratcheting up.
    /// </summary>
    [Theory]
    [InlineData(256)]
    [InlineData(512)]
    [InlineData(NativePagePool.DefaultPageSize)]
    public void Churn_returns_pages_and_depth_to_proportion(int pageSize)
    {
        var pool = new NativePagePool(pageSize);
        NativeTreeAssert.Run(() =>
        {
            var random = new Random(11);
            const int n = 30_000;
            var keys = Enumerable.Range(0, n).Select(i => Key((uint)i)).OrderBy(_ => random.Next()).ToArray();
            var value = new byte[12];
            var tree = NativeTree.Empty(pool, NativeTreeOrder.Plain);
            tree = Apply(tree, keys, k => (k, value));
            var full = tree.Validate();

            // Delete 95%, in random order, a few hundred per edit.
            var doomed = keys.OrderBy(_ => random.Next()).Take(n * 95 / 100).ToArray();
            var before = tree;
            tree = Apply(tree, doomed, k => (k, null));
            var thin = tree.Validate();
            Assert.Equal(n - doomed.Length, thin.Entries);
            Assert.True(thin.Height <= full.Height);
            Assert.True(thin.Fill(pageSize) > 0.3, $"fill {thin.Fill(pageSize):P0} after deletes ({thin.Pages} pages)");
            Assert.True(thin.Pages <= full.Pages / 8, $"{thin.Pages} pages for 5% of the entries that needed {full.Pages}");

            // Once the superseded versions are gone, the pool holds exactly the live tree's pages:
            // deleted entries left no pages behind. The intermediate versions go to the finalizer;
            // the full one is released explicitly, since a caller frame may still reference it.
            before.Release();
            for (var round = 0; round < 5 && pool.PagesInUse != thin.Pages; round++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            Assert.Equal(thin.Pages, pool.PagesInUse);

            tree = Apply(tree, doomed, k => (k, value));
            var again = tree.Validate();
            Assert.Equal(n, again.Entries);
            Assert.True(again.Height <= full.Height + 1);
            Assert.True(again.Pages <= full.Pages * 3 / 2, $"{again.Pages} pages after churn, {full.Pages} before");

            // And all the way down to nothing.
            tree = Apply(tree, keys, k => (k, null));
            Assert.Equal(0, tree.Count);
            Assert.Equal(0, tree.Height);
        });

        NativeTreeAssert.Reclaimed(pool);

        static NativeTree Apply(NativeTree tree, byte[][] keys, Func<byte[], (byte[] Key, byte[]? Value)> op)
        {
            for (var start = 0; start < keys.Length; start += 300)
            {
                var edit = tree.BeginEdit();
                foreach (var key in keys.AsSpan(start, Math.Min(300, keys.Length - start)))
                {
                    var (k, v) = op(key);
                    if (v is null)
                        Assert.True(edit.Remove(k));
                    else
                        edit.Set(k, v);
                }

                tree = edit.Commit();
            }

            return tree;
        }
    }

    /// <summary>
    /// The bulk builder produces the model's tree with packed pages, rejects unordered input, and
    /// hands back an ordinary version that edits like any other.
    /// </summary>
    [Theory]
    [InlineData(256, 0)]
    [InlineData(256, 1)]
    [InlineData(256, 700)]
    [InlineData(512, 20_000)]
    [InlineData(NativePagePool.DefaultPageSize, 1)]
    [InlineData(NativePagePool.DefaultPageSize, 100_000)]
    public void Builder_matches_the_model_and_edits_afterwards(int pageSize, int count)
    {
        var pool = new NativePagePool(pageSize);
        NativeTreeAssert.Run(() =>
        {
            var random = new Random(count);
            var model = new SortedDictionary<byte[], byte[]>(BytewiseComparer.Instance);
            while (model.Count < count)
            {
                var key = new byte[random.Next(1, 10)];
                random.NextBytes(key);
                model[key] = NativeTreeAssert.RandomValue(random);
            }

            using var builder = new NativeTree.Builder(pool, NativeTreeOrder.Plain);
            foreach (var (key, value) in model)
                builder.Add(key, value);
            if (count > 0)
            {
                var last = model.Keys.Last();
                Assert.Throws<ArgumentException>(() => builder.Add(last, []));
                Assert.Throws<ArgumentException>(() => builder.Add(model.Keys.First(), []));
            }

            var tree = builder.ToTree();
            Assert.Throws<ObjectDisposedException>(() => builder.Add([1], []));
            NativeTreeAssert.Matches(tree, model.ToList(), random);
            var stats = tree.Validate();
            if (stats.LeafPages > 2)
                Assert.True(stats.Fill(pageSize) > 0.75, $"built fill {stats.Fill(pageSize):P0}");

            var edit = tree.BeginEdit();
            for (var op = 0; op < 500; op++)
            {
                var key = NativeTreeAssert.RandomKey(random);
                if (random.Next(3) == 0)
                {
                    Assert.Equal(model.Remove(key), edit.Remove(key));
                }
                else
                {
                    var value = NativeTreeAssert.RandomValue(random);
                    edit.Set(key, value);
                    model[key] = value;
                }
            }

            var edited = edit.Commit();
            NativeTreeAssert.Matches(edited, model.ToList(), random);
            Assert.Equal(count, tree.Count);
        });

        NativeTreeAssert.Reclaimed(pool);
    }

    [Fact]
    public void Builder_builds_pair_trees_and_abandoned_builds_free_everything()
    {
        var pool = new NativePagePool(256);
        NativeTreeAssert.Run(() =>
        {
            var entries = new SortedSet<IndexEntry>();
            var random = new Random(5);
            while (entries.Count < 3000)
            {
                var value = new byte[random.Next(0, 3)];
                random.NextBytes(value);
                var key = new byte[random.Next(0, 6)];
                random.NextBytes(key);
                entries.Add(new IndexEntry(new RowKey(value), new RowKey(key)));
            }

            using var builder = new NativeTree.Builder(pool, NativeTreeOrder.Pair);
            foreach (var entry in entries)
                builder.Add(entry.Value.Span, entry.Key.Span);
            var tree = builder.ToTree();
            tree.Validate();
            Assert.Equal(entries.ToList(), tree.Scan(static (v, k) => new IndexEntry(new RowKey(v), new RowKey(k))).ToList());

            // Abandoned midway, including out-of-line parts and pending separators.
            var before = pool.AllocatedBytes;
            using (var abandoned = new NativeTree.Builder(pool, NativeTreeOrder.Plain))
            {
                for (var i = 0; i < 5000; i++)
                    abandoned.Add(Key((uint)i), new byte[i % 7 == 0 ? 500 : 8]);
            }

            Assert.Equal(before, pool.AllocatedBytes);
            GC.KeepAlive(tree);
        });

        NativeTreeAssert.Reclaimed(pool);
    }

    [Fact]
    public void Abandoned_edits_free_what_they_allocated()
    {
        var pool = new NativePagePool(512);
        NativeTreeAssert.Run(() =>
        {
            var tree = Fill(pool, 2000);
            var before = pool.AllocatedBytes;

            using (var edit = tree.BeginEdit())
            {
                for (var i = 0; i < 3000; i++)
                    edit.Set(Key((uint)(i * 7)), new byte[i % 10 == 0 ? 300 : 12]);
                for (var i = 0; i < 1000; i++)
                    edit.Remove(Key((uint)i));
                Assert.True(pool.AllocatedBytes > before);
            }

            Assert.Equal(before, pool.AllocatedBytes);

            // Dropped without Dispose: the finalizer does the same.
            AbandonEdit(tree);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            Assert.Equal(before, pool.AllocatedBytes);

            // An edit that changes nothing commits to the same version.
            var unchanged = tree.BeginEdit();
            Assert.False(unchanged.Remove([9, 9, 9, 9, 9]));
            Assert.Same(tree, unchanged.Commit());
            Assert.Equal(before, pool.AllocatedBytes);
            Assert.Throws<ObjectDisposedException>(() => unchanged.Set([1], [1]));
        });

        NativeTreeAssert.Reclaimed(pool);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void AbandonEdit(NativeTree tree)
        {
            var edit = tree.BeginEdit();
            for (var i = 0; i < 500; i++)
                edit.Set(Key((uint)(i * 13)), new byte[40]);
        }
    }

    [Fact]
    public void Released_versions_free_their_pages_and_refuse_reads()
    {
        var pool = new NativePagePool(512);
        NativeTreeAssert.Run(() =>
        {
            var first = Fill(pool, 1000);
            var edit = first.BeginEdit();
            edit.Set(Key(5), [1, 2, 3]);
            var second = edit.Commit();

            // The second version shares all but one path with the first; releasing the first frees
            // only what the second does not hold.
            var shared = pool.PagesInUse;
            first.Release();
            Assert.True(pool.PagesInUse < shared);
            Assert.True(pool.PagesInUse >= second.Validate().Pages);
            Assert.Throws<ObjectDisposedException>(() => first.ContainsKey(Key(5)));
            Assert.Throws<ObjectDisposedException>(() => first.Keys().ToList());

            Assert.True(second.TryGet(Key(5), out var value));
            Assert.Equal([1, 2, 3], value);
            Assert.Equal(1000, second.Keys().Count());
            second.Release();
            Assert.Equal(0, pool.AllocatedBytes);
        });

        NativeTreeAssert.Reclaimed(pool);
    }

    /// <summary>An enumerator mid-walk keeps its version — and so its pages — alive after the caller lets go.</summary>
    [Fact]
    public void An_enumerator_keeps_its_version_alive()
    {
        // Only the box reaches the enumerator, so once it is emptied nothing does — no caller
        // frame holds the enumerator or its version in a temporary.
        var pool = new NativePagePool(256);
        var box = new StrongBox<IEnumerator<byte[]>?>();
        Start(pool, box);
        for (var i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.True(pool.PagesInUse > 0);
        Assert.Equal(3000, Drain(box));
        NativeTreeAssert.Reclaimed(pool);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void Start(NativePagePool pool, StrongBox<IEnumerator<byte[]>?> box)
        {
            box.Value = Fill(pool, 3000).Keys().GetEnumerator();
            Assert.True(box.Value.MoveNext());
            Assert.Equal(Key(0), box.Value.Current);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static int Drain(StrongBox<IEnumerator<byte[]>?> box)
        {
            var enumerator = box.Value!;
            box.Value = null;
            var count = 1;
            while (enumerator.MoveNext())
            {
                Assert.Equal(Key((uint)count), enumerator.Current);
                count++;
            }

            enumerator.Dispose();
            return count;
        }
    }

    [Fact]
    public void Value_projection_reads_in_place()
    {
        var pool = new NativePagePool();
        NativeTreeAssert.Run(() =>
        {
            var edit = NativeTree.Empty(pool, NativeTreeOrder.Plain).BeginEdit();
            var value = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(value, 0x1234_5678_9ABC);
            edit.Set([1], value);
            edit.Set([], [7]);
            var tree = edit.Commit();
            Assert.True(tree.TryGet([1], static v => BinaryPrimitives.ReadInt64LittleEndian(v), out var read));
            Assert.Equal(0x1234_5678_9ABC, read);
            Assert.False(tree.TryGet([2], static v => v.Length, out _));
            Assert.True(tree.TryGet([], out var empty));
            Assert.Equal([7], empty);
            Assert.Equal([[], [1]], tree.Keys().ToArray());
            Assert.Throws<InvalidOperationException>(() => tree.BeginEdit().Remove([1], [2]));
        });

        NativeTreeAssert.Reclaimed(pool);
    }

    internal static byte[] Key(uint id)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(key, 7);
        BinaryPrimitives.WriteUInt32BigEndian(key.AsSpan(4), id);
        return key;
    }

    private static NativeTree Fill(NativePagePool pool, int count)
    {
        var edit = NativeTree.Empty(pool, NativeTreeOrder.Plain).BeginEdit();
        for (var i = 0; i < count; i++)
            edit.Set(Key((uint)i), new byte[12]);
        return edit.Commit();
    }
}
