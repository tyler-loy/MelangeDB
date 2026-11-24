using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// One writer committing continuously while readers walk the latest version and older ones they
/// hold on to, with the finalizer thread freeing superseded versions underneath both. Each version
/// is published with its number and a content hash the writer maintains incrementally from its own
/// model, so a reader verifies a version's <em>entire</em> content — not just that it looks sane —
/// against what was committed, however long after the commit it reads it.
/// </summary>
public sealed class NativeTreeConcurrencyTests
{
    private sealed record Published(NativeTree Tree, long Number, ulong Hash);

    [Theory]
    [InlineData(256)]
    [InlineData(NativePagePool.DefaultPageSize)]
    public void Readers_see_exactly_what_each_version_published_while_the_writer_and_finalizer_run(int pageSize)
    {
        var pool = new NativePagePool(pageSize);
        var (commits, reads) = Run(pool, TimeSpan.FromSeconds(2));
        Assert.True(commits > 100, $"only {commits} commits");
        Assert.True(reads > 20, $"only {reads} full reads");
        NativeTreeAssert.Reclaimed(pool);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (long Commits, long Reads) Run(NativePagePool pool, TimeSpan duration)
    {
        Published? latest = new(NativeTree.Empty(pool, NativeTreeOrder.Plain), 0, 0);
        var stop = 0;
        var reads = 0L;
        var failures = new List<Exception>();

        var readers = Enumerable.Range(0, 4).Select(r => new Thread(() =>
        {
            try
            {
                var random = new Random(r);
                var held = new List<Published>();
                while (Volatile.Read(ref stop) == 0)
                {
                    var current = Volatile.Read(ref latest)!;
                    if (random.Next(4) == 0)
                    {
                        if (held.Count == 4)
                            held.RemoveAt(random.Next(held.Count));
                        held.Add(current);
                    }

                    var subject = held.Count > 0 && random.Next(2) == 0 ? held[random.Next(held.Count)] : current;
                    Verify(subject, random);
                    Interlocked.Increment(ref reads);
                }
            }
            catch (Exception e)
            {
                lock (failures)
                    failures.Add(e);
                Volatile.Write(ref stop, 1);
            }
        })).ToList();
        readers.ForEach(t => t.Start());

        // The writer: a model of key -> value, and an XOR of every entry's hash maintained with it.
        var random = new Random(99);
        var model = new Dictionary<uint, byte[]>();
        ulong hash = 0;
        var number = 0L;
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < duration && Volatile.Read(ref stop) == 0)
        {
            number++;
            var edit = latest!.Tree.BeginEdit();
            var operations = random.Next(5) == 0 ? random.Next(50, 300) : random.Next(1, 8);
            for (var op = 0; op < operations; op++)
            {
                var id = (uint)random.Next(4000);
                var key = NativeTreeTests.Key(id);
                if (model.TryGetValue(id, out var old))
                    hash ^= EntryHash(key, old);
                if (random.Next(10) < 3)
                {
                    edit.Remove(key);
                    model.Remove(id);
                }
                else
                {
                    var value = new byte[random.Next(8, random.Next(20) == 0 ? 3000 : 40)];
                    random.NextBytes(value);
                    BinaryPrimitives.WriteInt64LittleEndian(value, number);
                    edit.Set(key, value);
                    model[id] = value;
                    hash ^= EntryHash(key, value);
                }
            }

            Volatile.Write(ref latest, new Published(edit.Commit(), number, hash));
        }

        Volatile.Write(ref stop, 1);
        readers.ForEach(t => t.Join());
        latest = null;
        if (failures.Count > 0)
            throw new AggregateException(failures);
        return (number, reads);
    }

    private static void Verify(Published published, Random random)
    {
        var tree = published.Tree;
        ulong hash = 0;
        long count = 0;
        byte[]? previous = null;
        foreach (var (key, value) in tree.Entries())
        {
            if (previous is not null)
                Assert.True(previous.AsSpan().SequenceCompareTo(key) < 0, "keys out of order");
            var written = BinaryPrimitives.ReadInt64LittleEndian(value);
            Assert.True(written >= 1 && written <= published.Number, $"version {published.Number} holds a value from {written}");
            hash ^= EntryHash(key, value);
            previous = key;
            count++;
        }

        Assert.Equal(tree.Count, count);
        Assert.Equal(published.Hash, hash);

        for (var i = 0; i < 20; i++)
        {
            var key = NativeTreeTests.Key((uint)random.Next(4000));
            if (tree.TryGet(key, out var value))
                Assert.True(BinaryPrimitives.ReadInt64LittleEndian(value) <= published.Number);
        }
    }

    private static ulong EntryHash(byte[] key, byte[] value) =>
        XxHash3.HashToUInt64(key) * 31 + XxHash3.HashToUInt64(value);
}
