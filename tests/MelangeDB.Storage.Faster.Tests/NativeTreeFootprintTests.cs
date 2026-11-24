using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// What a resident entry costs: native pool bytes per entry for a million-entry <see cref="NativeTree"/>,
/// against the managed bytes per entry of the persistent containers it replaces, measured as the
/// GC heap delta of holding one. The shapes are the hot store's: a key directory of 8-byte keys and
/// small values, and a secondary index of 4-byte values over 8-byte keys.
/// </summary>
[Trait("Category", "Slow")]
public sealed class NativeTreeFootprintTests(ITestOutputHelper output)
{
    private const int Entries = 1_000_000;

    [Fact]
    public void Native_entries_cost_tens_of_bytes_where_managed_ones_cost_hundreds()
    {
        var plainBuilt = NativeBytesPerEntry(NativeTreeOrder.Plain, bulk: true);
        var plainEdited = NativeBytesPerEntry(NativeTreeOrder.Plain, bulk: false);
        var pairBuilt = NativeBytesPerEntry(NativeTreeOrder.Pair, bulk: true);
        var pairEdited = NativeBytesPerEntry(NativeTreeOrder.Pair, bulk: false);
        var directory = ManagedBytesPerEntry(BuildDirectory);
        var index = ManagedBytesPerEntry(BuildIndex);

        output.WriteLine($"plain  (8 B key, 12 B value): native {plainBuilt:F1} B/entry built, {plainEdited:F1} B/entry by random edits; RowDirectory {directory:F1} B/entry managed");
        output.WriteLine($"pair   (4 B value, 8 B key):  native {pairBuilt:F1} B/entry built, {pairEdited:F1} B/entry by random edits; SecondaryIndex {index:F1} B/entry managed");

        Assert.True(plainBuilt < 40, $"plain built: {plainBuilt:F1} B/entry");
        Assert.True(plainEdited < 50, $"plain edited: {plainEdited:F1} B/entry");
        Assert.True(pairBuilt < 32, $"pair built: {pairBuilt:F1} B/entry");
        Assert.True(pairEdited < 40, $"pair edited: {pairEdited:F1} B/entry");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static double NativeBytesPerEntry(NativeTreeOrder order, bool bulk)
    {
        var pool = new NativePagePool();
        var tree = bulk ? Build(pool, order) : Edit(pool, order);
        Assert.Equal(Entries, tree.Count);

        // Superseded versions from the edits must be finalized before the pool holds just this one.
        for (var round = 0; round < 5; round++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        var bytes = pool.AllocatedBytes;
        tree.Release();
        Assert.Equal(0, pool.AllocatedBytes);
        return bytes / (double)Entries;
    }

    private static NativeTree Build(NativePagePool pool, NativeTreeOrder order)
    {
        using var builder = new NativeTree.Builder(pool, order);
        Span<byte> a = stackalloc byte[8];
        Span<byte> b = stackalloc byte[12];
        for (var i = 0; i < Entries; i++)
        {
            if (order == NativeTreeOrder.Plain)
            {
                BinaryPrimitives.WriteInt64BigEndian(a, i);
                builder.Add(a, b);
            }
            else
            {
                // Pairs sorted by value then key: value i / 4, key i.
                BinaryPrimitives.WriteInt32BigEndian(b, i / 4);
                BinaryPrimitives.WriteInt64BigEndian(a, i);
                builder.Add(b[..4], a);
            }
        }

        return builder.ToTree();
    }

    private static NativeTree Edit(NativePagePool pool, NativeTreeOrder order)
    {
        var random = new Random(1);
        var ids = Enumerable.Range(0, Entries).OrderBy(_ => random.Next()).ToArray();
        var tree = NativeTree.Empty(pool, order);
        Span<byte> a = stackalloc byte[8];
        Span<byte> b = stackalloc byte[12];
        for (var start = 0; start < Entries; start += 1000)
        {
            var edit = tree.BeginEdit();
            for (var i = start; i < start + 1000; i++)
            {
                BinaryPrimitives.WriteInt64BigEndian(a, ids[i]);
                if (order == NativeTreeOrder.Plain)
                {
                    edit.Set(a, b);
                }
                else
                {
                    BinaryPrimitives.WriteInt32BigEndian(b, ids[i] / 4);
                    edit.Set(b[..4], a);
                }
            }

            tree = edit.Commit();
        }

        return tree;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static double ManagedBytesPerEntry(Func<object> build)
    {
        var before = GC.GetTotalMemory(forceFullCollection: true);
        var held = build();
        var after = GC.GetTotalMemory(forceFullCollection: true);
        GC.KeepAlive(held);
        return (after - before) / (double)Entries;
    }

    private static object BuildDirectory()
    {
        var builder = RowDirectory<byte[]>.Empty.ToBuilder();
        var key = new byte[8];
        for (var i = 0; i < Entries; i++)
        {
            BinaryPrimitives.WriteInt64BigEndian(key, i);
            builder[new RowKey(key)] = new byte[12];
        }

        return builder.ToImmutable();
    }

    private static object BuildIndex()
    {
        var builder = SecondaryIndex.Empty.ToBuilder();
        var key = new byte[8];
        var value = new byte[4];
        for (var i = 0; i < Entries; i++)
        {
            BinaryPrimitives.WriteInt64BigEndian(key, i);
            BinaryPrimitives.WriteInt32BigEndian(value, i / 4);
            builder.Add(new RowKey(value), new RowKey(key));
        }

        return builder.ToImmutable();
    }
}
