using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The key directory's contract, and the one thing about it no result assertion can check.
/// <para>
/// A range read that walked from the leftmost key and discarded everything below its window returns
/// exactly the same keys as one that seeks — which is why this survived a fix that was aimed at it:
/// the earlier pass stopped the range <em>materializing</em> rows below the window and left it
/// walking the keys. So the cost claim is pinned where it is observable, by counting comparisons
/// through the directory's own comparer. The rest cover the behaviour that flattening the container
/// from a sorted dictionary to a sorted set could plausibly have broken — replacement above all,
/// because a set's <c>Add</c> of an equal element is a no-op and would silently keep the old value.
/// </para>
/// </summary>
public class RowDirectoryTests
{
    private const int Rows = 4_000;

    [Fact]
    public void Reaching_a_window_at_the_end_costs_no_more_than_one_at_the_start()
    {
        // Asserting which keys come back proves nothing here: a linear walk returns the same ones.
        // What separates seeking from scanning is how much work each does to arrive, so this counts
        // comparisons — through the comparer both IndexOf and the indexer run through.
        var counter = new CountingComparer();
        var directory = Populated(RowDirectory<byte[]>.EmptyWith(counter));

        counter.Reset();
        var atStart = directory.KeyRange(Key(0), Key(9)).ToList();
        var startCost = counter.Comparisons;

        counter.Reset();
        var atEnd = directory.KeyRange(Key(Rows - 10), Key(Rows - 1)).ToList();
        var endCost = counter.Comparisons;

        Assert.Equal(10, atStart.Count);
        Assert.Equal(10, atEnd.Count);

        // Both windows hold ten keys, so the only difference between these two numbers is the cost
        // of reaching them. Walking made the far window cost thousands of comparisons against a
        // handful for the near one; seeking puts them within a small factor.
        Assert.True(
            endCost < startCost * 3,
            $"reaching the far window cost {endCost} comparisons against {startCost} for the near one, " +
            "which is the signature of walking to the lower bound instead of seeking to it");
    }

    [Fact]
    public void A_window_returns_both_bounds_and_nothing_outside_them()
    {
        var directory = Populated();

        Assert.Equal([Key(10), Key(11), Key(12)], directory.KeyRange(Key(10), Key(12)).ToList());

        // Bounds that name no row: the seek lands on the first key at or after the low bound, and
        // the walk stops at the first past the high one.
        Assert.Equal([Key(0)], directory.KeyRange(Missing(), Key(0)).ToList());
        Assert.Empty(directory.KeyRange(Key(Rows), Key(Rows + 10)).ToList());
        Assert.Equal(Rows, directory.KeyRange(Key(0), Key(Rows + 10)).Count());

        // A single-key window, and an inverted one, which is empty rather than an error.
        Assert.Equal([Key(7)], directory.KeyRange(Key(7), Key(7)).ToList());
        Assert.Empty(directory.KeyRange(Key(12), Key(10)).ToList());
    }

    [Fact]
    public void Setting_an_existing_key_replaces_its_value_rather_than_keeping_the_first()
    {
        // The failure mode flattening to a set invites: Add on an element the comparer calls equal
        // is a no-op, so an update that forgot to remove first would silently keep the old row.
        var directory = RowDirectory<byte[]>.Empty
            .SetItem(Key(1), [1])
            .SetItem(Key(1), [2]);

        Assert.Equal(1, directory.Count);
        Assert.True(directory.TryGetValue(Key(1), out var value));
        Assert.Equal([2], value);
        Assert.Equal([2], directory[Key(1)]);
        Assert.Equal([Key(1)], directory.KeyRange(Key(0), Key(9)).ToList());
    }

    [Fact]
    public void A_published_directory_is_unchanged_by_writes_to_the_one_derived_from_it()
    {
        // Persistence is the whole basis of the pinned read view: an older version must keep
        // answering as it did however the live one moves on.
        var original = Populated();
        var changed = original.SetItem(Key(0), [99]).Remove(Key(1)).SetItem(Key(Rows + 1), [7]);

        Assert.Equal(Rows, original.Count);
        Assert.Equal(Rows, changed.Count);
        Assert.Equal(Value(0), original[Key(0)]);
        Assert.Equal([99], changed[Key(0)]);
        Assert.True(original.ContainsKey(Key(1)));
        Assert.False(changed.ContainsKey(Key(1)));
        Assert.False(original.ContainsKey(Key(Rows + 1)));
    }

    [Fact]
    public void Removing_and_re_adding_a_key_leaves_the_order_intact()
    {
        var directory = Populated().Remove(Key(5)).SetItem(Key(5), [55]);

        Assert.Equal(Rows, directory.Count);
        Assert.Equal([Key(4), Key(5), Key(6)], directory.KeyRange(Key(4), Key(6)).ToList());
        Assert.Equal([55], directory[Key(5)]);
        Assert.Equal(Enumerable.Range(0, Rows).Select(Key).ToList(), directory.Keys.ToList());
    }

    [Fact]
    public void Removing_a_key_that_is_not_there_changes_nothing()
    {
        var directory = Populated();
        Assert.Equal(Rows, directory.Remove(Missing()).Count);
        Assert.False(directory.TryGetValue(Missing(), out _));
    }

    [Fact]
    public void The_builder_agrees_with_the_directory_it_was_taken_from()
    {
        var builder = Populated().ToBuilder();
        builder[Key(0)] = [42];
        builder.Remove(Key(1));
        builder[Key(Rows + 1)] = [7];

        Assert.True(builder.TryGetValue(Key(0), out var replaced));
        Assert.Equal([42], replaced);
        Assert.False(builder.ContainsKey(Key(1)));

        var built = builder.ToImmutable();
        Assert.Equal(Rows, built.Count);
        Assert.Equal([42], built[Key(0)]);
        Assert.False(built.ContainsKey(Key(1)));
        Assert.Equal([7], built[Key(Rows + 1)]);

        // Order survives the round trip, which is what every range read depends on.
        Assert.Equal(built.Keys.ToList(), built.Keys.OrderBy(static k => k).ToList());
        Assert.Equal([Key(2), Key(3)], built.KeyRange(Key(1), Key(3)).ToList());

        builder.Clear();
        Assert.Equal(0, builder.ToImmutable().Count);
    }

    [Fact]
    public void Range_and_KeyRange_answer_the_same_window()
    {
        var directory = Populated();
        var pairs = directory.Range(Key(100), Key(104)).ToList();

        Assert.Equal(directory.KeyRange(Key(100), Key(104)).ToList(), pairs.Select(static p => p.Key).ToList());
        Assert.Equal(Value(100), pairs[0].Value);
    }

    private sealed class CountingComparer : IComparer<RowKey>
    {
        public int Comparisons { get; private set; }

        public void Reset() => Comparisons = 0;

        public int Compare(RowKey x, RowKey y)
        {
            Comparisons++;
            return x.CompareTo(y);
        }
    }

    private static RowDirectory<byte[]> Populated(RowDirectory<byte[]>? seed = null)
    {
        var builder = (seed ?? RowDirectory<byte[]>.Empty).ToBuilder();
        for (var i = 0; i < Rows; i++)
            builder[Key(i)] = Value(i);
        return builder.ToImmutable();
    }

    private static byte[] Value(int key) => [(byte)(key & 0xFF)];

    private static RowKey Key(int key) => Encode((ulong)key);

    /// <summary>A key that sorts below every populated one, so a seek for it lands at position 0.</summary>
    private static RowKey Missing()
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, 0);
        return new RowKey(buffer);
    }

    private static RowKey Encode(ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        return new RowKey(buffer);
    }
}
