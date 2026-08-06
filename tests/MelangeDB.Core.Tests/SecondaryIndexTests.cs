using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The secondary index's contract, and the one thing about it no result assertion can check.
/// <para>
/// A range scan that walked from the leftmost entry and discarded everything below its window
/// returns exactly the same rows as one that seeks — that is precisely why the defect survived. So
/// the cost claim is pinned where it is observable: <see cref="SecondaryIndex.Seek"/> must land near
/// the end of the index for a value near the end. The rest of these cover the behaviour flattening
/// the container could plausibly have broken.
/// </para>
/// </summary>
public class SecondaryIndexTests
{
    private const int Values = 1_000;
    private const int KeysPerValue = 4;

    [Fact]
    public void Reaching_a_window_at_the_end_costs_no_more_than_one_at_the_start()
    {
        // The assertion that matters, and the one that took two tries to write. Asserting the
        // position a seek returns proves nothing: a linear walk returns the same position, so the
        // test passed against a deliberately scanning implementation. What separates them is how
        // much work they do, so this counts comparisons — through the set's own comparer, which is
        // what both IndexOf and the indexer run through.
        var counter = new CountingComparer();
        var index = Populated(SecondaryIndex.EmptyWith(counter));

        counter.Reset();
        var atStart = index.Range(Value(0), Value(2)).ToList();
        var startCost = counter.Comparisons;

        counter.Reset();
        var atEnd = index.Range(Value(Values - 3), Value(Values - 1)).ToList();
        var endCost = counter.Comparisons;

        Assert.Equal(3 * KeysPerValue, atStart.Count);
        Assert.Equal(3 * KeysPerValue, atEnd.Count);

        // Both windows hold the same number of entries, so the only difference between these two
        // numbers is the cost of reaching them. Scanning made the far window cost ~4,000 comparisons
        // against ~30 for the near one; seeking puts them within a small factor.
        Assert.True(
            endCost < startCost * 3,
            $"reaching the far window cost {endCost} comparisons against {startCost} for the near one, " +
            "which is the signature of scanning to the lower bound instead of seeking to it");
    }

    [Fact]
    public void A_seek_costs_logarithmically_in_the_size_of_the_index()
    {
        // The absolute bound, so that "no worse than the near window" cannot be satisfied by making
        // both of them bad. 4,000 entries is ~12 levels; the walk over the window adds its own
        // descents, so the ceiling is generous and still nowhere near the 4,000 a scan would cost.
        var counter = new CountingComparer();
        var index = Populated(SecondaryIndex.EmptyWith(counter));

        counter.Reset();
        _ = index.Range(Value(Values - 1), Value(Values - 1)).ToList();

        Assert.Equal(Values * KeysPerValue, index.Count);
        Assert.True(
            counter.Comparisons < 200,
            $"a single-value lookup at the end of a {index.Count}-entry index cost {counter.Comparisons} comparisons");
    }

    [Fact]
    public void A_bound_the_index_does_not_hold_still_selects_the_right_window()
    {
        // Gaps are the common case: a range's bounds are query values, not stored ones.
        var index = SecondaryIndex.Empty
            .Add(Value(10), Key(1))
            .Add(Value(30), Key(2));

        Assert.Equal([Key(1), Key(2)], index.Range(Value(5), Value(50)));
        Assert.Equal([Key(2)], index.Range(Value(20), Value(50)));
        Assert.Empty(index.Range(Value(11), Value(29)));
    }

    [Fact]
    public void A_range_returns_every_key_in_the_window_and_nothing_else()
    {
        var index = Populated();

        var keys = index.Range(Value(500), Value(502)).ToList();

        Assert.Equal(3 * KeysPerValue, keys.Count);
        Assert.Equal(keys, keys.OrderBy(k => k).ToList());
        Assert.Equal(ExpectedKeys(500, 502), keys);
    }

    [Fact]
    public void A_range_includes_both_of_its_bounds()
    {
        // Half-open would silently drop the top value of every window — a bug that looks like
        // missing data at the edge of a query and nowhere else.
        var index = Populated();

        Assert.Equal(ExpectedKeys(0, 0), index.Range(Value(0), Value(0)));
        Assert.Equal(ExpectedKeys(Values - 1, Values - 1), index.Range(Value(Values - 1), Value(Values - 1)));
    }

    [Fact]
    public void A_range_wide_enough_to_cover_the_index_returns_all_of_it()
    {
        // Above the threshold this takes the sequential path rather than walking by position. The
        // two must agree exactly, or which path a query takes becomes observable.
        var index = Populated();

        var all = index.Range(Value(0), Value(Values - 1)).ToList();

        Assert.Equal(Values * KeysPerValue, all.Count);
        Assert.Equal(ExpectedKeys(0, Values - 1), all);
    }

    [Fact]
    public void A_range_that_matches_nothing_is_empty()
    {
        var index = Populated();

        Assert.Empty(index.Range(Value(Values + 10), Value(Values + 20)));
        Assert.Empty(SecondaryIndex.Empty.Range(Value(0), Value(100)));
    }

    [Fact]
    public void Equal_returns_only_the_keys_holding_that_value()
    {
        // The unique-constraint check runs through this, so a neighbouring value leaking in would
        // reject a legal insert.
        var index = Populated();

        Assert.Equal(ExpectedKeys(700, 700), index.Equal(Value(700)));
        Assert.Empty(index.Equal(Value(Values + 1)));
    }

    [Fact]
    public void Removing_one_key_leaves_its_neighbours_under_the_same_value()
    {
        // Flattening means a value is no longer one entry holding a set. Removing a key must remove
        // exactly its own entry, not the run every key with that value shares.
        var index = Populated().Remove(Value(300), Key((300 * KeysPerValue) + 1));

        var remaining = index.Equal(Value(300)).ToList();

        Assert.Equal(KeysPerValue - 1, remaining.Count);
        Assert.DoesNotContain(Key((300 * KeysPerValue) + 1), remaining);
        Assert.Contains(Key(300 * KeysPerValue), remaining);
    }

    [Fact]
    public void Removing_the_last_key_of_a_value_removes_the_value()
    {
        var index = SecondaryIndex.Empty.Add(Value(1), Key(1)).Remove(Value(1), Key(1));

        Assert.Equal(0, index.Count);
        Assert.Empty(index.Equal(Value(1)));
    }

    [Fact]
    public void The_same_key_under_two_values_is_two_entries()
    {
        // One row, two indexed columns, same key. Nothing may collapse them.
        var index = SecondaryIndex.Empty.Add(Value(1), Key(9)).Add(Value(2), Key(9));

        Assert.Equal(2, index.Count);
        Assert.Equal([Key(9)], index.Equal(Value(1)));
        Assert.Equal([Key(9)], index.Equal(Value(2)));
    }

    [Fact]
    public void A_builder_produces_the_same_index_as_repeated_adds()
    {
        var builder = SecondaryIndex.Empty.ToBuilder();
        for (var v = 0; v < 10; v++)
        {
            for (var k = 0; k < KeysPerValue; k++)
                builder.Add(Value(v), Key((v * KeysPerValue) + k));
        }

        var built = builder.ToImmutable();
        var added = SecondaryIndex.Empty;
        for (var v = 0; v < 10; v++)
        {
            for (var k = 0; k < KeysPerValue; k++)
                added = added.Add(Value(v), Key((v * KeysPerValue) + k));
        }

        Assert.Equal(added.Count, built.Count);
        Assert.Equal(added.Range(Value(0), Value(9)), built.Range(Value(0), Value(9)));
    }

    private static SecondaryIndex Populated(SecondaryIndex? seed = null)
    {
        var builder = (seed ?? SecondaryIndex.Empty).ToBuilder();
        for (var v = 0; v < Values; v++)
        {
            for (var k = 0; k < KeysPerValue; k++)
                builder.Add(Value(v), Key((v * KeysPerValue) + k));
        }

        return builder.ToImmutable();
    }

    /// <summary>
    /// Counts every comparison the sorted set makes. Both <c>IndexOf</c> and the indexer descend the
    /// tree through this, so it sees the work a seek does and the work a scan would have done.
    /// </summary>
    private sealed class CountingComparer : IComparer<IndexEntry>
    {
        public int Comparisons { get; private set; }

        public void Reset() => Comparisons = 0;

        public int Compare(IndexEntry x, IndexEntry y)
        {
            Comparisons++;
            return x.CompareTo(y);
        }
    }

    private static IEnumerable<RowKey> ExpectedKeys(int lowValue, int highValue)
    {
        for (var v = lowValue; v <= highValue; v++)
        {
            for (var k = 0; k < KeysPerValue; k++)
                yield return Key((v * KeysPerValue) + k);
        }
    }

    private static RowKey Value(int value) => Encode((ulong)value);

    private static RowKey Key(int key) => Encode((ulong)key);

    private static RowKey Encode(ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        return new RowKey(buffer);
    }
}
