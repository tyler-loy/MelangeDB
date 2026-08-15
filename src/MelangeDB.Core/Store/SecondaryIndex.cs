using System.Collections.Immutable;

namespace MelangeDB.Core;

/// <summary>
/// One entry in a secondary index: an indexed column's encoded value, and the primary key of a row
/// holding it. Ordered by value first and key second, so every row sharing a value forms a
/// contiguous run and a range of values is a contiguous span.
/// </summary>
internal readonly struct IndexEntry(RowKey value, RowKey key) : IComparable<IndexEntry>, IEquatable<IndexEntry>
{
    public RowKey Value { get; } = value;

    public RowKey Key { get; } = key;

    public int CompareTo(IndexEntry other)
    {
        var byValue = Value.CompareTo(other.Value);
        return byValue != 0 ? byValue : Key.CompareTo(other.Key);
    }

    public bool Equals(IndexEntry other) => Value == other.Value && Key == other.Key;

    public override bool Equals(object? obj) => obj is IndexEntry other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Value, Key);
}

/// <summary>
/// A persistent secondary index: encoded column value → the primary keys holding it, stored as one
/// sorted set of <see cref="IndexEntry"/> rather than as a dictionary of nested sets.
/// <para>
/// The shape is the point. As a <c>ImmutableSortedDictionary&lt;RowKey, ImmutableSortedSet&lt;RowKey&gt;&gt;</c>
/// this could not <b>seek</b>: the public API offers no way to begin enumerating at a lower bound, so
/// a range query walked from the leftmost value and discarded everything below its window. A ten-row
/// window at the far end of a large index paid for the whole index. Flattening to one sorted set
/// makes the lower bound a binary search, because <see cref="ImmutableSortedSet{T}"/> — unlike the
/// dictionary — exposes both <c>IndexOf</c> and an indexer, each O(log n).
/// </para>
/// <para>
/// Reading the window by position costs a tree descent per entry rather than a step of an
/// enumerator. That is not a new order of cost: resolving each key to its row is a lookup in the
/// row map, already O(log n) per key, so the walk adds a constant factor to work the caller was
/// always going to do. What it removes is the O(n) prefix, which the caller was doing for nothing.
/// </para>
/// <para>
/// Maintenance got cheaper on the way past. Adding a row to an index used to mean reading the inner
/// set, rebuilding it, and writing it back into the outer dictionary — three persistent-container
/// operations, each allocating its own path copy. Here it is one <c>Add</c> of one entry.
/// </para>
/// </summary>
internal sealed class SecondaryIndex
{
    public static readonly SecondaryIndex Empty = new(ImmutableSortedSet<IndexEntry>.Empty);

    private readonly ImmutableSortedSet<IndexEntry> _entries;

    private SecondaryIndex(ImmutableSortedSet<IndexEntry> entries) => _entries = entries;

    public int Count => _entries.Count;

    public SecondaryIndex Add(RowKey value, RowKey key) => new(_entries.Add(new IndexEntry(value, key)));

    public SecondaryIndex Remove(RowKey value, RowKey key) => new(_entries.Remove(new IndexEntry(value, key)));

    /// <summary>The keys of every row whose indexed value equals <paramref name="value"/>.</summary>
    public IEnumerable<RowKey> Equal(RowKey value) => Range(value, value);

    /// <summary>
    /// The keys of every row whose indexed value lies in <c>[low, high]</c>, both bounds included,
    /// in value-then-key order. Seeks to the lower bound rather than scanning to it.
    /// </summary>
    public IEnumerable<RowKey> Range(RowKey low, RowKey high)
    {
        var start = Seek(low);
        return start >= _entries.Count ? [] : Walk(_entries, start, high);
    }

    /// <summary>
    /// An index whose comparisons run through <paramref name="comparer"/>. Exists so a test can
    /// count them: a seek and a scan return the same positions and the same rows, and differ only in
    /// how much work they do to get there, so counting comparisons is the only way to tell them
    /// apart from outside this class.
    /// </summary>
    internal static SecondaryIndex EmptyWith(IComparer<IndexEntry> comparer) =>
        new(ImmutableSortedSet.Create<IndexEntry>(comparer));

    /// <summary>The position of the first entry at or after <paramref name="value"/>.</summary>
    private int Seek(RowKey value)
    {
        // A zero-length key sorts before every real one, so (value, default) is the position of the
        // first entry that could hold this value, whether or not that exact pair exists.
        var probe = _entries.IndexOf(new IndexEntry(value, default));
        return probe < 0 ? ~probe : probe;
    }

    private static IEnumerable<RowKey> Walk(ImmutableSortedSet<IndexEntry> entries, int start, RowKey high)
    {
        for (var i = start; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Value.CompareTo(high) > 0)
                yield break;
            yield return entry.Key;
        }
    }

    /// <summary>
    /// A builder seeded with this index's entries, for bulk load — the recovery path, where no read
    /// view exists yet and publishing a version per row would be pure waste.
    /// </summary>
    public Builder ToBuilder() => new(_entries.ToBuilder());

    internal sealed class Builder(ImmutableSortedSet<IndexEntry>.Builder entries)
    {
        public void Add(RowKey value, RowKey key) => entries.Add(new IndexEntry(value, key));

        public void Remove(RowKey value, RowKey key) => entries.Remove(new IndexEntry(value, key));

        public SecondaryIndex ToImmutable() => new(entries.ToImmutable());
    }
}
