using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace MelangeDB.Core;

/// <summary>
/// A persistent map from <see cref="RowKey"/> to <typeparamref name="TValue"/>, ordered by key and
/// — unlike the <see cref="ImmutableSortedDictionary{TKey,TValue}"/> it replaces — able to
/// <b>seek</b>.
/// <para>
/// This is the same shape change <see cref="SecondaryIndex"/> made, for the same reason, one level
/// down. A store's key directory is ordered, so a query for the keys in a window should begin at
/// the window; the dictionary's public API offers no way to start enumerating at a lower bound, so
/// every range read walked from the leftmost key and discarded everything below its window. That
/// cost is O(keys before the window) — it grows with the table and with where in key order the
/// caller happens to be looking, which is exactly the wrong shape for a moving-window subscription
/// over a large table. Flattening to one sorted set of entries makes the lower bound a binary
/// search, because <see cref="ImmutableSortedSet{T}"/> exposes both <c>IndexOf</c> and an indexer,
/// each O(log n).
/// </para>
/// <para>
/// The trade is one extra tree operation per <see cref="SetItem"/> that replaces an existing key: a
/// set has no "replace the payload at this position", so an update is a remove and an add where the
/// dictionary had a single descent. It is a constant factor on an operation that was already
/// O(log n) with a path copy, it produces short-lived garbage rather than live bytes, and it sits
/// behind an fsync in every configuration that commits. The alternative — keeping the dictionary
/// and carrying a second, seekable set of keys beside it — pays instead in <em>resident</em> memory,
/// one tree node per row for as long as the row exists. On a store whose defining constraint is the
/// RAM ceiling that is the worse currency, and it is the trade this type exists to refuse.
/// </para>
/// </summary>
internal sealed class RowDirectory<TValue> : IEnumerable<KeyValuePair<RowKey, TValue>>
{
    /// <summary>
    /// One key and its value as a single sorted-set element. Ordered by key alone, so the set
    /// behaves as a map: an entry is found, replaced, and removed by its key whatever it carries.
    /// </summary>
    private readonly record struct Entry(RowKey Key, TValue Value);

    private sealed class ByKey : IComparer<Entry>
    {
        public static readonly ByKey Instance = new();

        public int Compare(Entry x, Entry y) => x.Key.CompareTo(y.Key);
    }

    private static readonly IComparer<RowKey> Natural = Comparer<RowKey>.Default;

    public static readonly RowDirectory<TValue> Empty =
        new(ImmutableSortedSet.Create<Entry>(ByKey.Instance), Natural);

    private readonly ImmutableSortedSet<Entry> _entries;
    private readonly IComparer<RowKey> _comparer;

    private RowDirectory(ImmutableSortedSet<Entry> entries, IComparer<RowKey> comparer)
    {
        _entries = entries;
        _comparer = comparer;
    }

    public int Count => _entries.Count;

    /// <summary>The keys in order. Lazy: enumerating touches no row and allocates no list.</summary>
    public IEnumerable<RowKey> Keys
    {
        get
        {
            foreach (var entry in _entries)
                yield return entry.Key;
        }
    }

    public bool ContainsKey(in RowKey key) => _entries.Contains(new Entry(key, default!));

    public bool TryGetValue(in RowKey key, [MaybeNullWhen(false)] out TValue value)
    {
        var probe = _entries.IndexOf(new Entry(key, default!));
        if (probe < 0)
        {
            value = default;
            return false;
        }

        value = _entries[probe].Value;
        return true;
    }

    public TValue this[in RowKey key] =>
        TryGetValue(key, out var value) ? value : throw new KeyNotFoundException($"No row for key {key}.");

    /// <summary>
    /// Adds the key or replaces its value. Two tree operations rather than one when the key is
    /// already present — see the type's remarks for why that is the cheaper half of the trade.
    /// </summary>
    public RowDirectory<TValue> SetItem(in RowKey key, TValue value)
    {
        var entry = new Entry(key, value);
        var entries = _entries.Remove(entry).Add(entry);
        return new RowDirectory<TValue>(entries, _comparer);
    }

    public RowDirectory<TValue> Remove(in RowKey key) => new(_entries.Remove(new Entry(key, default!)), _comparer);

    /// <summary>
    /// The keys in <c>[low, high]</c>, both bounds included, in key order. <b>Seeks to the lower
    /// bound rather than scanning to it</b>, so the cost is the size of the window and not the
    /// distance to it.
    /// </summary>
    public IEnumerable<RowKey> KeyRange(RowKey low, RowKey high)
    {
        var start = Seek(low);
        return start >= _entries.Count ? [] : WalkKeys(_entries, start, high);
    }

    /// <summary>The entries in <c>[low, high]</c>, both bounds included; see <see cref="KeyRange"/>.</summary>
    public IEnumerable<KeyValuePair<RowKey, TValue>> Range(RowKey low, RowKey high)
    {
        var start = Seek(low);
        return start >= _entries.Count ? [] : Walk(_entries, start, high);
    }

    /// <summary>The position of the first entry at or after <paramref name="key"/>.</summary>
    private int Seek(RowKey key)
    {
        var probe = _entries.IndexOf(new Entry(key, default!));
        return probe < 0 ? ~probe : probe;
    }

    // Read by position rather than by stepping an enumerator, which costs a tree descent per entry.
    // That is not a new order of cost — the caller resolves each key to a row, already O(log n) —
    // and it is what removes the O(n) prefix the caller was walking for nothing.
    private static IEnumerable<RowKey> WalkKeys(ImmutableSortedSet<Entry> entries, int start, RowKey high)
    {
        for (var i = start; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Key.CompareTo(high) > 0)
                yield break;
            yield return entry.Key;
        }
    }

    private static IEnumerable<KeyValuePair<RowKey, TValue>> Walk(ImmutableSortedSet<Entry> entries, int start, RowKey high)
    {
        for (var i = start; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Key.CompareTo(high) > 0)
                yield break;
            yield return new KeyValuePair<RowKey, TValue>(entry.Key, entry.Value);
        }
    }

    public IEnumerator<KeyValuePair<RowKey, TValue>> GetEnumerator()
    {
        foreach (var entry in _entries)
            yield return new KeyValuePair<RowKey, TValue>(entry.Key, entry.Value);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// An empty directory whose comparisons run through <paramref name="comparer"/>. Exists so a
    /// test can count them: a seek and a scan return the same keys and differ only in how much work
    /// they do to get there, so counting comparisons is the only way to tell them apart from
    /// outside this class.
    /// </summary>
    internal static RowDirectory<TValue> EmptyWith(IComparer<RowKey> comparer) =>
        new(ImmutableSortedSet.Create<Entry>(new ByKeyThrough(comparer)), comparer);

    private sealed class ByKeyThrough(IComparer<RowKey> inner) : IComparer<Entry>
    {
        public int Compare(Entry x, Entry y) => inner.Compare(x.Key, y.Key);
    }

    /// <summary>
    /// A builder seeded with this directory's entries, for bulk load — the recovery path, where no
    /// read view exists yet and publishing a version per row would be pure waste.
    /// <para>
    /// Backed by an ordinary mutable sorted map rather than the persistent set's own builder. The
    /// builder exists precisely because nothing can observe the intermediate states, so it should
    /// pay neither the path copies nor the replace-as-remove-then-add this type accepts on the
    /// published side; and a mutable map keeps the lookup that recovery does per row at one tree
    /// descent. The set is assembled once, from already-ordered input, at <see cref="ToImmutable"/>.
    /// </para>
    /// </summary>
    public Builder ToBuilder()
    {
        var entries = new SortedDictionary<RowKey, TValue>(_comparer);
        foreach (var entry in _entries)
            entries.Add(entry.Key, entry.Value);
        return new Builder(entries, _comparer);
    }

    internal sealed class Builder(SortedDictionary<RowKey, TValue> entries, IComparer<RowKey> comparer)
        : IEnumerable<KeyValuePair<RowKey, TValue>>
    {
        public int Count => entries.Count;

        public bool ContainsKey(in RowKey key) => entries.ContainsKey(key);

        public bool TryGetValue(in RowKey key, [MaybeNullWhen(false)] out TValue value) =>
            entries.TryGetValue(key, out value);

        public TValue this[in RowKey key]
        {
            set => entries[key] = value;
        }

        public void Remove(in RowKey key) => entries.Remove(key);

        public void Clear() => entries.Clear();

        public RowDirectory<TValue> ToImmutable()
        {
            var builder = ImmutableSortedSet.CreateBuilder<Entry>(new ByKeyThrough(comparer));
            foreach (var (key, value) in entries)
                builder.Add(new Entry(key, value));
            return new RowDirectory<TValue>(builder.ToImmutable(), comparer);
        }

        public IEnumerator<KeyValuePair<RowKey, TValue>> GetEnumerator() => entries.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
