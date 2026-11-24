using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using MelangeDB.Core;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// A paged row's bookkeeping in the key directory: which byte[] columns live out of line (bit i
/// for the i-th bytes column), and the row's encoded index values, positionally aligned with the
/// schema's index list, so index maintenance never has to read the old row back from disk.
/// </summary>
internal sealed record DirectoryEntry(uint BlobMask, RowKey[]? IndexValues);

/// <summary>
/// One immutable version of a table's ordered bookkeeping: a paged table's key directory and every
/// table's secondary indexes. A read view, or a lazy enumeration of keys, holds a version and
/// keeps reading it while writers publish newer ones, so nothing reachable from a version ever
/// changes. A write opens an <see cref="TableTreesEdit"/>, applies a commit record's ops, and
/// publishes the next version once.
/// <para>
/// Two implementations: persistent managed trees (<see cref="ManagedTableTrees"/>), and native
/// copy-on-write B+trees that keep the per-row cost off the managed heap.
/// </para>
/// </summary>
internal abstract class TableTrees
{
    /// <summary>Rows in the key directory.</summary>
    public abstract long Count { get; }

    public abstract bool Contains(in RowKey key);

    public abstract bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry);

    /// <summary>The directory's keys in order. Lazy; holds this version for as long as it is walked.</summary>
    public abstract IEnumerable<RowKey> Keys { get; }

    /// <summary>The directory's keys in <c>[low, high]</c>, both bounds included, sought rather than scanned to.</summary>
    public abstract IEnumerable<RowKey> KeyRange(RowKey low, RowKey high);

    /// <summary>The keys of every row whose value in index <paramref name="index"/> equals <paramref name="value"/>.</summary>
    public abstract IEnumerable<RowKey> IndexEqual(int index, RowKey value);

    /// <summary>The keys of every row whose value in index <paramref name="index"/> lies in <c>[low, high]</c>, in value-then-key order.</summary>
    public abstract IEnumerable<RowKey> IndexRange(int index, RowKey low, RowKey high);

    /// <summary>
    /// Opens the next version. <paramref name="bulk"/> marks recovery's replay, which applies
    /// millions of ops before anything can read them; an implementation may use a cheaper
    /// unversioned representation until <see cref="TableTreesEdit.Commit"/>.
    /// </summary>
    public abstract TableTreesEdit BeginEdit(bool bulk);

    /// <summary>A fresh, empty version of the same kind, with <paramref name="indexes"/> indexes.</summary>
    public abstract TableTrees Empty(int indexes);
}

/// <summary>
/// The next version of a <see cref="TableTrees"/> under construction. Reads see the edit's own
/// writes. One writer at a time, under the store lock. Disposing without committing discards it.
/// </summary>
internal abstract class TableTreesEdit : IDisposable
{
    public abstract bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry);

    public abstract void SetEntry(RowKey key, DirectoryEntry entry);

    public abstract void RemoveEntry(RowKey key);

    public abstract void IndexAdd(int index, RowKey value, RowKey key);

    public abstract void IndexRemove(int index, RowKey value, RowKey key);

    /// <summary>Publishes the edit as an immutable version. The edit is spent afterwards.</summary>
    public abstract TableTrees Commit();

    public virtual void Dispose()
    {
    }
}

/// <summary>
/// The persistent managed trees: <see cref="RowDirectory{TValue}"/> for the key directory and
/// <see cref="SecondaryIndex"/> per index. About 300 bytes and five objects per paged row with one
/// index, all on the managed heap.
/// </summary>
internal sealed class ManagedTableTrees : TableTrees
{
    private readonly RowDirectory<DirectoryEntry> _directory;
    private readonly ImmutableArray<SecondaryIndex> _indexes;

    private ManagedTableTrees(RowDirectory<DirectoryEntry> directory, ImmutableArray<SecondaryIndex> indexes)
    {
        _directory = directory;
        _indexes = indexes;
    }

    public static ManagedTableTrees Create(int indexes) =>
        new(RowDirectory<DirectoryEntry>.Empty, [.. Enumerable.Repeat(SecondaryIndex.Empty, indexes)]);

    public override long Count => _directory.Count;

    public override bool Contains(in RowKey key) => _directory.ContainsKey(key);

    public override bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry) =>
        _directory.TryGetValue(key, out entry);

    public override IEnumerable<RowKey> Keys => _directory.Keys;

    public override IEnumerable<RowKey> KeyRange(RowKey low, RowKey high) => _directory.KeyRange(low, high);

    public override IEnumerable<RowKey> IndexEqual(int index, RowKey value) => _indexes[index].Equal(value);

    public override IEnumerable<RowKey> IndexRange(int index, RowKey low, RowKey high) => _indexes[index].Range(low, high);

    public override TableTreesEdit BeginEdit(bool bulk) => bulk ? new BulkEdit(this) : new IncrementalEdit(this);

    public override TableTrees Empty(int indexes) => Create(indexes);

    /// <summary>A commit's edit: each op is a path copy of the persistent tree, published together.</summary>
    private sealed class IncrementalEdit(ManagedTableTrees from) : TableTreesEdit
    {
        private readonly SecondaryIndex[] _indexes = [.. from._indexes];
        private RowDirectory<DirectoryEntry> _directory = from._directory;

        public override bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry) =>
            _directory.TryGetValue(key, out entry);

        public override void SetEntry(RowKey key, DirectoryEntry entry) => _directory = _directory.SetItem(key, entry);

        public override void RemoveEntry(RowKey key) => _directory = _directory.Remove(key);

        public override void IndexAdd(int index, RowKey value, RowKey key) => _indexes[index] = _indexes[index].Add(value, key);

        public override void IndexRemove(int index, RowKey value, RowKey key) => _indexes[index] = _indexes[index].Remove(value, key);

        public override TableTrees Commit() => new ManagedTableTrees(_directory, [.. _indexes]);
    }

    /// <summary>
    /// Recovery's edit: mutable builders seeded from the version, assembled once at commit. Replay
    /// applies one op per record, and a path copy per op was paying for versions no reader could
    /// observe.
    /// </summary>
    private sealed class BulkEdit(ManagedTableTrees from) : TableTreesEdit
    {
        private readonly RowDirectory<DirectoryEntry>.Builder _directory = from._directory.ToBuilder();
        private readonly SecondaryIndex.Builder[] _indexes = [.. from._indexes.Select(static index => index.ToBuilder())];

        public override bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry) =>
            _directory.TryGetValue(key, out entry);

        public override void SetEntry(RowKey key, DirectoryEntry entry) => _directory[key] = entry;

        public override void RemoveEntry(RowKey key) => _directory.Remove(key);

        public override void IndexAdd(int index, RowKey value, RowKey key) => _indexes[index].Add(value, key);

        public override void IndexRemove(int index, RowKey value, RowKey key) => _indexes[index].Remove(value, key);

        public override TableTrees Commit() =>
            new ManagedTableTrees(_directory.ToImmutable(), [.. _indexes.Select(static builder => builder.ToImmutable())]);
    }
}
