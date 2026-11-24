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

    /// <summary>
    /// Called once, when the table version holding this one has no reader left and never will:
    /// native trees give their pages back. Managed trees have nothing to do; the GC has them.
    /// </summary>
    public virtual void Release()
    {
    }
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

/// <summary>
/// The native copy-on-write trees (<see cref="NativeTree"/>): the key directory as a plain tree
/// from row key to an encoded <see cref="DirectoryEntry"/>, and each secondary index as a pair tree
/// of (encoded value, row key). A paged row costs some tens of bytes of native memory and no
/// managed objects; a version's pages are freed once the GC has collected every version, edit and
/// enumerator that could still reach them.
/// <para>
/// A directory value is <c>varint blob mask, varint (index values + 1, or 0 for none), then per
/// index value: varint length, bytes</c>.
/// </para>
/// </summary>
internal sealed class NativeTableTrees : TableTrees
{
    private static readonly NativeEntryProjector<RowKey> KeyOf = static (key, _) => new RowKey(key);
    private static readonly NativeEntryProjector<RowKey> PairKeyOf = static (_, key) => new RowKey(key);

    private readonly Holder _directoryHolder;
    private readonly Holder[] _indexHolders;

    private NativeTableTrees(Holder directory, Holder[] indexes)
    {
        _directoryHolder = directory;
        _indexHolders = indexes;
    }

    private NativeTree DirectoryTree => _directoryHolder.Tree;

    public static NativeTableTrees Create(NativePagePool pool, int indexes) =>
        new(
            new Holder(NativeTree.Empty(pool, NativeTreeOrder.Plain)),
            [.. Enumerable.Range(0, indexes).Select(_ => new Holder(NativeTree.Empty(pool, NativeTreeOrder.Pair)))]);

    private NativeTree Index(int index) => _indexHolders[index].Tree;

    public override void Release()
    {
        _directoryHolder.Release();
        foreach (var holder in _indexHolders)
            holder.Release();
    }

    /// <summary>
    /// One native tree and the table-tree versions holding it. A commit that leaves a tree alone
    /// shares it with the next version, so a tree is released when its last holder is, not when the
    /// version that produced it is.
    /// </summary>
    private sealed class Holder(NativeTree tree)
    {
        private int _holders = 1;

        public NativeTree Tree { get; } = tree;

        public Holder Share()
        {
            Interlocked.Increment(ref _holders);
            return this;
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _holders) == 0)
                Tree.Release();
        }
    }

    public override long Count => DirectoryTree.Count;

    public override bool Contains(in RowKey key) => DirectoryTree.ContainsKey(key.Span);

    public override bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry)
    {
        if (DirectoryTree.TryGet(key.Span, out var value))
        {
            entry = Decode(value);
            return true;
        }

        entry = null;
        return false;
    }

    public override IEnumerable<RowKey> Keys => DirectoryTree.Scan(KeyOf);

    public override IEnumerable<RowKey> KeyRange(RowKey low, RowKey high) =>
        DirectoryTree.Range(low.ToArray(), high.ToArray(), KeyOf);

    public override IEnumerable<RowKey> IndexEqual(int index, RowKey value)
    {
        var bound = value.ToArray();
        return Index(index).Range(bound, bound, PairKeyOf);
    }

    public override IEnumerable<RowKey> IndexRange(int index, RowKey low, RowKey high) =>
        Index(index).Range(low.ToArray(), high.ToArray(), PairKeyOf);

    public override TableTreesEdit BeginEdit(bool bulk) => new Edit(this);

    public override TableTrees Empty(int indexes) => Create(DirectoryTree.Pool, indexes);

    internal static byte[] Encode(DirectoryEntry entry)
    {
        var length = VarintLength(entry.BlobMask) + VarintLength((uint)(entry.IndexValues?.Length + 1 ?? 0));
        if (entry.IndexValues is { } values)
        {
            foreach (var value in values)
                length += VarintLength((uint)value.Length) + value.Length;
        }

        var bytes = new byte[length];
        var at = WriteVarint(bytes, 0, entry.BlobMask);
        at = WriteVarint(bytes, at, (uint)(entry.IndexValues?.Length + 1 ?? 0));
        if (entry.IndexValues is { } indexValues)
        {
            foreach (var value in indexValues)
            {
                at = WriteVarint(bytes, at, (uint)value.Length);
                value.Span.CopyTo(bytes.AsSpan(at));
                at += value.Length;
            }
        }

        return bytes;
    }

    internal static DirectoryEntry Decode(ReadOnlySpan<byte> bytes)
    {
        var mask = ReadVarint(ref bytes);
        var count = ReadVarint(ref bytes);
        if (count == 0)
            return new DirectoryEntry(mask, null);
        var values = new RowKey[count - 1];
        for (var i = 0; i < values.Length; i++)
        {
            var length = (int)ReadVarint(ref bytes);
            if (length > bytes.Length)
                throw new InvalidDataException("A native directory entry ends inside an index value.");
            values[i] = new RowKey(bytes[..length]);
            bytes = bytes[length..];
        }

        return new DirectoryEntry(mask, values);
    }

    private static int VarintLength(uint value)
    {
        var length = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            length++;
        }

        return length;
    }

    private static int WriteVarint(byte[] bytes, int at, uint value)
    {
        while (value >= 0x80)
        {
            bytes[at++] = (byte)(value | 0x80);
            value >>= 7;
        }

        bytes[at++] = (byte)value;
        return at;
    }

    private static uint ReadVarint(ref ReadOnlySpan<byte> bytes)
    {
        uint value = 0;
        for (var shift = 0; shift < 35; shift += 7)
        {
            if (bytes.IsEmpty)
                throw new InvalidDataException("A native directory entry ends inside a length.");
            var b = bytes[0];
            bytes = bytes[1..];
            value |= (uint)(b & 0x7F) << shift;
            if (b < 0x80)
                return value;
        }

        throw new InvalidDataException("A native directory entry has a malformed length.");
    }

    /// <summary>
    /// The next version: an edit per tree, opened on first touch, so a record that changes only
    /// the directory copies no index page. Pages the edit creates are changed in place until
    /// commit, so a record's ops copy each page at most once, and recovery's replay is one edit
    /// that copies nothing at all.
    /// </summary>
    private sealed class Edit(NativeTableTrees from) : TableTreesEdit
    {
        private readonly NativeTree.Edit?[] _indexes = new NativeTree.Edit?[from._indexHolders.Length];
        private NativeTree.Edit? _directory;

        public override bool TryGetEntry(in RowKey key, [NotNullWhen(true)] out DirectoryEntry? entry)
        {
            var found = _directory is { } directory
                ? directory.TryGet(key.Span, out var value)
                : from.DirectoryTree.TryGet(key.Span, out value);
            entry = found ? Decode(value) : null;
            return found;
        }

        public override void SetEntry(RowKey key, DirectoryEntry entry) =>
            (_directory ??= from.DirectoryTree.BeginEdit()).Set(key.Span, Encode(entry));

        public override void RemoveEntry(RowKey key) => (_directory ??= from.DirectoryTree.BeginEdit()).Remove(key.Span);

        public override void IndexAdd(int index, RowKey value, RowKey key) =>
            (_indexes[index] ??= from.Index(index).BeginEdit()).Set(value.Span, key.Span);

        public override void IndexRemove(int index, RowKey value, RowKey key) =>
            (_indexes[index] ??= from.Index(index).BeginEdit()).Remove(value.Span, key.Span);

        public override TableTrees Commit()
        {
            var indexes = new Holder[_indexes.Length];
            for (var i = 0; i < indexes.Length; i++)
                indexes[i] = Next(from._indexHolders[i], _indexes[i]);
            var directory = Next(from._directoryHolder, _directory);
            Array.Clear(_indexes);
            _directory = null;
            return new NativeTableTrees(directory, indexes);
        }

        public override void Dispose()
        {
            _directory?.Dispose();
            foreach (var index in _indexes)
                index?.Dispose();
        }

        /// <summary>The tree's holder for the next version: shared when the edit changed nothing.</summary>
        private static Holder Next(Holder current, NativeTree.Edit? edit)
        {
            if (edit is null)
                return current.Share();
            var committed = edit.Commit();
            return ReferenceEquals(committed, current.Tree) ? current.Share() : new Holder(committed);
        }
    }
}
