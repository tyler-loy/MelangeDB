using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace MelangeDB.Storage.Faster;

/// <summary>Builds a result from one entry's key and value, read in place.</summary>
internal delegate T NativeEntryProjector<out T>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

/// <summary>Builds a result from one entry's value, read in place.</summary>
internal delegate T NativeValueProjector<out T>(ReadOnlySpan<byte> value);

/// <summary>What <see cref="NativeTree.Validate"/> found.</summary>
internal readonly record struct NativeTreeStats(
    long Entries, int Height, long Pages, long LeafPages, long InternalPages, long UsedBytes, long OutOfLineParts)
{
    /// <summary>The fraction of page bytes (after headers) in use, over every page.</summary>
    public double Fill(int pageSize) => Pages == 0 ? 0 : UsedBytes / (double)(Pages * (pageSize - NativePage.HeaderSize));
}

/// <summary>
/// One immutable version of a persistent, copy-on-write B+tree held in native memory: an ordered
/// map of byte strings (<see cref="NativeTreeOrder.Plain"/>) or an ordered set of byte-string pairs
/// (<see cref="NativeTreeOrder.Pair"/>), with pages from a <see cref="NativePagePool"/>.
/// <para>
/// <b>Why native.</b> The persistent containers it replaces (<c>RowDirectory</c> and
/// <c>SecondaryIndex</c>, over <c>ImmutableSortedSet</c>) cost about five GC objects and three
/// hundred bytes per row — a node, the key's array, the value — all of which the GC must trace on
/// every full collection, for as long as the row exists. Here a row is an entry of tens of bytes on
/// a shared page, and the only managed object is one per <em>version</em>.
/// </para>
/// <para>
/// <b>Versions.</b> A version never changes once published. An <see cref="Edit"/> produces the next
/// version by copying only the pages on the paths it changes, so a version shares every untouched
/// page with its parent; a <see cref="Builder"/> produces a version from sorted input bottom-up.
/// Any number of threads may read any live versions concurrently with the single writer.
/// </para>
/// <para>
/// <b>Reclamation.</b> Pages and blobs are refcounted (<see cref="NativePage.Release"/>): a page
/// holds a reference to each child and blob it points at, and each version holds one reference to
/// its root. A version's reference is dropped by its finalizer — so it is released only once the GC
/// has proven that nothing can read it any more: no caller, no enumerator (each holds its version),
/// no edit (each holds the version it started from). Dropping the root cascades down to every page
/// and blob no other version shares. Because a page reachable from a live version always holds at
/// least that version's chain of references, its count cannot reach zero while it is readable; and
/// the writer only ever adds references to pages it reached through a live version, so it never
/// resurrects a page from zero. That is the whole safety argument, and the counts are
/// <see cref="Interlocked"/> because the finalizer thread releases concurrently with the writer.
/// </para>
/// <para>
/// Every method that touches native memory ends with <see cref="GC.KeepAlive"/> on the version:
/// without it the JIT may consider the version dead once its fields are read, and a finalizer could
/// free pages the method is still walking.
/// </para>
/// <para>
/// <see cref="Release"/> drops the reference early, for callers that can prove no reader exists;
/// the finalizer alone is correct, and the early release is an optimization only.
/// </para>
/// </summary>
[SkipLocalsInit]
internal sealed unsafe partial class NativeTree
{
    private readonly NativePagePool _pool;
    private readonly NativeTreeOrder _order;
    private readonly int _height;
    private readonly long _count;
    private nint _root;
    private int _released;

    private NativeTree(NativePagePool pool, NativeTreeOrder order, nint root, int height, long count)
    {
        _pool = pool;
        _order = order;
        _root = root;
        _height = height;
        _count = count;
        if (root == 0)
            GC.SuppressFinalize(this);
    }

    ~NativeTree() => ReleaseRoot();

    /// <summary>An empty version, the start of every edit history.</summary>
    public static NativeTree Empty(NativePagePool pool, NativeTreeOrder order) => new(pool, order, 0, 0, 0);

    public NativePagePool Pool => _pool;

    public NativeTreeOrder Order => _order;

    /// <summary>Entries in this version. O(1): maintained by the edit that produced it.</summary>
    public long Count => _count;

    /// <summary>Levels from root to leaf; 0 when empty.</summary>
    public int Height => _height;

    /// <summary>
    /// Starts an edit producing the next version from this one. The edit holds this version, and
    /// a reference to its root, until it is committed or disposed.
    /// </summary>
    public Edit BeginEdit()
    {
        var edit = new Edit(this, Root());
        GC.KeepAlive(this);
        return edit;
    }

    /// <summary>
    /// Releases this version's pages now rather than at finalization. <b>Only safe when no reader,
    /// enumerator or edit of this version can still run</b>: nothing here can detect one that is
    /// mid-walk. Later reads of this version throw <see cref="ObjectDisposedException"/>.
    /// </summary>
    public void Release()
    {
        ReleaseRoot();
        GC.SuppressFinalize(this);
    }

    private void ReleaseRoot()
    {
        Volatile.Write(ref _released, 1);
        var root = Interlocked.Exchange(ref _root, 0);
        if (root != 0)
            NativePage.Release(_pool, (byte*)root);
    }

    private byte* Root()
    {
        var root = _root;
        if (root == 0 && Volatile.Read(ref _released) != 0)
            throw new ObjectDisposedException(nameof(NativeTree), "This version has been released.");
        return (byte*)root;
    }

    /// <summary>
    /// The value stored under <paramref name="key"/>, copied out. In a
    /// <see cref="NativeTreeOrder.Pair"/> tree: the key part of the first pair whose value part is
    /// <paramref name="key"/>.
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> key, [NotNullWhen(true)] out byte[]? value) =>
        TryGet(key, static v => v.ToArray(), out value);

    /// <summary>
    /// <see cref="TryGet(ReadOnlySpan{byte}, out byte[])"/>, handing the value to
    /// <paramref name="projector"/> in place: nothing is copied unless the projector copies it.
    /// </summary>
    public bool TryGet<T>(ReadOnlySpan<byte> key, NativeValueProjector<T> projector, [MaybeNullWhen(false)] out T result)
    {
        var found = Find(_pool, _order, Root(), _height, key, out var block, out var available);
        result = found ? projector(NativePage.ReadBlock(block, available).B) : default;
        GC.KeepAlive(this);
        return found;
    }

    /// <summary>Whether any entry has the key (in a pair tree: the value part) <paramref name="key"/>.</summary>
    public bool ContainsKey(ReadOnlySpan<byte> key)
    {
        var found = Find(_pool, _order, Root(), _height, key, out _, out _);
        GC.KeepAlive(this);
        return found;
    }

    /// <summary>Whether this exact (key, value) entry is present.</summary>
    public bool Contains(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var found = FindExact(_pool, _order, Root(), _height, key, value);
        GC.KeepAlive(this);
        return found;
    }

    /// <summary>
    /// The entries whose key lies in <c>[low, high]</c>, both inclusive, in order — in a pair tree,
    /// whose <em>value part</em> lies in that range, every key part included. A null bound is open.
    /// Lazy: the walk seeks to <paramref name="low"/> and reads forward one entry per step, and the
    /// enumerator holds this version for as long as it lives.
    /// </summary>
    public IEnumerable<T> Range<T>(byte[]? low, byte[]? high, NativeEntryProjector<T> projector) =>
        new RangeIterator<T>(this, low, null, high, projector);

    /// <summary>
    /// <see cref="Range{T}"/> starting at the exact entry (<paramref name="lowKey"/>,
    /// <paramref name="lowValue"/>) inclusive — for resuming a walk over a pair tree mid-value. In a
    /// plain tree <paramref name="lowValue"/> is ignored.
    /// </summary>
    public IEnumerable<T> RangeFrom<T>(byte[] lowKey, byte[] lowValue, byte[]? high, NativeEntryProjector<T> projector) =>
        new RangeIterator<T>(this, lowKey, lowValue, high, projector);

    /// <summary>Every entry, in order, through <paramref name="projector"/>.</summary>
    public IEnumerable<T> Scan<T>(NativeEntryProjector<T> projector) => new RangeIterator<T>(this, null, null, null, projector);

    /// <summary>The keys in <c>[low, high]</c>, copied out; see <see cref="Range{T}"/>.</summary>
    public IEnumerable<byte[]> Keys(byte[]? low = null, byte[]? high = null) => Range(low, high, static (k, _) => k.ToArray());

    /// <summary>
    /// The values of the entries in <c>[low, high]</c>, copied out; in a pair tree, the primary
    /// keys of the rows whose indexed value lies in the range.
    /// </summary>
    public IEnumerable<byte[]> Values(byte[]? low = null, byte[]? high = null) => Range(low, high, static (_, v) => v.ToArray());

    /// <summary>The entries in <c>[low, high]</c>, copied out.</summary>
    public IEnumerable<KeyValuePair<byte[], byte[]>> Entries(byte[]? low = null, byte[]? high = null) =>
        Range(low, high, static (k, v) => new KeyValuePair<byte[], byte[]>(k.ToArray(), v.ToArray()));

    /// <summary>
    /// Walks the whole version checking every structural invariant — ordering within and across
    /// pages, separators bounding their subtrees, levels, page accounting, the maintained count —
    /// and throws on the first violation. O(n); for tests and diagnostics.
    /// </summary>
    public NativeTreeStats Validate()
    {
        var stats = Validate(_pool, _order, Root(), _height, _count);
        GC.KeepAlive(this);
        return stats;
    }

    // ---- Reading, shared with Edit (which reads its own unpublished root the same way). ----

    /// <summary>The first slot of <paramref name="page"/> whose entry is at or above the probe.</summary>
    internal static int LowerBound(
        NativeTreeOrder order, byte* page, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, int pageSize, out bool found)
    {
        var lo = 0;
        var hi = NativePage.Count(page);
        while (lo < hi)
        {
            var mid = (int)((uint)(lo + hi) >> 1);
            var block = NativePage.Block(page, mid, pageSize, out var available);
            var c = NativePage.Compare(order, a, b, block, available);
            if (c > 0)
            {
                lo = mid + 1;
            }
            else if (c < 0)
            {
                hi = mid;
            }
            else
            {
                found = true;
                return mid;
            }
        }

        found = false;
        return lo;
    }

    /// <summary>The child of an internal page whose subtree would hold the probe.</summary>
    internal static int ChildIndex(NativeTreeOrder order, byte* page, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, int pageSize)
    {
        // Slot 0's separator is never compared: everything below separator 1 belongs to child 0.
        var lo = 1;
        var hi = NativePage.Count(page) - 1;
        var result = 0;
        while (lo <= hi)
        {
            var mid = (int)((uint)(lo + hi) >> 1);
            var block = NativePage.Block(page, mid, pageSize, out var available);
            if (NativePage.Compare(order, a, b, block, available) >= 0)
            {
                result = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return result;
    }

    /// <summary>
    /// The block of the first entry whose key (value part, in a pair tree) equals
    /// <paramref name="key"/>.
    /// </summary>
    internal static bool Find(
        NativePagePool pool, NativeTreeOrder order, byte* root, int height, ReadOnlySpan<byte> key, out byte* block, out int available)
    {
        block = null;
        available = 0;
        if (root == null)
            return false;

        if (order == NativeTreeOrder.Plain)
        {
            var leaf = DescendTo(pool, order, root, key, default);
            var slot = LowerBound(order, leaf, key, default, pool.PageSize, out var found);
            if (found)
                block = NativePage.Block(leaf, slot, pool.PageSize, out available);
            return found;
        }

        // A pair tree: the first entry at or after (key, empty) may sit at the start of the next
        // leaf, so this needs a path to step along.
        Span<NativeFrame> path = stackalloc NativeFrame[height];
        if (!Seek(pool, order, root, key, default, path))
            return false;
        var frame = path[^1];
        block = NativePage.Block((byte*)frame.Page, frame.Slot, pool.PageSize, out available);
        return NativePage.ReadA(block, available).SequenceEqual(key);
    }

    internal static bool FindExact(
        NativePagePool pool, NativeTreeOrder order, byte* root, int height, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (root == null)
            return false;
        var leaf = DescendTo(pool, order, root, key, value);
        var slot = LowerBound(order, leaf, key, value, pool.PageSize, out var found);
        if (!found)
            return false;
        if (order == NativeTreeOrder.Pair)
            return true;
        var block = NativePage.Block(leaf, slot, pool.PageSize, out var available);
        return NativePage.ReadBlock(block, available).B.SequenceEqual(value);
    }

    private static byte* DescendTo(NativePagePool pool, NativeTreeOrder order, byte* page, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        while (!NativePage.IsLeaf(page))
            page = NativePage.Child(page, ChildIndex(order, page, a, b, pool.PageSize), pool.PageSize);
        return page;
    }

    /// <summary>
    /// Fills <paramref name="path"/> with the way to the first entry at or after the probe — or,
    /// with <paramref name="leftmost"/>, to the first entry of all. False when there is none.
    /// </summary>
    internal static bool Seek(
        NativePagePool pool, NativeTreeOrder order, byte* root, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, Span<NativeFrame> path, bool leftmost = false)
    {
        var pageSize = pool.PageSize;
        var page = root;
        for (var d = 0; d < path.Length; d++)
        {
            if (NativePage.IsLeaf(page))
            {
                if (d != path.Length - 1)
                    NativePage.Corrupt("a leaf above the bottom level");
                var slot = leftmost ? 0 : LowerBound(order, page, a, b, pageSize, out _);
                path[d] = new NativeFrame((nint)page, slot, false);
                return Settle(pageSize, path);
            }

            var index = leftmost ? 0 : ChildIndex(order, page, a, b, pageSize);
            path[d] = new NativeFrame((nint)page, index, false);
            page = NativePage.Child(page, index, pageSize);
        }

        NativePage.Corrupt("the tree is deeper than its recorded height");
        return false;
    }

    /// <summary>Moves the path to the next entry in order. False past the last one.</summary>
    internal static bool Advance(int pageSize, Span<NativeFrame> path)
    {
        path[^1].Slot++;
        return Settle(pageSize, path);
    }

    /// <summary>If the path points past the end of its leaf, moves it to the start of the next leaf.</summary>
    private static bool Settle(int pageSize, Span<NativeFrame> path)
    {
        var bottom = path.Length - 1;
        while (path[bottom].Slot >= NativePage.Count((byte*)path[bottom].Page))
        {
            var d = bottom - 1;
            while (d >= 0 && path[d].Slot + 1 >= NativePage.Count((byte*)path[d].Page))
                d--;
            if (d < 0)
                return false;

            path[d].Slot++;
            var page = (byte*)path[d].Page;
            for (d++; d <= bottom; d++)
            {
                page = NativePage.Child((byte*)path[d - 1].Page, path[d - 1].Slot, pageSize);
                path[d] = new NativeFrame((nint)page, 0, false);
            }
        }

        return true;
    }

    internal static NativeTreeStats Validate(NativePagePool pool, NativeTreeOrder order, byte* root, int height, long count)
    {
        if (root == null)
        {
            if (height != 0 || count != 0)
                throw new InvalidOperationException($"Empty tree with height {height} and count {count}.");
            return new NativeTreeStats(0, 0, 0, 0, 0, 0, 0);
        }

        var walk = new ValidationWalk(pool, order);
        walk.Visit(root, height - 1, null, 0, null, 0);
        if (NativePage.Level(root) > 0 && NativePage.Count(root) < 2)
            throw new InvalidOperationException("An internal root with a single child.");
        if (walk.Entries != count)
            throw new InvalidOperationException($"Count {count} but {walk.Entries} entries.");
        return new NativeTreeStats(
            walk.Entries, height, walk.LeafPages + walk.InternalPages, walk.LeafPages, walk.InternalPages, walk.UsedBytes, walk.OutOfLine);
    }

    private sealed class ValidationWalk(NativePagePool pool, NativeTreeOrder order)
    {
        public long Entries;
        public long LeafPages;
        public long InternalPages;
        public long UsedBytes;
        public long OutOfLine;

        /// <summary>Checks a page whose entries must lie in <c>[low, high)</c> (null bounds open).</summary>
        public void Visit(byte* page, int level, byte* low, int lowAvailable, byte* high, int highAvailable)
        {
            var pageSize = pool.PageSize;
            var h = NativePage.Head(page);
            Check(h->Magic == NativePage.MagicValue, "bad page magic");
            Check(h->RefCount > 0, "a reachable page with refcount 0");
            Check(h->Level == level, $"page at level {h->Level} where {level} belongs");
            Check(h->Count > 0, "an empty page");
            Check(h->HeapStart >= NativePage.HeaderSize + (NativePage.SlotSize * h->Count) && h->HeapStart <= pageSize, "heap overlaps slots");

            long live = 0;
            var blobs = false;
            byte* previous = null;
            var previousAvailable = 0;
            for (var i = 0; i < h->Count; i++)
            {
                var entry = NativePage.Entry(page, i, pageSize, out var available);
                live += NativePage.EntrySize(entry, level, available);
                var block = NativePage.Block(page, i, pageSize, out var blockAvailable);
                if (NativePage.BlockHasBlobs(block))
                {
                    // Decoding checks each blob pointer leads to a live, well-formed blob.
                    blobs = true;
                    Check(NativePage.ReadBlock(block, blockAvailable).Size > 0, "unreadable block");
                    OutOfLine += (Unsafe.ReadUnaligned<ushort>(block) >> 15) + (Unsafe.ReadUnaligned<ushort>(block + 2) >> 15);
                }

                if (level > 0 && i == 0)
                {
                    Check(NativePage.BlockSize(block, blockAvailable) == NativePage.EmptyBlockSize, "first internal entry carries a separator");
                }
                else
                {
                    if (previous != null)
                        Check(NativePage.CompareBlocks(order, previous, previousAvailable, block, blockAvailable) < 0, "entries out of order");
                    if (low != null)
                        Check(NativePage.CompareBlocks(order, low, lowAvailable, block, blockAvailable) <= 0, "entry below its lower bound");
                    if (high != null)
                        Check(NativePage.CompareBlocks(order, high, highAvailable, block, blockAvailable) > 0, "entry at or above its upper bound");
                    previous = block;
                    previousAvailable = blockAvailable;
                }
            }

            Check(live + h->Dead == pageSize - h->HeapStart, "heap accounting: live + dead != heap");
            Check(!blobs || (h->Flags & NativePage.FlagHasBlobs) != 0, "blob flag missing");
            UsedBytes += NativePage.Used(page, pageSize);

            if (level == 0)
            {
                LeafPages++;
                Entries += h->Count;
                return;
            }

            InternalPages++;
            for (var i = 0; i < h->Count; i++)
            {
                byte* childLow = low;
                var childLowAvailable = lowAvailable;
                if (i > 0)
                    childLow = NativePage.Block(page, i, pageSize, out childLowAvailable);
                byte* childHigh = high;
                var childHighAvailable = highAvailable;
                if (i + 1 < h->Count)
                    childHigh = NativePage.Block(page, i + 1, pageSize, out childHighAvailable);
                Visit(NativePage.Child(page, i, pageSize), level - 1, childLow, childLowAvailable, childHigh, childHighAvailable);
            }
        }

        private static void Check(bool condition, string what)
        {
            if (!condition)
                throw new InvalidOperationException($"Native tree invariant violated: {what}.");
        }
    }

    /// <summary>
    /// A lazy range walk. It is its own first enumerator (as a compiler-generated iterator is), so
    /// a range read costs this object and one small path array. It holds the version for its whole
    /// life, which is what keeps the pages under its path alive.
    /// </summary>
    private sealed class RangeIterator<T> : IEnumerable<T>, IEnumerator<T>
    {
        private const int Fresh = -2;
        private const int Ready = 0;
        private const int Running = 1;
        private const int Done = 2;

        private readonly NativeTree _tree;
        private readonly byte[]? _lowKey;
        private readonly byte[]? _lowValue;
        private readonly byte[]? _high;
        private readonly NativeEntryProjector<T> _projector;
        private readonly int _owner;
        private NativeFrame[]? _path;
        private int _state;
        private T _current = default!;

        public RangeIterator(NativeTree tree, byte[]? lowKey, byte[]? lowValue, byte[]? high, NativeEntryProjector<T> projector)
        {
            _tree = tree;
            _lowKey = lowKey;
            _lowValue = lowValue;
            _high = high;
            _projector = projector;
            _owner = Environment.CurrentManagedThreadId;
            _state = Fresh;
        }

        public T Current => _current;

        object? IEnumerator.Current => _current;

        public IEnumerator<T> GetEnumerator()
        {
            if (_state == Fresh && _owner == Environment.CurrentManagedThreadId)
            {
                _state = Ready;
                return this;
            }

            return new RangeIterator<T>(_tree, _lowKey, _lowValue, _high, _projector) { _state = Ready };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public bool MoveNext()
        {
            var tree = _tree;
            var pool = tree._pool;
            bool positioned;
            switch (_state)
            {
                case Ready:
                    var root = tree.Root();
                    if (root == null)
                    {
                        _state = Done;
                        return false;
                    }

                    _path = new NativeFrame[tree._height];
                    positioned = _lowKey is null
                        ? Seek(pool, tree._order, root, default, default, _path, leftmost: true)
                        : Seek(pool, tree._order, root, _lowKey, _lowValue, _path);
                    _state = Running;
                    break;
                case Running:
                    positioned = Advance(pool.PageSize, _path);
                    break;
                default:
                    return false;
            }

            if (positioned)
            {
                var frame = _path![^1];
                var block = NativePage.Block((byte*)frame.Page, frame.Slot, pool.PageSize, out var available);
                var parts = NativePage.ReadBlock(block, available);
                if (_high is null || parts.A.SequenceCompareTo(_high) <= 0)
                {
                    _current = _projector(parts.A, parts.B);
                    GC.KeepAlive(tree);
                    return true;
                }
            }

            _state = Done;
            _path = null;
            _current = default!;
            GC.KeepAlive(tree);
            return false;
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            _state = Done;
            _path = null;
        }
    }
}
