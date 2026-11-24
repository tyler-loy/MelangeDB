using System.Runtime.CompilerServices;

namespace MelangeDB.Storage.Faster;

internal sealed unsafe partial class NativeTree
{
    /// <summary>
    /// Builds a version bottom-up from entries supplied in ascending order — the recovery path,
    /// which loads millions of rows before anything can read them.
    /// <para>
    /// Each entry is appended to the rightmost leaf, with no search and no path copy; a leaf that
    /// reaches the fill target is closed and its parent entry appended one level up, the same way.
    /// So the cost per entry is one encode and one copy, and every page but the last at each level is
    /// filled to <c>fillFactor</c>. The default leaves a tenth of each page free, so the first edits
    /// after recovery — a value growing, a key arriving between two others — mostly fit in place
    /// rather than splitting a page that a fuller packing would have left no room in. On
    /// <see cref="ToTree"/> the last page at each level, if thin, is rebalanced with its neighbour.
    /// </para>
    /// <para>
    /// Disposing a builder that was never completed (or dropping it to the finalizer) frees every
    /// page and blob it allocated.
    /// </para>
    /// </summary>
    [SkipLocalsInit]
    internal sealed class Builder : IDisposable
    {
        private readonly NativePagePool _pool;
        private readonly NativeTreeOrder _order;
        private readonly NativeTreeWriter _writer;
        private readonly int _pageSize;
        private readonly int _fillLimit;

        // Per level, bottom up: the open (rightmost) page, and the encoded separator that page will
        // carry into its parent when it closes (empty for the leftmost page of a level). A pending
        // separator owns its blobs until it is written into a parent.
        private readonly List<nint> _open = [];
        private readonly List<byte[]> _pending = [];
        private readonly List<int> _pendingSize = [];
        private long _count;
        private bool _finished;

        public Builder(NativePagePool pool, NativeTreeOrder order, double fillFactor = 0.9)
        {
            if (fillFactor is < 0.5 or > 1.0)
                throw new ArgumentOutOfRangeException(nameof(fillFactor), fillFactor, "Fill factor must be in [0.5, 1].");
            _pool = pool;
            _order = order;
            _pageSize = pool.PageSize;
            _writer = new NativeTreeWriter(pool, order);
            _fillLimit = (int)(pool.Usable * fillFactor);
        }

        ~Builder() => ReleaseAll();

        /// <summary>Entries added so far.</summary>
        public long Count => _count;

        /// <summary>
        /// Appends an entry. Entries must arrive strictly ascending in the tree's order — by key for
        /// a plain tree, by (key, value) for a pair tree; anything else throws.
        /// </summary>
        public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            ObjectDisposedException.ThrowIf(_finished, this);
            var scratch = NativeTreeWriter.Scratch.For(_pageSize);
            if (_open.Count == 0)
                OpenLevel(0);

            var leaf = (byte*)_open[0];
            var count = NativePage.Count(leaf);
            if (count > 0)
            {
                var last = NativePage.Block(leaf, count - 1, _pageSize, out var available);
                if (NativePage.Compare(_order, key, value, last, available) <= 0)
                    throw new ArgumentException("Entries must be added in strictly ascending order.", nameof(key));
            }

            var size = NativePage.WriteBlock(_pool, scratch.NewEntry, key, value, _pool.MaxLeafEntry);
            if (count > 0 && NativePage.Used(leaf, _pageSize) + size + NativePage.SlotSize > _fillLimit)
            {
                // Close the leaf: it goes up with the separator it was opened with, and the new leaf
                // is opened with the separator between the closed leaf's last entry and this one.
                var last = NativePage.Block(leaf, count - 1, _pageSize, out var available);
                NativePage.Separator(
                    _order, NativePage.ReadBlock(last, available), NativePage.ReadBlock(scratch.NewEntry, size), out var a, out var b);
                var separator = scratch.Separator(1);
                var separatorSize = NativePage.WriteBlock(_pool, separator, a, b, _pool.MaxInternalEntry - NativePage.ChildSize);
                Close(0);
                SetPending(0, separator, separatorSize);
                leaf = (byte*)_open[0];
            }

            NativePage.Append(leaf, scratch.NewEntry, size);
            _count++;
            GC.KeepAlive(this);
        }

        /// <summary>Completes the tree and returns it as a version. The builder is finished afterwards.</summary>
        public NativeTree ToTree()
        {
            ObjectDisposedException.ThrowIf(_finished, this);
            _finished = true;
            GC.SuppressFinalize(this);
            if (_open.Count == 0)
                return Empty(_pool, _order);

            // Push the last page of every level into its parent; Close may add a level as it goes.
            for (var level = 0; level < _open.Count - 1; level++)
            {
                var last = (byte*)_open[level];
                Push(level + 1, last, _pending[level], _pendingSize[level]);
                _open[level] = 0;
                _pendingSize[level] = 0;
                Tidy(level + 1, last);
            }

            var root = (byte*)_open[^1];
            var height = _open.Count;
            _open.Clear();
            while (!NativePage.IsLeaf(root) && NativePage.Count(root) == 1)
            {
                var child = NativePage.Child(root, 0, _pageSize);
                _writer.FreeShell(root);
                root = child;
                height--;
            }

            var tree = new NativeTree(_pool, _order, (nint)root, height, _count);
            GC.KeepAlive(this);
            return tree;
        }

        /// <summary>Abandons the build, freeing everything allocated so far.</summary>
        public void Dispose()
        {
            if (_finished)
                return;
            _finished = true;
            GC.SuppressFinalize(this);
            ReleaseAll();
        }

        private void ReleaseAll()
        {
            for (var level = 0; level < _open.Count; level++)
            {
                if (_open[level] != 0)
                    NativePage.Release(_pool, (byte*)_open[level]);
                if (_pendingSize[level] > 0)
                {
                    fixed (byte* pending = _pending[level])
                        NativePage.ReleaseBlock(_pool, pending, _pendingSize[level]);
                }
            }

            _open.Clear();
        }

        private void OpenLevel(int level)
        {
            _open.Add((nint)_writer.NewPage(level));
            _pending.Add(new byte[_pool.MaxInternalEntry]);
            _pendingSize.Add(0);
        }

        private void SetPending(int level, byte* separator, int size)
        {
            fixed (byte* pending = _pending[level])
                Buffer.MemoryCopy(separator, pending, size, size);
            _pendingSize[level] = size;
        }

        /// <summary>Closes the open page at <paramref name="level"/> into its parent and opens a fresh one.</summary>
        private void Close(int level)
        {
            var page = (byte*)_open[level];
            Push(level + 1, page, _pending[level], _pendingSize[level]);
            _pendingSize[level] = 0;
            _open[level] = (nint)_writer.NewPage(level);
        }

        /// <summary>
        /// Appends (<paramref name="child"/>, separator) to the open page at <paramref name="level"/>,
        /// closing that page first if it is at the fill target. An empty separator marks the
        /// leftmost child of its level.
        /// </summary>
        private void Push(int level, byte* child, byte[] separator, int separatorSize)
        {
            if (_open.Count == level)
            {
                OpenLevel(level);
                if (separatorSize != 0)
                    NativePage.Corrupt("a new level must start with its leftmost child");
            }

            byte* entry = stackalloc byte[_pool.MaxInternalEntry];
            Unsafe.WriteUnaligned(entry, (nint)child);
            var page = (byte*)_open[level];
            var full = NativePage.Count(page) > 0
                && NativePage.Used(page, _pageSize) + NativePage.ChildSize + Math.Max(separatorSize, NativePage.EmptyBlockSize)
                    + NativePage.SlotSize > _fillLimit;
            if (full)
            {
                // The entry opens a new page: its separator becomes that page's own, carried up when
                // the page closes, and the entry itself is stored without one.
                Close(level);
                fixed (byte* bytes = separator)
                    SetPending(level, bytes, separatorSize);
                separatorSize = 0;
                page = (byte*)_open[level];
            }

            var size = NativePage.ChildSize;
            if (separatorSize == 0 || NativePage.Count(page) == 0)
            {
                Unsafe.WriteUnaligned(entry + size, 0u);
                size += NativePage.EmptyBlockSize;
            }
            else
            {
                fixed (byte* bytes = separator)
                    Buffer.MemoryCopy(bytes, entry + size, separatorSize, separatorSize);
                size += separatorSize;
            }

            NativePage.Append(page, entry, size);
        }

        /// <summary>
        /// After the last page of a level is pushed: if it is thin and shares a parent with its left
        /// neighbour, rebalance the two — provided the parent has room for a longer separator, so
        /// the rebalance cannot split the parent the builder is still filling.
        /// </summary>
        private void Tidy(int parentLevel, byte* last)
        {
            var parent = (byte*)_open[parentLevel];
            var count = NativePage.Count(parent);
            if (count < 2
                || NativePage.Child(parent, count - 1, _pageSize) != last
                || NativePage.Used(last, _pageSize) >= _pool.Usable / 4
                || NativePage.Free(parent) < _pool.MaxInternalEntry + NativePage.SlotSize)
            {
                return;
            }

            var outcome = _writer.Rebalance(parent, count - 1, isRoot: true);
            if (outcome != NativeTreeWriter.Outcome.None)
                NativePage.Corrupt("a builder rebalance split its parent");
        }
    }
}
