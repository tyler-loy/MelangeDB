using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace MelangeDB.Storage.Faster;

internal sealed unsafe partial class NativeTree
{
    /// <summary>
    /// The writer that turns one version into the next. Callers guarantee one writer at a time per
    /// tree; readers of every published version proceed concurrently and never see this edit's
    /// pages, which become visible only through the version <see cref="Commit"/> returns.
    /// <para>
    /// <b>Path copies.</b> A change descends read-only first, so a no-op (removing an absent key,
    /// adding a pair already present) copies nothing. Otherwise every page on the path that this
    /// edit did not create is replaced by a private copy stamped with the edit's generation; pages
    /// it did create are changed in place. So a one-operation edit copies one path, and a bulk edit
    /// of hundreds of operations copies each page it touches once, however often it touches it.
    /// </para>
    /// <para>
    /// <b>Ownership.</b> The edit holds one reference to its working root and the version it started
    /// from (so the pages it shares stay alive). <see cref="Commit"/> moves the root reference into
    /// the new version; <see cref="Dispose"/> — or the finalizer, for an edit simply dropped —
    /// releases it, which frees everything the edit allocated and returns the references it took on
    /// shared pages.
    /// </para>
    /// <para>
    /// If an operation throws midway (out of memory, or a corruption check), the edit is faulted: it
    /// refuses further use and abandons its pages rather than risk releasing a half-rebuilt
    /// structure twice. A leak is recoverable; a double free is not.
    /// </para>
    /// </summary>
    [SkipLocalsInit]
    internal sealed class Edit : IDisposable
    {
        private const int Open = 0;
        private const int Committed = 1;
        private const int Disposed = 2;
        private const int Faulted = 3;

        private readonly NativePagePool _pool;
        private readonly NativeTreeOrder _order;
        private readonly NativeTreeWriter _writer;
        private readonly int _pageSize;
        private NativeTree? _source;
        private nint _root;
        private int _height;
        private long _count;
        private int _state;
        private bool _changed;

        internal Edit(NativeTree source, byte* root)
        {
            _source = source;
            _pool = source._pool;
            _order = source._order;
            _pageSize = _pool.PageSize;
            _writer = new NativeTreeWriter(_pool, _order);
            _height = source._height;
            _count = source._count;
            if (root != null)
                NativePage.Retain(root);
            _root = (nint)root;
        }

        ~Edit()
        {
            if (_state == Open && _root != 0)
                NativePage.Release(_pool, (byte*)_root);
        }

        /// <summary>Entries in the edit's current state.</summary>
        public long Count => _count;

        /// <summary>
        /// Plain: sets <paramref name="key"/> to <paramref name="value"/>, adding or replacing.
        /// Pair: adds the pair (<paramref name="key"/>, <paramref name="value"/>) if absent. Returns
        /// whether an entry was added.
        /// </summary>
        public bool Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            ThrowIfNotOpen();
            try
            {
                var added = SetCore(key, value);
                GC.KeepAlive(this);
                return added;
            }
            catch
            {
                Fault();
                throw;
            }
        }

        /// <summary>Plain trees: removes <paramref name="key"/>; returns whether it was present.</summary>
        public bool Remove(ReadOnlySpan<byte> key)
        {
            if (_order != NativeTreeOrder.Plain)
                throw new InvalidOperationException("A pair tree removes by (key, value).");
            return RemoveChecked(key, default);
        }

        /// <summary>Pair trees: removes the pair; returns whether it was present.</summary>
        public bool Remove(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (_order != NativeTreeOrder.Pair)
                throw new InvalidOperationException("A plain tree removes by key.");
            return RemoveChecked(key, value);
        }

        /// <summary>Reads the edit's current state; see <see cref="NativeTree.TryGet(ReadOnlySpan{byte}, out byte[])"/>.</summary>
        public bool TryGet(ReadOnlySpan<byte> key, [NotNullWhen(true)] out byte[]? value)
        {
            ThrowIfNotOpen();
            var found = Find(_pool, _order, (byte*)_root, _height, key, out var block, out var available);
            value = found ? NativePage.ReadBlock(block, available).B.ToArray() : null;
            GC.KeepAlive(this);
            return found;
        }

        /// <summary>Reads the edit's current state; see <see cref="NativeTree.ContainsKey"/>.</summary>
        public bool ContainsKey(ReadOnlySpan<byte> key)
        {
            ThrowIfNotOpen();
            var found = Find(_pool, _order, (byte*)_root, _height, key, out _, out _);
            GC.KeepAlive(this);
            return found;
        }

        /// <summary>Checks the edit's current state; see <see cref="NativeTree.Validate()"/>.</summary>
        public NativeTreeStats Validate()
        {
            ThrowIfNotOpen();
            var stats = NativeTree.Validate(_pool, _order, (byte*)_root, _height, _count);
            GC.KeepAlive(this);
            return stats;
        }

        /// <summary>
        /// Publishes the edit's state as a new version and ends the edit. An edit that changed
        /// nothing returns the version it started from.
        /// </summary>
        public NativeTree Commit()
        {
            ThrowIfNotOpen();
            _state = Committed;
            GC.SuppressFinalize(this);
            var source = _source!;
            _source = null;
            var root = _root;
            _root = 0;
            if (!_changed)
            {
                if (root != 0)
                    NativePage.Release(_pool, (byte*)root);
                return source;
            }

            return new NativeTree(_pool, _order, root, _height, _count);
        }

        /// <summary>Abandons the edit, freeing every page and blob it allocated.</summary>
        public void Dispose()
        {
            if (_state != Open)
                return;
            _state = Disposed;
            GC.SuppressFinalize(this);
            _source = null;
            var root = _root;
            _root = 0;
            if (root != 0)
                NativePage.Release(_pool, (byte*)root);
        }

        private void ThrowIfNotOpen()
        {
            switch (_state)
            {
                case Open:
                    return;
                case Faulted:
                    throw new InvalidOperationException("This edit failed midway and can no longer be used.");
                default:
                    throw new ObjectDisposedException(nameof(Edit), "This edit has been committed or disposed.");
            }
        }

        private void Fault()
        {
            _state = Faulted;
            GC.SuppressFinalize(this);
            _root = 0;
        }

        private bool RemoveChecked(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            ThrowIfNotOpen();
            try
            {
                var removed = RemoveCore(key, value);
                GC.KeepAlive(this);
                return removed;
            }
            catch
            {
                Fault();
                throw;
            }
        }

        private bool SetCore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            var newEntry = NativeTreeWriter.Scratch.For(_pageSize).NewEntry;
            if (_root == 0)
            {
                var leaf = _writer.NewPage(0);
                var size = NativePage.WriteBlock(_pool, newEntry, key, value, _pool.MaxLeafEntry);
                NativePage.Append(leaf, newEntry, size);
                _root = (nint)leaf;
                _height = 1;
                _count = 1;
                _changed = true;
                return true;
            }

            Span<NativeFrame> path = stackalloc NativeFrame[_height];
            var found = Descend(key, value, path);
            if (found && _order == NativeTreeOrder.Pair)
                return false;

            MakePathWritable(path);
            var bottom = path[^1];
            var page = (byte*)bottom.Page;
            var entrySize = NativePage.WriteBlock(_pool, newEntry, key, value, _pool.MaxLeafEntry);
            Span<EntryRef> add = [new EntryRef(newEntry, entrySize)];
            NativeTreeWriter.Outcome outcome;
            if (found)
            {
                var old = NativePage.Entry(page, bottom.Slot, _pageSize, out var available);
                var oldSize = NativePage.BlockSize(old, available);
                NativePage.ReleaseBlock(_pool, old, available);
                if (oldSize == entrySize)
                {
                    // Same encoded size — the common case of a fixed-width value — overwrites in place.
                    Buffer.MemoryCopy(newEntry, old, entrySize, entrySize);
                    if (NativePage.BlockHasBlobs(newEntry))
                        NativePage.Head(page)->Flags |= NativePage.FlagHasBlobs;
                    outcome = NativeTreeWriter.Outcome.None;
                }
                else
                {
                    outcome = _writer.Splice(page, bottom.Slot, 1, add, _height == 1, bottom.RightEdge);
                }
            }
            else
            {
                outcome = _writer.Splice(page, bottom.Slot, 0, add, _height == 1, bottom.RightEdge);
                _count++;
            }

            Propagate(path, outcome);
            _changed = true;
            return !found;
        }

        private bool RemoveCore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (_root == 0)
                return false;
            Span<NativeFrame> path = stackalloc NativeFrame[_height];
            if (!Descend(key, value, path))
                return false;

            MakePathWritable(path);
            var bottom = path[^1];
            var page = (byte*)bottom.Page;
            var entry = NativePage.Entry(page, bottom.Slot, _pageSize, out var available);
            NativePage.ReleaseBlock(_pool, entry, available);
            var outcome = _writer.Splice(page, bottom.Slot, 1, [], _height == 1, rightEdge: false);
            _count--;
            Propagate(path, outcome);
            _changed = true;
            return true;
        }

        /// <summary>Read-only descent recording the path; the leaf frame holds the probe's lower bound.</summary>
        private bool Descend(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, Span<NativeFrame> path)
        {
            var page = (byte*)_root;
            var rightEdge = true;
            for (var d = 0; d < path.Length; d++)
            {
                if (NativePage.IsLeaf(page))
                {
                    if (d != path.Length - 1)
                        NativePage.Corrupt("a leaf above the bottom level");
                    var slot = LowerBound(_order, page, a, b, _pageSize, out var found);
                    path[d] = new NativeFrame((nint)page, slot, rightEdge);
                    return found;
                }

                var index = ChildIndex(_order, page, a, b, _pageSize);
                path[d] = new NativeFrame((nint)page, index, rightEdge);
                rightEdge &= index == NativePage.Count(page) - 1;
                page = NativePage.Child(page, index, _pageSize);
            }

            NativePage.Corrupt("the tree is deeper than its recorded height");
            return false;
        }

        /// <summary>Replaces every shared page on the path, top down, by a private copy.</summary>
        private void MakePathWritable(Span<NativeFrame> path)
        {
            var root = (byte*)_root;
            if (!_writer.IsWritable(root))
            {
                // The edit's own reference moves from the shared root to the copy.
                var copy = NativePage.Copy(_pool, root, _writer.Generation);
                _root = (nint)copy;
                NativePage.Release(_pool, root);
            }

            path[0].Page = _root;
            for (var d = 1; d < path.Length; d++)
                path[d].Page = (nint)_writer.MakeChildWritable((byte*)path[d - 1].Page, path[d - 1].Slot);
        }

        /// <summary>
        /// Carries a leaf's outcome up the path: a split adds entries to the parent, an underfull
        /// child is merged or redistributed by its parent, and either may cascade. A split root
        /// grows the tree by a level; a root left with one child shrinks it.
        /// </summary>
        private void Propagate(Span<NativeFrame> path, NativeTreeWriter.Outcome outcome)
        {
            Span<EntryRef> add = stackalloc EntryRef[NativeTreeWriter.MaxSplit];
            for (var d = path.Length - 2; d >= 0 && outcome != NativeTreeWriter.Outcome.None; d--)
            {
                var parent = (byte*)path[d].Page;
                var slot = path[d].Slot;
                if (outcome == NativeTreeWriter.Outcome.Split)
                {
                    // The parent's reference moves to the first page; the rest get entries after it.
                    NativePage.SetChild(parent, slot, _writer.SplitPage(0), _pageSize);
                    var count = _writer.SplitEntries(add);
                    outcome = _writer.Splice(parent, slot + 1, 0, add[..count], d == 0, path[d].RightEdge);
                }
                else
                {
                    outcome = _writer.Rebalance(parent, slot, d == 0);
                }
            }

            if (outcome == NativeTreeWriter.Outcome.Split)
            {
                _root = (nint)_writer.NewRoot(NativePage.Level(_writer.SplitPage(0)) + 1);
                _height++;
            }

            CollapseRoot();
        }

        private void CollapseRoot()
        {
            while (_root != 0)
            {
                var root = (byte*)_root;
                if (!NativePage.IsLeaf(root) && NativePage.Count(root) == 1)
                {
                    // The root's reference to its only child becomes the edit's.
                    var child = NativePage.Child(root, 0, _pageSize);
                    _writer.FreeShell(root);
                    _root = (nint)child;
                    _height--;
                }
                else if (NativePage.IsLeaf(root) && NativePage.Count(root) == 0)
                {
                    _writer.FreeShell(root);
                    _root = 0;
                    _height = 0;
                }
                else
                {
                    Debug.Assert(_height == NativePage.Level(root) + 1, "height out of step with the root's level");
                    return;
                }
            }
        }
    }
}
