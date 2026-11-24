using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace MelangeDB.Storage.Faster;

/// <summary>A reference to one encoded entry, wherever it lives: a page, or a scratch buffer.</summary>
internal readonly unsafe struct EntryRef(byte* pointer, int size)
{
    public readonly byte* Pointer = pointer;
    public readonly int Size = size;
}

/// <summary>One level of a root-to-leaf path: the page, and the slot taken (or found) in it.</summary>
internal struct NativeFrame(nint page, int slot, bool rightEdge)
{
    public nint Page = page;
    public int Slot = slot;

    /// <summary>The page is the last at its level — where ascending keys arrive.</summary>
    public bool RightEdge = rightEdge;
}

/// <summary>
/// The structural half of writing a <see cref="NativeTree"/>: splitting, merging and redistributing
/// pages, shared by <see cref="NativeTree.Edit"/> and <see cref="NativeTree.Builder"/>.
/// <para>
/// <b>Ownership.</b> Every structural operation works only on <em>writable</em> pages — pages
/// stamped with this writer's <see cref="Generation"/>, which nothing published can reach and which
/// therefore hold exactly one reference (their parent's, or the writer's root). Entries moved
/// between writable pages <em>move</em> their references: no refcount changes, and the emptied
/// page is freed as a bare shell (<see cref="FreeShell"/>). A shared page is first replaced by a
/// private copy (<see cref="MakeChildWritable"/>), which is where the only extra references are
/// taken. Holding to that rule is what makes the refcount bookkeeping auditable: references are
/// created by copies and by new blobs, destroyed by removals, and otherwise only move.
/// </para>
/// <para>
/// <b>Scratch.</b> Splits need buffers for the entries being repacked and the separators passed
/// upward. They come from a per-thread scratch area pinned once, so a writer allocates nothing on
/// the managed heap in steady state. A writer is used by one thread at a time and its operations do
/// not nest, which is all the per-thread scratch requires.
/// </para>
/// </summary>
[SkipLocalsInit]
internal sealed unsafe class NativeTreeWriter
{
    /// <summary>
    /// The most pages one repack can produce. The size limits guarantee two; the margin only turns
    /// a broken guarantee into an exception rather than an overrun.
    /// </summary>
    public const int MaxSplit = 4;

    /// <summary>What an operation on a page left for its parent to do.</summary>
    public enum Outcome
    {
        /// <summary>Nothing: the page changed in place and is healthy.</summary>
        None,

        /// <summary>The page shrank below a quarter full; its parent should merge or redistribute it.</summary>
        Underfull,

        /// <summary>
        /// The page was replaced by <see cref="SplitCount"/> pages (<see cref="SplitPage"/>), its
        /// shell freed. The first takes the page's place; the rest need entries after it.
        /// </summary>
        Split,
    }

    private readonly NativePagePool _pool;
    private readonly NativeTreeOrder _order;
    private readonly int _pageSize;

    public NativeTreeWriter(NativePagePool pool, NativeTreeOrder order)
    {
        _pool = pool;
        _order = order;
        _pageSize = pool.PageSize;
        Generation = pool.NextGeneration();
    }

    /// <summary>The stamp of every page this writer creates, and so may modify in place.</summary>
    public long Generation { get; }

    /// <summary>The number of pages the last split produced.</summary>
    public int SplitCount { get; private set; }

    public byte* SplitPage(int index) => (byte*)Scratch.For(_pageSize).Pages[index];

    public bool IsWritable(byte* page) => NativePage.Head(page)->Generation == Generation;

    /// <summary>A new, empty, writable page at <paramref name="level"/>.</summary>
    public byte* NewPage(int level)
    {
        var page = _pool.AllocatePage();
        NativePage.Init(page, _pageSize, level, Generation);
        return page;
    }

    /// <summary>
    /// Frees a writable page whose references have all been moved elsewhere (or released). Checked
    /// in every build: freeing a page someone else can still reach is the one mistake this design
    /// cannot survive.
    /// </summary>
    public void FreeShell(byte* page)
    {
        var h = NativePage.Head(page);
        if (h->Generation != Generation || h->RefCount != 1)
            NativePage.Corrupt("freeing a page this writer does not exclusively own");
        _pool.FreePage(page);
    }

    /// <summary>
    /// Makes the child at <paramref name="slot"/> of the writable <paramref name="parent"/>
    /// writable, replacing a shared child by a private copy, and returns it.
    /// </summary>
    public byte* MakeChildWritable(byte* parent, int slot)
    {
        Debug.Assert(IsWritable(parent), "parent must be writable");
        var child = NativePage.Child(parent, slot, _pageSize);
        if (IsWritable(child))
            return child;

        // The copy takes its own references first, then the parent's reference moves to it and the
        // old page loses one. The old page is still held by whatever version shares it, and if
        // nothing does, releasing it frees it — after its contents are safely retained by the copy.
        var copy = NativePage.Copy(_pool, child, Generation);
        NativePage.SetChild(parent, slot, copy, _pageSize);
        NativePage.Release(_pool, child);
        return copy;
    }

    /// <summary>
    /// Replaces entries <c>[start, start + removeCount)</c> of the writable <paramref name="page"/>
    /// by <paramref name="add"/>. The removed entries' references are the caller's to release or to
    /// have moved; the added entries' references move into the page. In place when the result fits
    /// (compacting the heap if needed); otherwise the page is repacked into new pages and its shell
    /// freed (<see cref="Outcome.Split"/>).
    /// </summary>
    public Outcome Splice(byte* page, int start, int removeCount, ReadOnlySpan<EntryRef> add, bool isRoot, bool rightEdge)
    {
        Debug.Assert(IsWritable(page) && NativePage.Head(page)->RefCount == 1, "splice into a shared page");
        var h = NativePage.Head(page);
        var count = h->Count;
        var level = h->Level;
        if ((uint)start > count || (uint)(start + removeCount) > count)
            NativePage.Corrupt("splice range outside the page");

        var removed = 0;
        for (var i = start; i < start + removeCount; i++)
        {
            var entry = NativePage.Entry(page, i, _pageSize, out var available);
            removed += NativePage.EntrySize(entry, level, available);
        }

        var added = 0;
        foreach (var entry in add)
            added += entry.Size;

        var usedBefore = NativePage.Used(page, _pageSize);
        var usedAfter = usedBefore - removed - (NativePage.SlotSize * removeCount) + added + (NativePage.SlotSize * add.Length);
        if (usedAfter > _pool.Usable)
            return Overflow(page, start, removeCount, add, rightEdge);

        var slots = NativePage.Slots(page);
        if (removeCount > 0)
        {
            h->Dead = (ushort)(h->Dead + removed);
            var tail = count - start - removeCount;
            Buffer.MemoryCopy(slots + start + removeCount, slots + start, tail * NativePage.SlotSize, tail * NativePage.SlotSize);
            h->Count = (ushort)(count - removeCount);
        }

        if (add.Length > 0)
        {
            count = h->Count;
            var contiguous = h->HeapStart - NativePage.HeaderSize - (NativePage.SlotSize * (count + add.Length));
            if (contiguous < added)
                NativePage.Compact(page, _pageSize, Scratch.For(_pageSize).Temp);

            var tail = count - start;
            Buffer.MemoryCopy(slots + start, slots + start + add.Length, tail * NativePage.SlotSize, tail * NativePage.SlotSize);
            int heap = h->HeapStart;
            for (var j = 0; j < add.Length; j++)
            {
                var entry = add[j];
                heap -= entry.Size;
                Buffer.MemoryCopy(entry.Pointer, page + heap, entry.Size, entry.Size);
                slots[start + j] = (ushort)heap;
                if (NativePage.BlockHasBlobs(level == 0 ? entry.Pointer : entry.Pointer + NativePage.ChildSize))
                    h->Flags |= NativePage.FlagHasBlobs;
            }

            if (heap < NativePage.HeaderSize + (NativePage.SlotSize * (count + add.Length)))
                NativePage.Corrupt("spliced entries overlap the slot array");
            h->HeapStart = (ushort)heap;
            h->Count = (ushort)(count + add.Length);
        }

        return !isRoot && usedAfter < usedBefore && usedAfter < _pool.Usable / 4 ? Outcome.Underfull : Outcome.None;
    }

    private Outcome Overflow(byte* page, int start, int removeCount, ReadOnlySpan<EntryRef> add, bool rightEdge)
    {
        var level = NativePage.Level(page);
        var count = NativePage.Count(page);
        var scratch = Scratch.For(_pageSize);
        var n = 0;
        AddEntries(scratch, ref n, page, 0, start);
        foreach (var entry in add)
            scratch.Add(ref n, entry);
        AddEntries(scratch, ref n, page, start + removeCount, count);

        // Ascending inserts land at the end of the rightmost page; splitting that page in the middle
        // would leave every page of a sequentially loaded table half empty. Splitting off just the
        // new entry instead leaves the left page full, as a bulk load would.
        Pack(n, level, 2, rightEdge && start + removeCount == count);
        FreeShell(page);
        return Outcome.Split;
    }

    private void AddEntries(Scratch scratch, ref int n, byte* page, int from, int to)
    {
        var level = NativePage.Level(page);
        for (var i = from; i < to; i++)
        {
            var entry = NativePage.Entry(page, i, _pageSize, out var available);
            scratch.Add(ref n, new EntryRef(entry, NativePage.EntrySize(entry, level, available)));
        }
    }

    /// <summary>
    /// The child at <paramref name="slot"/> of the writable <paramref name="parent"/> is underfull:
    /// merge it with a neighbour, or — if the two together would fill most of a page — redistribute
    /// their entries evenly. The parent then loses an entry or has a separator replaced, which may
    /// in turn leave it underfull or, when the new separator is longer, split it.
    /// </summary>
    public Outcome Rebalance(byte* parent, int slot, bool isRoot)
    {
        var count = NativePage.Count(parent);
        if (count < 2)
            return isRoot ? Outcome.None : Outcome.Underfull;

        var l = slot > 0 ? slot - 1 : slot;
        var r = l + 1;
        var left = MakeChildWritable(parent, l);
        var right = MakeChildWritable(parent, r);
        var level = NativePage.Level(left);
        var scratch = Scratch.For(_pageSize);
        var n = 0;
        AddEntries(scratch, ref n, left, 0, NativePage.Count(left));
        if (level == 0)
        {
            AddEntries(scratch, ref n, right, 0, NativePage.Count(right));
        }
        else
        {
            // Internal pages: the right page's first entry has no separator of its own; its lower
            // bound is the parent's separator for the right page, which moves down into it.
            var first = NativePage.Entry(right, 0, _pageSize, out _);
            var separator = NativePage.Block(parent, r, _pageSize, out var available);
            var separatorSize = NativePage.BlockSize(separator, available);
            var pulled = scratch.Pulled;
            Unsafe.WriteUnaligned(pulled, Unsafe.ReadUnaligned<nint>(first));
            Buffer.MemoryCopy(separator, pulled + NativePage.ChildSize, separatorSize, separatorSize);
            scratch.Add(ref n, new EntryRef(pulled, NativePage.ChildSize + separatorSize));
            AddEntries(scratch, ref n, right, 1, NativePage.Count(right));
        }

        long total = 0;
        for (var i = 0; i < n; i++)
            total += scratch.Entries[i].Size + NativePage.SlotSize;
        var pages = Pack(n, level, total <= _pool.Usable * 3L / 4 ? 1 : 2, appendBias: false);

        if (level == 0)
        {
            // Leaf children: the old separator is simply gone (a new one was computed if needed).
            var separator = NativePage.Block(parent, r, _pageSize, out var available);
            NativePage.ReleaseBlock(_pool, separator, available);
        }

        FreeShell(left);
        FreeShell(right);
        NativePage.SetChild(parent, l, SplitPage(0), _pageSize);
        Span<EntryRef> add = stackalloc EntryRef[MaxSplit];
        var added = SplitEntries(add);
        Debug.Assert(added == pages - 1);
        return Splice(parent, r, 1, add[..added], isRoot, rightEdge: false);
    }

    /// <summary>
    /// The parent entries for split pages 1.. of the last split — child pointer and separator —
    /// written to scratch and described in <paramref name="into"/>; returns how many.
    /// </summary>
    public int SplitEntries(Span<EntryRef> into)
    {
        var scratch = Scratch.For(_pageSize);
        for (var j = 1; j < SplitCount; j++)
        {
            var destination = scratch.ParentEntry(j);
            Unsafe.WriteUnaligned(destination, scratch.Pages[j]);
            var size = scratch.SeparatorSizes[j];
            Buffer.MemoryCopy(scratch.Separator(j), destination + NativePage.ChildSize, size, size);
            into[j - 1] = new EntryRef(destination, NativePage.ChildSize + size);
        }

        return SplitCount - 1;
    }

    /// <summary>A new root above the pages of the last split.</summary>
    public byte* NewRoot(int level)
    {
        var root = NewPage(level);
        byte* first = stackalloc byte[NativePage.FirstInternalEntrySize];
        Unsafe.WriteUnaligned(first, (nint)SplitPage(0));
        Unsafe.WriteUnaligned(first + NativePage.ChildSize, 0u);
        NativePage.Append(root, first, NativePage.FirstInternalEntrySize);
        Span<EntryRef> rest = stackalloc EntryRef[MaxSplit];
        var count = SplitEntries(rest);
        for (var j = 0; j < count; j++)
            NativePage.Append(root, rest[j].Pointer, rest[j].Size);
        return root;
    }

    /// <summary>
    /// Writes the first <paramref name="n"/> scratch entries, in order, into at least
    /// <paramref name="minPages"/> new writable pages at <paramref name="level"/>, cut so the pages
    /// are evenly filled (or, with <paramref name="appendBias"/>, so the last entry starts a page of
    /// its own). Pages land in <see cref="SplitPage"/>; for every page after the first, the
    /// separator its parent entry needs lands in scratch. Entries' references move into the pages.
    /// </summary>
    public int Pack(int n, int level, int minPages, bool appendBias)
    {
        var scratch = Scratch.For(_pageSize);
        var entries = scratch.Entries;
        var capacity = _pool.Usable;
        long total = 0;
        for (var i = 0; i < n; i++)
            total += entries[i].Size + NativePage.SlotSize;

        var k = (int)Math.Max(minPages, (total + capacity - 1) / capacity);
        k = Math.Max(1, Math.Min(k, n));
        Span<int> cuts = stackalloc int[MaxSplit + 1];
        while (k > MaxSplit || !ChooseCuts(entries, n, total, k, appendBias, capacity, cuts))
        {
            if (++k > MaxSplit)
                throw new InvalidOperationException($"{n} entries ({total} bytes) do not pack into {MaxSplit} pages.");
        }

        byte* first = stackalloc byte[NativePage.FirstInternalEntrySize];
        for (var p = 0; p < k; p++)
        {
            var page = NewPage(level);
            scratch.Pages[p] = (nint)page;
            for (var i = cuts[p]; i < cuts[p + 1]; i++)
            {
                var entry = entries[i];
                if (level > 0 && i == cuts[p])
                {
                    var separatorSize = entry.Size - NativePage.ChildSize;
                    if (p == 0)
                    {
                        // The first page keeps its parent's separator; its first entry has none.
                        if (separatorSize != NativePage.EmptyBlockSize)
                            NativePage.Corrupt("the first entry of an internal page carries a separator");
                    }
                    else
                    {
                        // Push the separator up; the entry stays behind without one.
                        Buffer.MemoryCopy(entry.Pointer + NativePage.ChildSize, scratch.Separator(p), separatorSize, separatorSize);
                        scratch.SeparatorSizes[p] = separatorSize;
                    }

                    Unsafe.WriteUnaligned(first, Unsafe.ReadUnaligned<nint>(entry.Pointer));
                    Unsafe.WriteUnaligned(first + NativePage.ChildSize, 0u);
                    NativePage.Append(page, first, NativePage.FirstInternalEntrySize);
                }
                else
                {
                    NativePage.Append(page, entry.Pointer, entry.Size);
                }
            }

            if (level == 0 && p > 0)
            {
                var below = entries[cuts[p] - 1];
                var above = entries[cuts[p]];
                NativePage.Separator(
                    _order,
                    NativePage.ReadBlock(below.Pointer, below.Size),
                    NativePage.ReadBlock(above.Pointer, above.Size),
                    out var a,
                    out var b);
                scratch.SeparatorSizes[p] = NativePage.WriteBlock(
                    _pool, scratch.Separator(p), a, b, _pool.MaxInternalEntry - NativePage.ChildSize);
            }
        }

        SplitCount = k;
        return k;
    }

    private static bool ChooseCuts(EntryRef[] entries, int n, long total, int k, bool appendBias, int capacity, Span<int> cuts)
    {
        cuts[0] = 0;
        cuts[k] = n;
        if (k == 1)
            return total <= capacity;
        if (k > n)
            return false;

        if (appendBias && k == 2)
        {
            long last = entries[n - 1].Size + NativePage.SlotSize;
            if (total - last <= capacity)
            {
                cuts[1] = n - 1;
                return true;
            }
        }

        var start = 0;
        var remaining = total;
        for (var p = 0; p < k - 1; p++)
        {
            var target = remaining / (k - p);
            long filled = 0;
            var j = start;
            var limit = n - (k - 1 - p);
            while (j < limit)
            {
                long size = entries[j].Size + NativePage.SlotSize;
                if (filled + size > capacity || (filled > 0 && filled + (size / 2) > target))
                    break;
                filled += size;
                j++;
            }

            if (j == start)
                return false;
            cuts[p + 1] = j;
            remaining -= filled;
            start = j;
        }

        return remaining <= capacity;
    }

    /// <summary>
    /// Per-thread buffers for structural operations, pinned for life (they live on the pinned
    /// object heap), sized for the largest page size the thread has written.
    /// </summary>
    internal sealed class Scratch
    {
        [ThreadStatic]
        private static Scratch? t_current;

        private readonly byte[] _buffer;
        private readonly int _stride;
        private readonly byte* _separators;
        private readonly byte* _parentEntries;

        private Scratch(int pageSize)
        {
            PageSize = pageSize;
            _stride = NativePagePool.MaxInternalEntryFor(pageSize);
            var leaf = NativePagePool.MaxLeafEntryFor(pageSize);
            var size = pageSize + leaf + _stride + (2 * MaxSplit * _stride);
            _buffer = GC.AllocateUninitializedArray<byte>(size, pinned: true);
            var at = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));
            Temp = at;
            NewEntry = at += pageSize;
            Pulled = at += leaf;
            _separators = at += _stride;
            _parentEntries = at + (MaxSplit * _stride);
        }

        public static Scratch For(int pageSize)
        {
            var scratch = t_current;
            if (scratch is null || scratch.PageSize < pageSize)
                t_current = scratch = new Scratch(pageSize);
            return scratch;
        }

        public int PageSize { get; }

        /// <summary>A page-sized buffer, for compaction.</summary>
        public byte* Temp { get; }

        /// <summary>Room for one leaf entry being written.</summary>
        public byte* NewEntry { get; }

        /// <summary>Room for one internal entry with a separator pulled down from its parent.</summary>
        public byte* Pulled { get; }

        public EntryRef[] Entries { get; private set; } = new EntryRef[512];

        public nint[] Pages { get; } = new nint[MaxSplit];

        public int[] SeparatorSizes { get; } = new int[MaxSplit];

        public byte* Separator(int page) => _separators + (page * _stride);

        public byte* ParentEntry(int page) => _parentEntries + (page * _stride);

        public void Add(ref int n, EntryRef entry)
        {
            if (n == Entries.Length)
            {
                var grown = new EntryRef[n * 2];
                Array.Copy(Entries, grown, n);
                Entries = grown;
            }

            Entries[n++] = entry;
        }
    }
}
