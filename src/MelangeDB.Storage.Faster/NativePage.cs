using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace MelangeDB.Storage.Faster;

/// <summary>How a <see cref="NativeTree"/> orders its entries.</summary>
internal enum NativeTreeOrder
{
    /// <summary>
    /// A map: entries are (key, value) and ordered by key alone, byte-wise exactly as
    /// <see cref="MemoryExtensions.SequenceCompareTo{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/> orders
    /// them (a prefix sorts first). At most one entry per key.
    /// </summary>
    Plain,

    /// <summary>
    /// A set of pairs, for secondary indexes: entries are (indexed value, primary key), with no
    /// payload, ordered by value and then by key — each part byte-wise, exactly as
    /// <c>IndexEntry.CompareTo</c> orders them. Because an empty key sorts before every other, a
    /// seek to (value, empty) lands on the first entry holding that value.
    /// </summary>
    Pair,
}

/// <summary>
/// The on-page layout of <see cref="NativeTree"/>, and the primitives that read and write it. Every
/// pointer arithmetic on a page lives here, so this is the file to audit.
/// <para>
/// <b>Page</b> (fixed size <c>P</c>, 64-byte aligned, from <see cref="NativePagePool"/>) — a slotted
/// page: a header, then an array of 16-bit slot offsets growing up, then entry bytes growing down
/// from the end of the page. Slots are in key order; entry bytes are in no particular order, and the
/// bytes of removed or replaced entries are only counted (<see cref="Header.Dead"/>) until a
/// compaction rewrites the heap.
/// <code>
///   0  int    RefCount     parents + version roots + edits holding this page (Interlocked)
///   4  byte   Level        0 for a leaf; a parent is always one above its children
///   5  byte   Magic        0xB7, checked by debug asserts to catch stale pointers
///   6  ushort Count        number of slots
///   8  ushort HeapStart    entry bytes occupy [HeapStart, P)
///  10  ushort Dead         bytes inside the heap that belong to no slot
///  12  byte   Flags        bit 0: some entry may hold an out-of-line blob
///  16  long   Generation   the edit (or builder) that created the page
///  32  ushort Slots[Count] offsets of the entries, in order
/// </code>
/// </para>
/// <para>
/// <b>Block</b> — the encoding of one ordered pair of byte strings (A, B), shared by leaf entries
/// and separators: <c>[ushort fieldA][ushort fieldB][part A][part B]</c>. A field below 0x8000 is
/// the length of a part stored inline; the field 0x8000 means the part is out of line, and in its
/// place sits an 8-byte pointer to a blob. In <see cref="NativeTreeOrder.Plain"/> A is the key and
/// B the value; in <see cref="NativeTreeOrder.Pair"/> A is the indexed value and B the primary key.
/// </para>
/// <para>
/// <b>Leaf entry</b> — just a block. <b>Internal entry</b> — <c>[8-byte child pointer][block]</c>,
/// where the block is the separator: the least possible entry of that child's subtree, so every
/// entry of child <c>i</c> is at least separator <c>i</c> and below separator <c>i + 1</c>. The
/// first entry of an internal page carries an empty separator that is never compared: its lower
/// bound is whatever bounded the page itself. Separators are the shortest strings that separate two
/// neighbouring leaf entries, so they are usually a few bytes even when keys are long.
/// </para>
/// <para>
/// <b>Blob</b> — <c>[int RefCount][int Length][data]</c>, a separate native allocation for a part
/// too long to sit inline. A page copy shares its blobs (incrementing their refcounts) rather than
/// copying them, so a 64 KiB value is never copied by a path copy.
/// </para>
/// <para>
/// <b>Size limits</b> keep the split arithmetic valid. A leaf entry is at most a quarter of the
/// usable page (<see cref="NativePagePool.MaxLeafEntry"/>, slot included) and an internal entry at
/// most an eighth (<see cref="NativePagePool.MaxInternalEntry"/>); parts move out of line, longest
/// first, until the entry fits. With entries capped at C/4 for a usable size C, a page that held at
/// most C and receives one more entry holds at most 5C/4, and cutting it where the left half first
/// reaches half the total leaves both sides below C/2 + C/4 — two pages always suffice. Rebalancing
/// two siblings when one has fallen below C/4 likewise always fits in two.
/// </para>
/// <para>
/// <b>Bounds.</b> Every length read from a page is checked against the page (or blob) it came
/// from before it is used to form a span; a violation throws rather than reading outside the
/// allocation. These checks are a handful of integer comparisons on paths that already do a
/// byte-wise comparison, so they stay on in release builds.
/// </para>
/// </summary>
[SkipLocalsInit]
internal static unsafe class NativePage
{
    public const int HeaderSize = 32;
    public const int SlotSize = 2;
    public const int ChildSize = 8;
    public const int BlockHeaderSize = 4;
    public const int PointerSize = 8;
    public const int BlobHeaderSize = 8;
    public const ushort OutOfLine = 0x8000;
    public const int MaxInlinePart = 0x7FFF;
    public const byte MagicValue = 0xB7;
    public const byte FlagHasBlobs = 1;

    /// <summary>The smallest block: two empty inline parts.</summary>
    public const int EmptyBlockSize = BlockHeaderSize;

    /// <summary>An internal entry with an empty separator, as the first entry of every internal page.</summary>
    public const int FirstInternalEntrySize = ChildSize + EmptyBlockSize;

    [StructLayout(LayoutKind.Explicit, Size = HeaderSize)]
    public struct Header
    {
        [FieldOffset(0)] public int RefCount;
        [FieldOffset(4)] public byte Level;
        [FieldOffset(5)] public byte Magic;
        [FieldOffset(6)] public ushort Count;
        [FieldOffset(8)] public ushort HeapStart;
        [FieldOffset(10)] public ushort Dead;
        [FieldOffset(12)] public byte Flags;
        [FieldOffset(16)] public long Generation;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BlobHeader
    {
        public int RefCount;
        public int Length;
    }

    /// <summary>A decoded block: both parts as spans over the page or blob that holds them.</summary>
    public readonly ref struct Parts(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, int size)
    {
        public readonly ReadOnlySpan<byte> A = a;
        public readonly ReadOnlySpan<byte> B = b;

        /// <summary>The encoded size of the block, in bytes.</summary>
        public readonly int Size = size;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Header* Head(byte* page) => (Header*)page;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort* Slots(byte* page) => (ushort*)(page + HeaderSize);

    public static int Count(byte* page) => ((Header*)page)->Count;

    public static int Level(byte* page) => ((Header*)page)->Level;

    public static bool IsLeaf(byte* page) => ((Header*)page)->Level == 0;

    /// <summary>Bytes of the page in use by live entries and their slots.</summary>
    public static int Used(byte* page, int pageSize)
    {
        var h = Head(page);
        return pageSize - h->HeapStart - h->Dead + (SlotSize * h->Count);
    }

    /// <summary>Bytes that could still be added, counting dead bytes a compaction would reclaim.</summary>
    public static int Free(byte* page) =>
        Head(page)->HeapStart - HeaderSize - (SlotSize * Head(page)->Count) + Head(page)->Dead;

    public static void Init(byte* page, int pageSize, int level, long generation)
    {
        var h = Head(page);
        *h = default;
        h->RefCount = 1;
        h->Level = (byte)level;
        h->Magic = MagicValue;
        h->HeapStart = (ushort)pageSize;
        h->Generation = generation;
    }

    [Conditional("DEBUG")]
    public static void AssertPage(byte* page)
    {
        Debug.Assert(page != null, "null page");
        Debug.Assert(Head(page)->Magic == MagicValue, "page magic: a stale or foreign pointer");
        Debug.Assert(Head(page)->RefCount > 0, "page refcount: read after free");
    }

    /// <summary>The entry at <paramref name="slot"/>, and how many page bytes follow its start.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte* Entry(byte* page, int slot, int pageSize, out int available)
    {
        var h = Head(page);
        Debug.Assert((uint)slot < h->Count, "slot out of range");
        int offset = Slots(page)[slot];
        if (offset < h->HeapStart || offset >= pageSize)
            Corrupt("slot offset outside the page heap");
        available = pageSize - offset;
        return page + offset;
    }

    /// <summary>The block of the entry at <paramref name="slot"/>: the entry itself in a leaf, its separator otherwise.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte* Block(byte* page, int slot, int pageSize, out int available)
    {
        var entry = Entry(page, slot, pageSize, out available);
        if (Head(page)->Level == 0)
            return entry;
        available -= ChildSize;
        if (available < BlockHeaderSize)
            Corrupt("internal entry truncated by the page end");
        return entry + ChildSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte* Child(byte* page, int slot, int pageSize)
    {
        Debug.Assert(Head(page)->Level > 0, "child of a leaf");
        var entry = Entry(page, slot, pageSize, out var available);
        if (available < FirstInternalEntrySize)
            Corrupt("internal entry truncated by the page end");
        var child = (byte*)Unsafe.ReadUnaligned<nint>(entry);
        AssertPage(child);
        return child;
    }

    public static void SetChild(byte* page, int slot, byte* child, int pageSize)
    {
        var entry = Entry(page, slot, pageSize, out _);
        Unsafe.WriteUnaligned(entry, (nint)child);
    }

    /// <summary>The encoded size of the block at <paramref name="block"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int BlockSize(byte* block, int available)
    {
        if (available < BlockHeaderSize)
            Corrupt("block header truncated");
        var size = BlockHeaderSize + PartSize(Unsafe.ReadUnaligned<ushort>(block))
            + PartSize(Unsafe.ReadUnaligned<ushort>(block + 2));
        if (size > available)
            Corrupt("block runs past the end of its page");
        return size;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int EntrySize(byte* entry, int level, int available) =>
        level == 0 ? BlockSize(entry, available) : ChildSize + BlockSize(entry + ChildSize, available - ChildSize);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int PartSize(ushort field) => (field & OutOfLine) != 0 ? PointerSize : field;

    public static bool BlockHasBlobs(byte* block) =>
        ((Unsafe.ReadUnaligned<ushort>(block) | Unsafe.ReadUnaligned<ushort>(block + 2)) & OutOfLine) != 0;

    /// <summary>Part A of a block alone — all a <see cref="NativeTreeOrder.Plain"/> comparison needs.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> ReadA(byte* block, int available)
    {
        if (available < BlockHeaderSize)
            Corrupt("block header truncated");
        var field = Unsafe.ReadUnaligned<ushort>(block);
        if (BlockHeaderSize + PartSize(field) > available)
            Corrupt("block part runs past the end of its page");
        return Part(block + BlockHeaderSize, field);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Parts ReadBlock(byte* block, int available)
    {
        var size = BlockSize(block, available);
        var fieldA = Unsafe.ReadUnaligned<ushort>(block);
        var fieldB = Unsafe.ReadUnaligned<ushort>(block + 2);
        var a = Part(block + BlockHeaderSize, fieldA);
        var b = Part(block + BlockHeaderSize + PartSize(fieldA), fieldB);
        return new Parts(a, b, size);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<byte> Part(byte* at, ushort field)
    {
        if ((field & OutOfLine) == 0)
            return new ReadOnlySpan<byte>(at, field);
        if (field != OutOfLine)
            Corrupt("out-of-line field carries a length");
        var blob = (byte*)Unsafe.ReadUnaligned<nint>(at);
        var header = (BlobHeader*)blob;
        Debug.Assert(header->RefCount > 0, "blob refcount: read after free");
        if (header->Length < 0)
            Corrupt("negative blob length");
        return new ReadOnlySpan<byte>(blob + BlobHeaderSize, header->Length);
    }

    /// <summary>
    /// Compares the probe (<paramref name="a"/>, <paramref name="b"/>) with a block, under
    /// <paramref name="order"/>: part A alone for <see cref="NativeTreeOrder.Plain"/>, A then B for
    /// <see cref="NativeTreeOrder.Pair"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Compare(NativeTreeOrder order, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, byte* block, int available)
    {
        if (order == NativeTreeOrder.Plain)
            return a.SequenceCompareTo(ReadA(block, available));
        var parts = ReadBlock(block, available);
        var byA = a.SequenceCompareTo(parts.A);
        return byA != 0 ? byA : b.SequenceCompareTo(parts.B);
    }

    /// <summary>Compares two blocks under <paramref name="order"/>.</summary>
    public static int CompareBlocks(NativeTreeOrder order, byte* x, int xAvailable, byte* y, int yAvailable)
    {
        var px = ReadBlock(x, xAvailable);
        return Compare(order, px.A, px.B, y, yAvailable);
    }

    /// <summary>
    /// Writes (<paramref name="a"/>, <paramref name="b"/>) as a block at <paramref name="destination"/>,
    /// moving parts out of line — the longest first — until the block fits in
    /// <paramref name="maxBlock"/> bytes; a part longer than an inline field can describe always
    /// goes out of line. Returns the encoded size. Blobs are allocated with refcount 1, owned by the
    /// block just written.
    /// </summary>
    public static int WriteBlock(NativePagePool pool, byte* destination, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, int maxBlock)
    {
        var aOut = a.Length > MaxInlinePart;
        var bOut = b.Length > MaxInlinePart;
        int size;
        while ((size = BlockHeaderSize + (aOut ? PointerSize : a.Length) + (bOut ? PointerSize : b.Length)) > maxBlock)
        {
            if (!aOut && (bOut || a.Length >= b.Length))
                aOut = true;
            else if (!bOut)
                bOut = true;
            else
                throw new InvalidOperationException("Two out-of-line parts exceed the block limit; the page is too small.");
        }

        var at = destination + BlockHeaderSize;
        at = WritePart(pool, destination, at, a, aOut);
        _ = WritePart(pool, destination + 2, at, b, bOut);
        return size;
    }

    private static byte* WritePart(NativePagePool pool, byte* field, byte* at, ReadOnlySpan<byte> part, bool outOfLine)
    {
        if (!outOfLine)
        {
            Unsafe.WriteUnaligned(field, (ushort)part.Length);
            part.CopyTo(new Span<byte>(at, part.Length));
            return at + part.Length;
        }

        var blob = pool.AllocateBlob(part.Length);
        part.CopyTo(new Span<byte>(blob + BlobHeaderSize, part.Length));
        Unsafe.WriteUnaligned(field, OutOfLine);
        Unsafe.WriteUnaligned(at, (nint)blob);
        return at + PointerSize;
    }

    /// <summary>
    /// The shortest separator between two neighbouring leaf entries <paramref name="left"/> &lt;
    /// <paramref name="right"/>: a pair S with left &lt; S &lt;= right under <paramref name="order"/>,
    /// returned as prefixes of right's parts.
    /// <para>
    /// The prefix of right's first differing part, one byte past the longest common prefix, is
    /// greater than left's part (it either has a greater byte where they differ, or left's part is
    /// a proper prefix of it), and at most right's part (it is a prefix of it). Where A parts tie
    /// in a pair index, the same argument applies to B; where they differ, B is left empty, which
    /// sorts first.
    /// </para>
    /// </summary>
    public static void Separator(
        NativeTreeOrder order, Parts left, Parts right, out ReadOnlySpan<byte> a, out ReadOnlySpan<byte> b)
    {
        var commonA = left.A.CommonPrefixLength(right.A);
        if (order == NativeTreeOrder.Plain || commonA < right.A.Length || commonA < left.A.Length)
        {
            Debug.Assert(commonA < right.A.Length, "left must sort below right");
            a = right.A[..(commonA + 1)];
            b = default;
            return;
        }

        // Equal A parts: separate on B.
        var commonB = left.B.CommonPrefixLength(right.B);
        Debug.Assert(commonB < right.B.Length, "left must sort below right");
        a = right.A;
        b = right.B[..(commonB + 1)];
    }

    public static void Retain(byte* page)
    {
        AssertPage(page);
        var count = Interlocked.Increment(ref Head(page)->RefCount);
        Debug.Assert(count > 1, "retained a page nothing held");
    }

    /// <summary>
    /// Drops one reference to <paramref name="page"/>; the last one frees it, after releasing every
    /// child page and blob its entries reference. A reference is only ever dropped by its holder,
    /// and a page reachable from anything live is held by that thing, so a page reaching zero is
    /// unreachable — no reader or writer can be about to touch it.
    /// </summary>
    public static void Release(NativePagePool pool, byte* page)
    {
        AssertPage(page);
        var count = Interlocked.Decrement(ref Head(page)->RefCount);
        if (count > 0)
            return;
        if (count < 0)
            Corrupt("page released more times than retained");
        ReleaseContents(pool, page);
        pool.FreePage(page);
    }

    /// <summary>Releases every reference held by the entries of <paramref name="page"/>.</summary>
    public static void ReleaseContents(NativePagePool pool, byte* page)
    {
        var h = Head(page);
        if (h->Level == 0 && (h->Flags & FlagHasBlobs) == 0)
            return;
        var pageSize = pool.PageSize;
        for (var i = 0; i < h->Count; i++)
        {
            var entry = Entry(page, i, pageSize, out var available);
            ReleaseEntry(pool, entry, h->Level, available);
        }
    }

    /// <summary>Takes one more reference to every child page and blob the entries of <paramref name="page"/> hold.</summary>
    public static void RetainContents(byte* page, int pageSize)
    {
        var h = Head(page);
        if (h->Level == 0 && (h->Flags & FlagHasBlobs) == 0)
            return;
        for (var i = 0; i < h->Count; i++)
        {
            var entry = Entry(page, i, pageSize, out var available);
            if (h->Level > 0)
            {
                Retain((byte*)Unsafe.ReadUnaligned<nint>(entry));
                entry += ChildSize;
                available -= ChildSize;
            }

            if ((h->Flags & FlagHasBlobs) != 0)
                RetainBlock(entry, available);
        }
    }

    /// <summary>Releases the child (if internal) and the blobs of one entry.</summary>
    public static void ReleaseEntry(NativePagePool pool, byte* entry, int level, int available)
    {
        if (level > 0)
        {
            Release(pool, (byte*)Unsafe.ReadUnaligned<nint>(entry));
            entry += ChildSize;
            available -= ChildSize;
        }

        ReleaseBlock(pool, entry, available);
    }

    public static void ReleaseBlock(NativePagePool pool, byte* block, int available)
    {
        _ = BlockSize(block, available);
        var fieldA = Unsafe.ReadUnaligned<ushort>(block);
        var fieldB = Unsafe.ReadUnaligned<ushort>(block + 2);
        if ((fieldA & OutOfLine) != 0)
            ReleaseBlob(pool, (byte*)Unsafe.ReadUnaligned<nint>(block + BlockHeaderSize));
        if ((fieldB & OutOfLine) != 0)
            ReleaseBlob(pool, (byte*)Unsafe.ReadUnaligned<nint>(block + BlockHeaderSize + PartSize(fieldA)));
    }

    public static void RetainBlock(byte* block, int available)
    {
        _ = BlockSize(block, available);
        var fieldA = Unsafe.ReadUnaligned<ushort>(block);
        var fieldB = Unsafe.ReadUnaligned<ushort>(block + 2);
        if ((fieldA & OutOfLine) != 0)
            RetainBlob((byte*)Unsafe.ReadUnaligned<nint>(block + BlockHeaderSize));
        if ((fieldB & OutOfLine) != 0)
            RetainBlob((byte*)Unsafe.ReadUnaligned<nint>(block + BlockHeaderSize + PartSize(fieldA)));
    }

    private static void RetainBlob(byte* blob)
    {
        var count = Interlocked.Increment(ref ((BlobHeader*)blob)->RefCount);
        Debug.Assert(count > 1, "retained a blob nothing held");
    }

    private static void ReleaseBlob(NativePagePool pool, byte* blob)
    {
        var count = Interlocked.Decrement(ref ((BlobHeader*)blob)->RefCount);
        if (count > 0)
            return;
        if (count < 0)
            Corrupt("blob released more times than retained");
        pool.FreeBlob(blob);
    }

    /// <summary>
    /// A private copy of <paramref name="source"/> for the edit of <paramref name="generation"/>:
    /// same layout, refcount 1, and one more reference to everything the entries point at, so the
    /// copy and the original each own their references and either may be released first.
    /// </summary>
    public static byte* Copy(NativePagePool pool, byte* source, long generation)
    {
        AssertPage(source);
        var pageSize = pool.PageSize;
        var copy = pool.AllocatePage();
        var h = Head(source);
        var slotsEnd = HeaderSize + (SlotSize * h->Count);
        Buffer.MemoryCopy(source, copy, slotsEnd, slotsEnd);
        var heap = pageSize - h->HeapStart;
        Buffer.MemoryCopy(source + h->HeapStart, copy + h->HeapStart, heap, heap);
        Head(copy)->RefCount = 1;
        Head(copy)->Generation = generation;
        RetainContents(copy, pageSize);
        return copy;
    }

    /// <summary>
    /// Appends an entry after the last slot. The caller has checked that it fits contiguously; the
    /// entry's references move into the page.
    /// </summary>
    public static void Append(byte* page, byte* entry, int size)
    {
        var h = Head(page);
        var heap = h->HeapStart - size;
        if (heap < HeaderSize + (SlotSize * (h->Count + 1)))
            Corrupt("append past the free space of a page");
        Buffer.MemoryCopy(entry, page + heap, size, size);
        Slots(page)[h->Count] = (ushort)heap;
        h->Count++;
        h->HeapStart = (ushort)heap;
        if (BlockHasBlobs(h->Level == 0 ? entry : entry + ChildSize))
            h->Flags |= FlagHasBlobs;
    }

    /// <summary>
    /// Rewrites the heap of <paramref name="page"/> without dead bytes, through
    /// <paramref name="scratch"/> (a page-sized buffer). Slots and entries are unchanged.
    /// </summary>
    public static void Compact(byte* page, int pageSize, byte* scratch)
    {
        Buffer.MemoryCopy(page, scratch, pageSize, pageSize);
        var h = Head(page);
        var level = h->Level;
        var heap = pageSize;
        var slots = Slots(page);
        for (var i = 0; i < h->Count; i++)
        {
            var entry = Entry(scratch, i, pageSize, out var available);
            var size = EntrySize(entry, level, available);
            heap -= size;
            Buffer.MemoryCopy(entry, page + heap, size, size);
            slots[i] = (ushort)heap;
        }

        if (heap < HeaderSize + (SlotSize * h->Count))
            Corrupt("live entries exceed the page");
        h->HeapStart = (ushort)heap;
        h->Dead = 0;
    }

    [DoesNotReturn]
    public static void Corrupt(string what) =>
        throw new InvalidOperationException($"Native tree page is corrupt: {what}.");
}
