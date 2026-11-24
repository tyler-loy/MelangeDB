using System.Runtime.InteropServices;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// The allocator behind <see cref="NativeTree"/>: fixed-size pages and variable-size out-of-line
/// blobs in native memory, with the accounting that makes them visible to the process and to the GC.
/// <para>
/// Native memory is invisible to the garbage collector, and that is a problem precisely here: the
/// only thing that frees a dead tree version is its finalizer, and the finalizer only runs after a
/// collection the GC sees no reason to start — a few hundred small managed version objects look like
/// nothing, while the pages they pin may be gigabytes. So the pool reports what it holds through
/// <see cref="GC.AddMemoryPressure"/>, in coarse steps (<see cref="PressureStep"/>) so that the
/// call stays off the per-page path, and the GC schedules collections in proportion to the native
/// bytes actually at stake.
/// </para>
/// <para>
/// The pool holds no memory of its own and needs no disposal: every allocation is owned by a page
/// or a blob, every page and blob by a refcount, and the last reference frees it. That is what
/// lets <see cref="AllocatedBytes"/> return to exactly zero once every version and edit is gone,
/// which the tests assert. One pool may serve any number of trees, of either ordering; the page
/// size and the inline thresholds derived from it are properties of the pool because they are
/// properties of the page.
/// </para>
/// <para>
/// Thread safety: allocation runs on the writer, frees run on the writer and on the finalizer
/// thread concurrently, so every counter is maintained with <see cref="Interlocked"/>.
/// </para>
/// </summary>
internal sealed unsafe class NativePagePool
{
    /// <summary>The smallest page the layout supports; small enough for tests to force deep trees.</summary>
    public const int MinPageSize = 256;

    /// <summary>
    /// The largest page the layout supports: offsets inside a page are 16-bit, and the top bit of an
    /// inline length is the out-of-line flag, so 32 KiB is the ceiling.
    /// </summary>
    public const int MaxPageSize = 32 * 1024;

    /// <summary>The default page: large enough for a fanout in the hundreds, small enough to path-copy.</summary>
    public const int DefaultPageSize = 8 * 1024;

    /// <summary>Granularity of the memory-pressure reports to the GC.</summary>
    public const long PressureStep = 1024 * 1024;

    // Cache-line alignment: the header's refcount is the hottest word on the page, and 64 bytes
    // costs little over malloc's own alignment.
    private const int PageAlignment = 64;

    private readonly Lock _pressureLock = new();
    private long _allocatedBytes;
    private long _pages;
    private long _blobs;
    private long _generation;
    private long _reportedPressure;

    public NativePagePool(int pageSize = DefaultPageSize)
    {
        if (pageSize is < MinPageSize or > MaxPageSize || pageSize % 8 != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize), pageSize, $"Page size must be a multiple of 8 in [{MinPageSize}, {MaxPageSize}].");
        }

        PageSize = pageSize;
        Usable = pageSize - NativePage.HeaderSize;
        MaxLeafEntry = MaxLeafEntryFor(pageSize);
        MaxInternalEntry = MaxInternalEntryFor(pageSize);
    }

    /// <summary>
    /// A leaf entry may take at most a quarter of the page, slot included, so that a full page plus
    /// one more entry always splits into two pages that fit (see <see cref="NativePage"/>'s remarks).
    /// </summary>
    internal static int MaxLeafEntryFor(int pageSize) => ((pageSize - NativePage.HeaderSize) / 4) - NativePage.SlotSize;

    /// <summary>
    /// An internal entry is held to an eighth of the page, so a full internal page has a fanout of
    /// at least eight however long the keys run — but never below the 28 bytes a child pointer and
    /// two out-of-line separator parts need.
    /// </summary>
    internal static int MaxInternalEntryFor(int pageSize) =>
        Math.Max((pageSize - NativePage.HeaderSize) / 8, 32) - NativePage.SlotSize;

    /// <summary>The fixed size of every page, in bytes.</summary>
    public int PageSize { get; }

    /// <summary>The bytes of a page available to slots and entries: everything after the header.</summary>
    internal int Usable { get; }

    /// <summary>The largest encoded leaf entry; anything larger moves its parts out of line.</summary>
    internal int MaxLeafEntry { get; }

    /// <summary>The largest encoded internal entry (child pointer included).</summary>
    internal int MaxInternalEntry { get; }

    /// <summary>Native bytes currently allocated: pages plus blobs (blob headers included).</summary>
    public long AllocatedBytes => Volatile.Read(ref _allocatedBytes);

    /// <summary>Pages currently allocated, across every tree, version and edit on this pool.</summary>
    public long PagesInUse => Volatile.Read(ref _pages);

    /// <summary>Out-of-line blobs currently allocated.</summary>
    public long BlobsInUse => Volatile.Read(ref _blobs);

    /// <summary>
    /// A fresh edit generation. A page stamped with an edit's generation was created by that edit
    /// and is reachable from nothing published, so that edit may change it in place.
    /// </summary>
    internal long NextGeneration() => Interlocked.Increment(ref _generation);

    internal byte* AllocatePage()
    {
        var page = (byte*)NativeMemory.AlignedAlloc((nuint)PageSize, PageAlignment);
        Interlocked.Increment(ref _pages);
        Account(PageSize);
        return page;
    }

    internal void FreePage(byte* page)
    {
#if DEBUG
        // Poison, so a stale pointer reads garbage that fails the magic check rather than data
        // that looks plausible.
        new Span<byte>(page, PageSize).Fill(0xDD);
#endif
        NativeMemory.AlignedFree(page);
        Interlocked.Decrement(ref _pages);
        Account(-PageSize);
    }

    /// <summary>A blob of <paramref name="length"/> data bytes, refcount 1, data uninitialized.</summary>
    internal byte* AllocateBlob(int length)
    {
        var size = NativePage.BlobHeaderSize + (long)length;
        var blob = (byte*)NativeMemory.Alloc((nuint)size);
        var header = (NativePage.BlobHeader*)blob;
        header->RefCount = 1;
        header->Length = length;
        Interlocked.Increment(ref _blobs);
        Account(size);
        return blob;
    }

    internal void FreeBlob(byte* blob)
    {
        var size = NativePage.BlobHeaderSize + (long)((NativePage.BlobHeader*)blob)->Length;
        NativeMemory.Free(blob);
        Interlocked.Decrement(ref _blobs);
        Account(-size);
    }

    private void Account(long delta)
    {
        var now = Interlocked.Add(ref _allocatedBytes, delta);
        if (Math.Abs(now - Volatile.Read(ref _reportedPressure)) >= PressureStep)
            AdjustPressure();
    }

    private void AdjustPressure()
    {
        lock (_pressureLock)
        {
            // Re-read under the lock: another thread may have adjusted already. Reported pressure
            // is always a whole number of steps, so it can be returned exactly.
            var target = Volatile.Read(ref _allocatedBytes) / PressureStep * PressureStep;
            var delta = target - _reportedPressure;
            if (delta > 0)
                GC.AddMemoryPressure(delta);
            else if (delta < 0)
                GC.RemoveMemoryPressure(-delta);
            Volatile.Write(ref _reportedPressure, target);
        }
    }
}
