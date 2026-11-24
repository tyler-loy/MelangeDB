using System.IO.Hashing;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace MelangeDB.Storage.Faster;

/// <summary>How a <see cref="MelangeLogPagedKv"/> reads a record that has left memory.</summary>
internal enum MelangeLogReadMode
{
    /// <summary>A pointer into a read-only memory map of the record's segment file: a page fault, not a system call and a copy.</summary>
    Mmap,

    /// <summary><c>RandomAccess.Read</c> into a scratch buffer: a system call and a copy, through the OS page cache.</summary>
    Pread,
}

/// <summary>
/// Settings the bake-off varies for <see cref="MelangeLogPagedKv"/>. Page sizes are log2 bytes and are
/// clamped to an eighth of the space's budget. <see cref="RandomAdvice"/> marks every segment map
/// <c>MADV_RANDOM</c> (Linux), turning off the kernel's read-around on faults. <see cref="Prefetch"/>
/// has <c>ReadMany</c> ask the kernel for every pending record's pages before copying any of them, so
/// a batch's disk reads are in flight together. <see cref="BlobPointReadsViaPread"/> reads a blob
/// space's single-key misses with one <c>RandomAccess.Read</c> instead of through the map: a 4 KB
/// payload spans two OS pages, which costs a map two synchronous faults but a read one request. Off by
/// default: under the 1 GiB Linux cap it gained within noise (7.7k vs 7.2k blob reads/s), and uncapped
/// on Windows, where the file is cached, it halved blob reads (126k to 58k/s).
/// </summary>
internal sealed record MelangeLogTuning(
    MelangeLogReadMode ReadMode,
    bool RandomAdvice,
    bool Prefetch,
    int MainPageBits,
    int BlobPageBits,
    bool BlobPointReadsViaPread)
{
    public static MelangeLogTuning Default { get; } =
        new(MelangeLogReadMode.Mmap, RandomAdvice: false, Prefetch: true, MainPageBits: 16, BlobPageBits: 20, BlobPointReadsViaPread: false);
}

/// <summary>
/// Opens <see cref="MelangeLogPagedKv"/> spaces: <c>main.mlog.N</c> / <c>blob.mlog.N</c> segment files
/// in the store directory, swept on open and deleted on close.
/// </summary>
internal sealed class MelangeLogPagedKvFactory(MelangeLogTuning? tuning = null) : IPagedKvFactory
{
    private readonly MelangeLogTuning _tuning = tuning ?? MelangeLogTuning.Default;

    public string EngineName => "melangelog";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        if (memoryBudgetBytes < 1L << 15 || !BitOperations.IsPow2(memoryBudgetBytes))
            throw new ArgumentOutOfRangeException(nameof(memoryBudgetBytes), memoryBudgetBytes, "Must be a power of two, at least 32 KiB.");

        var budgetBits = BitOperations.Log2((ulong)memoryBudgetBytes);

        // At least eight pages in the ring, so eviction is fine-grained; at least 4 KiB a page.
        var pageBits = Math.Max(12, Math.Min(space == PagedKvSpace.Main ? _tuning.MainPageBits : _tuning.BlobPageBits, budgetBits - 3));

        // Segments are preallocated and mapped whole, so keep them near the space's size when it is
        // small (the test suite opens hundreds of 4 MiB spaces) and at 64 MiB otherwise.
        var segmentBits = Math.Max(pageBits, Math.Min(26, budgetBits + 1));
        var name = space == PagedKvSpace.Main ? "main.mlog" : "blob.mlog";

        Directory.CreateDirectory(directory);
        foreach (var file in Directory.EnumerateFiles(directory, name + "*"))
        {
            if (Path.GetFileName(file).StartsWith(name, StringComparison.Ordinal))
                File.Delete(file);
        }

        return new MelangeLogPagedKv(
            Path.Combine(directory, name),
            pageBits,
            ringPageBits: budgetBits - pageBits,
            segmentBits,
            hashBucketsHint,
            _tuning,
            readGuessBytes: space == PagedKvSpace.Main ? 2048 : 8448,
            preadPointReads: space == PagedKvSpace.Blob && _tuning.BlobPointReadsViaPread);
    }
}

/// <summary>
/// One paged-store KV space on MelangeDB's own log: Tsavorite's write path and LMDB's read path.
/// <para>
/// <b>Log.</b> A single append-only address space. The newest <c>2^ringPageBits</c> pages of it live in
/// a ring of native memory (<c>NativeMemory.AlignedAlloc</c>, off the GC heap); older pages live in
/// segment files. A record is <see cref="RecordHeader"/> + key + value, 8-byte aligned. A record that
/// fits a page never straddles one (the tail skips to the next page instead); a larger one starts on
/// a page boundary and runs across as many pages as it needs. So a record's header and key are always
/// contiguous, and a page never straddles a segment.
/// </para>
/// <para>
/// <b>Writes.</b> An upsert whose newest record is still in the ring and has the capacity rewrites it
/// in place, setting the new length (the #137 contract: a shorter value reads back at exactly its
/// length). Anything else is a blind append at the tail: a record that has left memory is never read
/// to be replaced. When the tail needs a page and the ring is full, the oldest page is written to its
/// segment with <c>RandomAccess.Write</c> on the calling thread, and evicted.
/// </para>
/// <para>
/// <b>Reads.</b> A record in the ring is read straight from it. One below the ring is read through a
/// read-only memory map of its segment (<see cref="MelangeLogReadMode.Mmap"/>), which is created as
/// soon as the segment file is and stays coherent with the buffered writes landing in it. That read
/// is reported as faulted, whatever the OS page cache did with it. Batched reads (<c>ReadMany</c>, i.e.
/// scans) prefetch their pages first. <see cref="MelangeLogTuning"/> has the <c>RandomAccess.Read</c>
/// fallbacks the bake-off measured.
/// </para>
/// <para>
/// <b>Index.</b> Open addressing with linear probing in native memory: 64-bit key hash (XxHash3) to the
/// newest record's address, grown 2× by rehash past 75% load, with no ceiling. A blind append cannot
/// check that the record it supersedes has the same key, so each record carries the address of the
/// one it superseded (<see cref="RecordHeader.Prev"/>): a hash's records form a chain, newest first,
/// and a read walks it until the key matches. Distinct keys share a chain only on a full 64-bit hash
/// collision, so a read is one hop in practice, but a collision is still exact.
/// </para>
/// <para>
/// Every call arrives under the store's lock, one at a time, so nothing here is thread-safe. Nothing
/// is durable: the store is rebuilt from the commit log on every start.
/// </para>
/// </summary>
internal sealed unsafe class MelangeLogPagedKv : IPagedKv
{
    internal const int HeaderSize = 24;
    private const long FirstAddress = 64;
    private const int MaxKeyLength = 1024;
    private const long OsPage = 4096;

    private readonly string _basePath;
    private readonly MelangeLogTuning _tuning;
    private readonly int _pageBits;
    private readonly int _pageSize;
    private readonly long _pageOffsetMask;
    private readonly long _ringPages;
    private readonly long _ringMask;
    private readonly int _segmentBits;
    private readonly long _segmentSize;
    private readonly long _segmentOffsetMask;
    private readonly int _readGuess;
    private readonly ulong _hashMask;
    private readonly List<Segment> _segments = [];
    private readonly byte[] _scratch;
    private readonly bool _batchPread;
    private readonly bool _pointPread;

    private byte* _ring;
    private long _tail = FirstAddress;
    private long _headPage;
    private long _headAddress;
    private long _tailPage;

    private IndexEntry* _index;
    private long _indexMask;
    private long _indexCount;
    private long _indexGrowAt;

    private long[] _pendingAddresses = [];
    private int[] _pendingSlots = [];
    private bool _disposed;

    /// <param name="hashMaskForTest">ANDed into every key hash; a small mask forces collisions (self-check only).</param>
    internal MelangeLogPagedKv(
        string basePath, int pageBits, int ringPageBits, int segmentBits, long hashBucketsHint,
        MelangeLogTuning tuning, int readGuessBytes, bool preadPointReads = false, ulong hashMaskForTest = ulong.MaxValue)
    {
        if (pageBits < 12 || ringPageBits < 1 || segmentBits < pageBits)
            throw new ArgumentOutOfRangeException(nameof(pageBits), "Need 4 KiB+ pages, two+ ring pages, and segments no smaller than a page.");

        _basePath = basePath;
        _tuning = tuning;
        _pageBits = pageBits;
        _pageSize = 1 << pageBits;
        _pageOffsetMask = _pageSize - 1;
        _ringPages = 1L << ringPageBits;
        _ringMask = _ringPages - 1;
        _segmentBits = segmentBits;
        _segmentSize = 1L << segmentBits;
        _segmentOffsetMask = _segmentSize - 1;
        _readGuess = Math.Min(_pageSize, Math.Max(readGuessBytes, HeaderSize + MaxKeyLength));
        _hashMask = hashMaskForTest;
        _batchPread = tuning.ReadMode == MelangeLogReadMode.Pread;
        _pointPread = _batchPread || preadPointReads;
        _scratch = _pointPread ? new byte[_readGuess] : [];

        CapacityBytes = _ringPages << pageBits;
        _ring = (byte*)NativeMemory.AlignedAlloc((nuint)CapacityBytes, (nuint)OsPage);

        var capacity = (long)BitOperations.RoundUpToPowerOf2((ulong)Math.Clamp(hashBucketsHint * 2, 1L << 12, 1L << 30));
        AllocateIndex(capacity);
    }

    public long CapacityBytes { get; }

    /// <summary>Diagnostics: the log tail, the lowest in-memory address, and the live index entries.</summary>
    internal (long Tail, long Head, long IndexEntries) Addresses => (_tail, _headAddress, _indexCount);

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        faulted = false;
        var hash = Hash(key);
        var slot = FindSlot(hash);
        return slot < 0 ? null : ReadChain(key, hash, _index[slot].Address, _pointPread, ref faulted);
    }

    /// <summary>
    /// Serves ring hits directly, then reads the rest in address order, after asking the kernel for
    /// all of their pages at once (<see cref="Prefetch"/>). Returns how many reads went below the ring.
    /// </summary>
    public int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (values.Length < keys.Length)
            throw new ArgumentException("values must be at least as long as keys", nameof(values));
        if (_pendingAddresses.Length < keys.Length)
        {
            _pendingAddresses = new long[keys.Length];
            _pendingSlots = new int[keys.Length];
        }

        var faults = 0;
        var pending = 0;
        for (var i = 0; i < keys.Length; i++)
        {
            values[i] = null;
            var hash = Hash(keys[i]);
            var slot = FindSlot(hash);
            if (slot < 0)
                continue;
            var address = _index[slot].Address;
            if (address >= _headAddress)
            {
                var faulted = false;
                values[i] = ReadChain(keys[i], hash, address, _batchPread, ref faulted);
                if (faulted)
                    faults++;
                continue;
            }

            _pendingAddresses[pending] = address;
            _pendingSlots[pending++] = i;
        }

        if (pending == 0)
            return faults;

        var addresses = _pendingAddresses.AsSpan(0, pending);
        var slots = _pendingSlots.AsSpan(0, pending);
        addresses.Sort(slots);
        if (_tuning.Prefetch && !_batchPread)
            Prefetch(addresses);

        for (var j = 0; j < pending; j++)
        {
            var key = keys[slots[j]];
            var faulted = true;
            values[slots[j]] = ReadChain(key, Hash(key), addresses[j], _batchPread, ref faulted);
        }

        return faults + pending;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CheckKey(key);
        if (_indexCount >= _indexGrowAt)
            GrowIndex();

        var hash = Hash(key);
        var slot = FindSlot(hash);
        long prev = 0;
        if (slot >= 0)
        {
            var head = _index[slot].Address;
            prev = head;
            if (head >= _headAddress)
            {
                var record = RingPointer(head);
                ref var header = ref Unsafe.AsRef<RecordHeader>(record);
                ValidateHeader(head, header);
                if (header.KeyLength == key.Length && new ReadOnlySpan<byte>(record + HeaderSize, key.Length).SequenceEqual(key))
                {
                    if (header.ValueLength >= 0 && header.Capacity >= value.Length)
                    {
                        // In place: the record is in the ring, so it has not been flushed yet and nothing
                        // on disk goes stale. Setting the length is what makes a shrink read back exact.
                        header.ValueLength = value.Length;
                        header.Checksum = Checksum(hash, value.Length, header.Capacity, header.Prev, value);
                        WriteInPlace(head + HeaderSize + key.Length, value);
                        return;
                    }

                    // The new version replaces this one in the chain rather than linking to it.
                    prev = header.Prev;
                }
            }
        }

        var address = Append(key, value, hash, prev, tombstone: false);
        if (slot >= 0)
        {
            _index[slot].Address = address;
        }
        else
        {
            ref var entry = ref _index[~slot];
            entry.Hash = hash;
            entry.Address = address;
            _indexCount++;
        }
    }

    /// <summary>
    /// Drops the key's index entry. Unlike an upsert, a delete reads the chain (from the ring or the
    /// map) to confirm the key is there and that no other key shares its hash; if one does, which
    /// takes a full 64-bit collision, it appends a tombstone instead so the other key stays reachable.
    /// </summary>
    public void Delete(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CheckKey(key);
        var hash = Hash(key);
        var slot = FindSlot(hash);
        if (slot < 0)
            return;

        var head = _index[slot].Address;
        bool found = false, live = false, otherKeys = false, faulted = false;
        for (var address = head; address != 0;)
        {
            var view = RecordView(address, _batchPread, ref faulted);
            var header = MemoryMarshal.Read<RecordHeader>(view);
            ValidateHeader(address, header);
            if (header.KeyLength == key.Length && view.Slice(HeaderSize, key.Length).SequenceEqual(key))
            {
                if (!found)
                {
                    found = true;
                    live = header.ValueLength >= 0;
                }
            }
            else
            {
                otherKeys = true;
            }

            if (found && otherKeys)
                break;
            address = header.Prev;
        }

        if (!found || !live)
            return;
        if (!otherKeys)
            RemoveSlot(slot);
        else
            _index[slot].Address = Append(key, default, hash, head, tombstone: true);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        NativeMemory.AlignedFree(_ring);
        _ring = null;
        NativeMemory.Free(_index);
        _index = null;
        foreach (var segment in _segments)
            segment.Dispose();
        _segments.Clear();
    }

    // ---- Reads ------------------------------------------------------------------------------

    private byte[]? ReadChain(ReadOnlySpan<byte> key, ulong hash, long address, bool pread, ref bool faulted)
    {
        while (address != 0)
        {
            var view = RecordView(address, pread, ref faulted);
            var header = MemoryMarshal.Read<RecordHeader>(view);
            ValidateHeader(address, header);
            if (header.KeyLength != key.Length || !view.Slice(HeaderSize, key.Length).SequenceEqual(key))
            {
                address = header.Prev;
                continue;
            }

            if (header.ValueLength < 0)
                return null;

            var value = GC.AllocateUninitializedArray<byte>(header.ValueLength);
            var valueOffset = HeaderSize + key.Length;
            if (view.Length >= valueOffset + value.Length)
                view.Slice(valueOffset, value.Length).CopyTo(value);
            else
                CopyOut(address + valueOffset, value, pread, ref faulted);

            if (Checksum(hash, header.ValueLength, header.Capacity, header.Prev, value) != header.Checksum)
                throw new InvalidDataException($"melangelog: checksum mismatch in the record at {address} ({_basePath}).");
            return value;
        }

        return null;
    }

    /// <summary>
    /// The record at <paramref name="address"/>, from its header to the end of its page: from the ring,
    /// from the segment's map, or (pread mode) as much of it as a guess-sized read brings in. Its header
    /// and key are always included, since they never straddle a page.
    /// </summary>
    private ReadOnlySpan<byte> RecordView(long address, bool pread, ref bool faulted)
    {
        var inPage = (int)(_pageSize - (address & _pageOffsetMask));
        if (address >= _headAddress)
            return new ReadOnlySpan<byte>(RingPointer(address), inPage);

        faulted = true;
        if (!pread)
            return new ReadOnlySpan<byte>(MappedPointer(address), inPage);

        var length = Math.Min(inPage, _readGuess);
        var buffer = _scratch.AsSpan(0, length);
        ReadFromFile(address, buffer);
        return buffer;
    }

    /// <summary>Copies the log's bytes at <paramref name="address"/> out, page by page: from the ring, or from disk.</summary>
    private void CopyOut(long address, Span<byte> destination, bool pread, ref bool faulted)
    {
        while (!destination.IsEmpty)
        {
            var count = (int)Math.Min(destination.Length, _pageSize - (address & _pageOffsetMask));
            var piece = destination[..count];
            if (address >= _headAddress)
            {
                new ReadOnlySpan<byte>(RingPointer(address), count).CopyTo(piece);
            }
            else
            {
                faulted = true;
                if (!pread)
                    new ReadOnlySpan<byte>(MappedPointer(address), count).CopyTo(piece);
                else
                    ReadFromFile(address, piece);
            }

            address += count;
            destination = destination[count..];
        }
    }

    /// <summary>A read within one page (so within one segment) through the file, for pread mode.</summary>
    private void ReadFromFile(long address, Span<byte> buffer)
    {
        var segment = _segments[(int)(address >> _segmentBits)];
        var offset = address & _segmentOffsetMask;
        while (!buffer.IsEmpty)
        {
            var read = RandomAccess.Read(segment.Handle, buffer, offset);
            if (read <= 0)
                throw new InvalidDataException($"melangelog: short read at {address} ({_basePath}).");
            buffer = buffer[read..];
            offset += read;
        }
    }

    /// <summary>
    /// The mmap analogue of issuing a batch of reads together: <c>MADV_WILLNEED</c> (Linux) or
    /// <c>PrefetchVirtualMemory</c> (Windows) over every pending record, so the kernel has the disk
    /// reads in flight before the copy loop faults on the first. Two rounds: the first OS page of each
    /// record (header, key, and for a small row all of it), then, with the headers readable, the rest
    /// of any record that runs past that page.
    /// </summary>
    private void Prefetch(ReadOnlySpan<long> addresses)
    {
        var ranges = new PrefetchRanges();
        foreach (var address in addresses)
            ranges.Add(MappedPointer(address), HeaderSize);
        ranges.Flush();

        foreach (var address in addresses)
        {
            var header = Unsafe.ReadUnaligned<RecordHeader>(MappedPointer(address));
            var length = (long)HeaderSize + Math.Clamp(header.KeyLength, 0, MaxKeyLength) + Math.Max(header.Capacity, 0);
            var firstPageEnd = (address & ~(OsPage - 1)) + OsPage;
            var end = Math.Min(address + length, (address | _segmentOffsetMask) + 1);
            if (end > firstPageEnd)
                ranges.Add(MappedPointer(firstPageEnd), end - firstPageEnd);
        }

        ranges.Flush();
    }

    // ---- Writes -----------------------------------------------------------------------------

    private long Append(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong hash, long prev, bool tombstone)
    {
        var size = (HeaderSize + key.Length + (long)value.Length + 7) & ~7L;
        var capacity = checked((int)(size - HeaderSize - key.Length));
        var valueLength = tombstone ? -1 : value.Length;

        // A record that fits a page never straddles one; a bigger one starts on a page boundary.
        var offset = _tail & _pageOffsetMask;
        if (size <= _pageSize ? offset + size > _pageSize : offset != 0)
            _tail = (_tail | _pageOffsetMask) + 1;

        var address = _tail;
        EnsurePage(address >> _pageBits);
        var record = RingPointer(address);
        Unsafe.AsRef<RecordHeader>(record) = new RecordHeader
        {
            KeyLength = key.Length,
            ValueLength = valueLength,
            Capacity = capacity,
            Checksum = Checksum(hash, valueLength, capacity, prev, value),
            Prev = prev,
        };
        key.CopyTo(new Span<byte>(record + HeaderSize, key.Length));
        _tail = address + size;

        // The value, page by page: only a record bigger than a page touches more than one.
        var at = address + HeaderSize + key.Length;
        while (!value.IsEmpty)
        {
            EnsurePage(at >> _pageBits);
            var count = (int)Math.Min(value.Length, _pageSize - (at & _pageOffsetMask));
            value[..count].CopyTo(new Span<byte>(RingPointer(at), count));
            at += count;
            value = value[count..];
        }

        return address;
    }

    /// <summary>Rewrites a value in a record that is wholly in the ring (its first page is, so every later one is).</summary>
    private void WriteInPlace(long address, ReadOnlySpan<byte> value)
    {
        while (!value.IsEmpty)
        {
            var count = (int)Math.Min(value.Length, _pageSize - (address & _pageOffsetMask));
            value[..count].CopyTo(new Span<byte>(RingPointer(address), count));
            address += count;
            value = value[count..];
        }
    }

    /// <summary>Makes <paramref name="page"/> the tail page, flushing and evicting the oldest pages to make room.</summary>
    private void EnsurePage(long page)
    {
        while (_tailPage < page)
        {
            var next = _tailPage + 1;
            if (next - _headPage >= _ringPages)
                EvictHead();
            _tailPage = next;
        }
    }

    /// <summary>
    /// Writes the oldest ring page to its segment and drops it from the ring: synchronous, on the
    /// writer's thread. Only ever a full page (the tail page is never the head while another is allocated).
    /// </summary>
    private void EvictHead()
    {
        var pageAddress = _headPage << _pageBits;
        var segment = SegmentAt(pageAddress >> _segmentBits);
        RandomAccess.Write(segment.Handle, new ReadOnlySpan<byte>(RingSlot(_headPage), _pageSize), pageAddress & _segmentOffsetMask);
        _headPage++;
        _headAddress = _headPage << _pageBits;
    }

    private Segment SegmentAt(long number)
    {
        while (_segments.Count <= number)
        {
            _segments.Add(new Segment(
                $"{_basePath}.{_segments.Count}",
                _segmentSize,
                map: _tuning.ReadMode == MelangeLogReadMode.Mmap,
                randomAdvice: _tuning.RandomAdvice));
        }

        return _segments[(int)number];
    }

    // ---- Index ------------------------------------------------------------------------------

    /// <summary>The slot holding <paramref name="hash"/>, or the complement of the empty slot it would go in.</summary>
    private long FindSlot(ulong hash)
    {
        var i = (long)hash & _indexMask;
        while (true)
        {
            ref var entry = ref _index[i];
            if (entry.Address == 0)
                return ~i;
            if (entry.Hash == hash)
                return i;
            i = (i + 1) & _indexMask;
        }
    }

    /// <summary>Backward-shift deletion: no tombstones in the table, so probe lengths never degrade.</summary>
    private void RemoveSlot(long slot)
    {
        var hole = slot;
        var j = slot;
        while (true)
        {
            j = (j + 1) & _indexMask;
            ref var entry = ref _index[j];
            if (entry.Address == 0)
                break;
            var ideal = (long)entry.Hash & _indexMask;
            if (((j - ideal) & _indexMask) >= ((j - hole) & _indexMask))
            {
                _index[hole] = entry;
                hole = j;
            }
        }

        _index[hole] = default;
        _indexCount--;
    }

    private void GrowIndex()
    {
        var old = _index;
        var oldCapacity = _indexMask + 1;
        AllocateIndex(oldCapacity * 2);
        for (long i = 0; i < oldCapacity; i++)
        {
            if (old[i].Address == 0)
                continue;
            var j = (long)old[i].Hash & _indexMask;
            while (_index[j].Address != 0)
                j = (j + 1) & _indexMask;
            _index[j] = old[i];
        }

        NativeMemory.Free(old);
    }

    private void AllocateIndex(long capacity)
    {
        _index = (IndexEntry*)NativeMemory.AllocZeroed((nuint)capacity, (nuint)sizeof(IndexEntry));
        _indexMask = capacity - 1;
        _indexGrowAt = capacity / 4 * 3;
    }

    // ---- Helpers ----------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong Hash(ReadOnlySpan<byte> key) => XxHash3.HashToUInt64(key) & _hashMask;

    /// <summary>
    /// Binds the value bytes to the key (through its hash), the lengths and the chain link, so a torn,
    /// misdirected or bit-rotted record throws rather than reading back as somebody's row.
    /// </summary>
    private static uint Checksum(ulong keyHash, int valueLength, int capacity, long prev, ReadOnlySpan<byte> value)
    {
        var seed = keyHash ^ ((((ulong)(uint)valueLength << 32) | (uint)capacity) * 0x9E3779B97F4A7C15UL) ^ (ulong)prev;
        return (uint)XxHash3.HashToUInt64(value, unchecked((long)seed));
    }

    private void ValidateHeader(long address, in RecordHeader header)
    {
        if (header.KeyLength is < 0 or > MaxKeyLength
            || header.Capacity < 0
            || header.ValueLength < -1
            || header.ValueLength > header.Capacity
            || header.Prev < 0
            || header.Prev >= address
            || address + HeaderSize + header.KeyLength + header.Capacity > _tail)
        {
            throw new InvalidDataException($"melangelog: corrupt record header at {address} ({_basePath}).");
        }
    }

    private static void CheckKey(ReadOnlySpan<byte> key)
    {
        if (key.Length > MaxKeyLength)
            throw new ArgumentException($"Keys are at most {MaxKeyLength} bytes.", nameof(key));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* RingSlot(long page) => _ring + ((page & _ringMask) << _pageBits);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* RingPointer(long address) => RingSlot(address >> _pageBits) + (address & _pageOffsetMask);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* MappedPointer(long address) => _segments[(int)(address >> _segmentBits)].Base + (address & _segmentOffsetMask);

    /// <summary>
    /// A record's fixed header. <see cref="Capacity"/> is the value room the record was laid out with
    /// (its length rounded up to the record's 8-byte alignment); an in-place write may use all of it.
    /// <see cref="ValueLength"/> -1 is a tombstone. <see cref="Prev"/> is the record this one superseded
    /// in its hash chain, 0 for none.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    private struct RecordHeader
    {
        public int KeyLength;
        public int ValueLength;
        public int Capacity;
        public uint Checksum;
        public long Prev;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct IndexEntry
    {
        public ulong Hash;
        public long Address; // 0 = empty slot
    }

    /// <summary>
    /// One segment file: preallocated to its full size and, in mmap mode, mapped read-only whole at
    /// creation. Pages are written through the handle (buffered), and the map, sharing the OS page
    /// cache, sees them; only flushed pages are ever read through it.
    /// </summary>
    private sealed class Segment : IDisposable
    {
        private readonly string _path;
        private readonly MemoryMappedFile? _map;
        private readonly MemoryMappedViewAccessor? _view;

        public Segment(string path, long size, bool map, bool randomAdvice)
        {
            _path = path;
            Handle = File.OpenHandle(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
            try
            {
                RandomAccess.SetLength(Handle, size);
                if (!map)
                    return;
                _map = MemoryMappedFile.CreateFromFile(Handle, null, size, MemoryMappedFileAccess.Read, HandleInheritability.None, leaveOpen: true);
                _view = _map.CreateViewAccessor(0, size, MemoryMappedFileAccess.Read);
                byte* pointer = null;
                _view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
                Base = pointer + _view.PointerOffset;
                if (randomAdvice)
                    MelangeLogNative.Advise(Base, size, MelangeLogNative.MadvRandom);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public SafeFileHandle Handle { get; }

        public byte* Base { get; private set; }

        public void Dispose()
        {
            if (Base != null)
            {
                _view!.SafeMemoryMappedViewHandle.ReleasePointer();
                Base = null;
            }

            _view?.Dispose();
            _map?.Dispose();
            Handle.Dispose();
            try
            {
                File.Delete(_path);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    /// <summary>Coalesces sorted address ranges into whole OS pages and hands them to the kernel as prefetch hints.</summary>
    private ref struct PrefetchRanges
    {
        private nint _start;
        private nint _end;

        public void Add(byte* pointer, long length)
        {
            var start = (nint)pointer & ~(nint)(OsPage - 1);
            var end = ((nint)pointer + (nint)length + (nint)(OsPage - 1)) & ~(nint)(OsPage - 1);
            if (_end != 0 && start <= _end && start >= _start)
            {
                _end = Math.Max(_end, end);
                return;
            }

            Flush();
            _start = start;
            _end = end;
        }

        public void Flush()
        {
            if (_end != 0)
                MelangeLogNative.WillNeed(_start, _end - _start);
            _start = 0;
            _end = 0;
        }
    }
}

/// <summary>The two OS calls the mmap read path uses as hints, resolved at runtime; absent, they are no-ops.</summary>
internal static unsafe class MelangeLogNative
{
    public const int MadvRandom = 1;
    private const int MadvWillNeed = 3;

    private static readonly delegate* unmanaged<nint, nuint, int, int> s_madvise = Resolve("libc.so.6", "madvise", OperatingSystem.IsLinux());
    private static readonly delegate* unmanaged<nint, nuint, nint*, uint, int> s_prefetchVirtualMemory =
        (delegate* unmanaged<nint, nuint, nint*, uint, int>)Resolve("kernel32.dll", "PrefetchVirtualMemory", OperatingSystem.IsWindows());

    public static void Advise(byte* start, long length, int advice)
    {
        if (s_madvise == null)
            return;
        var aligned = (nint)start & ~(nint)(Environment.SystemPageSize - 1);
        _ = s_madvise(aligned, (nuint)((nint)start + (nint)length - aligned), advice);
    }

    public static void WillNeed(nint start, nint length)
    {
        if (s_madvise != null)
        {
            _ = s_madvise(start, (nuint)length, MadvWillNeed);
        }
        else if (s_prefetchVirtualMemory != null)
        {
            // One WIN32_MEMORY_RANGE_ENTRY { VirtualAddress, NumberOfBytes } for the current process (-1).
            var range = stackalloc nint[2] { start, length };
            _ = s_prefetchVirtualMemory(-1, 1, range, 0);
        }
    }

    private static delegate* unmanaged<nint, nuint, int, int> Resolve(string library, string export, bool platform)
    {
        if (platform && NativeLibrary.TryLoad(library, out var handle) && NativeLibrary.TryGetExport(handle, export, out var function))
            return (delegate* unmanaged<nint, nuint, int, int>)function;
        return null;
    }
}
