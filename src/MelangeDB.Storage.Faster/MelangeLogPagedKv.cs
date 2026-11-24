using System.IO.Hashing;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using MelangeDB.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Win32.SafeHandles;

namespace MelangeDB.Storage.Faster;

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
/// <param name="tuning">The bake-off's knobs; <see cref="MelangeLogTuning.Default"/> when null.</param>
/// <param name="failFlushForTest">Fault injection: given a page about to be flushed, true makes that flush throw an <see cref="IOException"/>.</param>
/// <param name="logger">Where EventId 1520 goes, once, when the platform has no prefetch hint.</param>
internal sealed class MelangeLogPagedKvFactory(MelangeLogTuning? tuning = null, Func<long, bool>? failFlushForTest = null, ILogger? logger = null) : IPagedKvFactory
{
    private readonly MelangeLogTuning _tuning = tuning ?? MelangeLogTuning.Default;
    private readonly ILogger _logger = logger ?? NullLogger.Instance;
    private bool _prefetchChecked;

    public string EngineName => "melangelog";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        if (memoryBudgetBytes < 1L << 15 || !BitOperations.IsPow2(memoryBudgetBytes))
            throw new ArgumentOutOfRangeException(nameof(memoryBudgetBytes), memoryBudgetBytes, "Must be a power of two, at least 32 KiB.");

        var budgetBits = BitOperations.Log2((ulong)memoryBudgetBytes);

        // At least eight pages in the ring, so eviction is fine-grained; at least 4 KiB a page, and
        // never smaller than the OS page (16 KiB on some arm64 kernels), which the ring is aligned to.
        var minPageBits = Math.Max(12, BitOperations.Log2((uint)Environment.SystemPageSize));
        var pageBits = Math.Max(minPageBits, Math.Min(space == PagedKvSpace.Main ? _tuning.MainPageBits : _tuning.BlobPageBits, budgetBits - 3));

        // Segments are preallocated and mapped whole, so keep them near the space's size when it is
        // small (the test suite opens hundreds of 4 MiB spaces) and at 64 MiB otherwise.
        var segmentBits = Math.Max(pageBits, Math.Min(26, budgetBits + 1));
        var name = space == PagedKvSpace.Main ? "main.mlog" : "blob.mlog";

        if (!_prefetchChecked)
        {
            _prefetchChecked = true;
            if (_tuning.ReadMode == MelangeLogReadMode.Mmap && _tuning.Prefetch && !MelangeLogNative.PrefetchAvailable)
                MelangeLogEngineLog.PrefetchUnavailable(_logger);
        }

        Directory.CreateDirectory(directory);
        foreach (var file in Directory.EnumerateFiles(directory, name + "*"))
        {
            if (Path.GetFileName(file).StartsWith(name, StringComparison.Ordinal))
                File.Delete(file);
        }

        return new MelangeLogPagedKv(
            Path.Combine(directory, name),
            pageBits,
            ringPageBits: Math.Max(1, budgetBits - pageBits),
            segmentBits,
            hashBucketsHint,
            _tuning,
            readGuessBytes: space == PagedKvSpace.Main ? 2048 : 8448,
            preadPointReads: space == PagedKvSpace.Blob && _tuning.BlobPointReadsViaPread,
            failFlushForTest: failFlushForTest);
    }
}

/// <summary>
/// One paged-store KV space on MelangeDB's own log: Tsavorite's write path and LMDB's read path.
/// <para>
/// <b>Log.</b> A single append-only address space. The newest <c>2^ringPageBits</c> pages of it live in
/// a ring of native memory (<c>NativeMemory.AlignedAlloc</c>, off the GC heap); older pages live in
/// segment files. A record is <see cref="RecordHeader"/> + key + value, 8-byte aligned. A record that
/// fits a page never straddles one: the tail skips to the next page instead, leaving a pad marker
/// (a header with <see cref="PadKeyLength"/>) wherever the skipped gap can hold one, so a scan of the
/// log can always tell a record from the bytes a reused ring slot left behind. A larger record starts
/// on a page boundary and runs across as many pages as it needs, segments included; each segment
/// records where its first record starts. So a record's header and key are always contiguous, and a
/// page never straddles a segment.
/// </para>
/// <para>
/// <b>Writes.</b> An upsert whose record is still in the ring and has the capacity rewrites it in
/// place, setting the new length (the #137 contract: a shorter value reads back at exactly its
/// length). Anything else is a blind append at the tail: a record that has left memory is never read
/// to be replaced, and a delete reads nothing at all.
/// </para>
/// <para>
/// <b>Flushing.</b> The ring has three boundaries, <c>head ≤ readOnly ≤ tail</c>. Pages from the
/// read-only boundary up are mutable, and only a record there is rewritten in place. Pages below it are
/// immutable, and a flusher thread (one per space, started on first need) writes them to their segments
/// with <c>RandomAccess.Write</c>, a run of pages a call, creating segment files as it reaches them.
/// The read-only boundary trails the tail by the ring less an eighth of it, so the flusher works that
/// far ahead of eviction. A writer that needs a page evicts the head only once it is flushed, and waits
/// for the flusher only when it has fallen that far behind: disk slower than writes. That wait is
/// counted (<see cref="HotStoreLogStatistics.FlushStalls"/>). A flush that fails is rethrown to every writer
/// that needs a page from then on.
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
/// <b>Index.</b> Open addressing with linear probing in native memory, grown 2× by rehash past 75%
/// load, with no ceiling. A slot holds the key's 128-bit hash (XxHash128, seeded per space at random)
/// and its one record's address and size. The hash is the key's identity: blind appends and deletes
/// never read the record they replace, so a 64-bit hash would need chains to tell colliding keys
/// apart, and a chain makes "is this record still current" a disk read. At 128 bits a collision is
/// as likely as two random GUIDs matching. Reads still compare the stored key, so one would throw
/// rather than return another key's row.
/// </para>
/// <para>
/// <b>Accounting.</b> The slot's size is what makes space reclamation cheap. Every append adds a
/// record's bytes to the live count of each segment it touches; superseding or deleting it subtracts
/// them, from the slot, with no read. Live bytes against the log's extent is the garbage a compactor
/// has to reclaim (#287), and a segment whose live count reaches zero holds nothing current.
/// </para>
/// <para>
/// <b>Compaction.</b> <see cref="Compact"/> deletes a flushed segment once nothing in it is live,
/// and, while the log is more than a given multiple of its live bytes, picks the flushed segment
/// with the fewest live bytes, walks it record by record, copies each record the index still points
/// at to the tail (repointing the slot), and deletes it. Liveness is the index probe, exact because
/// the hash is the key's identity. A record that spans into the segment from the previous one is
/// checked too, since the segment holds part of it.
/// </para>
/// <para>
/// Every call arrives under the store's lock, one at a time, so nothing here is thread-safe, except
/// <see cref="Statistics"/>, which statistics read without the lock. Nothing is durable: the store is
/// rebuilt from the commit log on every start.
/// </para>
/// </summary>
internal sealed unsafe class MelangeLogPagedKv : IPagedKv, ICompactingPagedKv
{
    internal const int HeaderSize = 16;

    /// <summary>The key length of a pad marker: no record has one, since a header with it fails validation.</summary>
    internal const int PadKeyLength = -1;

    private const long FirstAddress = 64;

    /// <summary>The longest a compaction step holds the store lock: half a millisecond.</summary>
    private static readonly long MaxStepTicks = System.Diagnostics.Stopwatch.Frequency / 2000;
    private const int MaxKeyLength = 1024;

    // A slot packs the record's address and size, both in 8-byte units (records are 8-byte aligned):
    // 40 bits of address, an 8 TiB log; 24 bits of size, records up to 128 MiB, larger ones marked
    // with the all-ones size and measured from their header instead.
    private const int AddressBits = 40;
    private const ulong AddressMask = (1UL << AddressBits) - 1;
    private const long MaxAddress = 1L << (AddressBits + 3);
    private const ulong SizeUnknown = (1UL << (64 - AddressBits)) - 1;

    private static readonly long OsPage = Math.Max(4096, Environment.SystemPageSize);

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
    private readonly long _hashSeed;
    private readonly ulong _hashMask;
    private readonly long _readOnlyLag;
    private readonly long _flushBatchPages;
    private readonly Lock _segmentTableLock = new();
    private readonly ManualResetEventSlim _flushWork = new(false);
    private readonly ManualResetEventSlim _flushDone = new(false);
    private readonly List<SegmentInfo> _segmentInfo = [];
    private readonly byte[] _scratch;
    private readonly bool _batchPread;
    private readonly bool _pointPread;
    private readonly Func<long, bool>? _failFlushForTest;

    private byte* _ring;
    private long _tail = FirstAddress;
    private long _headPage;
    private long _headAddress;
    private long _tailPage;
    private long _liveBytes;

    // Segments by number, published copy-on-write: the flusher creates them, compaction deletes them
    // (both under _segmentTableLock), and readers index the published array with no lock.
    private Segment?[] _segmentTable = new Segment?[16];

    // Flushing: pages below _readOnlyPage are immutable (writer-owned, published to the flusher);
    // pages below _flushedPage are on disk (flusher-owned, published to the writer).
    private long _readOnlyPage;
    private long _readOnlyAddress;
    private long _flushedPage;
    private long _flushStalls;
    private Thread? _flusher;
    private volatile bool _stopping;
    private volatile Exception? _flushFault;
    private long _reclaimedBytes;
    private long _relocatedBytes;
    private long _inPlaceWrites;
    private long _appendedWrites;
    private long _indexResizes;
    private long _integrityFailures;

    // Compaction's place: the segment being compacted (-1 for none), the next address to read in it,
    // whether the record spanning into it from before still needs checking, and the segment itself,
    // which PrepareCompactionStep reads off the lock (on the compaction thread, which alone drops it).
    private long _victim = -1;
    private long _victimCursor;
    private bool _victimSpanPending;
    private Segment? _victimSegment;
    private long _prepareBytes = 1 << 20;

    private IndexEntry* _index;
    private long _indexMask;
    private long _indexCount;
    private long _indexGrowAt;

    // While the index grows: the table it is growing from, and the next slot of it to migrate.
    private IndexEntry* _oldIndex;
    private long _oldMask;
    private long _migrateCursor;

    private long[] _pendingAddresses = [];
    private int[] _pendingSlots = [];
    private bool _disposed;

    /// <param name="hashMaskForTest">
    /// ANDed into both halves of every key hash; a small mask forces collisions, which a read must
    /// detect (self-check only).
    /// </param>
    /// <param name="failFlushForTest">Fault injection: given a page about to be flushed, true makes that flush throw.</param>
    internal MelangeLogPagedKv(
        string basePath, int pageBits, int ringPageBits, int segmentBits, long hashBucketsHint,
        MelangeLogTuning tuning, int readGuessBytes, bool preadPointReads = false, ulong hashMaskForTest = ulong.MaxValue,
        Func<long, bool>? failFlushForTest = null)
    {
        _failFlushForTest = failFlushForTest;
        if (pageBits < 12 || ringPageBits < 1 || segmentBits < pageBits)
            throw new ArgumentOutOfRangeException(nameof(pageBits), "Need 4 KiB+ pages, two+ ring pages, and segments no smaller than a page.");

        _basePath = basePath;
        _tuning = tuning;
        _pageBits = pageBits;
        _pageSize = 1 << pageBits;
        _pageOffsetMask = _pageSize - 1;
        _ringPages = 1L << ringPageBits;
        _ringMask = _ringPages - 1;

        // The read-only region: an eighth of the ring, at least two pages where the ring allows, and
        // never the tail page, which always stays mutable.
        _readOnlyLag = _ringPages - Math.Clamp(_ringPages / 8, Math.Min(2, _ringPages - 1), _ringPages - 1);
        _flushBatchPages = Math.Max(1, (_ringPages - _readOnlyLag) / 4);
        _segmentBits = segmentBits;
        _segmentSize = 1L << segmentBits;
        _segmentOffsetMask = _segmentSize - 1;
        _readGuess = Math.Min(_pageSize, Math.Max(readGuessBytes, HeaderSize + MaxKeyLength));
        _hashSeed = Random.Shared.NextInt64();
        _hashMask = hashMaskForTest;
        _batchPread = tuning.ReadMode == MelangeLogReadMode.Pread;
        _pointPread = _batchPread || preadPointReads;
        _scratch = _pointPread ? new byte[_readGuess] : [];

        CapacityBytes = _ringPages << pageBits;
        _ring = (byte*)NativeMemory.AlignedAlloc((nuint)CapacityBytes, (nuint)Math.Min(OsPage, _pageSize));

        var capacity = (long)BitOperations.RoundUpToPowerOf2((ulong)Math.Clamp(hashBucketsHint * 2, 1L << 12, 1L << 30));
        AllocateIndex(capacity);
    }

    public long CapacityBytes { get; }

    /// <summary>
    /// The space's log statistics. Plain 64-bit reads of fields the writer, flusher and compactor
    /// publish, so safe off the lock and at worst a call behind; the segment count walks the
    /// published segment table.
    /// </summary>
    public HotStoreLogStatistics Statistics
    {
        get
        {
            var tail = Volatile.Read(ref _tail);
            var reclaimed = Volatile.Read(ref _reclaimedBytes);
            var segments = 0;
            foreach (var segment in Volatile.Read(ref _segmentTable))
            {
                if (segment is not null)
                    segments++;
            }

            return new HotStoreLogStatistics
            {
                LogBytes = tail - FirstAddress - reclaimed,
                LiveBytes = Volatile.Read(ref _liveBytes),
                ReclaimedBytes = reclaimed,
                RelocatedBytes = Volatile.Read(ref _relocatedBytes),
                InPlaceWrites = Volatile.Read(ref _inPlaceWrites),
                AppendedWrites = Volatile.Read(ref _appendedWrites),
                MemoryBytes = Math.Max(0, tail - Volatile.Read(ref _headAddress)),
                FlushLagBytes = Math.Max(0, Volatile.Read(ref _readOnlyPage) - Volatile.Read(ref _flushedPage)) << _pageBits,
                FlushStalls = Volatile.Read(ref _flushStalls),
                SegmentFiles = segments,
                IndexEntries = Volatile.Read(ref _indexCount),
                IndexCapacity = Volatile.Read(ref _indexMask) + 1,
                IndexResizes = Volatile.Read(ref _indexResizes),
                IntegrityFailures = Volatile.Read(ref _integrityFailures),
            };
        }
    }

    /// <summary>Diagnostics: the log tail, the lowest in-memory address, and the live index entries.</summary>
    internal (long Tail, long Head, long IndexEntries) Addresses => (_tail, _headAddress, _indexCount);

    /// <summary>Diagnostics: each segment's live bytes, first record address, and whether it was dropped, by segment number.</summary>
    internal IReadOnlyList<(long Live, long FirstRecord, bool Dropped)> SegmentAccounting =>
        _segmentInfo.Select(static info => (info.Live, info.FirstRecord, info.Dropped)).ToArray();

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        faulted = false;
        var hash = Hash(key);
        var entry = Find(hash);
        return entry == null ? null : ReadRecord(key, hash, AddressOf(entry->Packed), _pointPread, ref faulted);
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

        var pending = 0;
        for (var i = 0; i < keys.Length; i++)
        {
            values[i] = null;
            var hash = Hash(keys[i]);
            var entry = Find(hash);
            if (entry == null)
                continue;
            var address = AddressOf(entry->Packed);
            if (address >= _headAddress)
            {
                var faulted = false;
                values[i] = ReadRecord(keys[i], hash, address, _batchPread, ref faulted);
                continue;
            }

            _pendingAddresses[pending] = address;
            _pendingSlots[pending++] = i;
        }

        if (pending == 0)
            return 0;

        var addresses = _pendingAddresses.AsSpan(0, pending);
        var slots = _pendingSlots.AsSpan(0, pending);
        addresses.Sort(slots);
        if (_tuning.Prefetch && !_batchPread)
            Prefetch(addresses);

        for (var j = 0; j < pending; j++)
        {
            var key = keys[slots[j]];
            var faulted = true;
            values[slots[j]] = ReadRecord(key, Hash(key), addresses[j], _batchPread, ref faulted);
        }

        return pending;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CheckKey(key);
        MaintainIndex();

        var hash = Hash(key);
        var slot = Claim(hash);
        if (slot->Packed != 0)
        {
            var packed = slot->Packed;
            var current = AddressOf(packed);
            if (current >= _readOnlyAddress)
            {
                var record = RingPointer(current);
                ref var header = ref Unsafe.AsRef<RecordHeader>(record);
                ValidateHeader(current, header);
                if (header.KeyLength != key.Length || !new ReadOnlySpan<byte>(record + HeaderSize, key.Length).SequenceEqual(key))
                    throw Collision(current);

                if (header.Capacity >= value.Length)
                {
                    // In place: the record is in the ring's mutable region, so the flusher has not
                    // taken it and nothing on disk goes stale. Setting the length is what makes a
                    // shrink read back exact. Its size, and so the accounting, is unchanged.
                    Volatile.Write(ref _inPlaceWrites, _inPlaceWrites + 1);
                    header.ValueLength = value.Length;
                    header.Checksum = Checksum(hash, value.Length, header.Capacity, value);
                    WriteInPlace(current + HeaderSize + key.Length, value);
                    return;
                }
            }

            // The new version supersedes this one: its bytes stop being live wherever they are.
            AddLive(current, SizeOf(packed, current), -1);
        }

        Volatile.Write(ref _appendedWrites, _appendedWrites + 1);
        var (address, size) = Append(key, value, hash);
        if (slot->Packed == 0)
        {
            slot->HashLow = (ulong)hash;
            slot->HashHigh = (ulong)(hash >> 64);
            Volatile.Write(ref _indexCount, _indexCount + 1);
        }

        slot->Packed = Pack(address, size);
    }

    /// <summary>Drops the key's slot and stops counting its record as live. Reads nothing: the slot has the size.</summary>
    public void Delete(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CheckKey(key);
        MaintainIndex();
        var entry = Find(Hash(key));
        if (entry == null)
            return;

        var packed = entry->Packed;
        var address = AddressOf(packed);
        AddLive(address, SizeOf(packed, address), -1);
        Remove(entry);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        // The flusher reads the ring and writes the segments: stop it before either goes.
        _stopping = true;
        _flushWork.Set();
        _flusher?.Join();

        NativeMemory.AlignedFree(_ring);
        _ring = null;
        NativeMemory.Free(_index);
        _index = null;
        if (_oldIndex != null)
            NativeMemory.Free(_oldIndex);
        _oldIndex = null;
        foreach (var segment in _segmentTable)
            segment?.Dispose();
        _segmentTable = [];
        _victimSegment = null;
        _flushWork.Dispose();
        _flushDone.Dispose();
    }

    /// <summary>
    /// Fault injection: flips a byte of the key's record checksum where the record lives now, in the
    /// ring or in its segment file (which the map sees). Returns whether the record had left memory.
    /// </summary>
    internal bool CorruptForTest(ReadOnlySpan<byte> key)
    {
        var entry = Find(Hash(key));
        if (entry == null)
            throw new KeyNotFoundException();
        var at = AddressOf(entry->Packed) + 12;
        if (at >= _headAddress)
        {
            RingPointer(at)[0] ^= 0xFF;
            return false;
        }

        var segment = SegmentEntry(at >> _segmentBits)!;
        Span<byte> one = stackalloc byte[1];
        RandomAccess.Read(segment.Handle, one, at & _segmentOffsetMask);
        one[0] ^= 0xFF;
        RandomAccess.Write(segment.Handle, one, at & _segmentOffsetMask);
        return true;
    }

    // ---- Compaction ---------------------------------------------------------------------------

    public void PrepareCompactionStep()
    {
        var segment = _victimSegment;
        if (segment is null || segment.Base == null)
            return;

        var start = _victimCursor & _segmentOffsetMask & ~(OsPage - 1);
        var end = Math.Min(_segmentSize, start + _prepareBytes);
        if (end <= start)
            return;

        // Ask for the range, then fault it in here, off the lock: a step's reads are then copies.
        MelangeLogNative.WillNeed((nint)(segment.Base + start), (nint)(end - start));
        var sum = 0;
        for (var offset = start; offset < end; offset += OsPage)
            sum += Volatile.Read(ref segment.Base[offset]);
        GC.KeepAlive(sum);
    }

    public CompactionStep Compact(double triggerRatio, long byteBudget)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _prepareBytes = Math.Max(OsPage, byteBudget * 2);
        if (_victim < 0)
        {
            // Free first: a flushed segment with nothing live goes without a byte copied.
            var reclaimed = 0L;
            var dropped = 0;
            var sealedSegments = Math.Min(_segmentInfo.Count, _headAddress >> _segmentBits);
            for (var n = 0; n < sealedSegments; n++)
            {
                var info = _segmentInfo[n];
                if (!info.Dropped && info.Live == 0)
                {
                    reclaimed += Drop(n);
                    dropped++;
                }
            }

            if (dropped > 0)
                return new CompactionStep(true, 0, reclaimed, dropped);

            // Picking a victim is a step of its own, so the next one can prepare its pages first.
            return new CompactionStep(PickVictim(triggerRatio, sealedSegments), 0, 0, 0);
        }

        var victim = _victim;
        var victimInfo = _segmentInfo[(int)victim];
        try
        {
            var relocated = 0L;
            var scanned = 0L;
            if (_victimSpanPending)
            {
                _victimSpanPending = false;
                var from = victimInfo.SpanningFrom;
                if (from != 0 && !_segmentInfo[(int)(from >> _segmentBits)].Dropped)
                    relocated += RelocateIfLive(from);
                _victimCursor = victimInfo.FirstRecord;
            }

            // A step ends at its byte budget, copied or merely scanned (most of a victim is dead
            // records, each a hash and a probe), or at half a millisecond, whichever comes first:
            // writers and readers wait behind it. Its pages were faulted in off the lock beforehand.
            var segmentEnd = (victim + 1) << _segmentBits;
            var started = System.Diagnostics.Stopwatch.GetTimestamp();
            var records = 0;
            while (_victimCursor < segmentEnd && relocated < byteBudget && scanned < byteBudget)
            {
                if ((++records & 31) == 0 && System.Diagnostics.Stopwatch.GetTimestamp() - started > MaxStepTicks)
                    break;

                var address = _victimCursor;
                var inPage = _pageSize - (address & _pageOffsetMask);
                if (inPage < HeaderSize)
                {
                    _victimCursor = address + inPage;
                    continue;
                }

                var faulted = false;
                var header = MemoryMarshal.Read<RecordHeader>(RecordView(address, _batchPread, ref faulted));
                if (header.KeyLength == PadKeyLength)
                {
                    if (header.Capacity != inPage - HeaderSize)
                        throw Integrity($"melangelog: corrupt pad marker at {address} ({_basePath}).");
                    _victimCursor = address + inPage;
                    continue;
                }

                ValidateHeader(address, header);
                var size = HeaderSize + header.KeyLength + (long)header.Capacity;
                relocated += RelocateIfLive(address);
                scanned += size;
                _victimCursor = address + size;
            }

            if (_victimCursor < segmentEnd)
                return new CompactionStep(true, relocated, 0, 0);

            _victim = -1;
            _victimSegment = null;
            if (victimInfo.Live != 0)
            {
                victimInfo.Skipped = true;
                throw new InvalidDataException(
                    $"melangelog: segment {victim} still counts {victimInfo.Live} live bytes after compaction copied out every live record it found ({_basePath}).");
            }

            return new CompactionStep(true, relocated, Drop(victim), 1);
        }
        catch (InvalidDataException)
        {
            // A segment that cannot be walked is left in place, for good: dropping it could lose a
            // live record. The log is a projection, so a restart rebuilds it clean.
            victimInfo.Skipped = true;
            _victim = -1;
            _victimSegment = null;
            throw;
        }
    }

    /// <summary>
    /// Starts compacting the flushed segment with the fewest live bytes, if the log is over
    /// <paramref name="triggerRatio"/> times its live bytes, at least two segments' worth of it is
    /// garbage, and the candidate is not nearly all live.
    /// </summary>
    private bool PickVictim(double triggerRatio, long sealedSegments)
    {
        var logBytes = _tail - FirstAddress - _reclaimedBytes;
        var garbage = logBytes - _liveBytes;
        if (garbage < 2 * _segmentSize || logBytes <= triggerRatio * _liveBytes)
            return false;

        var best = -1;
        for (var n = 0; n < sealedSegments; n++)
        {
            var info = _segmentInfo[n];
            if (!info.Dropped && !info.Skipped && (best < 0 || info.Live < _segmentInfo[best].Live))
                best = n;
        }

        if (best < 0 || _segmentInfo[best].Live > _segmentSize / 10 * 9)
            return false;

        var start = Math.Max((long)best << _segmentBits, FirstAddress);
        _victim = best;
        _victimSegment = SegmentEntry(best);
        _victimCursor = start;
        _victimSpanPending = _segmentInfo[best].FirstRecord > start;
        if (!_victimSpanPending)
            _victimCursor = _segmentInfo[best].FirstRecord;
        return true;
    }

    /// <summary>
    /// Copies the record at <paramref name="address"/> to the tail if its slot still points at it,
    /// and repoints the slot. Returns the bytes copied, 0 for a record that is no longer current.
    /// </summary>
    private long RelocateIfLive(long address)
    {
        var faulted = false;
        var view = RecordView(address, _batchPread, ref faulted);
        var header = MemoryMarshal.Read<RecordHeader>(view);
        ValidateHeader(address, header);

        Span<byte> key = stackalloc byte[header.KeyLength];
        view.Slice(HeaderSize, header.KeyLength).CopyTo(key);
        var hash = Hash(key);
        var slot = Find(hash);
        if (slot == null || AddressOf(slot->Packed) != address)
            return 0;

        var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(header.ValueLength);
        try
        {
            var value = rented.AsSpan(0, header.ValueLength);
            var valueOffset = HeaderSize + header.KeyLength;
            if (view.Length >= valueOffset + value.Length)
                view.Slice(valueOffset, value.Length).CopyTo(value);
            else
                CopyOut(address + valueOffset, value, _batchPread, ref faulted);
            if (Checksum(hash, header.ValueLength, header.Capacity, value) != header.Checksum)
                throw Integrity($"melangelog: checksum mismatch in the record at {address} ({_basePath}).");

            AddLive(address, SizeOf(slot->Packed, address), -1);
            var (moved, size) = Append(key, value, hash);
            slot->Packed = Pack(moved, size);
            Volatile.Write(ref _relocatedBytes, _relocatedBytes + size);
            return size;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Unmaps, closes and deletes a segment's file. Returns the log bytes that gives back.</summary>
    private long Drop(long number)
    {
        var info = _segmentInfo[(int)number];
        info.Dropped = true;
        Segment? segment;
        lock (_segmentTableLock)
        {
            segment = SegmentEntry(number);
            if (segment is not null)
                _segmentTable[number] = null;
        }

        if (segment is not null)
        {
            if (ReferenceEquals(_victimSegment, segment))
                _victimSegment = null;
            segment.Dispose();
        }

        var bytes = ((number + 1) << _segmentBits) - Math.Max(number << _segmentBits, FirstAddress);
        Volatile.Write(ref _reclaimedBytes, _reclaimedBytes + bytes);
        return bytes;
    }

    // ---- Reads ------------------------------------------------------------------------------

    private byte[] ReadRecord(ReadOnlySpan<byte> key, UInt128 hash, long address, bool pread, ref bool faulted)
    {
        var view = RecordView(address, pread, ref faulted);
        var header = MemoryMarshal.Read<RecordHeader>(view);
        ValidateHeader(address, header);
        if (header.KeyLength != key.Length || !view.Slice(HeaderSize, key.Length).SequenceEqual(key))
            throw Collision(address);

        var value = GC.AllocateUninitializedArray<byte>(header.ValueLength);
        var valueOffset = HeaderSize + key.Length;
        if (view.Length >= valueOffset + value.Length)
            view.Slice(valueOffset, value.Length).CopyTo(value);
        else
            CopyOut(address + valueOffset, value, pread, ref faulted);

        if (Checksum(hash, header.ValueLength, header.Capacity, value) != header.Checksum)
            throw Integrity($"melangelog: checksum mismatch in the record at {address} ({_basePath}).");
        return value;
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
        var buffer = _scratch.Length >= length ? _scratch.AsSpan(0, length) : new byte[length];
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
        var segment = SegmentEntry(address >> _segmentBits) ?? throw DroppedSegment(address);
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

    private (long Address, long Size) Append(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, UInt128 hash)
    {
        var size = (HeaderSize + key.Length + (long)value.Length + 7) & ~7L;
        var capacity = checked((int)(size - HeaderSize - key.Length));

        // A record that fits a page never straddles one; a bigger one starts on a page boundary.
        var offset = _tail & _pageOffsetMask;
        if (size <= _pageSize ? offset + size > _pageSize : offset != 0)
        {
            MarkPad(_tail, _pageSize - offset);
            _tail = (_tail | _pageOffsetMask) + 1;
        }

        var address = _tail;
        if (address + size >= MaxAddress)
            throw new InvalidOperationException($"melangelog: the log reached its {MaxAddress >> 40} TiB address limit ({_basePath}).");

        EnsurePage(address >> _pageBits);
        var record = RingPointer(address);
        Unsafe.AsRef<RecordHeader>(record) = new RecordHeader
        {
            KeyLength = key.Length,
            ValueLength = value.Length,
            Capacity = capacity,
            Checksum = Checksum(hash, value.Length, capacity, value),
        };
        key.CopyTo(new Span<byte>(record + HeaderSize, key.Length));

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

        // The tail moves only once the record is whole, so a header is never validated against a
        // tail that does not yet cover it.
        Volatile.Write(ref _tail, address + size);
        NoteRecordSpan(address, size);
        AddLive(address, size, +1);
        return (address, size);
    }

    /// <summary>
    /// Marks the <paramref name="length"/> bytes the tail is about to skip, so nothing reading the log
    /// in order mistakes what a reused ring slot left there for a record. A gap too short for a header
    /// (8 bytes, given 8-byte alignment and 16-byte headers) needs no marker: a reader skips to the
    /// next page from anywhere a header cannot fit.
    /// </summary>
    private void MarkPad(long address, long length)
    {
        if (length < HeaderSize)
            return;
        Unsafe.AsRef<RecordHeader>(RingPointer(address)) = new RecordHeader
        {
            KeyLength = PadKeyLength,
            Capacity = (int)(length - HeaderSize),
        };
    }

    /// <summary>
    /// A record that runs past its segment's end covers the start of the next ones: each of those
    /// records where its first record actually begins, the end of this one or, for a segment it
    /// covers whole, the segment's end (no record starts there).
    /// </summary>
    private void NoteRecordSpan(long address, long size)
    {
        var first = address >> _segmentBits;
        var end = address + size;
        var last = (end - 1) >> _segmentBits;
        for (var segment = first + 1; segment <= last; segment++)
        {
            var info = Info(segment);
            info.FirstRecord = Math.Min(end, (segment + 1) << _segmentBits);
            info.SpanningFrom = address;
        }
    }

    /// <summary>Adds (or, with -1, removes) a record's bytes to the live count of each segment it touches.</summary>
    private void AddLive(long address, long size, int sign)
    {
        Volatile.Write(ref _liveBytes, _liveBytes + sign * size);
        while (size > 0)
        {
            var segment = address >> _segmentBits;
            var inSegment = Math.Min(size, ((segment + 1) << _segmentBits) - address);
            Info(segment).Live += sign * inSegment;
            address += inSegment;
            size -= inSegment;
        }
    }

    private SegmentInfo Info(long segment)
    {
        while (_segmentInfo.Count <= segment)
            _segmentInfo.Add(new SegmentInfo(Math.Max(FirstAddress, (long)_segmentInfo.Count << _segmentBits)));
        return _segmentInfo[(int)segment];
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

    /// <summary>
    /// Makes <paramref name="page"/> the tail page: evicts the oldest page once the ring is full, and
    /// moves the read-only boundary up behind the tail, handing the pages it passes to the flusher.
    /// </summary>
    private void EnsurePage(long page)
    {
        while (_tailPage < page)
        {
            var next = _tailPage + 1;

            // The boundary first: it is what puts the head page in the flusher's hands, so evicting
            // before moving it could wait on a page the flusher was never given (a two-page ring).
            // The flusher is woken once a quarter of the read-only region is waiting for it, not per
            // page: waking a parked thread is a system call on the writer's path.
            var readOnly = next - _readOnlyLag;
            if (readOnly > _readOnlyPage)
            {
                _readOnlyAddress = readOnly << _pageBits;
                Volatile.Write(ref _readOnlyPage, readOnly);
                if (readOnly - Volatile.Read(ref _flushedPage) >= _flushBatchPages)
                    SignalFlusher();
            }

            if (next - _headPage >= _ringPages)
                EvictHead();
            _tailPage = next;
        }
    }

    /// <summary>
    /// Drops the oldest page from the ring once the flusher has written it, waiting for that only if
    /// it has not: the stall that means the disk is behind the writes.
    /// </summary>
    private void EvictHead()
    {
        if (Volatile.Read(ref _flushedPage) <= _headPage)
            WaitForFlush(_headPage + 1);
        _headPage++;
        _headAddress = _headPage << _pageBits;
    }

    private void WaitForFlush(long page)
    {
        Volatile.Write(ref _flushStalls, _flushStalls + 1);
        while (true)
        {
            if (_flushFault is { } fault)
                throw new IOException($"melangelog: writing the log to disk failed ({_basePath}): {fault.Message}", fault);
            _flushDone.Reset();
            if (Volatile.Read(ref _flushedPage) >= page)
                return;
            SignalFlusher();
            _flushDone.Wait();
        }
    }

    private void SignalFlusher()
    {
        if (_flusher is null)
        {
            _flusher = new Thread(FlushLoop) { IsBackground = true, Name = "melange-mlog-flush" };
            _flusher.Start();
        }

        _flushWork.Set();
    }

    /// <summary>
    /// The flusher: writes every page below the read-only boundary that is not on disk yet, a run of
    /// pages contiguous in both the ring and one segment at a time, publishing its progress after each
    /// run. Reads only immutable pages, so it needs no lock with the writer.
    /// </summary>
    private void FlushLoop()
    {
        try
        {
            while (!_stopping)
            {
                _flushWork.Reset();
                var target = Volatile.Read(ref _readOnlyPage);
                var flushed = _flushedPage;
                if (flushed >= target)
                {
                    _flushWork.Wait();
                    continue;
                }

                while (flushed < target && !_stopping)
                {
                    // A run ends at the ring's wrap or the segment's end, whichever is first.
                    var ringRun = _ringPages - (flushed & _ringMask);
                    var segmentRun = ((((flushed << _pageBits) | _segmentOffsetMask) + 1) >> _pageBits) - flushed;
                    var pages = Math.Min(target - flushed, Math.Min(ringRun, segmentRun));
                    var address = flushed << _pageBits;
                    var segment = SegmentForFlush(address >> _segmentBits);
                    if (_failFlushForTest?.Invoke(flushed) == true)
                        throw new IOException($"injected flush failure at page {flushed}");
                    RandomAccess.Write(
                        segment.Handle,
                        new ReadOnlySpan<byte>(RingSlot(flushed), checked((int)(pages << _pageBits))),
                        address & _segmentOffsetMask);
                    flushed += pages;
                    Volatile.Write(ref _flushedPage, flushed);
                    _flushDone.Set();

                    // The segment after the one being written is made ahead, off every writer's path:
                    // creating, preallocating and mapping a file is the slow part, and a full disk
                    // then fails here, between pages, not partway through one.
                    SegmentForFlush(((flushed << _pageBits) >> _segmentBits) + 1);
                }
            }
        }
        catch (Exception exception)
        {
            _flushFault = exception;
            _flushDone.Set();
        }
    }

    /// <summary>The segment the flusher writes to, created (and published) on first reach.</summary>
    private Segment SegmentForFlush(long number)
    {
        if (SegmentEntry(number) is { } existing)
            return existing;

        var segment = new Segment(
            $"{_basePath}.{number}",
            _segmentSize,
            map: _tuning.ReadMode == MelangeLogReadMode.Mmap,
            randomAdvice: _tuning.RandomAdvice);
        lock (_segmentTableLock)
        {
            var table = _segmentTable;
            if (number >= table.Length)
            {
                var grown = new Segment?[Math.Max(table.Length * 2, number + 1)];
                table.CopyTo(grown, 0);
                table = grown;
            }

            table[number] = segment;
            Volatile.Write(ref _segmentTable, table);
        }

        return segment;
    }

    /// <summary>The segment with this number, or null if it was never created or compaction deleted it.</summary>
    private Segment? SegmentEntry(long number)
    {
        var table = Volatile.Read(ref _segmentTable);
        return number < table.Length ? table[number] : null;
    }

    // ---- Index ------------------------------------------------------------------------------

    // The index grows incrementally. Past 75% load a table twice the size takes over; the old one
    // stays readable, and every write migrates a few of its slots, so no single call rehashes the
    // whole index under the store lock (at a few million keys, tens of milliseconds). A key lives in
    // exactly one table: the new one is probed first, then the old; a write to a key still in the old
    // table moves it across first; a key deleted from the old table, or migrated out of it, leaves a
    // tombstone (Moved) there, so probes for the keys after it in that table still reach them.

    /// <summary>Old-table slots each write migrates while the index grows.</summary>
    private const int MigrateSlotsPerWrite = 64;

    /// <summary>An old-table slot whose key was migrated or deleted: skipped by probes, never ends one.</summary>
    private const ulong Moved = ulong.MaxValue;

    /// <summary>The key's entry, in whichever table holds it, or null.</summary>
    private IndexEntry* Find(UInt128 hash)
    {
        var entry = Probe(_index, _indexMask, hash, out _);
        if (entry != null || _oldIndex == null)
            return entry;
        return Probe(_oldIndex, _oldMask, hash, out _);
    }

    /// <summary>
    /// The key's entry in the current table, for a write: its existing one (moved across from the old
    /// table if it was there), or the empty slot it goes in (<c>Packed == 0</c>), filled by the caller.
    /// </summary>
    private IndexEntry* Claim(UInt128 hash)
    {
        var entry = Probe(_index, _indexMask, hash, out var empty);
        if (entry != null)
            return entry;
        if (_oldIndex != null)
        {
            var old = Probe(_oldIndex, _oldMask, hash, out _);
            if (old != null)
            {
                *empty = *old;
                old->Packed = Moved;
            }
        }

        return empty;
    }

    /// <summary>Drops an entry: backward-shift in the current table, a tombstone in the old one.</summary>
    private void Remove(IndexEntry* entry)
    {
        Volatile.Write(ref _indexCount, _indexCount - 1);
        if (_oldIndex != null && entry >= _oldIndex && entry <= _oldIndex + _oldMask)
        {
            entry->Packed = Moved;
            return;
        }

        var hole = entry - _index;
        var j = hole;
        while (true)
        {
            j = (j + 1) & _indexMask;
            ref var next = ref _index[j];
            if (next.Packed == 0)
                break;
            var ideal = (long)next.HashLow & _indexMask;
            if (((j - ideal) & _indexMask) >= ((j - hole) & _indexMask))
            {
                _index[hole] = next;
                hole = j;
            }
        }

        _index[hole] = default;
    }

    /// <summary>
    /// Linear probe of one table: the entry holding <paramref name="hash"/>, or null with
    /// <paramref name="empty"/> set to the first empty slot (tombstones are passed over, never reused,
    /// since only an old table has them and nothing is inserted into it).
    /// </summary>
    private static IndexEntry* Probe(IndexEntry* table, long mask, UInt128 hash, out IndexEntry* empty)
    {
        var low = (ulong)hash;
        var high = (ulong)(hash >> 64);
        var i = (long)low & mask;
        while (true)
        {
            var entry = table + i;
            if (entry->Packed == 0)
            {
                empty = entry;
                return null;
            }

            if (entry->Packed != Moved && entry->HashLow == low && entry->HashHigh == high)
            {
                empty = null;
                return entry;
            }

            i = (i + 1) & mask;
        }
    }

    /// <summary>Before each write: a step of any migration under way, and a new one once the table is 75% full.</summary>
    private void MaintainIndex()
    {
        if (_oldIndex != null)
            Migrate(MigrateSlotsPerWrite);
        if (_indexCount < _indexGrowAt)
            return;

        // Full again before the last growth finished (it cannot be, at 64 slots a write, but the
        // invariant is cheap to keep): finish it first, so there are never three tables.
        if (_oldIndex != null)
            Migrate(long.MaxValue);

        Volatile.Write(ref _indexResizes, _indexResizes + 1);
        _oldIndex = _index;
        _oldMask = _indexMask;
        _migrateCursor = 0;
        AllocateIndex((_indexMask + 1) * 2);
    }

    /// <summary>Moves up to <paramref name="slots"/> old-table slots' keys into the current table; frees the old one when done.</summary>
    private void Migrate(long slots)
    {
        var end = Math.Min(_oldMask + 1, _migrateCursor + Math.Min(slots, _oldMask + 1));
        for (; _migrateCursor < end; _migrateCursor++)
        {
            var old = _oldIndex + _migrateCursor;
            if (old->Packed == 0 || old->Packed == Moved)
                continue;
            _ = Probe(_index, _indexMask, new UInt128(old->HashHigh, old->HashLow), out var empty);
            *empty = *old;
            old->Packed = Moved;
        }

        if (_migrateCursor > _oldMask)
        {
            NativeMemory.Free(_oldIndex);
            _oldIndex = null;
        }
    }

    private void AllocateIndex(long capacity)
    {
        var index = (IndexEntry*)NativeMemory.AllocZeroed((nuint)capacity, (nuint)sizeof(IndexEntry));
        _index = index;
        Volatile.Write(ref _indexMask, capacity - 1);
        _indexGrowAt = capacity / 4 * 3;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Pack(long address, long size)
    {
        var units = (ulong)(size >> 3);
        return ((ulong)address >> 3) | (Math.Min(units, SizeUnknown) << AddressBits);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long AddressOf(ulong packed) => (long)(packed & AddressMask) << 3;

    /// <summary>A slot's record size: from the slot, or, for a record too large to pack, from its header.</summary>
    private long SizeOf(ulong packed, long address)
    {
        var units = packed >> AddressBits;
        if (units != SizeUnknown)
            return (long)units << 3;
        var faulted = false;
        var header = MemoryMarshal.Read<RecordHeader>(RecordView(address, _pointPread, ref faulted));
        ValidateHeader(address, header);
        return HeaderSize + header.KeyLength + (long)header.Capacity;
    }

    // ---- Helpers ----------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private UInt128 Hash(ReadOnlySpan<byte> key)
    {
        var hash = XxHash128.HashToUInt128(key, _hashSeed);
        return _hashMask == ulong.MaxValue ? hash : new UInt128((ulong)(hash >> 64) & _hashMask, (ulong)hash & _hashMask);
    }

    /// <summary>
    /// Binds the value bytes to the key (through its hash) and the lengths, so a torn, misdirected or
    /// bit-rotted record throws rather than reading back as somebody's row.
    /// </summary>
    private static uint Checksum(UInt128 keyHash, int valueLength, int capacity, ReadOnlySpan<byte> value)
    {
        var seed = (ulong)keyHash ^ (ulong)(keyHash >> 64) ^ ((((ulong)(uint)valueLength << 32) | (uint)capacity) * 0x9E3779B97F4A7C15UL);
        return (uint)XxHash3.HashToUInt64(value, unchecked((long)seed));
    }

    private void ValidateHeader(long address, in RecordHeader header)
    {
        if (header.KeyLength is < 0 or > MaxKeyLength
            || header.Capacity < 0
            || header.ValueLength < 0
            || header.ValueLength > header.Capacity
            || address + HeaderSize + header.KeyLength + header.Capacity > _tail)
        {
            throw Integrity($"melangelog: corrupt record header at {address} ({_basePath}).");
        }
    }

    /// <summary>A record that failed its integrity check: counted, then thrown.</summary>
    private InvalidDataException Integrity(string message)
    {
        Volatile.Write(ref _integrityFailures, _integrityFailures + 1);
        return new InvalidDataException(message);
    }

    private InvalidDataException DroppedSegment(long address) =>
        new($"melangelog: address {address} is in a segment compaction deleted ({_basePath}).");

    private InvalidDataException Collision(long address) =>
        new($"melangelog: the record at {address} holds a different key with the same 128-bit hash ({_basePath}).");

    public int MaxKeyBytes => MaxKeyLength;

    private static void CheckKey(ReadOnlySpan<byte> key)
    {
        if (key.Length > MaxKeyLength)
            throw new ArgumentException(
                $"MelangeLog keys are at most {MaxKeyLength} bytes, and this one is {key.Length}: a row with a primary key this long " +
                "cannot be stored on this engine. New writes are refused before the commit log; a log that already holds such a row " +
                "(written under another engine) starts with HotStore:PagedEngine set to Faster.",
                nameof(key));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* RingSlot(long page) => _ring + ((page & _ringMask) << _pageBits);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* RingPointer(long address) => RingSlot(address >> _pageBits) + (address & _pageOffsetMask);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* MappedPointer(long address) =>
        (SegmentEntry(address >> _segmentBits) ?? throw DroppedSegment(address)).Base + (address & _segmentOffsetMask);

    /// <summary>
    /// A record's fixed header. <see cref="Capacity"/> is the value room the record was laid out with
    /// (its length rounded up to the record's 8-byte alignment); an in-place write may use all of it,
    /// and the record's size is always <c>HeaderSize + KeyLength + Capacity</c>. A pad marker has
    /// <see cref="PadKeyLength"/> and the gap's remaining bytes as its capacity.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    private struct RecordHeader
    {
        public int KeyLength;
        public int ValueLength;
        public int Capacity;
        public uint Checksum;
    }

    /// <summary>
    /// A key's 128-bit hash, and its record's address and size packed (0 = empty slot): 24 bytes. Two
    /// <c>ulong</c> halves rather than a <c>UInt128</c>, whose 16-byte alignment would pad it to 32.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct IndexEntry
    {
        public ulong HashLow;
        public ulong HashHigh;
        public ulong Packed;
    }

    /// <summary>
    /// Per segment: the bytes of current records in it; where its first record starts, and the
    /// address of the record before it that runs into it (0 for none); whether compaction deleted it,
    /// or found it unreadable and left it.
    /// </summary>
    private sealed class SegmentInfo(long firstRecord)
    {
        public long Live;
        public long FirstRecord = firstRecord;
        public long SpanningFrom;
        public bool Dropped;
        public bool Skipped;
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
            // Preallocated for real (fallocate on Linux), not just sized: a sparse file takes its
            // blocks at write time, so a full disk would surface mid-flush, or, for a write the page
            // cache took, at writeback where nothing hears it. Here it fails the segment's creation.
            Handle = File.OpenHandle(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, FileOptions.None, preallocationSize: size);
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

    // madvise from the process's own symbol scope, which has whichever C library the host links
    // (glibc or musl); libc.so.6 by name is glibc's alone.
    private static readonly delegate* unmanaged<nint, nuint, int, int> s_madvise = OperatingSystem.IsLinux() ? ResolveMadvise() : null;
    private static readonly delegate* unmanaged<nint, nuint, nint*, uint, int> s_prefetchVirtualMemory =
        (delegate* unmanaged<nint, nuint, nint*, uint, int>)Resolve("kernel32.dll", "PrefetchVirtualMemory", OperatingSystem.IsWindows());

    /// <summary>Whether this platform has a prefetch hint at all (Linux madvise, or Windows PrefetchVirtualMemory).</summary>
    public static bool PrefetchAvailable => s_madvise != null || s_prefetchVirtualMemory != null;

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

    private static delegate* unmanaged<nint, nuint, int, int> ResolveMadvise()
    {
        if (NativeLibrary.TryGetExport(NativeLibrary.GetMainProgramHandle(), "madvise", out var function))
            return (delegate* unmanaged<nint, nuint, int, int>)function;
        return Resolve("libc.so.6", "madvise", platform: true);
    }

    private static delegate* unmanaged<nint, nuint, int, int> Resolve(string library, string export, bool platform)
    {
        if (platform && NativeLibrary.TryLoad(library, out var handle) && NativeLibrary.TryGetExport(handle, export, out var function))
            return (delegate* unmanaged<nint, nuint, int, int>)function;
        return null;
    }
}

internal static partial class MelangeLogEngineLog
{
    [LoggerMessage(EventId = 1520, EventName = "MelangeLogPrefetchUnavailable", Level = LogLevel.Information,
        Message = "MelangeLog found no prefetch hint on this platform (madvise or PrefetchVirtualMemory), so scans of paged " +
            "tables fault their pages in one at a time instead of together: correct, and slower on a cold store.")]
    public static partial void PrefetchUnavailable(ILogger logger);
}
