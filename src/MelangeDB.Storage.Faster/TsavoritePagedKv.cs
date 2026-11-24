using Tsavorite.core;


namespace MelangeDB.Storage.Faster;

using KvStoreFunctions = StoreFunctions<TsavoritePagedKv.KvKeyComparer, SpanByteRecordTriggers>;
using KvAllocator = SpanByteAllocator<StoreFunctions<TsavoritePagedKv.KvKeyComparer, SpanByteRecordTriggers>>;

/// <summary>
/// One paged-store KV space on Tsavorite (vendored from microsoft/garnet): a <c>TsavoriteKV</c> over the
/// <c>SpanByteAllocator</c> (keys and values inline in the hybrid log), driven through one
/// <c>ClientSession</c>'s <c>BasicContext</c> -- the counterpart of the single FASTER <c>ClientSession</c>.
/// <para>
/// Every call arrives under the store's lock, one at a time, possibly on different threads. That is safe:
/// <c>BasicContext</c> acquires and releases epoch protection around each operation (and around
/// <c>CompletePending</c>), so no thread holds the epoch between calls, and nothing is left pending
/// when a call returns.
/// </para>
/// <para>
/// Session input (<c>TInput</c>) is the caller's slot index. Tsavorite carries input through a pending
/// operation and hands it back on the completed output, so a batched read's disk completions are routed
/// to their slot directly rather than by matching key bytes as the FASTER store does.
/// </para>
/// </summary>
internal sealed class TsavoritePagedKv : IPagedKv
{
    private readonly IDevice _device;
    private readonly ICheckpointManager _checkpointManager;
    private readonly TsavoriteKV<KvStoreFunctions, KvAllocator> _store;
    private readonly ClientSession<KvKey, int, byte[]?, Empty, KvFunctions, KvStoreFunctions, KvAllocator> _session;
    private readonly BasicContext<KvKey, int, byte[]?, Empty, KvFunctions, KvStoreFunctions, KvAllocator> _context;
    private bool _disposed;

    internal TsavoritePagedKv(IDevice device, ICheckpointManager checkpointManager, KVSettings settings, long capacityBytes)
    {
        _device = device;
        _checkpointManager = checkpointManager;
        CapacityBytes = capacityBytes;
        _store = new TsavoriteKV<KvStoreFunctions, KvAllocator>(
            settings,
            StoreFunctions.Create(new KvKeyComparer(), SpanByteRecordTriggers.Instance),
            static (allocatorSettings, storeFunctions) => new KvAllocator(allocatorSettings, storeFunctions));
        _session = _store.NewSession<KvKey, int, byte[]?, Empty, KvFunctions>(new KvFunctions());
        _context = _session.BasicContext;
    }

    public long CapacityBytes { get; }

    /// <summary>The hybrid log's extent. Tsavorite cannot say how much of it is live.</summary>
    public PagedKvUsage Usage => new(_store.Log.TailAddress - _store.Log.BeginAddress, -1);

    /// <summary>Diagnostics: the log tail and the lowest in-memory address (reads below it fault).</summary>
    internal (long Tail, long Head) LogAddresses => (_store.Log.TailAddress, _store.Log.HeadAddress);

    /// <summary>Diagnostics: overflow hash buckets in use, and the most the overflow allocator may hold (bytes).</summary>
    internal (long Used, long MaxBytes) OverflowBuckets => (_store.OverflowBucketCount, _store.IndexOverflowMaxSizeBytes);

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        var input = -1;
        byte[]? output = null;
        var status = _context.Read(new KvKey(key), ref input, ref output);
        if (!status.IsPending)
        {
            faulted = false;
            ThrowIfFaulted(status);
            return status.Found ? output : null;
        }

        // Below HeadAddress: the record is on the device. Wait for this one read to land.
        faulted = true;
        output = null;
        _ = _context.CompletePendingWithOutputs(out var completed, wait: true);
        using (completed)
        {
            while (completed.Next())
            {
                ref var current = ref completed.Current;
                ThrowIfFaulted(current.Status);
                output = current.Status.Found ? current.Output : null;
            }
        }

        return output;
    }

    public int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values)
    {
        if (values.Length < keys.Length)
            throw new ArgumentException("values must be at least as long as keys", nameof(values));
        values[..keys.Length].Clear();
        if (keys.IsEmpty)
            return 0;

        // ReadWithPrefetch hashes a group of keys, prefetches their buckets and in-memory records, then
        // issues every read; the ones below HeadAddress go pending and their I/O is in flight together.
        var batch = new ReadBatch(keys, values);
        _context.ReadWithPrefetch(ref batch);
        if (batch.Faulted is { } error)
            ThrowIfFaulted(error);
        if (batch.Pending == 0)
            return 0;

        _ = _context.CompletePendingWithOutputs(out var completed, wait: true);
        using (completed)
        {
            while (completed.Next())
            {
                ref var current = ref completed.Current;
                ThrowIfFaulted(current.Status);
                values[current.Input] = current.Status.Found ? current.Output : null;
            }
        }

        return batch.Pending;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var status = _context.Upsert(new KvKey(key), value);
        if (status.IsPending)
            _ = _context.CompletePending(wait: true);
        else
            ThrowIfFaulted(status);
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        var status = _context.Delete(new KvKey(key));
        if (status.IsPending)
            _ = _context.CompletePending(wait: true);
        else
            ThrowIfFaulted(status);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        // Session first (it completes anything pending and unregisters from the store), then the store
        // (frees the log pages, index and its own epoch), then the device last: its deleteOnClose removes
        // the segment files, which the store may still be flushing to until it is disposed.
        _session.Dispose();
        _store.Dispose();
        _device.Dispose();
        _checkpointManager.Dispose();
    }

    private static void ThrowIfFaulted(Status status)
    {
        if (status.IsFaulted)
            throw new IOException($"Tsavorite operation failed: {status}");
        if (status.IsCanceled)
            throw new InvalidOperationException($"Tsavorite operation was canceled: {status}");
    }

    /// <summary>
    /// The key as Tsavorite sees it: borrowed bytes, no namespace. <see cref="IsPinned"/> is false because
    /// the caller's span may be a managed array or a reused scratch buffer, so Tsavorite copies the key
    /// (into an inline buffer for keys up to 32 bytes, a pooled one beyond) if the operation goes pending --
    /// the caller is free to reuse its key buffer as soon as a call returns, as with FASTER.
    /// </summary>
    internal readonly ref struct KvKey(ReadOnlySpan<byte> bytes) : IKey
    {
        private readonly ReadOnlySpan<byte> _bytes = bytes;

        public bool IsPinned => false;

        public bool IsEmpty => false;

        public ReadOnlySpan<byte> KeyBytes => _bytes;

        public bool HasNamespace => false;

        public ReadOnlySpan<byte> NamespaceBytes => [];
    }

    /// <summary>
    /// Key hashing and equality. Tsavorite's stock <c>SpanByteComparer</c> hashes with
    /// <c>Utility.HashBytes</c>, a multiply-accumulate (the same one FASTER 2.x's comparer used) whose high bits --
    /// the ones Tsavorite takes the 15-bit bucket-entry tag from -- barely move between keys that differ only in
    /// their trailing bytes, which is exactly MelangeDB's key shape (fixed table-id prefix + encoded primary key).
    /// Few distinct tags means many keys share one bucket entry and a lookup walks a long chain through the log,
    /// each hop below HeadAddress a disk read. <c>Utility.GetHashCodeWithMix</c> is the same hash with a full
    /// avalanche finalizer, so both the bucket index (low bits) and the tag (high bits) are well spread. The
    /// store is rebuilt on every start, so the hash never has to match anything on disk.
    /// </summary>
    internal readonly struct KvKeyComparer : IKeyComparer
    {
        public long GetHashCode64<TKey>(TKey key)
            where TKey : IKey, allows ref struct
            => Utility.GetHashCodeWithMix(key.KeyBytes);

        public bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey, allows ref struct
            where TSecondKey : IKey, allows ref struct
            => k1.KeyBytes.SequenceEqual(k2.KeyBytes);
    }

    /// <summary>A batch for <c>ReadWithPrefetch</c>: slot i's key, its index as input, its output written straight into the caller's span.</summary>
    private ref struct ReadBatch(ReadOnlySpan<byte[]> keys, Span<byte[]?> values) : IReadArgBatch<KvKey, int, byte[]?>
    {
        private readonly ReadOnlySpan<byte[]> _keys = keys;
        private readonly Span<byte[]?> _values = values;

        public int Pending { get; private set; }

        public Status? Faulted { get; private set; }

        public readonly int Count => _keys.Length;

        // Only consulted by the consistent-read contexts.
        public readonly ReadOnlySpan<PinnedSpanByte> Parameters => default;

        // A ref struct cannot inherit default interface members, so the two "inherit from the session" defaults are spelled out.
        public readonly int InitialIORecordSize => KVSettings.UseDefaultInitialIORecordSize;

        public readonly ReadCopyOptions ReadCopyOptions => default;

        public readonly void GetKey(int i, out KvKey key) => key = new KvKey(_keys[i]);

        public readonly void GetInput(int i, out int input) => input = i;

        public readonly void GetOutput(int i, out byte[]? output) => output = null;

        public readonly void SetOutput(int i, byte[]? output) => _values[i] = output;

        public void SetStatus(int i, Status status)
        {
            if (status.IsPending)
                Pending++;
            else if (status.IsFaulted || status.IsCanceled)
                Faulted ??= status;
        }
    }

    /// <summary>
    /// Session callbacks. Reads copy the value out as a fresh array. Writes are the base class's: the
    /// initial writer sizes a new record to the value, and the in-place writer calls
    /// <c>LogRecord.TrySetValueSpanAndPrepareOptionals</c>, which rewrites the record's value length
    /// (moving the freed bytes into the record's filler) when the new value is shorter, grows into that
    /// filler when it fits, and returns false -- sending the upsert to a fresh record at the tail -- when it
    /// does not. That is exactly the issue #137 contract FASTER's <c>ConcurrentWriter</c> had to implement
    /// by hand with <c>ShrinkSerializedLength</c>. (A record keeps at most 2040 bytes of filler; a bigger shrink
    /// splits the excess off as a dead record, so later regrowth past that headroom goes to the tail -- still
    /// correct, just not in place.)
    /// </summary>
    internal sealed class KvFunctions : SessionFunctionsBase<int, byte[]?, Empty>
    {
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref byte[]? output, ref ReadInfo readInfo)
        {
            output = srcLogRecord.ValueSpan.ToArray();
            return true;
        }
    }
}
