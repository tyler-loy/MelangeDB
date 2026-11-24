using System.Numerics;
using FASTER.core;

namespace MelangeDB.Storage.Faster;

/// <summary>The FASTER engine behind <see cref="IPagedKv"/>: one hybrid log and one session per space.</summary>
internal sealed class FasterPagedKvFactory : IPagedKvFactory
{
    public static readonly FasterPagedKvFactory Instance = new();

    public string EngineName => "faster";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        var name = space == PagedKvSpace.Main ? "main.hlog" : "blob.hlog";
        Directory.CreateDirectory(directory);
        foreach (var file in Directory.EnumerateFiles(directory))
        {
            if (Path.GetFileName(file).StartsWith(name, StringComparison.Ordinal))
                File.Delete(file);
        }

        var memoryBits = 63 - BitOperations.LeadingZeroCount((ulong)memoryBudgetBytes);
        return new FasterPagedKv(
            Path.Combine(directory, name),
            memoryBits,
            Math.Min(memoryBits - 1, space == PagedKvSpace.Main ? 19 : 22),
            hashBucketsHint);
    }
}

/// <summary>
/// One FASTER hybrid log with a single session. Called under the paged store's lock only, which is
/// what makes the single session safe.
/// </summary>
internal sealed class FasterPagedKv : IPagedKv
{
    private readonly IDevice _device;
    private readonly FasterKV<SpanByte, SpanByte> _store;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, byte[], Empty, StoreFunctions> _session;

    public FasterPagedKv(string logPath, int memoryBits, int pageBits, long hashBuckets)
    {
        _device = Devices.CreateLogDevice(logPath, deleteOnClose: true);
        _store = new FasterKV<SpanByte, SpanByte>(
            hashBuckets,
            new LogSettings
            {
                LogDevice = _device,
                MemorySizeBits = memoryBits,
                PageSizeBits = pageBits,
                SegmentSizeBits = 26,
            });
        _session = _store.For(new StoreFunctions()).NewSession<StoreFunctions>();
        CapacityBytes = 1L << memoryBits;
    }

    public long CapacityBytes { get; }

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        faulted = false;
        var issued = Issue(key, out var value);
        if (issued != IssuedRead.Pending)
            return issued == IssuedRead.Found ? value : null;

        faulted = true;
        byte[]? output = null;
        _session.CompletePendingWithOutputs(out var outputs, wait: true);
        using (outputs)
        {
            while (outputs.Next())
            {
                if (outputs.Current.Status.Found)
                    output = outputs.Current.Output;
            }
        }

        return output;
    }

    /// <summary>
    /// Issues every read, then completes the pending ones in one wait, so a block's disk reads are
    /// in flight together (#212). FASTER copies a pending read's key, so the caller's key arrays
    /// are only needed to match completions back to their slots.
    /// </summary>
    public int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values)
    {
        Dictionary<byte[], List<int>>? pending = null;
        for (var i = 0; i < keys.Length; i++)
        {
            switch (Issue(keys[i], out var value))
            {
                case IssuedRead.Found:
                    values[i] = value;
                    break;
                case IssuedRead.Pending:
                    values[i] = null;
                    pending ??= new Dictionary<byte[], List<int>>(ByteArrayComparer.Instance);
                    if (pending.TryGetValue(keys[i], out var slots))
                        slots.Add(i);
                    else
                        pending[keys[i]] = [i];
                    break;
                default:
                    values[i] = null;
                    break;
            }
        }

        if (pending is null)
            return 0;

        var resolved = new (int Slot, byte[] Value)[keys.Length];
        var resolvedCount = 0;
        _session.CompletePendingWithOutputs(out var outputs, wait: true);
        using (outputs)
        {
            while (outputs.Next())
            {
                if (!outputs.Current.Status.Found || !pending.TryGetValue(outputs.Current.Key.ToByteArray(), out var slots))
                    continue;
                foreach (var slot in slots)
                    resolved[resolvedCount++] = (slot, outputs.Current.Output);
            }
        }

        for (var i = 0; i < resolvedCount; i++)
            values[resolved[i].Slot] = resolved[i].Value;
        return pending.Count;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        unsafe
        {
            fixed (byte* keyPointer = key)
            fixed (byte* valuePointer = value)
            {
                var k = SpanByte.FromPointer(keyPointer, key.Length);
                var v = SpanByte.FromPointer(valuePointer, value.Length);
                if (_session.Upsert(ref k, ref v).IsPending)
                    _session.CompletePending(wait: true);
            }
        }
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        unsafe
        {
            fixed (byte* keyPointer = key)
            {
                var k = SpanByte.FromPointer(keyPointer, key.Length);
                if (_session.Delete(ref k).IsPending)
                    _session.CompletePending(wait: true);
            }
        }
    }

    public void Dispose()
    {
        _session.Dispose();
        _store.Dispose();
        _device.Dispose();
    }

    private IssuedRead Issue(ReadOnlySpan<byte> keyBytes, out byte[]? value)
    {
        Status status;
        unsafe
        {
            fixed (byte* keyPointer = keyBytes)
            {
                var key = SpanByte.FromPointer(keyPointer, keyBytes.Length);
                byte[] result = [];
                status = _session.Read(ref key, ref result);
                value = result;
            }
        }

        if (status.IsPending)
        {
            value = null;
            return IssuedRead.Pending;
        }

        if (status.Found)
            return IssuedRead.Found;
        value = null;
        return IssuedRead.NotFound;
    }

    private enum IssuedRead
    {
        NotFound,
        Found,
        Pending,
    }

    /// <summary>Content equality for key bytes, to match completed reads back to their slots.</summary>
    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) =>
            ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        public int GetHashCode(byte[] obj)
        {
            var hash = default(HashCode);
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }

    /// <summary>FASTER callbacks: reads copy the stored span out as a fresh array; writes copy the span in.</summary>
    private sealed class StoreFunctions : FunctionsBase<SpanByte, SpanByte, SpanByte, byte[], Empty>
    {
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            return true;
        }

        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            return true;
        }

        public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref byte[] output, ref UpsertInfo upsertInfo, WriteReason reason)
            => src.TryCopyTo(ref dst);

        // The in-place overwrite. dst is the existing record's value slot, sized for whatever was
        // written before; src is the new value. TryCopyTo alone copies src's bytes but leaves the
        // slot's serialized length unchanged, so a *shorter* new value reads back at the OLD length
        // with a stale tail — the row grew large once and shrank, and the read returns the larger,
        // now-wrong bytes. (That is issue #137: a blob written at 339 then rewritten at 315 read
        // back as 339, which no longer matched the main record's declared 315, and threw out of
        // every scan.) So a value too large for the slot goes to a fresh record (return false → the
        // store copy-updates), and one that fits shrinks the slot's length to match before copying,
        // so the record reads back at exactly the new length.
        public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref byte[] output, ref UpsertInfo upsertInfo)
        {
            if (dst.Length < src.Length)
                return false;
            dst.ShrinkSerializedLength(src.Length);
            src.CopyTo(ref dst);
            return true;
        }
    }
}
