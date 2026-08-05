using System.Buffers.Binary;
using System.Collections.Immutable;
using BenchmarkDotNet.Attributes;

namespace MelangeDB.Benchmarks;

/// <summary>
/// The measurement that settled how the in-memory store pins a read view: what does making the row
/// container <b>persistent</b> — so a pinned view is a reference capture rather than a copy — cost
/// the paths that run all the time?
/// <para>
/// The comparison is against <see cref="SortedDictionary{TKey,TValue}"/>, which is what
/// <c>InMemoryHotStore</c> held before, at the shapes the store actually uses: build a table
/// (recovery replay), read a row by key, scan a table in key order, write one row under the engine's
/// write lock, and pin a view. Memory is reported by the allocation column for the build cases; the
/// interesting result is that the two containers are within noise of each other on it.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class HotStoreContainerBenchmarks
{
    private const int RowBytes = 96;

    private RowKey[] _keys = [];
    private RowKey[] _shuffled = [];
    private byte[][] _rows = [];
    private RowKey[] _fresh = [];
    private byte[] _freshRow = [];

    private SortedDictionary<RowKey, byte[]> _mutable = [];
    private ImmutableSortedDictionary<RowKey, byte[]> _persistent = ImmutableSortedDictionary<RowKey, byte[]>.Empty;

    [Params(10_000, 100_000, 1_000_000)]
    public int Rows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _keys = new RowKey[Rows];
        _rows = new byte[Rows][];
        for (var i = 0; i < Rows; i++)
        {
            _keys[i] = Key((ulong)i);
            _rows[i] = new byte[RowBytes];
        }

        _shuffled = Shuffle(_keys);

        // A single transaction's worth of writes, at keys that do not already exist.
        _fresh = new RowKey[1_000];
        for (var i = 0; i < _fresh.Length; i++)
            _fresh[i] = Key((ulong)(Rows + i));
        _freshRow = new byte[RowBytes];

        _mutable = new SortedDictionary<RowKey, byte[]>();
        for (var i = 0; i < Rows; i++)
            _mutable[_keys[i]] = _rows[i];

        var builder = ImmutableSortedDictionary.CreateBuilder<RowKey, byte[]>();
        for (var i = 0; i < Rows; i++)
            builder[_keys[i]] = _rows[i];
        _persistent = builder.ToImmutable();
    }

    [Benchmark(Description = "build: mutable"), BenchmarkCategory("build")]
    public int BuildMutable()
    {
        var map = new SortedDictionary<RowKey, byte[]>();
        for (var i = 0; i < Rows; i++)
            map[_keys[i]] = _rows[i];
        return map.Count;
    }

    [Benchmark(Description = "build: persistent"), BenchmarkCategory("build")]
    public int BuildPersistent()
    {
        var builder = ImmutableSortedDictionary.CreateBuilder<RowKey, byte[]>();
        for (var i = 0; i < Rows; i++)
            builder[_keys[i]] = _rows[i];
        return builder.ToImmutable().Count;
    }

    [Benchmark(Description = "point read: mutable"), BenchmarkCategory("read")]
    public long PointReadMutable()
    {
        long hits = 0;
        foreach (var key in _shuffled)
        {
            if (_mutable.TryGetValue(key, out _))
                hits++;
        }

        return hits;
    }

    [Benchmark(Description = "point read: persistent"), BenchmarkCategory("read")]
    public long PointReadPersistent()
    {
        long hits = 0;
        foreach (var key in _shuffled)
        {
            if (_persistent.TryGetValue(key, out _))
                hits++;
        }

        return hits;
    }

    [Benchmark(Description = "scan: mutable"), BenchmarkCategory("scan")]
    public long ScanMutable()
    {
        long bytes = 0;
        foreach (var pair in _mutable)
            bytes += pair.Value.Length;
        return bytes;
    }

    [Benchmark(Description = "scan: persistent"), BenchmarkCategory("scan")]
    public long ScanPersistent()
    {
        long bytes = 0;
        foreach (var pair in _persistent)
            bytes += pair.Value.Length;
        return bytes;
    }

    [Benchmark(Description = "1,000 puts: mutable"), BenchmarkCategory("write")]
    public int PutMutable()
    {
        foreach (var key in _fresh)
            _mutable[key] = _freshRow;
        foreach (var key in _fresh)
            _mutable.Remove(key);
        return _mutable.Count;
    }

    [Benchmark(Description = "1,000 puts: persistent"), BenchmarkCategory("write")]
    public int PutPersistent()
    {
        var map = _persistent;
        foreach (var key in _fresh)
            map = map.SetItem(key, _freshRow);
        return map.Count;
    }

    [Benchmark(Description = "pin a view: mutable (clone)"), BenchmarkCategory("pin")]
    public int PinMutable() => new SortedDictionary<RowKey, byte[]>(_mutable).Count;

    [Benchmark(Description = "pin a view: persistent (capture)"), BenchmarkCategory("pin")]
    public int PinPersistent()
    {
        var pinned = _persistent;
        return pinned.Count;
    }

    private static RowKey Key(ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        return new RowKey(buffer);
    }

    private static RowKey[] Shuffle(RowKey[] source)
    {
        var copy = (RowKey[])source.Clone();
        var random = new Random(12345);
        for (var i = copy.Length - 1; i > 0; i--)
        {
            var j = random.Next(i + 1);
            (copy[i], copy[j]) = (copy[j], copy[i]);
        }

        return copy;
    }
}
