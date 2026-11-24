using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using MelangeDB.Core;
using MelangeDB.Storage.Faster;

namespace MelangeDB.Benchmarks;

/// <summary>Which persistent ordered container a <see cref="NativeTreeBenchmarks"/> case measures.</summary>
public enum OrderedContainer
{
    /// <summary>The managed key directory: <c>RowDirectory&lt;byte[]&gt;</c> over <c>ImmutableSortedSet</c>.</summary>
    RowDirectory,

    /// <summary>Its native replacement: a plain <c>NativeTree</c>.</summary>
    NativePlain,

    /// <summary>The managed secondary index: <c>SecondaryIndex</c> over <c>ImmutableSortedSet</c>.</summary>
    SecondaryIndex,

    /// <summary>Its native replacement: a pair <c>NativeTree</c>.</summary>
    NativePair,
}

/// <summary>
/// The native copy-on-write B+tree against the managed persistent containers it is meant to
/// replace, on the hot store's shapes: a directory of 100k 8-byte keys with 16-byte values, and an
/// index of 100k (4-byte value, 8-byte key) pairs, four keys per value.
/// <para>
/// The point of the native tree is resident memory — tens of native bytes per entry against a
/// hundred-odd managed ones, and no GC objects per entry (the footprint test in
/// <c>NativeTreeFootprintTests</c> measures that). These numbers answer the other question: what
/// does it cost per operation? Allocation is reported because it is half the answer: a managed path
/// copy is garbage the GC must collect, a native one is not.
/// </para>
/// <para>
/// The single-operation commit releases the superseded native version explicitly, so the measured
/// time includes freeing its private pages — work the finalizer thread would otherwise do off the
/// writer's clock. The range reads build a <see cref="RowKey"/> per entry, which is what the store
/// will do with them; the "in place" variant shows the walk alone.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class NativeTreeBenchmarks
{
    private const int Entries = 100_000;
    private const int Probes = 1000;
    private const int Commits = 100;
    private const int Ranges = 100;
    private const int Window = 100;

    private readonly byte[] _value = new byte[16];
    private NativePagePool _pool = null!;
    private NativeTree _tree = null!;
    private RowDirectory<byte[]> _directory = null!;
    private SecondaryIndex _index = null!;
    private byte[][] _keys = [];
    private RowKey[] _rowKeys = [];
    private byte[][] _probeA = [];
    private byte[][] _probeB = [];
    private RowKey[] _probeRowA = [];
    private RowKey[] _probeRowB = [];
    private int _next;

    [Params(OrderedContainer.RowDirectory, OrderedContainer.NativePlain, OrderedContainer.SecondaryIndex, OrderedContainer.NativePair)]
    public OrderedContainer Container { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pool = new NativePagePool();
        _keys = new byte[Entries][];
        _rowKeys = new RowKey[Entries];
        for (var i = 0; i < Entries; i++)
        {
            _keys[i] = Key(i);
            _rowKeys[i] = new RowKey(_keys[i]);
        }

        Build();

        // Point probes and range bounds: random positions, fixed per run.
        var random = new Random(42);
        _probeA = new byte[Probes][];
        _probeB = new byte[Probes][];
        _probeRowA = new RowKey[Probes];
        _probeRowB = new RowKey[Probes];
        for (var i = 0; i < Probes; i++)
        {
            var at = random.Next(Entries - Window);
            if (IsPair)
            {
                // A window of 25 values holds 100 entries.
                _probeA[i] = Value(at / 4);
                _probeB[i] = Value((at / 4) + (Window / 4) - 1);
            }
            else
            {
                _probeA[i] = _keys[at];
                _probeB[i] = _keys[at + Window - 1];
            }

            _probeRowA[i] = new RowKey(_probeA[i]);
            _probeRowB[i] = new RowKey(_probeB[i]);
        }
    }

    private bool IsPair => Container is OrderedContainer.SecondaryIndex or OrderedContainer.NativePair;

    [Benchmark(Description = "build 100k entries from sorted input")]
    public long Build()
    {
        switch (Container)
        {
            case OrderedContainer.RowDirectory:
            {
                var builder = RowDirectory<byte[]>.Empty.ToBuilder();
                for (var i = 0; i < Entries; i++)
                    builder[_rowKeys[i]] = new byte[16];
                _directory = builder.ToImmutable();
                return _directory.Count;
            }

            case OrderedContainer.SecondaryIndex:
            {
                var builder = SecondaryIndex.Empty.ToBuilder();
                for (var i = 0; i < Entries; i++)
                    builder.Add(new RowKey(Value(i / 4)), _rowKeys[i]);
                _index = builder.ToImmutable();
                return _index.Count;
            }

            default:
            {
                var pair = Container == OrderedContainer.NativePair;
                using var builder = new NativeTree.Builder(_pool, pair ? NativeTreeOrder.Pair : NativeTreeOrder.Plain);
                Span<byte> value = stackalloc byte[4];
                for (var i = 0; i < Entries; i++)
                {
                    if (pair)
                    {
                        BinaryPrimitives.WriteInt32BigEndian(value, i / 4);
                        builder.Add(value, _keys[i]);
                    }
                    else
                    {
                        builder.Add(_keys[i], _value);
                    }
                }

                // Replace the tree the other benchmarks use; the old one has no readers.
                var previous = _tree;
                _tree = builder.ToTree();
                previous?.Release();
                return _tree.Count;
            }
        }
    }

    [Benchmark(Description = "point lookup", OperationsPerInvoke = Probes)]
    public long Lookup()
    {
        long found = 0;
        for (var i = 0; i < Probes; i++)
        {
            switch (Container)
            {
                case OrderedContainer.RowDirectory:
                    if (_directory.TryGetValue(_probeRowA[i], out var value))
                        found += value.Length;
                    break;
                case OrderedContainer.SecondaryIndex:
                    foreach (var key in _index.Equal(_probeRowA[i]))
                    {
                        found += key.Length;
                        break;
                    }

                    break;
                default:
                    if (_tree.TryGet(_probeA[i], static v => v.Length, out var length))
                        found += length;
                    break;
            }
        }

        return found;
    }

    [Benchmark(Description = "one-operation edit + commit", OperationsPerInvoke = Commits)]
    public long EditCommit()
    {
        long total = 0;
        for (var i = 0; i < Commits; i++)
        {
            var at = _next++ % Entries;
            switch (Container)
            {
                case OrderedContainer.RowDirectory:
                    _directory = _directory.SetItem(_rowKeys[at], new byte[16]);
                    total += _directory.Count;
                    break;
                case OrderedContainer.SecondaryIndex:
                    // An indexed column changing value: one entry out, one in.
                    _index = _index.Remove(new RowKey(Value(at / 4)), _rowKeys[at]).Add(new RowKey(Value(at / 4)), _rowKeys[at]);
                    total += _index.Count;
                    break;
                default:
                {
                    var edit = _tree.BeginEdit();
                    if (Container == OrderedContainer.NativePair)
                    {
                        var value = Value(at / 4);
                        edit.Remove(value, _keys[at]);
                        edit.Set(value, _keys[at]);
                    }
                    else
                    {
                        edit.Set(_keys[at], _value);
                    }

                    var previous = _tree;
                    _tree = edit.Commit();
                    previous.Release();
                    total += _tree.Count;
                    break;
                }
            }
        }

        return total;
    }

    [Benchmark(Description = "range of 100 as RowKeys", OperationsPerInvoke = Ranges)]
    public long Range()
    {
        long total = 0;
        for (var i = 0; i < Ranges; i++)
        {
            switch (Container)
            {
                case OrderedContainer.RowDirectory:
                    foreach (var key in _directory.KeyRange(_probeRowA[i], _probeRowB[i]))
                        total += key.Length;
                    break;
                case OrderedContainer.SecondaryIndex:
                    foreach (var key in _index.Range(_probeRowA[i], _probeRowB[i]))
                        total += key.Length;
                    break;
                case OrderedContainer.NativePlain:
                    foreach (var key in _tree.Range(_probeA[i], _probeB[i], static (k, _) => new RowKey(k)))
                        total += key.Length;
                    break;
                default:
                    foreach (var key in _tree.Range(_probeA[i], _probeB[i], static (_, k) => new RowKey(k)))
                        total += key.Length;
                    break;
            }
        }

        return total;
    }

    [Benchmark(Description = "range of 100, read in place", OperationsPerInvoke = Ranges)]
    public long RangeInPlace()
    {
        if (Container is OrderedContainer.RowDirectory or OrderedContainer.SecondaryIndex)
            return Range();

        long total = 0;
        for (var i = 0; i < Ranges; i++)
        {
            foreach (var length in _tree.Range(_probeA[i], _probeB[i], static (k, v) => k.Length + v.Length))
                total += length;
        }

        return total;
    }

    private static byte[] Key(int i)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(key, i);
        return key;
    }

    private static byte[] Value(int v)
    {
        var value = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(value, v);
        return value;
    }
}
