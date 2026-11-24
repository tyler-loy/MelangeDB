using MelangeDB.Storage.Faster;
using RocksDbSharp;

namespace MelangeDB.HotStoreBakeoff;

/// <summary>RocksDB (via the RocksDB package's RocksDbSharp): an LSM tree, one database per space.</summary>
internal sealed class RocksDbPagedKvFactory : IPagedKvFactory
{
    public string EngineName => "rocksdb";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        var path = Path.Combine(directory, space == PagedKvSpace.Main ? "main.rocks" : "blob.rocks");
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
        return new RocksDbPagedKv(path, memoryBudgetBytes);
    }
}

/// <summary>
/// One RocksDB database. The memory budget is split the way RocksDB spends memory: half to the
/// block cache (reads), and a quarter to each of two memtables (writes). The write-ahead log is off,
/// because the store is a projection of MelangeDB's own log and is rebuilt from it on every start.
/// </summary>
internal sealed class RocksDbPagedKv : IPagedKv
{
    private readonly string _path;
    private readonly RocksDb _db;
    private readonly WriteOptions _write;

    public RocksDbPagedKv(string path, long memoryBudgetBytes)
    {
        _path = path;
        // Index and filter blocks stay in table-reader memory rather than competing for the block
        // cache, and compaction gets more background threads: with the WAL off and a write rate in the
        // hundreds of thousands, level 0 otherwise piles up files and every read probes each one.
        var table = new BlockBasedTableOptions()
            .SetBlockCache(Cache.CreateLru((ulong)(memoryBudgetBytes / 2)))
            .SetFilterPolicy(BloomFilterPolicy.Create(10, false));
        var options = new DbOptions()
            .SetCreateIfMissing(true)
            .SetBlockBasedTableFactory(table)
            .SetWriteBufferSize((ulong)(memoryBudgetBytes / 4))
            .SetMaxWriteBufferNumber(2)
            .SetCompression(Compression.No)
            .SetLevelCompactionDynamicLevelBytes(true)
            .IncreaseParallelism(4);
        _db = RocksDb.Open(options, path);
        _write = new WriteOptions().DisableWal(1).SetSync(false);
        CapacityBytes = memoryBudgetBytes;
    }

    public long CapacityBytes { get; }

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        faulted = false; // A block-cache miss is not visible per read without statistics on.
        return _db.Get(key.ToArray());
    }

    public int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values)
    {
        var results = _db.MultiGet(keys.ToArray());
        for (var i = 0; i < keys.Length; i++)
            values[i] = results[i].Value;
        return 0;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        _db.Put(key.ToArray(), value.ToArray(), null, _write);

    public void Delete(ReadOnlySpan<byte> key) => _db.Remove(key.ToArray(), null, _write);

    public void Dispose()
    {
        _db.Dispose();
        try
        {
            Directory.Delete(_path, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
