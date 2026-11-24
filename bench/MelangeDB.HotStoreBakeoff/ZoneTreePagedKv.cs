using MelangeDB.Storage.Faster;
using ZoneTree;
using ZoneTree.Comparers;
using ZoneTree.Options;
using ZoneTree.Serializers;

namespace MelangeDB.HotStoreBakeoff;

/// <summary>ZoneTree: a pure-managed LSM tree, one tree per space.</summary>
internal sealed class ZoneTreePagedKvFactory : IPagedKvFactory
{
    public string EngineName => "zonetree";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        var path = Path.Combine(directory, space == PagedKvSpace.Main ? "main.zone" : "blob.zone");
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
        Directory.CreateDirectory(path);
        return new ZoneTreePagedKv(path, memoryBudgetBytes, space == PagedKvSpace.Main ? 128 : 4096);
    }
}

/// <summary>
/// One ZoneTree. Its write-ahead log is off, because the store is a projection rebuilt from
/// MelangeDB's own log on every start. ZoneTree sizes its in-memory segment by record count, not
/// bytes, so the budget is turned into a count from a typical value size per space. A deleted value
/// is marked by an empty array, which no paged row or out-of-line payload ever is.
/// </summary>
internal sealed class ZoneTreePagedKv : IPagedKv
{
    private readonly string _path;
    private readonly IZoneTree<byte[], byte[]> _tree;
    private readonly IMaintainer _maintainer;

    public ZoneTreePagedKv(string path, long memoryBudgetBytes, int typicalValueBytes)
    {
        _path = path;
        var mutableRecords = (int)Math.Clamp(memoryBudgetBytes / 2 / typicalValueBytes, 10_000, 4_000_000);
        _tree = new ZoneTreeFactory<byte[], byte[]>()
            .SetDataDirectory(path)
            .SetComparer(new BytesLexicographicComparerAscending())
            .SetKeySerializer(new BytesSerializer())
            .SetValueSerializer(new BytesSerializer())
            .SetIsDeletedDelegate((in byte[] _, in byte[] value) => value.Length == 0)
            .SetMarkValueDeletedDelegate((ref byte[] value) => value = [])
            .SetMutableSegmentMaxItemCount(mutableRecords)
            .ConfigureWriteAheadLogOptions(wal => wal.Mode = WriteAheadLogMode.None)
            .ConfigureDiskSegmentOptions(disk => disk.CompressionMethod = CompressionMethod.None)
            .OpenOrCreate();
        _maintainer = _tree.CreateMaintainer();
        _maintainer.EnableInactiveCacheCleanup = true;
        CapacityBytes = memoryBudgetBytes;
    }

    public long CapacityBytes { get; }

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        faulted = false; // Segment caches are internal; a disk read is not visible per call.
        var k = key.ToArray();
        return _tree.TryGet(in k, out var value) ? value : null;
    }

    public int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values)
    {
        for (var i = 0; i < keys.Length; i++)
            values[i] = _tree.TryGet(in keys[i], out var value) ? value : null;
        return 0;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var k = key.ToArray();
        var v = value.ToArray();
        _tree.Upsert(in k, in v);
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        var k = key.ToArray();
        _tree.ForceDelete(in k);
    }

    public void Dispose()
    {
        _maintainer.TryCancelBackgroundThreads();
        _maintainer.WaitForBackgroundThreads();
        _maintainer.Dispose();
        _tree.Dispose();
        try
        {
            Directory.Delete(_path, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
