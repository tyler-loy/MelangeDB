using LightningDB;
using MelangeDB.Storage.Faster;

namespace MelangeDB.HotStoreBakeoff;

/// <summary>LMDB (via LightningDB): a copy-on-write B+tree in a memory map, one environment per space.</summary>
internal sealed class LmdbPagedKvFactory(LmdbTuning? tuning = null) : IPagedKvFactory
{
    private readonly LmdbTuning _tuning = tuning ?? LmdbTuning.Default;

    public string EngineName => "lmdb";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        var path = Path.Combine(directory, space == PagedKvSpace.Main ? "main.lmdb" : "blob.lmdb");
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
        Directory.CreateDirectory(path);
        return new LmdbPagedKv(path, memoryBudgetBytes, _tuning);
    }
}

/// <summary>
/// One LMDB environment holding one database. LMDB has no buffer pool of its own: the OS page cache
/// over its memory map is the cache, so the memory budget only seeds the map size.
/// <para>
/// One write transaction is held open and committed every <see cref="LmdbTuning.CommitEvery"/> writes. Every
/// call arrives under the paged store's lock, the store is a projection rebuilt from the commit
/// log (so nothing here needs to be durable), and a commit per op would measure LMDB's commit path
/// rather than its storage. Reads go through the same transaction, so they see its writes. The
/// uncommitted writes are kept so that a full map (MDB_MAP_FULL aborts the transaction) can be
/// grown and the batch replayed.
/// </para>
/// </summary>
internal sealed class LmdbPagedKv : IPagedKv
{
    private readonly string _path;
    private readonly LightningEnvironment _environment;
    private readonly LightningDatabase _database;
    private readonly List<(byte[] Key, byte[]? Value)> _uncommitted;
    private readonly int _commitEvery;
    private LightningTransaction _transaction;

    public LmdbPagedKv(string path, long memoryBudgetBytes, LmdbTuning tuning)
    {
        _path = path;
        _commitEvery = tuning.CommitEvery;
        _uncommitted = new(_commitEvery);
        var configuration = new EnvironmentConfiguration
        {
            MapSize = Math.Max(1L << 30, memoryBudgetBytes * 8),
            MaxDatabases = 1,
        };
        if (tuning.PageSize > 0)
            configuration.PageSize = tuning.PageSize;
        _environment = new LightningEnvironment(path, configuration);
        var flags = EnvironmentOpenFlags.NoSync | EnvironmentOpenFlags.NoMetaSync | EnvironmentOpenFlags.NoThreadLocalStorage;
        if (tuning.WriteMap)
            flags |= EnvironmentOpenFlags.WriteMap | EnvironmentOpenFlags.MapAsync;
        _environment.Open(flags);
        _transaction = _environment.BeginTransaction();
        _database = _transaction.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
        Commit();
        CapacityBytes = memoryBudgetBytes;
    }

    public long CapacityBytes { get; }

    /// <summary>Not measured for bake-off-only engines.</summary>
    public HotStoreLogStatistics Statistics => new();

    public byte[]? Read(ReadOnlySpan<byte> key, out bool faulted)
    {
        faulted = false; // The OS page cache decides; LMDB cannot tell a fault from a hit.
        var (code, _, value) = _transaction.Get(_database, key);
        return code == MDBResultCode.Success ? value.CopyToNewArray() : null;
    }

    public int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values)
    {
        for (var i = 0; i < keys.Length; i++)
            values[i] = Read(keys[i], out _);
        return 0;
    }

    public void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Write(key.ToArray(), value.ToArray());

    public void Delete(ReadOnlySpan<byte> key) => Write(key.ToArray(), null);

    public void Dispose()
    {
        _transaction.Commit();
        _transaction.Dispose();
        _database.Dispose();
        _environment.Dispose();
        try
        {
            Directory.Delete(_path, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private void Write(byte[] key, byte[]? value)
    {
        _uncommitted.Add((key, value));
        if (Apply(key, value) == MDBResultCode.MapFull)
            GrowAndReplay();
        if (_uncommitted.Count >= _commitEvery)
            Commit();
    }

    private MDBResultCode Apply(byte[] key, byte[]? value)
    {
        if (value is not null)
            return _transaction.Put(_database, key, value);
        var code = _transaction.Delete(_database, key);
        return code == MDBResultCode.NotFound ? MDBResultCode.Success : code;
    }

    private void GrowAndReplay()
    {
        while (true)
        {
            _transaction.Abort();
            _transaction.Dispose();
            _environment.MapSize *= 2;
            _transaction = _environment.BeginTransaction();
            var full = false;
            foreach (var (key, value) in _uncommitted)
            {
                var code = Apply(key, value);
                if (code == MDBResultCode.MapFull)
                {
                    full = true;
                    break;
                }

                if (code != MDBResultCode.Success)
                    throw new InvalidOperationException($"LMDB write failed: {code}.");
            }

            if (!full)
                return;
        }
    }

    private void Commit()
    {
        var code = _transaction.Commit();
        if (code != MDBResultCode.Success)
            throw new InvalidOperationException($"LMDB commit failed: {code}.");
        _transaction.Dispose();
        _uncommitted.Clear();
        _transaction = _environment.BeginTransaction();
    }
}

/// <summary>
/// LMDB settings the bake-off varies. <see cref="PageSize"/> 0 is LMDB's default (the OS page, 4 KB):
/// values over about half a page go to overflow pages, which every rewrite reallocates. A larger page
/// keeps 4 KB terrain values inline. <see cref="WriteMap"/> writes straight into the memory map
/// instead of copying dirty pages at commit. <see cref="CommitEvery"/> sets how many writes share one
/// transaction: a page touched several times in one is copied once.
/// </summary>
public sealed record LmdbTuning(int PageSize, bool WriteMap, int CommitEvery)
{
    public static LmdbTuning Default { get; } = new(0, false, 1024);
}
