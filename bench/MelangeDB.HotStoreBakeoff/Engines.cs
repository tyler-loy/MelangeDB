using MelangeDB.Core;
using MelangeDB.Storage.Faster;

namespace MelangeDB.HotStoreBakeoff;

/// <summary>The bake-off's engines by name, and a hot-store provider over each.</summary>
public static class Engines
{
    /// <summary>
    /// The engines a default run compares, FASTER (the shipped one) first. The Tsavorite device variants
    /// (lmdb-writemap, lmdb-bigtxn, lmdb-page16k, lmdb-page64k, lmdb-tuned, lmdb-tuned64k, tsavorite-buffered, tsavorite-unbuffered, tsavorite-uring, tsavorite-ra, tsavorite-ra-buffered) and melangelog variants (melangelog-pread, melangelog-random, melangelog-noprefetch, melangelog-pointpread) run when named with <c>--engines</c>.
    /// </summary>
    public static IReadOnlyList<string> Names { get; } = ["faster", "tsavorite", "melangelog", "lmdb", "rocksdb", "zonetree"];

    /// <summary>The paged hot store over <paramref name="engine"/>'s key-value spaces.</summary>
    public static IHotStoreProvider Provider(string engine) => new FasterHotStoreProvider(Factory(engine));

    internal static IPagedKvFactory Factory(string engine) => engine switch
    {
        "faster" => FasterPagedKvFactory.Instance,
        "tsavorite" => new TsavoritePagedKvFactory(),
        "tsavorite-uring" => new TsavoritePagedKvFactory(TsavoriteDevice.NativeUring),
        "tsavorite-buffered" => new TsavoritePagedKvFactory(TsavoriteDevice.NativeBuffered),
        "tsavorite-unbuffered" => new TsavoritePagedKvFactory(TsavoriteDevice.NativeUnbuffered),
        "tsavorite-ra" => new TsavoritePagedKvFactory(TsavoriteDevice.RandomAccess),
        "tsavorite-ra-buffered" => new TsavoritePagedKvFactory(TsavoriteDevice.RandomAccessBuffered),
        "melangelog" => new MelangeLogPagedKvFactory(),
        "melangelog-pread" => new MelangeLogPagedKvFactory(MelangeLogTuning.Default with { ReadMode = MelangeLogReadMode.Pread }),
        "melangelog-random" => new MelangeLogPagedKvFactory(MelangeLogTuning.Default with { RandomAdvice = true }),
        "melangelog-noprefetch" => new MelangeLogPagedKvFactory(MelangeLogTuning.Default with { Prefetch = false }),
        "melangelog-pointpread" => new MelangeLogPagedKvFactory(MelangeLogTuning.Default with { BlobPointReadsViaPread = true }),
        "lmdb" => new LmdbPagedKvFactory(),
        "lmdb-writemap" => new LmdbPagedKvFactory(LmdbTuning.Default with { WriteMap = true }),
        "lmdb-bigtxn" => new LmdbPagedKvFactory(LmdbTuning.Default with { CommitEvery = 8192 }),
        "lmdb-page16k" => new LmdbPagedKvFactory(LmdbTuning.Default with { PageSize = 16384 }),
        "lmdb-page64k" => new LmdbPagedKvFactory(LmdbTuning.Default with { PageSize = 65536 }),
        "lmdb-tuned" => new LmdbPagedKvFactory(new LmdbTuning(16384, WriteMap: true, CommitEvery: 8192)),
        "lmdb-tuned64k" => new LmdbPagedKvFactory(new LmdbTuning(65536, WriteMap: true, CommitEvery: 8192)),
        "rocksdb" => new RocksDbPagedKvFactory(),
        "zonetree" => new ZoneTreePagedKvFactory(),
        _ => throw new ArgumentException($"Unknown engine '{engine}'. Known: {string.Join(", ", Names)}.", nameof(engine)),
    };
}
