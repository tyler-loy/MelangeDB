using MelangeDB.Core;
using Microsoft.Extensions.Logging;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// Which of a paged store's two key-value spaces a <see cref="IPagedKv"/> holds: row records, or
/// the out-of-line payloads of large bytes columns (see <see cref="RowBlobSplitter"/>).
/// </summary>
internal enum PagedKvSpace
{
    Main,
    Blob,
}

/// <summary>
/// One key-value space of the paged hot store: the only part of <see cref="FasterHotStore"/> that
/// talks to a storage engine. Everything else the store does (the key directory, secondary
/// indexes, residency, read views, blob splitting) is engine-agnostic and stays in the store, so a
/// different engine is one implementation of this interface.
/// <para>
/// Every call is made under the store's own lock, one at a time, so an implementation need not be
/// thread-safe. Keys are short: a 4-byte big-endian table id and the encoded primary key, plus an
/// ordinal byte in the blob space.
/// </para>
/// </summary>
internal interface IPagedKv : IDisposable
{
    /// <summary>
    /// Reads a value, or null when absent. <paramref name="faulted"/> says the read waited on
    /// storage rather than being served from memory, as best the engine can tell.
    /// </summary>
    byte[]? Read(ReadOnlySpan<byte> key, out bool faulted);

    /// <summary>
    /// Reads <c>keys[i]</c> into <c>values[i]</c> (null when absent), with the reads in flight
    /// together where the engine can do that. Returns how many reads waited on storage.
    /// </summary>
    int ReadMany(ReadOnlySpan<byte[]> keys, Span<byte[]?> values);

    /// <summary>Inserts or overwrites. A value shorter than the previous one reads back at exactly its own length.</summary>
    void Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>Removes the key. Absent is a no-op.</summary>
    void Delete(ReadOnlySpan<byte> key);

    /// <summary>The memory this space may hold data in (its buffer pool or in-memory log), for statistics.</summary>
    long CapacityBytes { get; }
}

/// <summary>
/// Opens the key-value spaces of one store. The store is a projection rebuilt from the commit log
/// on every start, so a space always opens <b>empty</b>: files a previous run left for it are
/// stale and are deleted, and the engine's own durability, checkpoints and recovery stay off.
/// </summary>
internal interface IPagedKvFactory
{
    /// <summary>The engine's name, for logs and benchmark labels.</summary>
    string EngineName { get; }

    /// <summary>
    /// Opens <paramref name="space"/> in <paramref name="directory"/>, empty, holding up to
    /// <paramref name="memoryBudgetBytes"/> (a power of two) of data in memory.
    /// <paramref name="hashBucketsHint"/> is the store's index-size estimate, for engines that
    /// size a hash index up front.
    /// </summary>
    IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint);
}

/// <summary>The engine behind each <see cref="PagedEngine"/> value.</summary>
internal static class PagedEngines
{
    public static IPagedKvFactory For(PagedEngine engine, ILoggerFactory? loggerFactory = null) => engine switch
    {
        PagedEngine.Faster => FasterPagedKvFactory.Instance,
        PagedEngine.MelangeLog => new MelangeLogPagedKvFactory(),
        PagedEngine.Tsavorite => new TsavoritePagedKvFactory(logger: loggerFactory?.CreateLogger<TsavoritePagedKvFactory>()),
        _ => throw new ArgumentOutOfRangeException(nameof(engine), engine, "Not a paged engine this build knows."),
    };
}
