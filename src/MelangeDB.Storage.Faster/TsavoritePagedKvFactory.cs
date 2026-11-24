using System.Numerics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Tsavorite.core;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// Opens paged-store KV spaces on Tsavorite. Each space is its own <c>TsavoriteKV</c> with its own
/// hybrid-log device <c>main.hlog</c> / <c>blob.hlog</c> in the store directory (the same names the FASTER
/// store uses, so either engine's leftovers are swept by the other), deleted on close and swept on open.
/// No checkpointing or recovery: the store is a projection rebuilt from MelangeDB's commit log.
/// </summary>
internal sealed class TsavoritePagedKvFactory : IPagedKvFactory
{
    /// <summary>
    /// Overflow-bucket ceiling as a percentage of the main bucket count. Tsavorite caps the overflow-bucket
    /// allocator (default 300%, about 28 distinct hash tags per main bucket on average) and THROWS
    /// "MallocFixedPageSize&lt;HashBucket&gt; is full" past it; FASTER let chains grow without bound. The hint
    /// sizes the index from the memory budget, not the row count, and a paged table's whole point is holding
    /// far more rows than the budget -- 65,536 buckets at the default would cap out near 1.8M keys. 6400%
    /// (~450 tags per bucket) restores FASTER's "long chains, never a hard stop" behaviour well past any
    /// paged table we expect; pages of the allocator (4 MB each) are committed only as chains actually spill,
    /// so the ceiling itself costs nothing.
    /// </summary>
    internal const int IndexOverflowThresholdPercent = 6400;

    private readonly TsavoriteDevice _device;
    private readonly ILogger _logger;
    private bool? _nativeUsable;

    public TsavoritePagedKvFactory(TsavoriteDevice device = TsavoriteDevice.PlatformDefault, ILogger? logger = null)
    {
        _device = device;
        _logger = logger ?? NullLogger.Instance;
    }

    public string EngineName => "tsavorite";

    public IPagedKv Open(string directory, PagedKvSpace space, long memoryBudgetBytes, long hashBucketsHint)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        if (memoryBudgetBytes <= 0 || !BitOperations.IsPow2(memoryBudgetBytes))
            throw new ArgumentOutOfRangeException(nameof(memoryBudgetBytes), memoryBudgetBytes, "Must be a power of two.");
        if (hashBucketsHint <= 0)
            throw new ArgumentOutOfRangeException(nameof(hashBucketsHint), hashBucketsHint, "Must be positive.");

        var memoryBits = BitOperations.Log2((ulong)memoryBudgetBytes);
        var pageBits = Math.Min(memoryBits - 1, space == PagedKvSpace.Main ? 19 : 22);
        var name = space == PagedKvSpace.Main ? "main.hlog" : "blob.hlog";

        Directory.CreateDirectory(directory);
        DeleteStaleFiles(directory, name);

        var device = CreateDevice(Path.Combine(directory, name));

        // A checkpoint manager over null devices: Tsavorite always constructs one (it sizes sectors from it),
        // and the default is a local-disk one rooted at the current directory. This one can never write.
        var checkpointManager = new DeviceLogCommitCheckpointManager(
            new NullNamedDeviceFactoryCreator(),
            new DefaultCheckpointNamingScheme(Path.GetFullPath(directory)),
            removeOutdated: false);

        try
        {
            var settings = new KVSettings
            {
                // IndexSize is bytes; a hash bucket is one 64-byte cache line, and FASTER's constructor took
                // a bucket count, so the same index is hint * 64 bytes. Must be a power of two (it is).
                IndexSize = hashBucketsHint * KVSettings.HashBucketSizeBytes,
                IndexOverflowThreshold = IndexOverflowThresholdPercent,
                LogDevice = device,
                LogMemorySize = memoryBudgetBytes,
                PageSize = 1L << pageBits,
                SegmentSize = 1L << 26,
                CheckpointManager = checkpointManager,
                TryRecoverLatest = false,
            };
            return new TsavoritePagedKv(device, checkpointManager, settings, memoryBudgetBytes);
        }
        catch
        {
            device.Dispose();
            checkpointManager.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The log device. The platform default is Tsavorite's own: <c>LocalStorageDevice</c> (unbuffered
    /// overlapped I/O) on Windows, and on Linux <c>NativeStorageDevice</c> over Garnet's prebuilt
    /// <c>libnative_device.so</c> (libaio), vendored under <c>runtimes/</c>. The other variants exist to
    /// measure the choice: io_uring, and the managed <c>RandomAccess</c> device with and without the OS
    /// page cache.
    /// </summary>
    private IDevice CreateDevice(string path) => _device switch
    {
        // Linux: the native device through the page cache, the bake-off's pick ("Which Linux device" in
        // docs/design/hot-store-bakeoff.md). Windows: Tsavorite's default, unbuffered as FASTER is there.
        TsavoriteDevice.PlatformDefault when OperatingSystem.IsLinux() => NativeOrManaged(path),
        TsavoriteDevice.PlatformDefault => Devices.CreateLogDevice(path, DeviceType.Default, deleteOnClose: true),
        TsavoriteDevice.NativeUnbuffered => Devices.CreateLogDevice(path, DeviceType.Native, deleteOnClose: true),
        TsavoriteDevice.NativeUring => Devices.CreateLogDevice(
            path,
            DeviceType.Native,
            deleteOnClose: true,
            nativeDeviceOptions: new NativeDeviceOptions { IoBackend = NativeStorageDevice.IoBackend.Uring }),
        TsavoriteDevice.NativeBuffered => Devices.CreateLogDevice(path, DeviceType.Native, deleteOnClose: true, disableFileBuffering: false),
        TsavoriteDevice.RandomAccess => Devices.CreateLogDevice(path, DeviceType.RandomAccess, deleteOnClose: true),
        TsavoriteDevice.RandomAccessBuffered => new RandomAccessLocalStorageDevice(
            path, deleteOnClose: true, disableFileBuffering: false, osReadBuffering: true),
        _ => throw new ArgumentOutOfRangeException(nameof(_device), _device, null),
    };

    /// <summary>
    /// The native device through the page cache, or, when it cannot load, the managed device through
    /// the page cache: logged once (EventId 1513), never fatal. It cannot load when the host has no
    /// libaio (Debian and Ubuntu: <c>libaio1t64</c>); on a read-only file system where it needs to
    /// write its libaio compatibility symlink beside the library; on an architecture Garnet ships no
    /// build for; or in a RID-specific publish, which puts native files in the application root where
    /// Tsavorite does not look for them. The managed device writes at about 60% of the native one's rate.
    /// </summary>
    private IDevice NativeOrManaged(string path)
    {
        _nativeUsable ??= ProbeNative(Path.GetDirectoryName(path)!);
        return _nativeUsable == true
            ? Devices.CreateLogDevice(path, DeviceType.Native, deleteOnClose: true, disableFileBuffering: false)
            : new RandomAccessLocalStorageDevice(path, deleteOnClose: true, disableFileBuffering: false, osReadBuffering: true);
    }

    /// <summary>
    /// Whether the native device works here, decided by doing what it would do: one sector written
    /// and waited for, in a scratch file. Constructing the device proves nothing. It loads its
    /// library lazily, on the first write, which in a running store is a page flush on a thread-pool
    /// thread, where a load failure ends the process. The probe runs that first write on this thread,
    /// where the failure can be caught.
    /// </summary>
    private bool ProbeNative(string directory)
    {
        var probePath = Path.Combine(directory, "probe.native");
        try
        {
            using var device = Devices.CreateLogDevice(probePath, DeviceType.Native, deleteOnClose: true, disableFileBuffering: false);
            var sector = (int)Math.Max(device.SectorSize, 512);
            unsafe
            {
                var buffer = System.Runtime.InteropServices.NativeMemory.AlignedAlloc((nuint)sector, (nuint)sector);
                try
                {
                    using var done = new ManualResetEventSlim();
                    Exception? failure = null;
                    uint error = 0;
                    device.WriteAsync((IntPtr)buffer, 0, (uint)sector, (code, _, _, exception) =>
                    {
                        error = code;
                        failure = exception;
                        done.Set();
                    }, null);
                    if (!done.Wait(TimeSpan.FromSeconds(10)))
                        throw new TimeoutException("the native device's probe write did not complete in 10 s");
                    if (failure is not null)
                        throw failure;
                    if (error != 0)
                        throw new IOException($"the native device's probe write failed with error {error}");
                }
                finally
                {
                    System.Runtime.InteropServices.NativeMemory.AlignedFree(buffer);
                }
            }

            return true;
        }
        catch (Exception exception)
        {
            TsavoriteEngineLog.NativeDeviceUnavailable(_logger, exception.GetBaseException().Message);
            return false;
        }
        finally
        {
            foreach (var file in Directory.EnumerateFiles(directory, "probe.native*"))
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                }
            }
        }
    }

    /// <summary>Removes a previous run's segment files (<c>main.hlog.0</c>, <c>main.hlog.1</c>, ...) for this space.</summary>
    private static void DeleteStaleFiles(string directory, string name)
    {
        foreach (var file in Directory.EnumerateFiles(directory, name + "*"))
        {
            if (Path.GetFileName(file).StartsWith(name, StringComparison.Ordinal))
                File.Delete(file);
        }
    }
}

/// <summary>Which log device a Tsavorite space writes through; see <see cref="TsavoritePagedKvFactory"/>.</summary>
internal enum TsavoriteDevice
{
    /// <summary>Windows: LocalStorageDevice, unbuffered. Linux: the native device over libaio, through the page cache, or the managed device when it cannot load.</summary>
    PlatformDefault,

    /// <summary>Linux: the native device over io_uring.</summary>
    NativeUring,

    /// <summary>Linux: the native device over libaio, through the OS page cache (no O_DIRECT). The Linux default.</summary>
    NativeBuffered,

    /// <summary>Linux: the native device over libaio, unbuffered (O_DIRECT): memory stays at the budget, reads outside it go to disk.</summary>
    NativeUnbuffered,

    /// <summary>The managed RandomAccess device, unbuffered (O_DIRECT-style, aligned I/O).</summary>
    RandomAccess,

    /// <summary>The managed RandomAccess device through the OS page cache, reads and writes.</summary>
    RandomAccessBuffered,
}

internal static partial class TsavoriteEngineLog
{
    [LoggerMessage(EventId = 1513, EventName = "TsavoriteNativeDeviceUnavailable", Level = LogLevel.Warning,
        Message = "Tsavorite's native Linux device could not load ({Reason}), so the paged store uses its managed device: " +
            "correct, with writes at about 60% of the native rate. Install libaio (Debian/Ubuntu: libaio1t64), and do not " +
            "publish with a runtime identifier, which moves the native library where Tsavorite does not look for it.")]
    public static partial void NativeDeviceUnavailable(ILogger logger, string reason);
}
