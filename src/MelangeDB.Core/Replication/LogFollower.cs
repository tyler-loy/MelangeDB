using System.Buffers.Binary;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Core;

/// <summary>
/// A warm projection of a log someone else owns (road-to-0.4 phase 30, step 2c): the hot store a
/// shard's replica keeps current, so that promoting it costs the unapplied tail rather than a full
/// recovery. It reads the owner's log directory and writes nothing there: no lock, no torn-tail
/// truncation, no epoch, no shape sidecar. It owns no log and can commit nothing, and it is not a
/// <see cref="MelangeEngine"/>, so nothing that asks an engine "may I commit?" can mistake it for
/// an owner.
/// <para>
/// It bootstraps the way recovery does, from the snapshot then the log, and then follows the log
/// with <see cref="Poll"/>, applying records only up to <see cref="Ceiling"/>: the owner's durable
/// LSN, relayed by the hub. A record written but not yet durable can vanish with the owner's
/// machine, and a replica that had applied it would be ahead of the log it is about to own.
/// </para>
/// <para>
/// It starts over from the snapshot whenever its place in the log is gone or its reading of it may
/// be wrong: a truncation past its cursor, a new log epoch, or a changed shape sidecar (an owner's
/// migration changes what row bytes mean from its marker on).
/// </para>
/// </summary>
internal sealed class LogFollower : IDisposable
{
    private readonly MelangeDbOptions _options;
    private readonly SchemaRegistry _schema;
    private readonly ILoggerFactory _loggers;
    private readonly ILogger _logger;
    private readonly IHotStoreProvider? _provider;
    private readonly ushort _originator;
    private readonly IReadOnlySet<TableId> _archiveTables;
    private readonly string _directory;

    private IHotStore? _store;
    private ShapeResolution? _shapes;
    private LogSegments.Tail? _tail;
    private Guid _epoch;
    private (long Length, DateTime Written) _shapeStamp;
    private ulong _ceiling;
    private bool _disposed;

    public LogFollower(
        MelangeDbOptions options,
        SchemaRegistry schema,
        ushort originator,
        ILoggerFactory? loggerFactory = null,
        IHotStoreProvider? hotStoreProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schema);
        _options = options;
        _schema = schema;
        _originator = originator;
        _loggers = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggers.CreateLogger<LogFollower>();
        _provider = hotStoreProvider;
        _directory = options.CommitLog.Path;
        _archiveTables = schema.Tables.Where(static t => t.Tier == StorageTier.Archive).Select(static t => t.Id).ToHashSet();
        Directory.CreateDirectory(options.HotStore.Path);
    }

    /// <summary>The newest LSN applied to <see cref="Store"/>; 0 before the first bootstrap.</summary>
    public ulong AppliedLsn { get; private set; }

    /// <summary>The highest LSN <see cref="Poll"/> may apply: the owner's durable LSN. Nothing is applied until it is set.</summary>
    public ulong Ceiling
    {
        get => Volatile.Read(ref _ceiling);
        set => Volatile.Write(ref _ceiling, value);
    }

    /// <summary>The projection; null until the first bootstrap succeeds.</summary>
    public IHotStore? Store => _store;

    /// <summary>The AutoInc sequences as the applied records leave them.</summary>
    public AutoIncSequencer Sequencer { get; private set; } = new();

    /// <summary>The applied tail's commit timestamp: the scheduler's downtime anchor after a promotion.</summary>
    public Timestamp? TailTimestamp { get; private set; }

    /// <summary>Bootstraps so far; more than one means the follower had to start over.</summary>
    public int Bootstraps { get; private set; }

    /// <summary>
    /// Applies whatever the log holds beyond <see cref="AppliedLsn"/>, up to <see cref="Ceiling"/>,
    /// at most <paramref name="max"/> records; bootstraps first if needed. Returns the records
    /// applied. A directory whose owner has not booted yet applies nothing and is not an error.
    /// Single-threaded: the caller drives it.
    /// </summary>
    public int Poll(int max = 4096)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if ((_tail is null || MustStartOver()) && !Bootstrap())
            return 0;
        try
        {
            return Apply(_tail!.Read(Ceiling, max));
        }
        catch (TailGapException exception)
        {
            LogMessages.FollowerStartingOver(_logger, _directory, AppliedLsn, exception.Message);
            return Bootstrap() ? Apply(_tail!.Read(Ceiling, max)) : 0;
        }
    }

    private int Apply(List<CommitRecord> records)
    {
        foreach (var record in records)
        {
            var replay = _shapes!.TransformRecord(record);
            _store!.Apply(MelangeEngine.WithoutArchiveOps(replay, _schema, _archiveTables));
            Sequencer.Observe(replay, _schema);
            AppliedLsn = record.Lsn;
            TailTimestamp = replay.Timestamp;
        }

        return records.Count;
    }

    /// <summary>Whether the follower's place in the log, or its reading of it, can no longer be trusted.</summary>
    private bool MustStartOver()
    {
        if (ReadEpoch() is { } epoch && epoch != _epoch)
            return true;
        if (ReadBaseLsn() > AppliedLsn)
            return true;
        return ShapeStamp() != _shapeStamp;
    }

    /// <summary>
    /// Ordinary recovery, read-only, into a fresh store: the snapshot when it belongs to the log's
    /// epoch, then the log up to the ceiling. Returns false when the directory is not ready to be
    /// followed yet: no epoch or shape sidecar (the owner has not booted), or a truncated log whose
    /// covering snapshot is mid-rewrite.
    /// </summary>
    private bool Bootstrap()
    {
        if (ReadEpoch() is not { } epoch)
            return false;
        var baseLsn = ReadBaseLsn();
        ShapeResolution shapes;
        try
        {
            shapes = ShapeGuard.Resolve(_directory, _schema, baseLsn, allowAdoption: false, readOnly: true);
        }
        catch (ShapeNotRecordedException)
        {
            return false;
        }

        var shapeStamp = ShapeStamp();
        var store = MelangeEngine.CreateStore(_options, _schema, _provider, _loggers);
        var sequencer = new AutoIncSequencer(_originator);
        ulong applied = 0;
        Timestamp? tailTimestamp = null;
        var bulk = store as IBulkRecovery;
        try
        {
            bulk?.BeginRecovery();
            var snapshotPath = Path.Combine(_directory, SnapshotFile.FileName);
            if (File.Exists(snapshotPath))
            {
                using var reader = SnapshotFile.Open(snapshotPath);
                var header = reader.Header;
                if (header.Epoch == epoch && header.Lsn >= baseLsn)
                {
                    store.LoadSnapshot(header.Lsn, shapes.TransformSnapshotRows(header.Lsn, reader.Rows()));
                    foreach (var (table, next) in header.Sequences)
                        sequencer.RestoreSequence(table, next);
                    applied = header.Lsn;
                    tailTimestamp = header.Timestamp;
                }
                else if (baseLsn > 0)
                {
                    (store as IDisposable)?.Dispose();
                    return false; // The snapshot covering the truncation is being replaced; try again.
                }
            }
            else if (baseLsn > 0)
            {
                (store as IDisposable)?.Dispose();
                return false;
            }

            bulk?.CompleteRecovery();
        }
        catch
        {
            (store as IDisposable)?.Dispose();
            throw;
        }

        (_store as IDisposable)?.Dispose();
        _store = store;
        _shapes = shapes;
        _epoch = epoch;
        _shapeStamp = shapeStamp;
        Sequencer = sequencer;
        AppliedLsn = applied;
        TailTimestamp = tailTimestamp;
        _tail = new LogSegments.Tail(_directory, applied + 1);
        Bootstraps++;
        LogMessages.FollowerBootstrapped(_logger, _directory, applied, Bootstraps);
        return true;
    }

    private Guid? ReadEpoch()
    {
        var path = Path.Combine(_directory, FileCommitLog.EpochFileName);
        try
        {
            var bytes = File.ReadAllBytes(path);
            return bytes.Length == 16 ? new Guid(bytes) : null;
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    private ulong ReadBaseLsn()
    {
        try
        {
            var bytes = File.ReadAllBytes(Path.Combine(_directory, "melange.base"));
            return bytes.Length == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(bytes) : 0;
        }
        catch (FileNotFoundException)
        {
            return 0;
        }
    }

    private (long Length, DateTime Written) ShapeStamp()
    {
        var info = new FileInfo(Path.Combine(_directory, ShapeHistory.FileName));
        return info.Exists ? (info.Length, info.LastWriteTimeUtc) : (0, default);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        (_store as IDisposable)?.Dispose();
        _store = null;
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, ulong, int, Exception?> BootstrappedMessage =
            LoggerMessage.Define<string, ulong, int>(
                LogLevel.Information,
                new EventId(1012, "LogFollowerBootstrapped"),
                "Following '{LogDirectory}' from LSN {AppliedLsn} (bootstrap {Bootstraps}).");

        private static readonly Action<ILogger, string, ulong, string, Exception?> StartingOverMessage =
            LoggerMessage.Define<string, ulong, string>(
                LogLevel.Warning,
                new EventId(1013, "LogFollowerStartingOver"),
                "The follower of '{LogDirectory}' lost its place after LSN {AppliedLsn} ({Reason}) and starts again from the snapshot.");

        public static void FollowerBootstrapped(ILogger logger, string directory, ulong applied, int bootstraps) =>
            BootstrappedMessage(logger, directory, applied, bootstraps, null);

        public static void FollowerStartingOver(ILogger logger, string directory, ulong applied, string reason) =>
            StartingOverMessage(logger, directory, applied, reason, null);
    }
}
