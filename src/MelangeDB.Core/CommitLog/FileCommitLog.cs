using System.Buffers.Binary;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MelangeDB.Core;

/// <summary>
/// The append-only local commit log: one CRC-guarded, version-tagged record per transaction. On
/// open it scans the file, and a torn trailing record — a crash mid-append — is truncated to the
/// last intact LSN rather than being fatal. Corruption anywhere before the tail is fatal, because
/// it means committed history was damaged. The fsync policy is read per operation, so a changed
/// options value takes effect on the next commit.
/// </summary>
public sealed class FileCommitLog : ICommitLog
{
    private const uint Magic = 0x474F4C4Du; // "MLOG"
    private const ushort FileFormatVersion = 1;
    private const int HeaderSize = 8;
    private const int FrameSize = 8; // u32 payload length + u32 crc
    private const uint MaxRecordBytes = 256 * 1024 * 1024;

    private readonly CommitLogOptions _options;
    private readonly ILogger _logger;
    private readonly EngineTelemetry? _telemetry;
    private FileStream _stream;
    private readonly Lock _lock = new();
    private Timer? _flushTimer;
    private ulong _headLsn;
    private ulong _baseLsn;
    private Exception? _failure;
    private bool _disposed;

    public FileCommitLog(CommitLogOptions options, ILogger<FileCommitLog>? logger = null)
        : this(options, (ILogger?)logger, null)
    {
    }

    internal FileCommitLog(CommitLogOptions options, ILogger? logger, EngineTelemetry? telemetry)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _logger = logger ?? NullLogger.Instance;
        _telemetry = telemetry;
        Directory.CreateDirectory(options.Path);
        FilePath = System.IO.Path.Combine(options.Path, "melange.log");
        EpochFilePath = System.IO.Path.Combine(options.Path, "melange.epoch");
        BaseFilePath = System.IO.Path.Combine(options.Path, "melange.base");
        // FileShare.Delete throughout: log compaction atomically replaces the file, and every open
        // handle must permit that or a concurrent lazy reader would make truncation fail.
        _stream = new FileStream(FilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
        Recover();
    }

    /// <summary>The full path of the log file.</summary>
    public string FilePath { get; }

    /// <summary>
    /// The full path of the epoch sidecar. The epoch lives beside the log rather than in its
    /// header so phase-01 logs stay readable with no header version bump: initializing a fresh log
    /// file mints a new epoch, and an existing log without a sidecar is adopted under a freshly
    /// minted one exactly once.
    /// </summary>
    public string EpochFilePath { get; }

    public Guid EpochId { get; private set; }

    /// <summary>
    /// The full path of the truncation-base sidecar: the highest LSN compaction has removed, so a
    /// truncated log recovers its head even when every surviving record was truncated away. Zero
    /// (or an absent file) means the log has never been truncated.
    /// </summary>
    public string BaseFilePath { get; }

    /// <summary>
    /// The highest LSN removed by truncation; records exist only above it. Recovery of anything at
    /// or below this LSN must come from a snapshot.
    /// </summary>
    public ulong BaseLsn
    {
        get
        {
            lock (_lock)
            {
                return _baseLsn;
            }
        }
    }

    /// <summary>
    /// Test-only fault injection, invoked after a record's bytes are written but before the flush —
    /// the window where a real disk-full failure lands.
    /// </summary>
    internal Action<FileStream>? AppendFaultInjection { get; set; }

    /// <summary>
    /// The failure that poisoned the log, or null while it is writable. A poisoned log rejects
    /// every append until restart — the unhealthy signal for the <c>melange-log</c> health check.
    /// </summary>
    internal Exception? Failure
    {
        get
        {
            lock (_lock)
            {
                return _failure;
            }
        }
    }

    /// <summary>
    /// Milliseconds spent in the fsync inside the most recent <see cref="Append"/>, or null when
    /// that append did not fsync in line — <see cref="FsyncPolicy.Interval"/> flushes on a timer
    /// thread and <see cref="FsyncPolicy.OsBuffered"/> never flushes explicitly, so under either
    /// there is no durability cost to charge to the appending transaction, and the honest answer is
    /// "none" rather than "zero".
    /// <para>
    /// Read it on the thread that just returned from the <see cref="Append"/> whose cost is wanted:
    /// appends are serialized by the engine's write lock, so that thread is the only one that can
    /// have written the value it reads.
    /// </para>
    /// </summary>
    internal double? LastAppendFsyncMilliseconds { get; private set; }

    /// <summary>Forces buffered appends to stable storage regardless of the fsync policy.</summary>
    public void FlushToDisk()
    {
        lock (_lock)
        {
            if (_disposed || _failure is not null)
                return;
            _stream.Flush(flushToDisk: true);
        }
    }

    public ulong HeadLsn
    {
        get
        {
            lock (_lock)
            {
                return _headLsn;
            }
        }
    }

    public CommitRecord Append(in CommitRequest request)
    {
        ArgumentException.ThrowIfNullOrEmpty(request.ReducerName);
        ArgumentNullException.ThrowIfNull(request.WriteSet);
        lock (_lock)
        {
            if (_failure is not null)
            {
                throw new InvalidOperationException(
                    "The commit log is in a failed state: a partially written record could not be " +
                    "rolled back, so further appends would corrupt the log. Restart the process; " +
                    "recovery truncates the torn tail. See the inner exception for the original failure.",
                    _failure);
            }

            var lsn = _headLsn + 1;

            // The payload buffer comes from the pool and goes back below. CRC and the write both
            // take a span, so nothing here needs it to be a right-sized array — which is what the
            // old ToArray copy was paying for.
            var length = LogRecordCodec.WritePayload(lsn, request, out var buffer);
            try
            {
                var payload = buffer.AsSpan(0, length);
                Span<byte> frame = stackalloc byte[FrameSize];
                BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)length);
                BinaryPrimitives.WriteUInt32LittleEndian(frame[4..], Crc32.Compute(payload));

                // The append is atomic or it is nothing: if the flush fails (disk full being the
                // realistic case), the written bytes are rolled back so a later append can neither
                // land after an orphaned record of an aborted transaction nor re-mint its LSN.
                var previousLength = _stream.Length;
                try
                {
                    _stream.Seek(0, SeekOrigin.End);
                    _stream.Write(frame);
                    _stream.Write(payload);
                    AppendFaultInjection?.Invoke(_stream);
                    LastAppendFsyncMilliseconds = Flush(onCommit: true);
                }
                catch
                {
                    RollBackPartialAppend(previousLength);
                    throw;
                }

                _headLsn = lsn;
            }
            finally
            {
                LogRecordCodec.Release(buffer);
            }

            return new CommitRecord
            {
                Lsn = lsn,
                FormatVersion = LogRecordCodec.RecordFormatVersion,
                Timestamp = request.Timestamp,
                Caller = request.Caller,
                ReducerName = request.ReducerName,
                Arguments = request.Arguments,
                WriteSet = request.WriteSet,
                Events = request.Events ?? [],
                SerializedLength = FrameSize + length,
            };
        }
    }

    public IEnumerable<CommitRecord> ReadFrom(ulong firstLsn)
    {
        lock (_lock)
        {
            _stream.Flush(); // Make buffered appends visible to the read handle.
        }

        using var reader = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        if (reader.Length < HeaderSize)
            yield break;
        reader.Seek(HeaderSize, SeekOrigin.Begin);
        var frame = new byte[FrameSize];
        while (reader.Position + FrameSize <= reader.Length)
        {
            reader.ReadExactly(frame);
            var length = BinaryPrimitives.ReadUInt32LittleEndian(frame);
            var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(4));
            if (length == 0 || length > MaxRecordBytes || reader.Position + length > reader.Length)
                yield break; // Torn tail; Recover() already truncated the durable file.
            var payload = new byte[length];
            reader.ReadExactly(payload);
            if (Crc32.Compute(payload) != expectedCrc)
                yield break;
            var record = LogRecordCodec.ReadPayload(payload, (int)(FrameSize + length));
            if (record.Lsn >= firstLsn)
                yield return record;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;
            _disposed = true;
            _flushTimer?.Dispose();
            _flushTimer = null;
            if (_failure is null)
                _stream.Flush(flushToDisk: true);
            _stream.Dispose();
        }
    }

    private void RollBackPartialAppend(long previousLength)
    {
        try
        {
            _stream.SetLength(previousLength);
            _stream.Flush(flushToDisk: true);
        }
        catch (Exception rollbackFailure)
        {
            // The partial record could not be removed; appending after it would risk making an
            // aborted transaction's record durable. Poison the log so every subsequent append
            // fails loudly; recovery at next open truncates the torn tail.
            _failure = rollbackFailure;
            LogMessages.AppendRollbackFailed(_logger, FilePath, rollbackFailure);
        }
    }

    private void Recover()
    {
        if (_stream.Length < HeaderSize)
        {
            _stream.SetLength(0);
            Span<byte> header = stackalloc byte[HeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
            BinaryPrimitives.WriteUInt16LittleEndian(header[4..], FileFormatVersion);
            BinaryPrimitives.WriteUInt16LittleEndian(header[6..], 0);
            _stream.Write(header);
            _stream.Flush(flushToDisk: true);

            // A fresh log file is a fresh incarnation: any epoch or truncation base left behind by
            // a deleted log must not survive it, or an old resume cursor would count against the
            // wrong history.
            EpochId = MintEpoch();
            if (File.Exists(BaseFilePath))
                File.Delete(BaseFilePath);
            _baseLsn = 0;
            return;
        }

        EpochId = ReadOrMintEpoch();
        _baseLsn = ReadBaseLsn();
        _headLsn = _baseLsn;

        Span<byte> fileHeader = stackalloc byte[HeaderSize];
        _stream.Seek(0, SeekOrigin.Begin);
        _stream.ReadExactly(fileHeader);
        if (BinaryPrimitives.ReadUInt32LittleEndian(fileHeader) != Magic)
            throw new InvalidDataException($"'{FilePath}' is not a MelangeDB commit log.");
        var version = BinaryPrimitives.ReadUInt16LittleEndian(fileHeader[4..]);
        if (version != FileFormatVersion)
            throw new InvalidDataException($"'{FilePath}' has log format version {version}; this build reads version {FileFormatVersion}.");

        long intactEnd = HeaderSize;
        Span<byte> frame = stackalloc byte[FrameSize];
        var length = _stream.Length;
        while (true)
        {
            var position = intactEnd;
            if (position == length)
                break;
            if (position + FrameSize > length)
            {
                TruncateTorn(position, "torn frame header");
                break;
            }

            _stream.Seek(position, SeekOrigin.Begin);
            _stream.ReadExactly(frame);
            var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(frame);
            var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(frame[4..]);
            if (payloadLength == 0)
            {
                // A record payload is never empty, and a zero-filled torn tail would otherwise
                // pass the CRC check: CRC32 of zero bytes is zero, exactly what the zeroed frame
                // declares.
                TruncateTorn(position, "zero-length record in torn tail");
                break;
            }

            if (payloadLength > MaxRecordBytes || position + FrameSize + payloadLength > length)
            {
                TruncateTorn(position, "record extends past end of file");
                break;
            }

            var payload = new byte[payloadLength];
            _stream.ReadExactly(payload);
            if (Crc32.Compute(payload) != expectedCrc)
            {
                if (position + FrameSize + payloadLength == length)
                {
                    TruncateTorn(position, "CRC mismatch on trailing record");
                    break;
                }

                throw new InvalidDataException(
                    $"'{FilePath}': CRC mismatch at offset {position} with intact records after it. " +
                    "The log is corrupt beyond a torn tail; restore from backup.");
            }

            var record = LogRecordCodec.ReadPayload(payload, (int)(FrameSize + payloadLength));

            // Max, not assignment: a crash between writing the base sidecar and swapping the
            // compacted file can leave already-truncated records on disk below the base.
            _headLsn = Math.Max(_headLsn, record.Lsn);
            intactEnd = position + FrameSize + payloadLength;
        }
    }

    /// <summary>
    /// Removes every record at or below <paramref name="upToLsn"/> — log compaction's physical
    /// half. The caller (the engine's snapshot path) is responsible for the floors: a snapshot
    /// must cover the removed range, and no applier, live event subscriber, or resume-retention
    /// window may still need it. Atomic against a crash: the compacted file is fully written and
    /// flushed before it replaces the live one, and the base sidecar is written first, so a crash
    /// at any point leaves either the old log or a consistent truncated one.
    /// </summary>
    internal void TruncateBefore(ulong upToLsn)
    {
        lock (_lock)
        {
            if (_disposed || _failure is not null)
                return;
            upToLsn = Math.Min(upToLsn, _headLsn);
            if (upToLsn <= _baseLsn)
                return;

            _stream.Flush();
            var tempPath = FilePath + ".compact";
            using (var compact = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                Span<byte> header = stackalloc byte[HeaderSize];
                BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
                BinaryPrimitives.WriteUInt16LittleEndian(header[4..], FileFormatVersion);
                BinaryPrimitives.WriteUInt16LittleEndian(header[6..], 0);
                compact.Write(header);

                var frame = new byte[FrameSize];
                _stream.Seek(HeaderSize, SeekOrigin.Begin);
                while (_stream.Position + FrameSize <= _stream.Length)
                {
                    _stream.ReadExactly(frame);
                    var length = BinaryPrimitives.ReadUInt32LittleEndian(frame);
                    if (length == 0 || length > MaxRecordBytes || _stream.Position + length > _stream.Length)
                        break;
                    var payload = new byte[length];
                    _stream.ReadExactly(payload);
                    var record = LogRecordCodec.ReadPayload(payload, (int)(FrameSize + length));
                    if (record.Lsn > upToLsn)
                    {
                        compact.Write(frame);
                        compact.Write(payload);
                    }
                }

                compact.Flush(flushToDisk: true);
            }

            WriteBaseLsn(upToLsn);
            _stream.Dispose();
            File.Move(tempPath, FilePath, overwrite: true);
            _stream = new FileStream(FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
            _stream.Seek(0, SeekOrigin.End);
            _baseLsn = upToLsn;
        }
    }

    private ulong ReadBaseLsn()
    {
        if (!File.Exists(BaseFilePath))
            return 0;
        var bytes = File.ReadAllBytes(BaseFilePath);
        return bytes.Length == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(bytes) : 0;
    }

    private void WriteBaseLsn(ulong baseLsn)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, baseLsn);
        var tempPath = BaseFilePath + ".tmp";
        File.WriteAllBytes(tempPath, bytes);
        File.Move(tempPath, BaseFilePath, overwrite: true);
    }

    private Guid MintEpoch()
    {
        var epoch = Guid.NewGuid();
        File.WriteAllBytes(EpochFilePath, epoch.ToByteArray());
        return epoch;
    }

    private Guid ReadOrMintEpoch()
    {
        if (File.Exists(EpochFilePath))
        {
            var bytes = File.ReadAllBytes(EpochFilePath);
            if (bytes.Length == 16)
                return new Guid(bytes);
        }

        return MintEpoch();
    }

    private void TruncateTorn(long intactEnd, string reason)
    {
        LogMessages.TornRecordTruncated(_logger, FilePath, _stream.Length - intactEnd, reason, _headLsn);
        _stream.SetLength(intactEnd);
        _stream.Flush(flushToDisk: true);
    }

    /// <summary>
    /// Flushes per the configured policy, returning the milliseconds spent in an in-line fsync or
    /// null when this policy does not fsync in line.
    /// </summary>
    private double? Flush(bool onCommit)
    {
        switch (_options.FsyncPolicy)
        {
            case FsyncPolicy.OnCommit:
                using (var fsync = _telemetry?.StartFsync())
                {
                    var started = Stopwatch.GetTimestamp();
                    _stream.Flush(flushToDisk: true);
                    var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                    _telemetry?.RecordFsyncDuration(elapsed);
                    return elapsed;
                }

            case FsyncPolicy.Interval:
                _stream.Flush();
                if (onCommit)
                    EnsureFlushTimer();
                return null;
            case FsyncPolicy.OsBuffered:
                _stream.Flush();
                return null;
            default:
                throw new InvalidOperationException($"Unknown fsync policy {_options.FsyncPolicy}.");
        }
    }

    private void EnsureFlushTimer()
    {
        var interval = TimeSpan.FromMilliseconds(_options.FsyncIntervalMs);
        _flushTimer ??= new Timer(_ => FlushTimerTick(), null, interval, interval);
    }

    private void FlushTimerTick()
    {
        lock (_lock)
        {
            if (_flushTimer is null)
                return;
            if (_options.FsyncPolicy != FsyncPolicy.Interval)
                return;
            var started = Stopwatch.GetTimestamp();
            _stream.Flush(flushToDisk: true);
            _telemetry?.RecordFsyncDuration(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
    }

    private static class LogMessages
    {
        private static readonly Action<ILogger, string, long, string, ulong, Exception?> TornRecordTruncatedMessage =
            LoggerMessage.Define<string, long, string, ulong>(
                LogLevel.Warning,
                new EventId(1001, "TornRecordTruncated"),
                "Commit log '{Path}': truncated {Bytes} torn trailing bytes ({Reason}); recovered to LSN {Lsn}.");

        public static void TornRecordTruncated(ILogger logger, string path, long bytes, string reason, ulong lsn) =>
            TornRecordTruncatedMessage(logger, path, bytes, reason, lsn, null);

        private static readonly Action<ILogger, string, Exception?> AppendRollbackFailedMessage =
            LoggerMessage.Define<string>(
                LogLevel.Critical,
                new EventId(1002, "AppendRollbackFailed"),
                "Commit log '{Path}': a failed append could not be rolled back; the log is now failed and rejects further appends until restart.");

        public static void AppendRollbackFailed(ILogger logger, string path, Exception failure) =>
            AppendRollbackFailedMessage(logger, path, failure);
    }
}
