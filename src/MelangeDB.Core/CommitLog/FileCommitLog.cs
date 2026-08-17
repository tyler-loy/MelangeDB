using System.Buffers.Binary;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Win32.SafeHandles;

namespace MelangeDB.Core;

/// <summary>
/// The append-only local commit log: one CRC-guarded, version-tagged record per transaction. On
/// open it scans the file, and a torn tail — everything from the first invalid record to the end,
/// which after a crash may span several records and even hold intact-looking ones, because the OS
/// persists buffered pages in no particular order — is truncated to the last intact LSN rather
/// than being fatal. Corruption of a record the durable floor proves was fsynced is fatal, because
/// it means committed history was damaged. The fsync policy is read per operation, so a changed
/// options value takes effect on the next commit.
/// <para>
/// Under <see cref="FsyncPolicy.OnCommit"/> the commit is split in two — the group-commit design:
/// <see cref="Append"/> writes the record buffered and returns, and <see cref="WaitDurable"/>
/// completes durability, with whoever finds the flusher idle fsyncing everything buffered so far.
/// Batches form from contention itself: a lone caller fsyncs immediately and pays exactly the old
/// inline latency, while concurrent callers park behind the in-flight fsync and are covered
/// together by the next one. A committed transaction is durable when <em>its</em>
/// <see cref="WaitDurable"/> returns — which is why the engine calls it before any commit returns
/// to its caller.
/// </para>
/// </summary>
public sealed class FileCommitLog : ICommitLog
{
    // Shared with the backup archive's read-only walker and restore's materializer, which speak
    // this file format without constructing a FileCommitLog — construction runs recovery, and
    // recovery mutates the file (mints epochs, truncates torn tails), which a backup must never do.
    internal const uint Magic = 0x474F4C4Du; // "MLOG"
    internal const ushort FileFormatVersion = 1;
    internal const int HeaderSize = 8;
    internal const int FrameSize = 8; // u32 payload length + u32 crc
    internal const uint MaxRecordBytes = 256 * 1024 * 1024;

    /// <summary>
    /// The liveness-lock sidecar's file name. Windows enforces <see cref="FileShare"/> natively,
    /// but Unix maps only <see cref="FileShare.None"/> onto an advisory lock — the log's own
    /// Read|Delete handle excludes nothing there. This empty sidecar, held exclusively for the
    /// log's lifetime, is the cross-platform "this directory is live" signal: a second open of the
    /// same directory refuses here, and the offline backup probes the same file so that capturing
    /// a live directory refuses instead of producing a torn archive.
    /// </summary>
    internal const string LockFileName = "melange.lock";

    /// <summary>The epoch sidecar's file name; see <see cref="EpochFilePath"/>.</summary>
    internal const string EpochFileName = "melange.epoch";

    private readonly CommitLogOptions _options;
    private readonly ILogger _logger;
    private readonly EngineTelemetry? _telemetry;
    private readonly ulong _durableFloor;
    private FileStream _stream;

    // The group flusher's own handle to the same file. Measured, not theoretical: on Windows a
    // FlushFileBuffers serializes against writes on the same handle, so fsyncing through the
    // append handle stalls the next append — which holds the engine's write lock — behind the
    // in-flight flush, and batches never form (mean write latency 2.4 ms against 0.1 ms through
    // a second handle, probed on the dev box). The OS flushes the file's dirty pages whichever
    // handle asks, so durability is identical; only the blocking differs.
    private SafeFileHandle _flushHandle;
    private readonly FileStream _lockFile;
    private readonly Lock _lock = new();

    // Serializes fsyncs against each other and against every swap or disposal of _stream, so the
    // group flusher can fsync through the file handle while _lock is free for appends — which is
    // the entire point: the fsync must not be what the next append waits behind. Lock order where
    // both are taken is always _fsyncLock before _lock; nothing holding either takes _flushGate
    // first except FlushBatch's watermark read, and nothing holding _flushGate takes _fsyncLock,
    // so no cycle exists.
    private readonly Lock _fsyncLock = new();

    // Durability waiters park here; guards the durable watermark and the flusher election.
    private readonly object _flushGate = new();
    private ulong _durableLsn;
    private long _durableLength;
    private bool _flusherActive;
    private long _fsyncCount;

    private Timer? _flushTimer;
    private ulong _headLsn;
    private ulong _baseLsn;
    private Exception? _failure;
    private bool _disposed;

    public FileCommitLog(CommitLogOptions options, ILogger<FileCommitLog>? logger = null)
        : this(options, (ILogger?)logger, null)
    {
    }

    internal FileCommitLog(CommitLogOptions options, ILogger? logger, EngineTelemetry? telemetry, ulong durableFloor = 0)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _logger = logger ?? NullLogger.Instance;
        _telemetry = telemetry;
        _durableFloor = durableFloor;
        Directory.CreateDirectory(options.Path);
        FilePath = System.IO.Path.Combine(options.Path, "melange.log");
        EpochFilePath = System.IO.Path.Combine(options.Path, EpochFileName);
        BaseFilePath = System.IO.Path.Combine(options.Path, "melange.base");
        try
        {
            _lockFile = new FileStream(
                System.IO.Path.Combine(options.Path, LockFileName),
                FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException(
                $"'{options.Path}' is already open — by a live server or an in-progress backup. " +
                "Two processes must never share a data directory; stop the other one and retry.",
                exception);
        }

        try
        {
            // FileShare.Delete throughout: log compaction atomically replaces the file, and every
            // open handle must permit that or a concurrent lazy reader would make truncation fail.
            // Write sharing exists solely for the flush handle below — the melange.lock sidecar is
            // what refuses a second writer, and has been since it was introduced; the share mode
            // never guarded against non-melange writers anyway.
            _stream = new FileStream(FilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
            Recover();
            _flushHandle = OpenFlushHandle();

            // Everything that survived recovery is durable: the file's intact prefix is what the
            // disk actually held, and recovery fsyncs its own truncations.
            _durableLsn = _headLsn;
            _durableLength = _stream.Length;
        }
        catch
        {
            _flushHandle?.Dispose();
            _stream?.Dispose();
            _lockFile.Dispose();
            throw;
        }
    }

    private SafeFileHandle OpenFlushHandle() =>
        File.OpenHandle(FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);

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
    /// The newest LSN the configured fsync policy promises will survive a crash. Under
    /// <see cref="FsyncPolicy.OnCommit"/> this is the fsynced watermark the group flusher advances;
    /// under <see cref="FsyncPolicy.Interval"/> and <see cref="FsyncPolicy.OsBuffered"/> it is
    /// <see cref="HeadLsn"/>, because those policies promise nothing beyond the OS's own writeback
    /// and gating a reader on a promise never made would only add the timer's latency. This is the
    /// ceiling anything that leaves the process — a subscription delta, a replica stream, a
    /// relational-tier apply — must stay under: an LSN served beyond it could be untold by a crash.
    /// </summary>
    public ulong DurableLsn => _options.FsyncPolicy == FsyncPolicy.OnCommit ? FsyncedLsn : HeadLsn;

    /// <summary>
    /// The raw fsynced watermark, whatever the policy — what the snapshot path checks before it
    /// writes a file whose LSN would otherwise be a durability claim the log has not yet made.
    /// </summary>
    internal ulong FsyncedLsn
    {
        get
        {
            lock (_flushGate)
            {
                return _durableLsn;
            }
        }
    }

    /// <summary>Fsyncs performed over this log's lifetime — the group-commit tests' observable.</summary>
    internal long FsyncCount => Interlocked.Read(ref _fsyncCount);

    /// <summary>
    /// The log file's current size on disk. Records are the unit truncation reasons in, but bytes
    /// are the unit the operator fears, so every truncation decision reports both.
    /// </summary>
    internal long FileLengthBytes
    {
        get
        {
            lock (_lock)
            {
                return _stream.Length;
            }
        }
    }

    /// <summary>
    /// The egress gate, without the engine's cost attribution: same wait, discarded measurement.
    /// Only meaningful under <see cref="FsyncPolicy.OnCommit"/> — the internal overload documents
    /// why the other policies return immediately.
    /// </summary>
    void ICommitLog.WaitDurable(ulong lsn) => WaitDurable(lsn);

    /// <summary>
    /// Test-only fault injection, invoked after a record's bytes are written but before the flush —
    /// the window where a real disk-full failure lands.
    /// </summary>
    internal Action<FileStream>? AppendFaultInjection { get; set; }

    /// <summary>
    /// Test-only fault injection, invoked by the group flusher immediately before the fsync — the
    /// window where a batch-wide durability failure lands, and (as a blocking delegate) the hook
    /// that holds a flush hostage so a test can force a batch to form behind it.
    /// </summary>
    internal Action? FlushFaultInjection { get; set; }

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

    /// <summary>Forces buffered appends to stable storage regardless of the fsync policy.</summary>
    public void FlushToDisk()
    {
        ulong head;
        long length;
        lock (_fsyncLock)
        {
            lock (_lock)
            {
                if (_disposed || _failure is not null)
                    return;
                _stream.Flush(flushToDisk: true);
                head = _headLsn;
                length = _stream.Length;
            }
        }

        AdvanceDurable(head, length);
    }

    /// <summary>
    /// Applies a live change to <c>CommitLog:FsyncPolicy</c> / <c>CommitLog:GroupCommit</c>, which
    /// is a <b>durability boundary</b>: everything appended under the outgoing policy is made
    /// durable before the new one takes effect, so a policy that answers
    /// <see cref="DurableLsn"/> with the head can never do so over a buffered record.
    /// <para>
    /// The new policy is published under both locks, so no append can land between the flush and
    /// the change and be covered by an answer it was not written under.
    /// </para>
    /// </summary>
    internal void ApplyDurabilityPolicy(FsyncPolicy policy, bool groupCommit)
    {
        lock (_fsyncLock)
        {
            ulong head;
            long length;
            lock (_lock)
            {
                if (_options.FsyncPolicy == policy && _options.GroupCommit == groupCommit)
                    return;
                if (_disposed || _failure is not null)
                {
                    // A poisoned or closed log promises nothing and accepts no appends; the policy
                    // is bookkeeping at that point, and forcing a flush would only throw over it.
                    _options.FsyncPolicy = policy;
                    _options.GroupCommit = groupCommit;
                    return;
                }

                _stream.Flush(flushToDisk: true);
                Interlocked.Increment(ref _fsyncCount);
                head = _headLsn;
                length = _stream.Length;
                _options.FsyncPolicy = policy;
                _options.GroupCommit = groupCommit;
            }

            AdvanceDurable(head, length);
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

    /// <summary>
    /// Appends one committed transaction and assigns the next LSN. Under
    /// <see cref="FsyncPolicy.OnCommit"/> the record is buffered to the OS and durability is
    /// completed by <see cref="WaitDurable"/> — the group-commit split; a caller for whom the
    /// commit point matters must call it before acting on the returned record.
    /// </summary>
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

                // The append is atomic or it is nothing: if the write fails (disk full being the
                // realistic case), the written bytes are rolled back so a later append can neither
                // land after an orphaned record of an aborted transaction nor re-mint its LSN.
                var previousLength = _stream.Length;
                try
                {
                    _stream.Seek(0, SeekOrigin.End);
                    _stream.Write(frame);
                    _stream.Write(payload);
                    AppendFaultInjection?.Invoke(_stream);

                    // Buffer to the OS unconditionally: a lazy reader must see every appended
                    // byte, and the group flusher's fsync covers only what the stream has handed
                    // over. Durability itself is the policy's business — OnCommit completes it in
                    // WaitDurable, Interval on the timer, OsBuffered never.
                    _stream.Flush();
                    if (_options.FsyncPolicy == FsyncPolicy.Interval)
                    {
                        EnsureFlushTimer();
                    }
                    else if (_options.FsyncPolicy == FsyncPolicy.OnCommit && !_options.GroupCommit)
                    {
                        // CommitLog:GroupCommit = false restores the phase-01 inline fsync:
                        // durability completes here, under the append lock, and WaitDurable finds
                        // the watermark already advanced. A failure takes the rollback path below —
                        // the single-append contract, exactly as before the split.
                        //
                        // This advances the watermark without holding _fsyncLock, which is safe
                        // only because no group flush can exist while GroupCommit is false: every
                        // append advances the watermark itself, so no waiter ever parks and none
                        // becomes a flusher. ApplyDurabilityPolicy is what makes the transition
                        // into and out of that state hold the invariant, by flushing under both
                        // locks before the flag changes.
                        using var fsync = _telemetry?.StartFsync();
                        var fsyncStarted = Stopwatch.GetTimestamp();
                        _stream.Flush(flushToDisk: true);
                        _telemetry?.RecordFsyncDuration(Stopwatch.GetElapsedTime(fsyncStarted).TotalMilliseconds);
                        Interlocked.Increment(ref _fsyncCount);
                        AdvanceDurable(lsn, _stream.Length);
                    }
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

    /// <summary>
    /// Blocks until the record at <paramref name="lsn"/> is durable, returning the milliseconds
    /// this caller actually waited — the honest per-commit durability cost under a shared fsync —
    /// or null immediately when the policy makes no on-commit promise
    /// (<see cref="FsyncPolicy.Interval"/> flushes on its timer, <see cref="FsyncPolicy.OsBuffered"/>
    /// never explicitly), so under either there is no durability cost to charge and the honest
    /// answer is "none" rather than "zero".
    /// <para>
    /// Sync piggybacking: if no flush is in flight the caller performs one itself, covering
    /// everything buffered so far; otherwise it parks, and the flush that just finished elects the
    /// next flusher from whoever is still uncovered. A failed fsync fails every commit in the
    /// covered range — each waiter throws, with the original failure inner — and poisons the log;
    /// see <see cref="FlushBatch"/>.
    /// </para>
    /// </summary>
    internal double? WaitDurable(ulong lsn)
    {
        if (_options.FsyncPolicy != FsyncPolicy.OnCommit)
            return null;

        // A wait for an unwritten LSN would elect no-op flushers forever; only appended records
        // have a durability to wait for.
        ArgumentOutOfRangeException.ThrowIfGreaterThan(lsn, HeadLsn);
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var flush = false;
            lock (_flushGate)
            {
                if (_durableLsn >= lsn)
                    return Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                if (_flusherActive)
                    Monitor.Wait(_flushGate);
                else
                {
                    _flusherActive = true;
                    flush = true;
                }
            }

            if (flush)
                FlushBatch();

            // Woken (or just flushed): either the watermark now covers this record and the next
            // pass returns, or the batch failed and this throw is how every covered commit fails.
            ThrowIfUnwritable();
        }
    }

    /// <summary>
    /// One group flush: capture the buffered head under the append lock, fsync through the file
    /// handle with the append lock <em>free</em> — appends proceeding during the fsync are what
    /// forms the next batch — then advance the watermark and wake every parked waiter. Never
    /// throws: a failure poisons the log (batch-wide, with the file rolled back to the last
    /// durable length) and the waiters' own <see cref="ThrowIfUnwritable"/> checks deliver it.
    /// </summary>
    private void FlushBatch()
    {
        ulong target = 0;
        long targetLength = 0;
        ulong covered = 0;
        var flushed = false;
        double elapsed = 0;
        try
        {
            lock (_fsyncLock)
            {
                SafeFileHandle? handle = null;
                lock (_lock)
                {
                    if (!_disposed && _failure is null)
                    {
                        target = _headLsn;
                        targetLength = _stream.Length;
                        handle = _flushHandle;
                    }
                }

                if (handle is not null)
                {
                    ulong durableBefore;
                    lock (_flushGate)
                    {
                        durableBefore = _durableLsn;
                    }

                    if (target > durableBefore)
                    {
                        using var fsync = _telemetry?.StartFsync();
                        var fsyncStarted = Stopwatch.GetTimestamp();
                        FlushFaultInjection?.Invoke();
                        RandomAccess.FlushToDisk(handle);
                        elapsed = Stopwatch.GetElapsedTime(fsyncStarted).TotalMilliseconds;
                        covered = target - durableBefore;
                        flushed = true;
                    }
                }
            }
        }
        catch (Exception failure)
        {
            PoisonBatch(failure);
            lock (_flushGate)
            {
                _flusherActive = false;
                Monitor.PulseAll(_flushGate);
            }

            return;
        }

        if (flushed)
        {
            Interlocked.Increment(ref _fsyncCount);
            _telemetry?.RecordFsyncDuration(elapsed);
            _telemetry?.RecordGroupCommitBatch((long)covered);
        }

        lock (_flushGate)
        {
            if (flushed && target > _durableLsn)
            {
                _durableLsn = target;
                _durableLength = targetLength;
            }

            _flusherActive = false;
            Monitor.PulseAll(_flushGate);
        }
    }

    /// <summary>
    /// A group fsync failed, so every record above the durable watermark is a commit that will be
    /// reported failed — the bytes must not outlive the report. The file is rolled back to the
    /// last durable length (were they left in place, the OS could persist them later and a
    /// "failed" transaction would materialize at the next boot), and the log poisons so no append
    /// can land after the rollback point until restart.
    /// </summary>
    private void PoisonBatch(Exception failure)
    {
        long durableLength;
        lock (_flushGate)
        {
            durableLength = _durableLength;
        }

        lock (_lock)
        {
            if (_disposed || _failure is not null)
                return;
            try
            {
                _stream.SetLength(durableLength);
                _stream.Flush(flushToDisk: true);
            }
            catch
            {
                // The disk is failing; recovery truncates whatever survives at the next open.
            }

            _failure = failure;
            LogMessages.GroupFlushFailed(_logger, FilePath, failure);
        }
    }

    private void ThrowIfUnwritable()
    {
        lock (_lock)
        {
            if (_failure is not null)
            {
                throw new InvalidOperationException(
                    "The commit log could not make appended records durable: a batch fsync failed, " +
                    "so every commit it covered has been failed and rolled back, and the log rejects " +
                    "further appends until restart. See the inner exception for the original failure.",
                    _failure);
            }

            ObjectDisposedException.ThrowIf(_disposed, this);
        }
    }

    /// <summary>Advances the durable watermark (never backwards) and wakes parked waiters.</summary>
    private void AdvanceDurable(ulong lsn, long length)
    {
        lock (_flushGate)
        {
            if (lsn > _durableLsn)
            {
                _durableLsn = lsn;
                _durableLength = length;
            }

            Monitor.PulseAll(_flushGate);
        }
    }

    /// <summary>
    /// Makes buffered appends visible to a reader opening the file directly — what
    /// <see cref="ReadFrom"/> does for its own read handle, exposed for the online backup, which
    /// walks the file bytes itself because it wants verbatim record payloads, not decoded records.
    /// </summary>
    internal void FlushBuffers()
    {
        lock (_lock)
        {
            if (!_disposed && _failure is null)
                _stream.Flush();
        }
    }

    /// <summary>
    /// Reads records in LSN order from <paramref name="firstLsn"/>, up to <see cref="DurableLsn"/>:
    /// a record beyond the policy's durability promise is never served, because every consumer of
    /// this enumeration forwards records somewhere a crash cannot untell them — a lagging applier's
    /// projection, a resume replay, a replica stream. The cap is re-read per record, so an
    /// enumeration racing the flusher simply stops at the promise as of that moment; the next
    /// catch-up pass sees further.
    /// </summary>
    public IEnumerable<CommitRecord> ReadFrom(ulong firstLsn) => Read(firstLsn, capAtDurable: true);

    /// <summary>
    /// Reads records with no durability cap — for in-process projections only, which the applier
    /// pipeline drives under the write lock: a projection rebuilt from the log at boot loses an
    /// un-durable record together with the log, so applying it early is crash-consistent, and
    /// capping here would starve a catching-up applier of the record whose commit is still parked
    /// in <see cref="WaitDurable"/>. Anything whose effects leave the process must use
    /// <see cref="ReadFrom"/>.
    /// </summary>
    internal IEnumerable<CommitRecord> ReadUncapped(ulong firstLsn) => Read(firstLsn, capAtDurable: false);

    private IEnumerable<CommitRecord> Read(ulong firstLsn, bool capAtDurable)
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
            if (capAtDurable && record.Lsn > DurableLsn)
                yield break;
            if (record.Lsn >= firstLsn)
                yield return record;
        }
    }

    public void Dispose()
    {
        ulong head = 0;
        long length = 0;
        var flushed = false;
        lock (_fsyncLock)
        {
            lock (_lock)
            {
                if (_disposed)
                    return;
                _disposed = true;
                _flushTimer?.Dispose();
                _flushTimer = null;
                if (_failure is null)
                {
                    _stream.Flush(flushToDisk: true);
                    head = _headLsn;
                    length = _stream.Length;
                    flushed = true;
                }

                _flushHandle.Dispose();
                _stream.Dispose();
                _lockFile.Dispose();
            }
        }

        if (flushed)
        {
            AdvanceDurable(head, length);
        }
        else
        {
            // A poisoned close still wakes parked waiters, whose unwritable check fails them.
            lock (_flushGate)
            {
                Monitor.PulseAll(_flushGate);
            }
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
            // fails loudly; recovery at next open truncates the torn tail. Parked durability
            // waiters need no wake here: waiters only park behind an active flusher, and its
            // completion wakes them into the unwritable check.
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
                TruncateTornOrThrow(position, "torn frame header");
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
                TruncateTornOrThrow(position, "zero-length record in torn tail");
                break;
            }

            if (payloadLength > MaxRecordBytes || position + FrameSize + payloadLength > length)
            {
                TruncateTornOrThrow(position, "record extends past end of file");
                break;
            }

            var payload = new byte[payloadLength];
            _stream.ReadExactly(payload);
            if (Crc32.Compute(payload) != expectedCrc)
            {
                TruncateTornOrThrow(position, position + FrameSize + payloadLength == length
                    ? "CRC mismatch on trailing record"
                    : "CRC mismatch inside the unflushed tail");
                break;
            }

            var record = LogRecordCodec.ReadPayload(payload, (int)(FrameSize + payloadLength));

            // Max, not assignment: a crash between writing the base sidecar and swapping the
            // compacted file can leave already-truncated records on disk below the base.
            _headLsn = Math.Max(_headLsn, record.Lsn);
            intactEnd = position + FrameSize + payloadLength;
        }
    }

    /// <summary>
    /// Decides what an invalid record means. Records are sequential, so the record at the tear —
    /// were it intact — is the one after the last intact LSN. If the durable floor (the newest
    /// snapshot's LSN, and a snapshot forces the log durable through its LSN before the file is
    /// written) covers that LSN, the record provably survived an fsync and this is damaged
    /// committed history: fatal, restore from backup. Above the floor it is a torn tail — under
    /// buffered group commit a crash's tail can span several records, and the OS's writeback order
    /// means intact-looking records can sit <em>beyond</em> the tear; every one of them belongs to
    /// a commit whose caller never got an acknowledgment, so truncating them all is the correct
    /// (and loudly logged) recovery. The residual risk is deliberate: damage to a not-yet-floored
    /// record is indistinguishable from a tear and truncates with it — the floor lags by at most
    /// one snapshot interval, and tightening it further would mean fsyncing a watermark on every
    /// flush, which is the cost group commit exists to remove.
    /// </summary>
    private void TruncateTornOrThrow(long position, string reason)
    {
        var tornLsn = Math.Max(_headLsn, _baseLsn) + 1;
        if (tornLsn <= _durableFloor)
        {
            throw new InvalidDataException(
                $"'{FilePath}': {reason} at offset {position}, but LSN {tornLsn} was durable — the " +
                $"snapshot at LSN {_durableFloor} proves it. The log is corrupt beyond a torn tail; " +
                "restore from backup.");
        }

        TruncateTorn(position, reason);
    }

    /// <summary>
    /// Removes every record at or below <paramref name="upToLsn"/> — log compaction's physical
    /// half. The caller (the engine's snapshot path) is responsible for the floors: a snapshot
    /// must cover the removed range, and no applier, live event subscriber, or resume-retention
    /// window may still need it. Atomic against a crash: the compacted file is fully written and
    /// flushed before it replaces the live one, and the base sidecar is written first, so a crash
    /// at any point leaves either the old log or a consistent truncated one. The rewrite is also a
    /// full fsync of every surviving record, so the durable watermark advances to the head behind
    /// it — a compaction can complete parked durability waiters.
    /// </summary>
    internal void TruncateBefore(ulong upToLsn)
    {
        ulong head;
        long newLength;
        lock (_fsyncLock)
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
                _flushHandle.Dispose();
                _stream.Dispose();
                File.Move(tempPath, FilePath, overwrite: true);
                _stream = new FileStream(FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                _stream.Seek(0, SeekOrigin.End);
                _flushHandle = OpenFlushHandle();
                _baseLsn = upToLsn;
                head = _headLsn;
                newLength = _stream.Length;
            }
        }

        AdvanceDurable(head, newLength);
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

    private void EnsureFlushTimer()
    {
        var interval = TimeSpan.FromMilliseconds(_options.FsyncIntervalMs);
        _flushTimer ??= new Timer(_ => FlushTimerTick(), null, interval, interval);
    }

    private void FlushTimerTick()
    {
        ulong head;
        long length;
        double elapsed;
        lock (_fsyncLock)
        {
            lock (_lock)
            {
                if (_flushTimer is null || _disposed || _failure is not null)
                    return;
                if (_options.FsyncPolicy != FsyncPolicy.Interval)
                    return;
                var started = Stopwatch.GetTimestamp();
                _stream.Flush(flushToDisk: true);
                elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                head = _headLsn;
                length = _stream.Length;
            }
        }

        _telemetry?.RecordFsyncDuration(elapsed);
        AdvanceDurable(head, length);
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

        private static readonly Action<ILogger, string, Exception?> GroupFlushFailedMessage =
            LoggerMessage.Define<string>(
                LogLevel.Critical,
                new EventId(1007, "GroupFlushFailed"),
                "Commit log '{Path}': a group fsync failed; every commit it covered is failed and rolled back, and the log rejects further appends until restart.");

        public static void GroupFlushFailed(ILogger logger, string path, Exception failure) =>
            GroupFlushFailedMessage(logger, path, failure);
    }
}
