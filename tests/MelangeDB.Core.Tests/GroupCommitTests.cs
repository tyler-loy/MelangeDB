using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The group-commit split (road-to-0.2 phase 17): <see cref="FileCommitLog.Append"/> buffers,
/// <see cref="FileCommitLog.WaitDurable"/> completes durability, and whoever finds the flusher
/// idle fsyncs everything buffered so far — batches form from contention itself. The blocking
/// <see cref="FileCommitLog.FlushFaultInjection"/> hook is how these tests hold a flush hostage
/// so a batch forms behind it deterministically.
/// </summary>
public class GroupCommitTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-group-commit-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private CommitLogOptions OptionsFor(FsyncPolicy policy = FsyncPolicy.OnCommit) =>
        new() { Path = Path.Combine(_root, "log"), FsyncPolicy = policy };

    [Fact]
    public void A_lone_caller_fsyncs_immediately_and_pays_exactly_one_flush()
    {
        using var log = new FileCommitLog(OptionsFor());
        var record = log.Append(MakeRequest("Only"));
        Assert.Equal(0UL, log.DurableLsn); // Buffered: the append alone promises nothing.

        var waited = log.WaitDurable(record.Lsn);

        Assert.NotNull(waited);
        Assert.Equal(1L, log.FsyncCount);
        Assert.Equal(1UL, log.DurableLsn);
    }

    [Fact]
    public async Task Commits_arriving_behind_an_in_flight_flush_share_the_next_fsync()
    {
        var ct = TestContext.Current.CancellationToken;
        using var log = new FileCommitLog(OptionsFor());
        log.Append(MakeRequest("First"));
        log.WaitDurable(1);
        Assert.Equal(1L, log.FsyncCount);

        // Hold the second flush hostage: the waiter for record 2 elects itself flusher, captures
        // its target, and blocks inside the injection with the append lock free.
        using var flushEntered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        log.FlushFaultInjection = () =>
        {
            flushEntered.Set();
            Assert.True(release.Wait(TimeSpan.FromSeconds(30), ct), "the test released the hostage flush");
        };

        log.Append(MakeRequest("Second"));
        var hostage = Task.Run(() => log.WaitDurable(2), ct);
        Assert.True(flushEntered.Wait(TimeSpan.FromSeconds(30), ct), "the hostage flush started");

        // Three commits land while the flush is in flight; their waiters park behind it. Each
        // waits for the LSN its own append was actually assigned — the appends race each other.
        using var appended = new CountdownEvent(3);
        var parked = Enumerable.Range(0, 3).Select(i => Task.Run(() =>
        {
            var lsn = log.Append(MakeRequest($"Parked{i}")).Lsn;
            appended.Signal();
            log.WaitDurable(lsn);
        }, ct)).ToArray();
        Assert.True(appended.Wait(TimeSpan.FromSeconds(30), ct), "the parked commits appended");

        release.Set();
        await Task.WhenAll([hostage, .. parked]).WaitAsync(TimeSpan.FromSeconds(30), ct);

        // Five records, three fsyncs: one for the lone first commit, the hostage flush covering
        // record 2, and one shared flush covering records 3-5 — the batch contention formed.
        Assert.Equal(3L, log.FsyncCount);
        Assert.Equal(5UL, log.DurableLsn);
        Assert.Equal(5UL, log.HeadLsn);
    }

    [Fact]
    public async Task A_failed_batch_fsync_fails_every_covered_commit_rolls_back_and_poisons()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = OptionsFor();
        using (var log = new FileCommitLog(options))
        {
            log.Append(MakeRequest("Durable"));
            log.WaitDurable(1);

            log.FlushFaultInjection = () => throw new IOException("injected: cache flush failed");
            using var appended = new CountdownEvent(3);
            var doomed = Enumerable.Range(0, 3).Select(i => Task.Run(() =>
            {
                var lsn = log.Append(MakeRequest($"Doomed{i}")).Lsn;
                appended.Signal();
                return Assert.Throws<InvalidOperationException>(() => log.WaitDurable(lsn));
            }, ct)).ToArray();
            Assert.True(appended.Wait(TimeSpan.FromSeconds(30), ct), "the doomed commits appended");
            var failures = await Task.WhenAll(doomed).WaitAsync(TimeSpan.FromSeconds(30), ct);

            // One failure answers for the whole range, with the original fault attached to each.
            Assert.All(failures, failure => Assert.IsType<IOException>(failure.InnerException));
            Assert.NotNull(log.Failure);

            var poisoned = Assert.Throws<InvalidOperationException>(() => log.Append(MakeRequest("After")));
            Assert.Contains("failed state", poisoned.Message);
        }

        // The rollback removed every record above the durable watermark — were the bytes left in
        // place, the OS could persist them later and a commit reported failed would materialize.
        using var reopened = new FileCommitLog(options);
        Assert.Equal(1UL, reopened.HeadLsn);
        Assert.Equal(new[] { "Durable" }, reopened.ReadFrom(1).Select(r => r.ReducerName));
    }

    [Fact]
    public async Task ReadFrom_serves_nothing_beyond_the_durable_watermark()
    {
        var ct = TestContext.Current.CancellationToken;
        using var log = new FileCommitLog(OptionsFor());
        log.Append(MakeRequest("First"));
        log.WaitDurable(1);

        using var flushEntered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        log.FlushFaultInjection = () =>
        {
            flushEntered.Set();
            Assert.True(release.Wait(TimeSpan.FromSeconds(30), ct), "the test released the hostage flush");
        };
        log.Append(MakeRequest("Buffered"));
        var waiter = Task.Run(() => log.WaitDurable(2), ct);
        Assert.True(flushEntered.Wait(TimeSpan.FromSeconds(30), ct), "the hostage flush started");

        // Record 2 is appended, readable in the file, and not durable: a reader must not get it —
        // every ReadFrom consumer forwards records somewhere a crash could not untell them.
        Assert.Equal(new ulong[] { 1 }, log.ReadFrom(1).Select(r => r.Lsn));

        release.Set();
        await waiter.WaitAsync(TimeSpan.FromSeconds(30), ct);
        Assert.Equal(new ulong[] { 1, 2 }, log.ReadFrom(1).Select(r => r.Lsn));
    }

    [Fact]
    public void Disabling_group_commit_restores_the_inline_fsync()
    {
        var options = OptionsFor();
        options.GroupCommit = false;
        using var log = new FileCommitLog(options);

        // The phase-01 contract: the append itself completes durability, under the locks.
        var record = log.Append(MakeRequest("Inline"));
        Assert.Equal(1UL, log.DurableLsn);
        Assert.Equal(1L, log.FsyncCount);

        // The wait still answers (the engine always calls it), finds the watermark already
        // advanced, and performs no second flush.
        Assert.NotNull(log.WaitDurable(record.Lsn));
        Assert.Equal(1L, log.FsyncCount);
    }

    [Fact]
    public void WaitDurable_reports_no_cost_under_policies_that_defer_durability()
    {
        foreach (var policy in new[] { FsyncPolicy.Interval, FsyncPolicy.OsBuffered })
        {
            using var log = new FileCommitLog(new CommitLogOptions
            {
                Path = Path.Combine(_root, $"log-{policy}"),
                FsyncPolicy = policy,
            });
            var record = log.Append(MakeRequest("Relaxed"));
            Assert.Null(log.WaitDurable(record.Lsn));
        }
    }

    [Fact]
    public void Recovery_truncates_a_multi_record_torn_tail_even_past_intact_looking_records()
    {
        var options = OptionsFor();
        using (var log = new FileCommitLog(options))
        {
            for (var i = 1; i <= 4; i++)
                log.Append(MakeRequest($"R{i}"));
            log.WaitDurable(4);
        }

        // A crash's writeback ordering can persist later pages while earlier ones are lost, so an
        // invalid record with intact-looking records beyond it is an expected crash artifact.
        // Every record past the tear belongs to a commit whose caller was never acknowledged.
        CorruptRecordPayload(Path.Combine(options.Path, "melange.log"), ordinal: 2);

        using var reopened = new FileCommitLog(options);
        Assert.Equal(1UL, reopened.HeadLsn);
        Assert.Equal(new[] { "R1" }, reopened.ReadFrom(1).Select(r => r.ReducerName));
    }

    [Fact]
    public void Recovery_refuses_a_tear_the_durable_floor_proves_was_fsynced()
    {
        var options = OptionsFor();
        using (var log = new FileCommitLog(options))
        {
            for (var i = 1; i <= 4; i++)
                log.Append(MakeRequest($"R{i}"));
            log.WaitDurable(4);
        }

        CorruptRecordPayload(Path.Combine(options.Path, "melange.log"), ordinal: 2);

        // The same file, but with a durable floor covering the damaged record: it provably
        // survived an fsync, so this is damaged committed history, never a torn tail.
        var fatal = Assert.Throws<InvalidDataException>(
            () => new FileCommitLog(options, null, null, durableFloor: 4));
        Assert.Contains("restore from backup", fatal.Message);
    }

    [Fact]
    public async Task Concurrent_engine_committers_share_fsyncs_and_every_acked_lsn_survives_restart()
    {
        var ct = TestContext.Current.CancellationToken;
        using var harness = new EngineHarness();
        const int Writers = 8;
        const int PerWriter = 25;
        var acked = new ulong[Writers * PerWriter];
        var tasks = Enumerable.Range(0, Writers).Select(writer => Task.Run(() =>
        {
            for (var i = 0; i < PerWriter; i++)
            {
                var chunk = writer * PerWriter + i;
                acked[chunk] = harness.Engine.Invoke(
                    "Carve",
                    EngineHarness.Caller,
                    ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = chunk, Data = [(byte)writer], Kind = ChunkKind.Rock }));
            }
        }, ct)).ToArray();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(120), ct);

        // Every ack is a distinct LSN already durable at return. No assertion on the fsync count:
        // batch formation depends on how long a flush holds relative to commit arrival — on a
        // fast disk a lone flush wins the race and batches of one are correct behaviour. The
        // hostage-flush test above proves batching deterministically; this test pins the
        // semantics that must hold at any batch size.
        Assert.Equal(Writers * PerWriter, acked.Distinct().Count());
        Assert.Equal((ulong)(Writers * PerWriter), harness.Engine.Log.HeadLsn);
        Assert.Equal(harness.Engine.Log.HeadLsn, harness.Engine.Log.DurableLsn);

        harness.Restart();
        var table = harness.Engine.Schema.Get(typeof(TerrainChunk));
        Assert.Equal(Writers * PerWriter, harness.Engine.HotStore.Scan(table.Id).Count());
    }

    [Fact]
    public void A_poisoned_log_fails_the_committing_caller_and_a_restart_reconverges()
    {
        using var harness = new EngineHarness();
        harness.Invoke("Durable", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1], Kind = ChunkKind.Rock }));

        harness.Engine.LogFile.FlushFaultInjection = () => throw new IOException("injected: fsync failed");
        var failed = Assert.Throws<InvalidOperationException>(() =>
            harness.Invoke("Doomed", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 2, Data = [2], Kind = ChunkKind.Rock })));
        Assert.IsType<IOException>(failed.InnerException);

        // The documented divergence window: the in-memory projection applied the record before
        // the fsync failed, so reads see it until restart — while the poisoned log refuses every
        // further commit, which is what bounds the window.
        var table = harness.Engine.Schema.Get(typeof(TerrainChunk));
        Assert.Equal(2, harness.Engine.HotStore.Scan(table.Id).Count());
        harness.Engine.LogFile.FlushFaultInjection = null;
        var refused = Assert.Throws<InvalidOperationException>(() =>
            harness.Invoke("After", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 3, Data = [3], Kind = ChunkKind.Rock })));
        Assert.Contains("failed state", refused.Message);

        // Restart reconverges on the durable prefix: the failed commit's effects are gone.
        harness.Restart();
        Assert.Equal(1UL, harness.Engine.Log.HeadLsn);
        Assert.Single(harness.Engine.HotStore.Scan(table.Id));
    }

    [Fact]
    public void A_snapshot_forces_the_log_durable_through_its_own_lsn_under_every_policy()
    {
        using var harness = new EngineHarness(FsyncPolicy.OsBuffered);
        harness.Invoke("First", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 1, Data = [1], Kind = ChunkKind.Rock }));
        harness.Invoke("Second", ctx => ctx.Db.Insert(new TerrainChunk { ChunkId = 2, Data = [2], Kind = ChunkKind.Rock }));
        Assert.Equal(0UL, harness.Engine.LogFile.FsyncedLsn); // OsBuffered never fsyncs on its own.

        Assert.Equal(2UL, harness.Engine.TakeSnapshot());

        // The snapshot file's LSN is recovery's durable floor, so the log must be fsynced through
        // it before the file exists — otherwise a crash could boot a snapshot ahead of its log.
        Assert.True(harness.Engine.LogFile.FsyncedLsn >= 2UL);
    }

    /// <summary>
    /// Flips a byte inside the <paramref name="ordinal"/>-th record's payload (1-based), walking
    /// the frame chain rather than assuming offsets. Length and frame stay intact; the CRC no
    /// longer matches — the shape of both real bit rot and a crash's partially persisted page.
    /// </summary>
    internal static void CorruptRecordPayload(string logFilePath, int ordinal)
    {
        using var file = new FileStream(logFilePath, FileMode.Open, FileAccess.ReadWrite);
        file.Seek(FileCommitLog.HeaderSize, SeekOrigin.Begin);
        var frame = new byte[FileCommitLog.FrameSize];
        for (var i = 1; ; i++)
        {
            file.ReadExactly(frame);
            var length = BinaryPrimitives.ReadUInt32LittleEndian(frame);
            if (i == ordinal)
            {
                var b = file.ReadByte();
                file.Seek(-1, SeekOrigin.Current);
                file.WriteByte((byte)(b ^ 0xFF));
                return;
            }

            file.Seek(length, SeekOrigin.Current);
        }
    }

    private static CommitRequest MakeRequest(string reducerName)
    {
        var op = new RowOp(RowOpKind.Insert, TableId.FromName("Whatever"), new RowKey([1, 2, 3]), new byte[] { 4, 5, 6 });
        return new CommitRequest(new Timestamp(1), EngineHarness.Caller, reducerName, ReadOnlyMemory<byte>.Empty, [op]);
    }
}
