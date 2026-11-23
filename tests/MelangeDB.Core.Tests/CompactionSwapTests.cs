using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The last step of a log compaction: the log closes its file, renames the compacted file over it,
/// and reopens it. Until these tests, a failure there left the log holding a closed file. It kept
/// accepting appends, which then threw <see cref="ObjectDisposedException"/>, and its own disposal
/// threw too. On Windows the failure is ordinary, not a failing disk: a replace is refused while
/// any other handle has the file open, and every read of the log opens one.
/// </summary>
public class CompactionSwapTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-log-swap-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A handle Windows has not released yet; the temp reaper gets it.
        }
    }

    [Fact]
    public void A_refused_rename_keeps_the_old_file_and_the_log_writable()
    {
        var options = new CommitLogOptions { Path = _root };
        using (var log = new FileCommitLog(options))
        {
            Append(log, 1, 1_000);
            var full = log.FileLengthBytes;

            log.SwapFaultInjection = () => throw new IOException("Access to the path is denied.");
            log.TruncateBefore(400);
            log.SwapFaultInjection = null;

            // The truncation stands: the base moved, and reads start above it even though the
            // records below it are still in the file.
            Assert.Equal(1, log.FailedSwaps);
            Assert.Null(log.Failure);
            Assert.Equal(400UL, log.BaseLsn);
            Assert.Equal(full, log.FileLengthBytes);
            Assert.Equal(Lsns(401, 1_000), log.ReadFrom(1).Select(r => r.Lsn));
            Assert.False(File.Exists(log.FilePath + ".compact"));

            // Appends land and become durable.
            Append(log, 1_001, 1_100);
            Assert.Equal(Lsns(1_050, 1_100), log.ReadFrom(1_050).Select(r => r.Lsn));

            // The next compaction removes what this one could not.
            log.TruncateBefore(900);
            Assert.True(log.FileLengthBytes < full / 2, $"the log is {log.FileLengthBytes} bytes of {full} after the retry");
            Assert.Equal(Lsns(901, 1_100), log.ReadFrom(1).Select(r => r.Lsn));
        }

        using var reopened = new FileCommitLog(options);
        Assert.Equal(900UL, reopened.BaseLsn);
        Assert.Equal(1_100UL, reopened.HeadLsn);
        Assert.Equal(Lsns(901, 1_100), reopened.ReadFrom(1).Select(r => r.Lsn));
    }

    [Fact]
    public void A_refused_rename_reopens_to_the_same_records()
    {
        // The old file and the base sidecar are what a crash between the compaction's two steps
        // leaves, and recovery has to read them the way the running log did.
        var options = new CommitLogOptions { Path = _root };
        using (var log = new FileCommitLog(options))
        {
            Append(log, 1, 500);
            log.SwapFaultInjection = () => throw new IOException("Access to the path is denied.");
            log.TruncateBefore(200);
        }

        using var reopened = new FileCommitLog(options);
        Assert.Equal(200UL, reopened.BaseLsn);
        Assert.Equal(500UL, reopened.HeadLsn);
        Assert.Equal(Lsns(201, 500), reopened.ReadFrom(1).Select(r => r.Lsn));
        Assert.Equal(Lsns(201, 500), reopened.ReadUncapped(150).Select(r => r.Lsn));
    }

    [Fact]
    public void A_rename_that_may_not_be_durable_poisons_the_log()
    {
        // The compacted file is in place, but the step that makes the rename survive power loss
        // failed. Appends acknowledged into the new file could vanish with a reverted rename, so
        // the log stops taking them. It still closes cleanly, and the next open recovers.
        var options = new CommitLogOptions { Path = _root };
        var log = new FileCommitLog(options);
        Append(log, 1, 1_000);

        log.SwapFaultInjection = () =>
        {
            File.Move(log.FilePath + ".compact", log.FilePath, overwrite: true);
            throw new IOException("Could not fsync directory (errno 5).");
        };
        log.TruncateBefore(400);

        Assert.IsType<IOException>(log.Failure);
        var refused = Assert.Throws<InvalidOperationException>(() => log.Append(Request(1_001)));
        Assert.IsType<IOException>(refused.InnerException);
        Assert.Equal(0, log.FileLengthBytes);
        log.Dispose();

        using var reopened = new FileCommitLog(options);
        Assert.Null(reopened.Failure);
        Assert.Equal(400UL, reopened.BaseLsn);
        Assert.Equal(1_000UL, reopened.HeadLsn);
        Assert.Equal(Lsns(401, 1_000), reopened.ReadFrom(1).Select(r => r.Lsn));
    }

    [Fact]
    public void A_compaction_during_an_open_read_leaves_the_log_working()
    {
        // The real cause, not an injected one. A read is lazy and holds its file handle open
        // between records; a compaction that lands meanwhile is what a busy node does.
        var options = new CommitLogOptions { Path = _root };
        using (var log = new FileCommitLog(options))
        {
            Append(log, 1, 1_000);

            using var read = log.ReadFrom(1).GetEnumerator();
            Assert.True(read.MoveNext());
            log.TruncateBefore(400);

            // The open read finishes on the file it started with.
            var seen = 1;
            while (read.MoveNext())
                seen++;
            Assert.Equal(1_000, seen);
            read.Dispose();

            Assert.Null(log.Failure);
            Assert.Equal(400UL, log.BaseLsn);
            Append(log, 1_001, 1_010);
            Assert.Equal(Lsns(401, 1_010), log.ReadFrom(1).Select(r => r.Lsn));
        }

        using var reopened = new FileCommitLog(options);
        Assert.Equal(Lsns(401, 1_010), reopened.ReadFrom(1).Select(r => r.Lsn));
    }

    private static void Append(FileCommitLog log, int first, int last)
    {
        for (var i = first; i <= last; i++)
            log.Append(Request(i));
        log.FlushToDisk();
    }

    private static IEnumerable<ulong> Lsns(int first, int last) =>
        Enumerable.Range(first, last - first + 1).Select(i => (ulong)i);

    private static CommitRequest Request(long timestampMicros)
    {
        var op = new RowOp(RowOpKind.Insert, TableId.FromName("Whatever"), new RowKey([1, 2, 3]), new byte[] { 4, 5, 6 });
        return new CommitRequest(
            new Timestamp(timestampMicros),
            EngineHarness.Caller,
            "Swap",
            new byte[64],
            [op]);
    }
}
