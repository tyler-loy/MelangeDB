using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The commit log's seek, and the one property of it no result assertion can check.
/// <para>
/// A read that scanned the file from the header and discarded every record below the LSN it was
/// asked for returned exactly the records a seek returns — which is how every incremental reader
/// of the log (a resume replay, a replica stream, an applier's catch-up, the cluster's event and
/// border pumps) came to re-read the whole retained log on every batch without any test noticing:
/// the answers were right, and the test logs were small enough that reaching the far end of them
/// cost nothing measurable. So the cost claim is pinned where it is observable, by counting the
/// frames a read passes over. The rest cover what the index could plausibly get wrong — a
/// truncation moving every survivor's offset under it, appends landing mid-compaction, a reopen.
/// </para>
/// </summary>
public class CommitLogSeekTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-log-seek-").FullName;

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
    public void Reading_from_a_late_lsn_costs_one_stride_not_the_log()
    {
        // Arguments sized so a stride of the index holds a few dozen records: the seek lands
        // within one stride of the target and hops the rest, and that hop count is the assertion.
        using var log = Open();
        const int records = 4_000;
        for (var i = 0; i < records; i++)
            log.Append(Request(i + 1, argumentBytes: 1_024));
        log.FlushToDisk();

        var before = log.SkippedFrames;
        var tail = log.ReadFrom(records - 9).Select(r => r.Lsn).ToList();
        var skipped = log.SkippedFrames - before;

        Assert.Equal(Enumerable.Range(records - 9, 10).Select(i => (ulong)i), tail);

        // The scan this replaced passed over 3,990 records to serve these ten. A stride of
        // 1 KB-plus records is ~30; anything in the hundreds means the read walked from the header.
        Assert.True(
            skipped < 100,
            $"reading the last ten records passed over {skipped} frames, which is the signature of " +
            "scanning from the start of the file instead of seeking to the window");

        // And the seek is not a cache of one position: the middle is as cheap as the end.
        before = log.SkippedFrames;
        Assert.Equal(2_000UL, log.ReadFrom(2_000).First().Lsn);
        Assert.True(log.SkippedFrames - before < 100);
    }

    [Fact]
    public void Every_starting_lsn_reads_the_same_records_as_a_scan_would()
    {
        using var log = Open();
        const int records = 600;
        for (var i = 0; i < records; i++)
            log.Append(Request(i + 1, argumentBytes: 200 + (i % 7) * 50));
        log.FlushToDisk();

        // Bounds that name nothing in particular, a bound below the first record (which serves from
        // the first, as the scan did), and a bound past the head (which serves nothing).
        foreach (var first in new ulong[] { 0, 1, 2, 37, 128, 129, 300, 599, 600 })
        {
            var lsns = log.ReadFrom(first).Select(r => r.Lsn).ToList();
            var expected = Enumerable.Range((int)Math.Max(first, 1UL), records - (int)Math.Max(first, 1UL) + 1).Select(i => (ulong)i);
            Assert.Equal(expected, lsns);
        }

        Assert.Empty(log.ReadFrom(601));
        Assert.Empty(log.ReadFrom(10_000));
    }

    [Fact]
    public void Truncation_moves_every_survivor_and_the_seeks_follow()
    {
        var options = new CommitLogOptions { Path = _root };
        using (var log = new FileCommitLog(options))
        {
            for (var i = 0; i < 3_000; i++)
                log.Append(Request(i + 1, argumentBytes: 512));
            log.FlushToDisk();

            log.TruncateBefore(1_500);
            Assert.Equal(1_500UL, log.BaseLsn);

            for (var i = 3_000; i < 3_500; i++)
                log.Append(Request(i + 1, argumentBytes: 512));
            log.FlushToDisk();

            // Reads below the base serve from the first survivor, exactly as before; reads inside
            // the survivors land on the right record; and they still seek — an index that kept its
            // pre-compaction offsets would fail verification and fall back to the scan.
            Assert.Equal(1_501UL, log.ReadFrom(1).First().Lsn);
            Assert.Equal(1_501UL, log.ReadFrom(1_500).First().Lsn);
            Assert.Equal(Enumerable.Range(2_990, 511).Select(i => (ulong)i), log.ReadFrom(2_990).Select(r => r.Lsn));
            Assert.Equal(3_500UL, log.ReadFrom(3_500).Single().Lsn);

            var before = log.SkippedFrames;
            Assert.Equal(3_490UL, log.ReadFrom(3_490).First().Lsn);
            Assert.True(
                log.SkippedFrames - before < 100,
                $"after truncation a late read passed over {log.SkippedFrames - before} frames: the index did not follow the compaction");
        }

        // A reopen rebuilds the index from the compacted file.
        using (var reopened = new FileCommitLog(options))
        {
            Assert.Equal(1_500UL, reopened.BaseLsn);
            Assert.Equal(3_500UL, reopened.HeadLsn);
            Assert.Equal(1_501UL, reopened.ReadFrom(1).First().Lsn);
            Assert.Equal(Enumerable.Range(3_400, 101).Select(i => (ulong)i), reopened.ReadFrom(3_400).Select(r => r.Lsn));
            var before = reopened.SkippedFrames;
            Assert.Equal(3_490UL, reopened.ReadFrom(3_490).First().Lsn);
            Assert.True(reopened.SkippedFrames - before < 100);
        }
    }

    [Fact]
    public void Appends_that_land_during_a_compaction_are_carried_into_the_compacted_log()
    {
        // The compaction copies the survivors without holding the append lock, so records can
        // arrive while it does. They belong to the compacted file, and they must be readable,
        // durable and indexed there like everything else.
        using var log = Open();
        for (var i = 0; i < 1_000; i++)
            log.Append(Request(i + 1));
        log.FlushToDisk();

        log.BetweenCompactionPhases = () =>
        {
            for (var i = 1_000; i < 1_050; i++)
                log.Append(Request(i + 1));
        };
        log.TruncateBefore(400);
        log.BetweenCompactionPhases = null;

        Assert.Equal(400UL, log.BaseLsn);
        Assert.Equal(1_050UL, log.HeadLsn);
        Assert.Equal(Enumerable.Range(401, 650).Select(i => (ulong)i), log.ReadFrom(1).Select(r => r.Lsn));
        Assert.Equal(Enumerable.Range(1_000, 51).Select(i => (ulong)i), log.ReadFrom(1_000).Select(r => r.Lsn));

        // And the next truncation starts from a file whose index describes it.
        log.TruncateBefore(1_020);
        Assert.Equal(Enumerable.Range(1_021, 30).Select(i => (ulong)i), log.ReadFrom(1).Select(r => r.Lsn));
        log.Append(Request(1_051));
        log.FlushToDisk(); // ReadFrom serves nothing beyond the durable watermark.
        Assert.Equal(1_051UL, log.ReadFrom(1_051).Single().Lsn);
    }

    [Fact]
    public void Compaction_copies_only_the_survivors()
    {
        using var log = Open();
        for (var i = 0; i < 2_000; i++)
            log.Append(Request(i + 1, argumentBytes: 1_024));
        log.FlushToDisk();
        var full = log.FileLengthBytes;

        log.TruncateBefore(1_900);

        // 100 of 2,000 records survive; the file should be about a twentieth of what it was.
        Assert.True(
            log.FileLengthBytes < full / 10,
            $"the compacted log is {log.FileLengthBytes} bytes of an original {full}; the survivors were not what got copied");
        Assert.Equal(Enumerable.Range(1_901, 100).Select(i => (ulong)i), log.ReadFrom(1).Select(r => r.Lsn));
    }

    [Fact]
    public void The_oldest_record_inside_a_window_is_found_by_search()
    {
        // Timestamps equal to LSNs, so the answer is legible.
        using var log = Open();
        for (var i = 0; i < 1_000; i++)
            log.Append(Request(i + 1));
        log.FlushToDisk();

        Assert.Equal(700UL, log.FirstLsnAtOrAfter(700, 1, 1_000));
        Assert.Equal(1UL, log.FirstLsnAtOrAfter(0, 1, 1_000));
        Assert.Equal(1UL, log.FirstLsnAtOrAfter(1, 1, 1_000));
        Assert.Equal(1_000UL, log.FirstLsnAtOrAfter(1_000, 1, 1_000));
        Assert.Null(log.FirstLsnAtOrAfter(1_001, 1, 1_000));

        // The search respects its ceiling: a record past it does not count even when it qualifies.
        Assert.Null(log.FirstLsnAtOrAfter(700, 1, 500));
        Assert.Equal(700UL, log.FirstLsnAtOrAfter(650, 700, 900));

        // A ceiling past the head, and a range below the base after a truncation.
        Assert.Equal(999UL, log.FirstLsnAtOrAfter(999, 1, 5_000));
        log.TruncateBefore(200);
        Assert.Equal(300UL, log.FirstLsnAtOrAfter(300, 201, 1_000));
        Assert.Equal(201UL, log.FirstLsnAtOrAfter(0, 201, 1_000));
    }

    private FileCommitLog Open() => new(new CommitLogOptions { Path = _root });

    private static CommitRequest Request(long timestampMicros, int argumentBytes = 16)
    {
        var op = new RowOp(RowOpKind.Insert, TableId.FromName("Whatever"), new RowKey([1, 2, 3]), new byte[] { 4, 5, 6 });
        return new CommitRequest(
            new Timestamp(timestampMicros),
            EngineHarness.Caller,
            "Seek",
            new byte[argumentBytes],
            [op]);
    }
}
