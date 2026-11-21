using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Phase 30 step 2b, the storage fence: one log segment per ownership term. Opening under a higher
/// term seals the previous owner's segment at its last valid record (the cut), and nothing past a
/// cut is ever read again, so a previous owner that was paused rather than dead, and resumes
/// writing, writes where nobody looks. This holds whatever the shared filesystem does with
/// <c>melange.lock</c>, which the tests sidestep by opening one log at a time and appending to the
/// old segment's file directly, as a writer on another host would.
/// </summary>
public class LogSegmentTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-segments-").FullName;
    private readonly List<string> _extraRoots = [];

    public void Dispose()
    {
        foreach (var root in _extraRoots.Append(_root))
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
                // A handle Windows has not released yet; the temp reaper gets it.
            }
        }
    }

    private FileCommitLog Open(long? term, string? path = null) => new(new CommitLogOptions { Path = path ?? _root, Term = term });

    private static CommitRequest Request(string reducer, long timestampMicros = 1) =>
        new(
            new Timestamp(timestampMicros),
            EngineHarness.Caller,
            reducer,
            new byte[8],
            [new RowOp(RowOpKind.Insert, TableId.FromName("Whatever"), new RowKey([1, 2, 3]), new byte[] { 4, 5, 6 })]);

    private static void Append(FileCommitLog log, string reducer, int count, long timestampMicros = 1)
    {
        for (var i = 0; i < count; i++)
            log.WaitDurable(log.Append(Request(reducer, timestampMicros + i)).Lsn);
    }

    private static List<(ulong Lsn, string Reducer)> All(FileCommitLog log) =>
        [.. log.ReadFrom(1).Select(static record => (record.Lsn, record.ReducerName))];

    [Fact]
    public void A_log_from_before_segments_is_sealed_where_it_stands_and_the_new_term_continues_it()
    {
        Guid epoch;
        using (var legacy = Open(term: null))
        {
            Append(legacy, "old", 3);
            epoch = legacy.EpochId;
        }

        Assert.False(File.Exists(Path.Combine(_root, LogSegments.FileName)));

        using (var owner = Open(term: 3))
        {
            Assert.Equal(epoch, owner.EpochId); // One log, not a new incarnation.
            Assert.Equal(3UL, owner.HeadLsn);
            Append(owner, "new", 2);
            Assert.EndsWith("melange.log.3", owner.FilePath);
            Assert.Equal(
                [(1UL, "old"), (2UL, "old"), (3UL, "old"), (4UL, "new"), (5UL, "new")],
                All(owner));
        }

        // Nothing was renamed: the legacy file is the first sealed segment, cut at its last record.
        var manifest = LogSegments.Load(_root);
        var sealedSegment = Assert.Single(manifest.SealedSegments);
        Assert.Equal(new LogSegments.Sealed(0, "melange.log", 3), sealedSegment);
        Assert.Equal(new LogSegments.Active(3, "melange.log.3"), manifest.ActiveSegment);

        // Reopening under the same term is a restart, not a new owner: no new segment.
        using var restarted = Open(term: 3);
        Assert.Equal(5UL, restarted.HeadLsn);
        Assert.Single(LogSegments.Load(_root).SealedSegments);

        // And a tool that adopts whatever term the directory is at reads the whole log.
        restarted.Dispose();
        using var tool = Open(term: null);
        Assert.Equal(5, All(tool).Count);
    }

    [Fact]
    public void A_previous_owner_cannot_reopen_the_log()
    {
        using (var owner = Open(term: 4))
            Append(owner, "a", 1);

        var refused = Assert.Throws<StaleTermException>(() => Open(term: 3));
        Assert.Contains("term 4", refused.Message);

        // Refusing released the directory's lock: the current owner reopens at once.
        using var current = Open(term: 4);
        Assert.Equal(1UL, current.HeadLsn);
    }

    [Fact]
    public void A_paused_previous_owner_that_resumes_writes_past_the_cut_where_nobody_reads()
    {
        using (var first = Open(term: 1))
            Append(first, "first", 3);

        // A frame the previous owner would have written as LSN 4, made by a log of its own.
        var staleFrame = FrameOfLastRecord(log => Append(log, "stale", 4));

        using (var second = Open(term: 2))
        {
            Append(second, "second", 2);

            // The previous owner wakes up and completes its write, into its own segment, after the
            // cut. On storage that honours melange.lock it could not have opened the log; this is
            // the storage that does not.
            using (var oldSegment = new FileStream(Path.Combine(_root, "melange.log.1"), FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
                oldSegment.Write(staleFrame);

            Assert.Equal(
                [(1UL, "first"), (2UL, "first"), (3UL, "first"), (4UL, "second"), (5UL, "second")],
                All(second));
        }

        // Neither a restart nor the next owner sees it either: the cut is the end of that segment.
        using (var restarted = Open(term: 2))
            Assert.Equal(5UL, restarted.HeadLsn);
        using var third = Open(term: 3);
        Assert.Equal(5UL, third.HeadLsn);
        Assert.DoesNotContain(All(third), static entry => entry.Reducer == "stale");
        Assert.Equal([1UL, 2UL, 3UL, 4UL, 5UL], All(third).Select(static entry => entry.Lsn));
    }

    [Fact]
    public void Truncation_drops_sealed_segments_whole_and_rewrites_only_the_live_one()
    {
        using (var first = Open(term: 1))
            Append(first, "t1", 3); // LSN 1-3
        using (var second = Open(term: 2))
            Append(second, "t2", 3); // LSN 4-6
        using var log = Open(term: 3);
        Append(log, "t3", 3); // LSN 7-9

        // Inside the first sealed segment: only the base moves; the segment keeps its later records.
        log.TruncateBefore(2);
        Assert.Equal(2UL, log.BaseLsn);
        Assert.Equal(2, LogSegments.Load(_root).SealedSegments.Count);
        Assert.Equal([3UL, 4UL, 5UL, 6UL, 7UL, 8UL, 9UL], log.ReadFrom(3).Select(static r => r.Lsn));

        // At its cut: the segment and its file are gone.
        log.TruncateBefore(3);
        Assert.False(File.Exists(Path.Combine(_root, "melange.log.1")));
        Assert.Single(LogSegments.Load(_root).SealedSegments);

        // Into the live segment: every sealed one goes, and the live one is compacted as before.
        log.TruncateBefore(7);
        Assert.Equal(7UL, log.BaseLsn);
        Assert.Empty(LogSegments.Load(_root).SealedSegments);
        Assert.False(File.Exists(Path.Combine(_root, "melange.log.2")));
        Assert.Equal([8UL, 9UL], log.ReadFrom(8).Select(static r => r.Lsn));

        log.Dispose();
        using var reopened = Open(term: 3);
        Assert.Equal(9UL, reopened.HeadLsn);
        Assert.Equal(7UL, reopened.BaseLsn);
        Assert.Equal([8UL, 9UL], reopened.ReadFrom(1).Select(static r => r.Lsn));
    }

    [Fact]
    public void The_retention_search_finds_timestamps_in_sealed_segments()
    {
        using (var first = Open(term: 1))
            Append(first, "t1", 5, timestampMicros: 100); // LSN 1-5 at 100-104
        using var log = Open(term: 2);
        Append(log, "t2", 5, timestampMicros: 200); // LSN 6-10 at 200-204

        Assert.Equal(3UL, log.FirstLsnAtOrAfter(102, 1, 10));
        Assert.Equal(6UL, log.FirstLsnAtOrAfter(150, 1, 10));
        Assert.Equal(9UL, log.FirstLsnAtOrAfter(203, 1, 10));
        Assert.Null(log.FirstLsnAtOrAfter(999, 1, 10));
    }

    [Fact]
    public void A_shard_created_under_a_term_starts_in_that_terms_segment()
    {
        using var log = Open(term: 5);
        Append(log, "a", 2);
        Assert.EndsWith("melange.log.5", log.FilePath);
        Assert.Empty(LogSegments.Load(_root).SealedSegments);
        Assert.False(File.Exists(Path.Combine(_root, "melange.log")));
    }

    [Fact]
    public void An_engine_recovers_its_projection_across_segments_and_a_backup_round_trips_them()
    {
        var registry = EngineHarness.GeneratedRegistry(typeof(Player));
        MelangeEngine Boot(long? term, string logDirectory) => new(
            new MelangeDbOptions
            {
                CommitLog = { Path = logDirectory, Term = term },
                HotStore = { Path = Path.Combine(TempDir(), "hot") },
            },
            registry);

        for (var term = 1; term <= 3; term++)
        {
            using var owner = Boot(term, _root);
            var name = $"p{term}";
            owner.Invoke("Spawn", EngineHarness.Caller, ctx => ctx.Db.Insert(new Player { Id = Identity.Hash(name), Name = name, RoomId = term }));
        }

        Assert.Equal(2, LogSegments.Load(_root).SealedSegments.Count);
        using (var recovered = Boot(3, _root))
            Assert.Equal(3, recovered.CommittedView.Count<Player>());

        var archive = Path.Combine(TempDir(), "segments.mbak");
        var backup = Assert.Single(MelangeBackup.Create(_root, archive).Engines);
        Assert.Equal(3UL, backup.HeadLsn);
        var restoredDir = Path.Combine(TempDir(), "restored");
        MelangeBackup.Restore(archive, restoredDir);
        using var restored = Boot(term: null, restoredDir);
        Assert.Equal(3, restored.CommittedView.Count<Player>());
    }

    private string TempDir()
    {
        var dir = Directory.CreateTempSubdirectory("melange-segments-extra-").FullName;
        _extraRoots.Add(dir);
        return dir;
    }

    /// <summary>The raw frame (header and payload) of the last record a scratch log was given.</summary>
    private byte[] FrameOfLastRecord(Action<FileCommitLog> write)
    {
        var scratch = TempDir();
        using (var log = Open(term: null, scratch))
            write(log);
        var bytes = File.ReadAllBytes(Path.Combine(scratch, "melange.log"));
        var position = FileCommitLog.HeaderSize;
        var last = position;
        while (position < bytes.Length)
        {
            last = position;
            position += FileCommitLog.FrameSize + (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(position));
        }

        return bytes[last..];
    }
}
