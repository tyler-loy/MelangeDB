using System.Buffers.Binary;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Phase 30 step 2c: the warm projection a shard's replica keeps. It follows a log another engine
/// owns, applies only up to the ceiling it is given (the owner's durable LSN), writes nothing in
/// the owner's directory, and starts over from the snapshot when its place in the log is gone.
/// </summary>
public class LogFollowerTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly SchemaRegistry _registry = EngineHarness.GeneratedRegistry(typeof(Player), typeof(InventoryItem));

    public void Dispose()
    {
        foreach (var root in _roots)
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

    private string TempDir()
    {
        var dir = Directory.CreateTempSubdirectory("melange-follower-").FullName;
        _roots.Add(dir);
        return dir;
    }

    private MelangeEngine Owner(string logDirectory, long? term = null, int retentionSeconds = 3600) => new(
        new MelangeDbOptions
        {
            CommitLog = { Path = logDirectory, Term = term },
            HotStore = { Path = Path.Combine(TempDir(), "hot") },
            Resume = { RetentionWindowSeconds = retentionSeconds },
        },
        _registry);

    private LogFollower Follower(string logDirectory) => new(
        new MelangeDbOptions
        {
            CommitLog = { Path = logDirectory },
            HotStore = { Path = Path.Combine(TempDir(), "follower-hot") },
        },
        _registry,
        originator: 0); // The shard's own originator, as the owner's: a follower continues its sequences.

    private static void Spawn(MelangeEngine engine, string name, int room = 1) =>
        engine.Invoke("Spawn", EngineHarness.Caller, ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash(name), Name = name, RoomId = room });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash(name), ItemName = "torch", Quantity = 1 });
        });

    private static List<string> Dump(SchemaRegistry schema, IHotStore store) =>
        [.. schema.Tables.SelectMany(table => store.Scan(table.Id).Select(pair => $"{table.Name}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}"))];

    private static void CatchUp(LogFollower follower, ulong ceiling)
    {
        follower.Ceiling = ceiling;
        while (follower.Poll() > 0)
        {
        }
    }

    [Fact]
    public void It_follows_a_live_owner_up_to_the_ceiling_and_no_further()
    {
        var dir = TempDir();
        using var owner = Owner(dir);
        using var follower = Follower(dir);
        for (var i = 0; i < 5; i++)
            Spawn(owner, $"p{i}");

        Assert.Equal(0, follower.Poll()); // No ceiling yet: nothing.
        CatchUp(follower, ceiling: 3);
        Assert.Equal(3UL, follower.AppliedLsn);

        CatchUp(follower, owner.Log.DurableLsn);
        Assert.Equal(owner.Log.HeadLsn, follower.AppliedLsn);
        Assert.Equal(Dump(_registry, owner.HotStore), Dump(_registry, follower.Store!));

        // AutoInc ids continue from what the follower observed, under its own originator.
        var nextOwnerId = owner.CommittedView.Scan<InventoryItem>().Max(static item => item.Id) & AutoIncSequencer.MaxSequence;
        Assert.Equal(nextOwnerId + 1, follower.Sequencer.PeekNextSequence(_registry.Get(typeof(InventoryItem)).Id));
    }

    [Fact]
    public void A_truncation_past_its_cursor_sends_it_back_to_the_snapshot()
    {
        var dir = TempDir();
        using var owner = Owner(dir, retentionSeconds: 0);
        using var follower = Follower(dir);
        Spawn(owner, "a");
        CatchUp(follower, owner.Log.DurableLsn);
        Assert.Equal(1, follower.Bootstraps);

        for (var i = 0; i < 4; i++)
            Spawn(owner, $"b{i}");
        Assert.NotNull(owner.TakeSnapshot());
        Assert.True(owner.Log.BaseLsn > follower.AppliedLsn, "the snapshot should have truncated past the follower");
        Spawn(owner, "c");

        CatchUp(follower, owner.Log.DurableLsn);
        Assert.Equal(2, follower.Bootstraps);
        Assert.Equal(owner.Log.HeadLsn, follower.AppliedLsn);
        Assert.Equal(Dump(_registry, owner.HotStore), Dump(_registry, follower.Store!));
    }

    [Fact]
    public void It_follows_the_log_across_a_change_of_owner_and_never_past_a_cut()
    {
        var dir = TempDir();
        using var follower = Follower(dir);
        using (var first = Owner(dir, term: 1))
        {
            Spawn(first, "first-a");
            Spawn(first, "first-b");

            // One record behind, so the follower is still inside the first segment when its
            // previous owner's stale write lands right after the cut.
            CatchUp(follower, 1);
        }

        // The previous owner, paused, completes a write into its own segment after the next owner
        // has sealed it. The follower must never apply it.
        using var second = Owner(dir, term: 2);
        AppendStaleFrame(Path.Combine(dir, "melange.log.1"), lsn: 3);
        Spawn(second, "second-a");

        CatchUp(follower, second.Log.DurableLsn);
        Assert.Equal(3UL, follower.AppliedLsn);
        Assert.Equal(Dump(_registry, second.HotStore), Dump(_registry, follower.Store!));
        Assert.Equal(1, follower.Bootstraps);
    }

    [Fact]
    public void It_writes_nothing_in_the_owners_directory()
    {
        var dir = TempDir();
        using (var owner = Owner(dir, term: 1))
        {
            Spawn(owner, "a");
            owner.TakeSnapshot();
            Spawn(owner, "b");
        }

        var before = Listing(dir);
        using (var follower = Follower(dir))
            CatchUp(follower, ulong.MaxValue);
        Assert.Equal(before, Listing(dir));
    }

    [Fact]
    public void A_directory_whose_owner_has_not_booted_is_not_an_error()
    {
        using var follower = Follower(TempDir());
        follower.Ceiling = ulong.MaxValue;
        Assert.Equal(0, follower.Poll());
        Assert.Null(follower.Store);
    }

    private static List<string> Listing(string dir) =>
        [.. Directory.GetFiles(dir).Order().Select(static path => $"{Path.GetFileName(path)}|{new FileInfo(path).Length}|{File.GetLastWriteTimeUtc(path):O}")];

    /// <summary>Appends a well-formed frame carrying <paramref name="lsn"/>, as a paused owner finishing its write would.</summary>
    private void AppendStaleFrame(string segmentPath, ulong lsn)
    {
        var scratch = TempDir();
        using (var log = new FileCommitLog(new CommitLogOptions { Path = scratch }))
        {
            for (ulong i = 1; i <= lsn; i++)
            {
                var record = log.Append(new CommitRequest(new Timestamp(1), EngineHarness.Caller, "stale", new byte[4], []));
                log.WaitDurable(record.Lsn);
            }
        }

        var bytes = File.ReadAllBytes(Path.Combine(scratch, "melange.log"));
        var position = FileCommitLog.HeaderSize;
        var last = position;
        while (position < bytes.Length)
        {
            last = position;
            position += FileCommitLog.FrameSize + (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(position));
        }

        using var segment = new FileStream(segmentPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        segment.Write(bytes.AsSpan(last));
    }
}
