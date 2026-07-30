using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// The phase 01 kill-test pattern extended to the FASTER store: under the default OnCommit fsync
/// policy every committed record is durable before the commit returns, so abandoning the engine
/// and rebuilding from disk is equivalent to a mid-run kill for every committed transaction, and
/// the torn-tail cases cover a kill mid-append. The FASTER store adds nothing to lose here by
/// design — its hybrid log is a projection rebuilt from snapshot + log on every start, so there is
/// no FASTER-side state to tear.
/// </summary>
[Trait("Category", "Slow")]
public class CrashConsistencyTests
{
    public static TheoryData<StoreKind> Stores => new(StoreKind.InMemory, StoreKind.Faster);

    [Theory]
    [MemberData(nameof(Stores))]
    public void Kill_during_heavy_writes_loses_nothing_committed_and_tears_nothing(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        for (var i = 0; i < 200; i++)
        {
            var id = i;
            harness.Invoke("heavy", ctx =>
            {
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = id % 8, Data = StoreContractTests.MakeBlob(id, 700) });
                if (id % 3 == 0 && id > 0)
                {
                    var previous = ctx.Db.Find<TerrainBlob>((long)(id - 1))!.Value;
                    ctx.Db.Update(previous with { Data = StoreContractTests.MakeBlob(id + 5000, 900) });
                }

                if (id % 7 == 0 && id > 10)
                    ctx.Db.Delete<TerrainBlob>((long)(id - 10));
            });
        }

        var committed = harness.Dump();

        // The kill: abandon the engine, then corrupt the tail the way a mid-append power cut
        // does — half a record's worth of garbage after the last durable commit.
        harness.Engine.Dispose();
        using (var stream = new FileStream(Path.Combine(harness.Options.CommitLog.Path, "melange.log"), FileMode.Append))
            stream.Write(new byte[137]);

        harness.Restart();
        Assert.Equal(committed, harness.Dump());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Failed_append_rolls_back_and_later_commits_survive_restart(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new TerrainBlob { ChunkId = 1, Region = 0, Data = StoreContractTests.MakeBlob(1, 400) }));

        var log = (FileCommitLog)harness.Engine.Log;
        log.AppendFaultInjection = _ => throw new IOException("injected: disk full");
        Assert.Throws<IOException>(() => harness.Invoke("doomed", ctx =>
            ctx.Db.Insert(new TerrainBlob { ChunkId = 2, Region = 0, Data = StoreContractTests.MakeBlob(2, 400) })));
        log.AppendFaultInjection = null;

        // The aborted transaction left zero trace; the log keeps accepting appends.
        harness.Invoke("after", ctx =>
        {
            Assert.Null(ctx.Db.Find<TerrainBlob>(2L));
            ctx.Db.Insert(new TerrainBlob { ChunkId = 3, Region = 0, Data = StoreContractTests.MakeBlob(3, 400) });
        });

        var before = harness.Dump();
        harness.Restart();
        Assert.Equal(before, harness.Dump());
        Assert.Equal(2, before.Count(l => l.StartsWith("TerrainBlob", StringComparison.Ordinal)));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Kill_after_snapshot_truncation_recovers_snapshot_plus_tail(StoreKind kind)
    {
        var clock = new FakeClock();
        using var harness = new StoreHarness(
            kind,
            options => options.Resume.RetentionWindowSeconds = 0,
            timeProvider: clock);

        for (var i = 0; i < 50; i++)
        {
            var id = i;
            harness.Invoke("seed", ctx =>
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = id % 4, Data = StoreContractTests.MakeBlob(id, 800) }));
            clock.Advance(TimeSpan.FromSeconds(1));
        }

        clock.Advance(TimeSpan.FromMinutes(10));
        harness.Engine.TakeSnapshot();

        for (var i = 50; i < 70; i++)
        {
            var id = i;
            harness.Invoke("tail", ctx =>
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = id % 4, Data = StoreContractTests.MakeBlob(id, 800) }));
        }

        var committed = harness.Dump();
        harness.Engine.Dispose();
        using (var stream = new FileStream(Path.Combine(harness.Options.CommitLog.Path, "melange.log"), FileMode.Append))
            stream.Write(new byte[64]); // Torn tail on top of a truncated log.

        harness.Restart();
        Assert.Equal(committed, harness.Dump());
    }
}
