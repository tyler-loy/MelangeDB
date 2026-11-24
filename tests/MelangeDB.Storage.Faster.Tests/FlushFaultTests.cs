using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// A paged store whose disk fails. MelangeLog's flusher hits the error off the writer's thread; the
/// next write that needs a page hears it, the store's apply throws with the record already in the
/// commit log, and the engine closes to writes rather than carry on over a part-applied projection.
/// A restart rebuilds the store from the log.
/// </summary>
public sealed class FlushFaultTests
{
    [Fact]
    public void A_failed_flush_is_rethrown_to_every_writer_that_needs_a_page()
    {
        var directory = Directory.CreateTempSubdirectory("melange-mlog-fault-").FullName;
        using (var kv = new MelangeLogPagedKv(
                   Path.Combine(directory, "fault.mlog"), 12, 3, 14, 16, MelangeLogTuning.Default, 512, failFlushForTest: page => page >= 2))
        {
            var value = new byte[1000];
            var first = Assert.Throws<IOException>(() =>
            {
                for (var i = 0; i < 10_000; i++)
                    kv.Upsert(BitConverter.GetBytes(i), value);
            });
            Assert.Contains("injected flush failure", first.Message);

            // Fail-stop: the space stays failed; nothing quietly resumes over a torn log.
            Assert.Throws<IOException>(() =>
            {
                for (var i = 0; i < 100; i++)
                    kv.Upsert(BitConverter.GetBytes(20_000 + i), value);
            });
        }

        Directory.Delete(directory, recursive: true);
    }

    [Fact]
    public void A_store_that_cannot_flush_closes_the_engine_to_writes_and_a_restart_rebuilds_it()
    {
        var failing = true;
        var provider = new FasterHotStoreProvider(new MelangeLogPagedKvFactory(failFlushForTest: page => Volatile.Read(ref failing) && page >= 4));
        using var harness = new StoreHarness(
            StoreKind.Faster,
            options => options.Residency.Default = Residency.Paged,
            tables: [typeof(TerrainBlob)],
            provider: provider);

        // 64 KiB a row through a 4 MiB blob space: the flusher fails within the first few dozen.
        var acknowledged = 0;
        var unknown = Assert.Throws<CommitOutcomeUnknownException>(() =>
        {
            for (var i = 0; i < 1_000; i++)
            {
                var id = i;
                harness.Invoke("insert", ctx => ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = 0, Data = StoreContractTests.MakeBlob(id, 64 * 1024) }));
                acknowledged++;
            }
        });
        Assert.IsType<IOException>(unknown.InnerException);
        Assert.NotNull(harness.Engine.ProjectionFailure);

        // Closed: the next write is refused before anything is appended, as retryable.
        var refused = Assert.Throws<TransientRejectionException>(() =>
            harness.Invoke("insert", ctx => ctx.Db.Insert(new TerrainBlob { ChunkId = 99_999, Region = 0, Data = [1] })));
        Assert.Contains("hot store", refused.Message);

        // The next process: the store rebuilds from the log, every acknowledged write in it.
        Volatile.Write(ref failing, false);
        harness.Restart();
        Assert.Null(harness.Engine.ProjectionFailure);
        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        var count = harness.Engine.HotStore.Count(table);
        Assert.InRange(count, acknowledged, acknowledged + 1);
        harness.Invoke("verify", ctx =>
        {
            for (var i = 0; i < acknowledged; i++)
                Assert.Equal(StoreContractTests.MakeBlob(i, 64 * 1024), ctx.Db.Find<TerrainBlob>((long)i)!.Value.Data);
        });
        harness.Invoke("insert", ctx => ctx.Db.Insert(new TerrainBlob { ChunkId = 99_999, Region = 0, Data = [1] }));
    }
}
