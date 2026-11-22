using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// An entity that outruns its own handoff (phase 31): a shard's write carrying a row it owned past
/// its border band is a retry, not a contract violation, so it is refused as transient and counted.
/// Anything else that resolves past the band is still a span violation.
/// </summary>
public class BandOverrunTests
{
    private static readonly ShardKey Home = SpatialShardStrategy.ShardOfBlock(0, 0);

    private static (MelangeEngine Engine, string Root, Func<long> Overruns) CreateEngine()
    {
        var root = Directory.CreateTempSubdirectory("melange-overrun-").FullName;
        var registry = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables());
        var engine = new MelangeEngine(
            new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(root, "hot") },
                CommitLog = { Path = Path.Combine(root, "log") },
            },
            registry);
        var strategy = new SpatialShardStrategy(
            engine.Schema,
            new SpatialGeometry { BlockWidthChunks = 4, BlockHeightChunks = 4, DecodeChunk = Chunks.At },
            static _ => Home,
            new SpatialStrategyTests.StaticOptionsMonitor(new MelangeDbOptions { Cluster = { BorderBandChunks = 2, HandoffMarginChunks = 1 } }));
        long overruns = 0;
        engine.AddCommitGuard(new ShardCommitGuard(
            engine.Schema,
            () => engine.HotStore,
            Home,
            strategy,
            spanCheckEnabled: static () => true,
            leaseValid: static () => true,
            frozenRows: static () => new HashSet<(TableId, RowKey)>(),
            borrowedOwner: static (_, _) => null,
            bandOverrun: () => Interlocked.Increment(ref overruns)));
        return (engine, root, () => Interlocked.Read(ref overruns));
    }

    private static void MoveTo(MelangeEngine engine, int cx, int cy) =>
        engine.Invoke("Move", ClusterFixture.Caller, ctx =>
            ctx.Db.Update(new PlayerPos { PlayerId = ClusterFixture.Caller, ChunkId = Chunks.Id(cx, cy), Steps = 1 }));

    [Fact]
    public void An_owned_entity_walking_past_the_band_is_refused_as_transient_and_counted()
    {
        using var metrics = new MetricCapture("melange.shard.band_overruns");
        var (engine, root, overruns) = CreateEngine();
        try
        {
            engine.Invoke("Spawn", ClusterFixture.Caller, ctx =>
                ctx.Db.Insert(new PlayerPos { PlayerId = ClusterFixture.Caller, ChunkId = Chunks.Id(3, 0), Steps = 0 }));

            // Two chunks into the neighbour: the seam, still this shard's to write.
            MoveTo(engine, 5, 0);
            var head = engine.Log.HeadLsn;

            // A third chunk is past the band: the handoff has not kept up.
            var refused = Assert.Throws<TransientRejectionException>(() => MoveTo(engine, 6, 0));
            Assert.Contains("past this shard's border band", refused.Message);
            Assert.Contains("retry", refused.Message);
            Assert.IsNotType<ShardSpanException>(refused);
            Assert.Equal(1, overruns());
            Assert.Equal(head, engine.Log.HeadLsn);
            Assert.Contains(metrics.Of("melange.shard.band_overruns"), static m => (string?)m.Tags["table"] == "PlayerPos");
        }
        finally
        {
            engine.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void A_row_the_shard_never_owned_past_the_band_is_still_a_span_violation()
    {
        var (engine, root, overruns) = CreateEngine();
        try
        {
            // An insert has no pre-image here: nothing this shard owned moved, so it is the
            // contract the check exists for, and stays loud.
            Assert.Throws<ShardSpanException>(() => engine.Invoke("Spawn", ClusterFixture.Caller, ctx =>
                ctx.Db.Insert(new PlayerPos { PlayerId = ClusterFixture.Caller, ChunkId = Chunks.Id(9, 0), Steps = 0 })));
            Assert.Equal(0, overruns());
        }
        finally
        {
            engine.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }
}
