using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The boundary monitor's cooldown maps are suppression windows, not registries: an entry per
/// entity that ever touched a boundary must not outlive its usefulness, or a long-lived shard
/// with entity churn leaks memory one crossing at a time. Driven on a manual clock, because the
/// bound is temporal and the test must not be.
/// </summary>
public class BoundaryMonitorCooldownTests
{
    private sealed class ManualTime : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Fact]
    public void Cooldown_entries_past_the_retention_floor_are_pruned_and_fresh_ones_survive()
    {
        var root = Directory.CreateTempSubdirectory("melange-monitor-").FullName;
        try
        {
            var options = new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(root, "hot") },
                CommitLog = { Path = Path.Combine(root, "log") },
                Events = { DeadLetterPath = Path.Combine(root, "deadletter") },
            };
            var optionsMonitor = new SpatialStrategyTests.StaticOptionsMonitor(options);
            var schema = SchemaRegistry.FromTypes(typeof(Terrain), typeof(PlayerPos), typeof(Pack), typeof(Critter));
            using var engine = new MelangeEngine(options, schema);
            var strategy = new SpatialShardStrategy(
                schema,
                new SpatialGeometry { BlockWidthChunks = 4, BlockHeightChunks = 4, DecodeChunk = Chunks.At },
                static _ => SpatialShardStrategy.ShardOfBlock(0, 0),
                optionsMonitor);
            var time = new ManualTime();
            using var monitor = new BoundaryMonitor(
                engine,
                SpatialShardStrategy.ShardOfBlock(0, 0),
                strategy,
                new SpatialAnchors(),
                static () => null,
                static () => 0,
                static (_, _) => null,
                static (_, _) => false,
                optionsMonitor,
                time);

            // An old entity's cooldowns, then two minutes pass, then a fresh entity's.
            var stale = TestTokens.IdentityOf("long-gone");
            var fresh = TestTokens.IdentityOf("still-here");
            var neighbour = SpatialShardStrategy.ShardOfBlock(1, 0);
            monitor.SeedCooldownsForTest(stale, neighbour, time.GetUtcNow().UtcTicks);
            time.Now += TimeSpan.FromMinutes(2);
            monitor.SeedCooldownsForTest(fresh, neighbour, time.GetUtcNow().UtcTicks);
            Assert.Equal(4, monitor.CooldownEntryCount);

            // The prune drops exactly the entries no cooldown window could ever consult again.
            monitor.PruneCooldowns();
            Assert.Equal(2, monitor.CooldownEntryCount);

            // And once the fresh entries age past the floor, they go too — the maps are bounded
            // by recent activity, never by history.
            time.Now += TimeSpan.FromMinutes(2);
            monitor.PruneCooldowns();
            Assert.Equal(0, monitor.CooldownEntryCount);
        }
        finally
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }
}
