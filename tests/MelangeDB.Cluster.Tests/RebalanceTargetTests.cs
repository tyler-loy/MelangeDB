using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Where a rebalance move may land. Issue #204: a shard whose window held no heartbeat sample (a
/// late heartbeat, or a shard just moved in) was left out of its node's sum, so the node read as
/// idle. The loop then moved a hot shard onto an equally hot node, and the scale-in curve test
/// saw two drains where it expected one. Unknown load is not zero load.
/// </summary>
public class RebalanceTargetTests
{
    [Fact]
    public void A_node_with_a_shard_of_unknown_load_is_not_a_target_however_idle_it_reads()
    {
        var utilization = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["node-a"] = 0.9, // the origin
            ["node-b"] = 0.0, // reads idle only because its shard had no sample in the window
            ["node-c"] = 0.4,
        };

        var target = HubRuntime.LeastLoadedObservedTarget(utilization, "node-a", new HashSet<string>(StringComparer.Ordinal) { "node-b" });

        Assert.NotNull(target);
        Assert.Equal("node-c", target.Value.Key);
    }

    [Fact]
    public void No_target_when_every_other_node_is_partially_observed()
    {
        var utilization = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["node-a"] = 0.9,
            ["node-b"] = 0.0,
        };

        Assert.Null(HubRuntime.LeastLoadedObservedTarget(utilization, "node-a", new HashSet<string>(StringComparer.Ordinal) { "node-b" }));
    }

    [Fact]
    public void A_fresh_node_with_no_shards_is_fully_observed_and_is_the_target()
    {
        // A provisioned node owns nothing yet, so there is nothing about it to be unsure of: this
        // is the node scale-out exists to fill, and excluding it would stall every scale-out.
        var utilization = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["node-a"] = 0.9,
            ["node-b"] = 0.8,
            ["node-c"] = 0.0,
        };

        var target = HubRuntime.LeastLoadedObservedTarget(utilization, "node-a", new HashSet<string>(StringComparer.Ordinal));

        Assert.Equal("node-c", target!.Value.Key);
    }
}
