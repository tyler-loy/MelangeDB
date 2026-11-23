using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The rebalance loop's choice of which shard to move, on fixed loads. The integration test in
/// <see cref="RebalanceLoopTests"/> exercises it through real heartbeats; these pin the arithmetic,
/// including the readings that made the loop move an idle shard.
/// </summary>
public sealed class RebalanceMoveTests
{
    private static readonly ShardKey A = new(80);
    private static readonly ShardKey B = new(81);
    private static readonly ShardKey C = new(82);

    [Fact]
    public void A_shard_whose_load_is_noise_is_not_moved_between_level_nodes()
    {
        // Recorded from a failing run: shard 80 had just moved to this node, and shard 81, which
        // nothing wrote to, read 0.004% for the heartbeat after its own load-sampling pass. The
        // nodes were level to within 0.01%.
        var loads = new Dictionary<ShardKey, double> { [A] = 0.02360, [B] = 4.153e-5 };
        bool OutsideFloor(ShardKey shard) => shard != A;

        Assert.Null(HubRuntime.ChooseRebalanceMove([A, B], loads, origin: 0.02365, target: 0.02357, minGain: 0.1, OutsideFloor));

        // The old rule, a strict improvement of any size, took it.
        Assert.Equal(B, HubRuntime.ChooseRebalanceMove([A, B], loads, origin: 0.02365, target: 0.02357, minGain: 0, OutsideFloor));
    }

    [Fact]
    public void Two_busy_shards_on_one_node_are_split()
    {
        var loads = new Dictionary<ShardKey, double> { [A] = 0.05175, [C] = 0.05080 };

        Assert.Equal(A, HubRuntime.ChooseRebalanceMove([A, C], loads, origin: 0.10255, target: 0, minGain: 0.1, static _ => true));
    }

    [Fact]
    public void A_whole_hotspot_is_not_relocated()
    {
        var loads = new Dictionary<ShardKey, double> { [A] = 0.9, [B] = 0 };

        Assert.Null(HubRuntime.ChooseRebalanceMove([A, B], loads, origin: 0.9, target: 0, minGain: 0, static _ => true));
    }

    [Fact]
    public void A_shard_that_would_overshoot_is_passed_over_for_one_that_helps()
    {
        // Moving A leaves the target at 1.2, worse than the origin's 1.0; moving C leaves the pair
        // at 0.8, a gain of 0.2.
        var loads = new Dictionary<ShardKey, double> { [A] = 0.7, [C] = 0.3 };

        Assert.Equal(C, HubRuntime.ChooseRebalanceMove([A, C], loads, origin: 1.0, target: 0.5, minGain: 0.1, static _ => true));
        Assert.Null(HubRuntime.ChooseRebalanceMove([A, C], loads, origin: 1.0, target: 0.5, minGain: 0.25, static _ => true));
    }

    [Fact]
    public void A_shard_inside_its_move_floor_is_not_moved()
    {
        var loads = new Dictionary<ShardKey, double> { [A] = 0.5, [C] = 0.4 };

        Assert.Equal(C, HubRuntime.ChooseRebalanceMove([A, C], loads, origin: 0.9, target: 0, minGain: 0.1, shard => shard != A));
    }
}
