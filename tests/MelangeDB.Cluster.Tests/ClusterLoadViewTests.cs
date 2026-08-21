using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The hub's side of the emptiness signal: which samples reach <see cref="ClusterLoadView"/>'s
/// narrowing, and which are markers rather than measurements.
/// </summary>
public class ClusterLoadViewTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.UnixEpoch.AddDays(20_000);

    private static ShardLoadDto Load(ulong shard, long authoritative, bool draining = false, double utilization = 0.9) =>
        new(shard, utilization, HeadLsn: 10, ResidentBytes: 0, BorrowedRows: 0, AuthoritativeRows: authoritative, Draining: draining);

    [Fact]
    public void A_draining_shard_leaves_the_narrowing_even_though_it_reports_no_rows()
    {
        using var view = new ClusterLoadView();

        // Empty and open: a candidate worth asking about.
        view.Record("a", [Load(1, authoritative: 0)], T0);
        Assert.Contains(1UL, view.ShardsHoldingNothing(T0, TimeSpan.FromSeconds(30)).Select(l => l.Shard.Value));

        // Then it quiesces. The owner reports the mark rather than a measurement, and the shard
        // stops being a candidate — without this the hub keeps the pre-quiesce sample, which says
        // nothing is draining, for a whole freshness window.
        view.Record("a", [Load(1, authoritative: 0, draining: true)], T0.AddSeconds(1));
        Assert.Empty(view.ShardsHoldingNothing(T0.AddSeconds(1), TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void A_draining_marker_does_not_enter_the_utilization_history_the_rebalancer_reads()
    {
        using var view = new ClusterLoadView();
        var window = TimeSpan.FromSeconds(10);

        for (var i = 0; i < 5; i++)
            view.Record("a", [Load(1, authoritative: 5, utilization: 0.8)], T0.AddSeconds(i));

        var before = view.SustainedUtilization(new ShardKey(1), window, T0.AddSeconds(4));

        // A drain marker carries a zero because there is nothing to measure. Letting it into the
        // series would drag the shard's sustained load down and colour the next decision about it.
        view.Record("a", [Load(1, authoritative: 0, draining: true, utilization: 0)], T0.AddSeconds(5));

        Assert.Equal(before, view.SustainedUtilization(new ShardKey(1), window, T0.AddSeconds(5)));
    }
}
