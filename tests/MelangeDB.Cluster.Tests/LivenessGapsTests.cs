using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The liveness gap record phase 30's timing soak reads: one run at a long failure timeout says how
/// many gaps each shorter timeout would have turned into a death.
/// </summary>
public class LivenessGapsTests
{
    [Fact]
    public void Gaps_are_counted_against_every_threshold_they_exceed()
    {
        var gaps = new LivenessGaps();
        gaps.Record(TimeSpan.FromMilliseconds(260));
        gaps.Record(TimeSpan.FromMilliseconds(1_200));
        gaps.Record(TimeSpan.FromMilliseconds(2_000)); // Exactly at a threshold is not over it.
        gaps.Record(TimeSpan.FromMilliseconds(3_400));

        Assert.Equal(4, gaps.Count);
        Assert.Equal(3_400, gaps.MaxMs, precision: 3);
        Assert.Equal(3, gaps.Over(500));
        Assert.Equal(3, gaps.Over(1_000));
        Assert.Equal(2, gaps.Over(1_500));
        Assert.Equal(1, gaps.Over(2_000));
        Assert.Equal(1, gaps.Over(3_000));
        Assert.Equal(0, gaps.Over(5_000));
        Assert.Throws<ArgumentOutOfRangeException>(() => gaps.Over(1_234));
    }
}
