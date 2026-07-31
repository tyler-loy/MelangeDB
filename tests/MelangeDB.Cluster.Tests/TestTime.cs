namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Wall-clock dilation for slow test hardware. <c>MELANGE_TEST_TIME_SCALE</c> (default 1)
/// multiplies the deadlines of the suite's wait helpers — CI's shared vCPUs get proportionally
/// more wall clock while every assertion stays identical. Scaling deadlines is honest where
/// loosening assertions would not be: the condition still has to become true, it is just allowed
/// to take longer on hardware that runs everything slower.
/// </summary>
internal static class TestTime
{
    public static int Scale { get; } =
        int.TryParse(Environment.GetEnvironmentVariable("MELANGE_TEST_TIME_SCALE"), out var scale) && scale > 0
            ? scale
            : 1;

    public static TimeSpan Dilated(TimeSpan timeout) => timeout * Scale;
}
