using System.Text.RegularExpressions;
using MelangeDB.LoadTest;
using Xunit;

namespace MelangeDB.LoadTest.Tests;

/// <summary>
/// The tool's smoke bar: a whole run through the real CLI entry point at <c>--smoke</c> scale —
/// two shard nodes, a handful of players, real websockets — must exit cleanly with a parseable
/// summary, non-zero acked calls, and at least one completed handoff. In-process server on
/// purpose (the test host is the process, so <c>all</c>'s spawn-myself mode would launch the
/// wrong executable); floors, never ceilings, so machine speed cannot flake it.
/// </summary>
public class SmokeTests(ITestOutputHelper output)
{
    [Fact]
    public async Task A_smoke_run_passes_with_acked_calls_and_at_least_one_handoff()
    {
        using var console = new StringWriter();
        var exitCode = await LoadTestTool.RunAsync(
            ["all", "--smoke", "--in-process-server"],
            console,
            TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromMinutes(5), TestContext.Current.CancellationToken);
        var transcript = console.ToString();
        output.WriteLine(transcript);

        Assert.Equal(0, exitCode);
        var summary = transcript.Split('\n')
            .Select(static line => line.TrimEnd('\r'))
            .SingleOrDefault(static line => line.StartsWith("LOADTEST RESULT=", StringComparison.Ordinal));
        Assert.NotNull(summary);

        // The summary is the scripted-use contract: key=value pairs on one line.
        Assert.StartsWith("LOADTEST RESULT=PASS", summary, StringComparison.Ordinal);
        var acked = long.Parse(Regex.Match(summary, @"\backed=(\d+)").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(acked > 0, $"acked calls must be non-zero: {summary}");
        var handoffs = Regex.Match(summary, @"\bhandoffs_completed=(\d+)").Groups[1].Value;
        Assert.False(string.IsNullOrEmpty(handoffs), $"handoffs_completed must be numeric (stats reachable): {summary}");
        Assert.True(long.Parse(handoffs, System.Globalization.CultureInfo.InvariantCulture) >= 1,
            $"at least one seam walker must have completed a handoff: {summary}");
    }
}
