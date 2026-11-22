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

        // The headline metric must have measured something: a matching bug once left it at
        // "no samples" while every other number looked healthy.
        var p50 = double.Parse(Regex.Match(summary, @"\bcall_to_delta_ms_p50=([\d.]+)").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(p50 > 0, $"call-to-delta must have samples: {summary}");
        var handoffs = Regex.Match(summary, @"\bhandoffs_completed=(\d+)").Groups[1].Value;
        Assert.False(string.IsNullOrEmpty(handoffs), $"handoffs_completed must be numeric (stats reachable): {summary}");
        Assert.True(long.Parse(handoffs, System.Globalization.CultureInfo.InvariantCulture) >= 1,
            $"at least one seam walker must have completed a handoff: {summary}");
    }

    /// <summary>
    /// --aoi: every player holds BETWEEN strips and rescopes them on each step, through the
    /// gateway and across handoff swaps. The player's own row must keep arriving on a strip, and
    /// the rescoped caches must stay consistent.
    /// </summary>
    [Fact]
    public async Task An_area_of_interest_run_rescopes_and_keeps_measuring()
    {
        using var console = new StringWriter();
        var exitCode = await LoadTestTool.RunAsync(
            ["all", "--smoke", "--in-process-server", "--aoi", "1"],
            console,
            TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromMinutes(5), TestContext.Current.CancellationToken);
        var transcript = console.ToString();
        output.WriteLine(transcript);

        Assert.Equal(0, exitCode);
        var summary = transcript.Split('\n')
            .Select(static line => line.TrimEnd('\r'))
            .Single(static line => line.StartsWith("LOADTEST RESULT=", StringComparison.Ordinal));
        Assert.StartsWith("LOADTEST RESULT=PASS", summary, StringComparison.Ordinal);
        Assert.Contains("aoi_radius=1", summary, StringComparison.Ordinal);
        Assert.True(long.Parse(Regex.Match(summary, @"\brescopes=(\d+)").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture) > 0,
            $"walkers must have rescoped: {summary}");
        Assert.True(double.Parse(Regex.Match(summary, @"\bcall_to_delta_ms_p50=([\d.]+)").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture) > 0,
            $"self-deltas must arrive on the strips: {summary}");
        Assert.Contains("inconsistencies=0", summary, StringComparison.Ordinal);
    }
}

/// <summary>
/// The failover subcommand's smoke bar: a tiny shard, one kill, short timeouts. Asserts the run
/// completes with the shard writable on the survivor and every client restored — that the
/// harness measures a window at all — and nothing about how long it was.
/// </summary>
public class FailoverSmokeTests(ITestOutputHelper output)
{
    [Fact]
    public async Task A_failover_run_kills_the_owner_and_every_client_comes_back()
    {
        using var console = new StringWriter();
        var exitCode = await LoadTestTool.RunAsync(
            ["failover", "--rows", "500", "--tail", "50", "--repeat", "1", "--clients", "2",
             "--heartbeat-ms", "100", "--failure-timeout-ms", "1000"],
            console,
            TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromMinutes(3), TestContext.Current.CancellationToken);
        var transcript = console.ToString();
        output.WriteLine(transcript);

        Assert.Equal(0, exitCode);
        var summary = transcript.Split('\n')
            .Select(static line => line.TrimEnd('\r'))
            .Single(static line => line.StartsWith("FAILOVER ", StringComparison.Ordinal));
        Assert.Contains("kills=1", summary, StringComparison.Ordinal);
        Assert.Contains("clients_unrestored=0", summary, StringComparison.Ordinal);
        Assert.Matches(@"\bserver_window_ms=\d+", summary);
    }
}
