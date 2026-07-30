using System.Diagnostics;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// "It's just a null check" — verified once. With no listener attached, instrumentation must not
/// measurably slow the dispatch path. This is a guard-rail (generous bound, not a benchmark);
/// the measured numbers live in the phase 01 commit message.
/// </summary>
[Collection("Telemetry")]
public class InstrumentationOverheadTests
{
    private const int Iterations = 20_000;

    [Fact]
    public void No_listener_instrumentation_costs_no_measurable_throughput()
    {
        // Read-only invocations isolate the dispatch path: no append, no fsync — the telemetry
        // null checks are a meaningful share of what remains, which is the honest comparison.
        using var instrumented = new EngineHarness(telemetryEnabled: true);
        using var disabled = new EngineHarness(telemetryEnabled: false);

        var withTelemetry = Measure(instrumented);
        var withoutTelemetry = Measure(disabled);

        // Same order of magnitude: a real regression (span allocated per call, metric tags built
        // eagerly) is a 10x+ hit; scheduler noise is not.
        Assert.True(
            withTelemetry < withoutTelemetry * 5 + TimeSpan.FromMilliseconds(30).Ticks,
            $"No-listener instrumentation cost too much: {withTelemetry} ticks vs {withoutTelemetry} ticks for {Iterations} invocations.");
    }

    private static long Measure(EngineHarness harness)
    {
        static void Body(ReducerContext ctx)
        {
        }

        for (var i = 0; i < 2_000; i++)
            harness.Invoke("Warmup", Body);

        var best = long.MaxValue;
        for (var run = 0; run < 3; run++)
        {
            var watch = Stopwatch.StartNew();
            for (var i = 0; i < Iterations; i++)
                harness.Invoke("Noop", Body);
            watch.Stop();
            best = Math.Min(best, watch.ElapsedTicks);
        }

        return best;
    }
}
