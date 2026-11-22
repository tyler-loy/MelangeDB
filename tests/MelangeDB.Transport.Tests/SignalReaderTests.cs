using System.Diagnostics.Metrics;
using MelangeDB.Mcp;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The MCP <c>signals</c> tool reads every MelangeDB meter in the process. One observable that throws
/// when polled (a gauge over an engine already disposed, whose meter outlives it) must not fail the
/// whole reading: the others still report.
/// </summary>
public class SignalReaderTests
{
    [Fact]
    public void An_observable_that_throws_does_not_fail_the_reading()
    {
        using var meter = new Meter($"MelangeDB.SignalReaderTests.{Guid.NewGuid():N}");
        using var reader = new SignalReader();
        meter.CreateObservableGauge<int>("melange.test.gone", static int () => throw new ObjectDisposedException("engine"));
        meter.CreateObservableGauge<int>("melange.test.alive", static () => 5);

        var (series, _) = reader.Read("melange.test.", max: 10);

        var alive = Assert.Single(series);
        Assert.Equal("melange.test.alive", alive.Instrument);
        Assert.Equal(5, alive.Value);
    }
}
