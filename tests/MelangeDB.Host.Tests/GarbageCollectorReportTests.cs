using MelangeDB.Core;
using Microsoft.Extensions.Logging;
using Xunit;

namespace MelangeDB.Host.Tests;

/// <summary>
/// The startup warning for workstation GC (EventId 1108, road-to-0.4 phase 31): it costs half the
/// delivered throughput under load, and it is the default for a host not built on the Web SDK.
/// </summary>
public class GarbageCollectorReportTests
{
    [Theory]
    [InlineData(false, 16, true)]
    [InlineData(false, 4, true)]
    [InlineData(true, 16, false)]
    [InlineData(false, 2, false)]
    public void Workstation_gc_on_four_or_more_cores_is_reported(bool serverGc, int processors, bool warned)
    {
        var collector = new LogCollector();
        var logger = collector.CreateLogger("test");

        Assert.Equal(warned, MelangeDbHostedService.ReportGarbageCollector(serverGc, processors, logger));
        Assert.Equal(warned, collector.Has(1108));
        if (warned)
        {
            var entry = Assert.Single(collector.Entries);
            Assert.Equal(LogLevel.Warning, entry.Level);
            Assert.Equal("WorkstationGc", entry.EventName);
            Assert.Contains("ServerGarbageCollection", entry.Message, StringComparison.Ordinal);
        }
    }
}
