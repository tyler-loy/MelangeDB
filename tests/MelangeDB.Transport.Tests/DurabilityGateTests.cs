using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The transport's durability gate (road-to-0.2 phase 17): subscription fan-out enqueues a delta
/// under the engine's write lock, possibly before the record's group fsync completes, and the
/// sender must hold it until the log's durable watermark covers it — a client that applied an LSN
/// a crash then untold would silently diverge when recovery re-mints the same LSN for a different
/// record. The test holds the flush hostage through the log's fault-injection seam: while it is
/// held the record is committed, fanned out, and enqueued, and nothing may reach the client.
/// </summary>
public class DurabilityGateTests
{
    [Fact]
    public async Task A_subscriber_receives_no_delta_before_its_record_is_durable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = await TransportTestHost.StartAsync();
        await using var client = host.CreateClient();
        await client.ConnectAsync(ct);
        var subscription = await client.SubscribeAsync("SELECT * FROM Skill", cancellationToken: ct);
        Assert.Equal(0, subscription.Count);

        using var flushEntered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var log = host.Engine.LogFile;
        log.FlushFaultInjection = () =>
        {
            flushEntered.Set();
            Assert.True(release.Wait(TimeSpan.FromSeconds(30), ct), "the test released the hostage flush");
        };

        // The commit blocks in its durability wait; fan-out already ran under the write lock.
        var commit = Task.Run(() => host.Call("AddSkill", 7L, "mining", 1000L, 3), ct);
        Assert.True(flushEntered.Wait(TimeSpan.FromSeconds(30), ct), "the commit reached its flush");

        // Un-durable means undeliverable. The window is generous compared to normal delivery,
        // which lands within a few milliseconds when the gate is absent.
        await Task.Delay(300, ct);
        Assert.Equal(0, subscription.Count);

        release.Set();
        var head = await commit.WaitAsync(TimeSpan.FromSeconds(30), ct);
        await TransportTestHost.WaitUntilAsync(() => subscription.Count == 1, "the released delta to arrive");
        Assert.Equal(0, subscription.Inconsistencies);
        Assert.True(client.LastAckedLsn >= head, "the client acked the now-durable record");
    }
}
