using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The token bucket per identity per reducer: over-limit calls are rejected BEFORE a transaction
/// opens — asserted by the log head, not just the error — bursts pass, sustained rates refill on
/// the clock, per-reducer overrides apply, and in-process dispatch is never throttled.
/// </summary>
public class RateLimitTests
{
    [Fact]
    public async Task An_over_limit_call_is_rejected_before_any_transaction_opens()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:RateLimit:ReducerCallsPerSecond"] = "1",
            ["MelangeDb:RateLimit:BurstCapacity"] = "2",
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = "10000000",
        }, manualTime: true);

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        // The burst passes at human click speed.
        for (uint i = 1; i <= 2; i++)
        {
            await raw.SendAsync(
                new CallReducerFrame(i, "SetChunk", ReducerArgs.Encode([(long)i, 1L, new byte[] { 1 }]), null),
                TestContext.Current.CancellationToken);
            var ok = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == i);
            Assert.True(ok.Ok, ok.Message);
        }

        var headAfterBurst = host.Engine.Log.HeadLsn;

        // The macro does not: rejected, with no log record appended — the write never existed.
        await raw.SendAsync(
            new CallReducerFrame(3, "SetChunk", ReducerArgs.Encode([3L, 1L, new byte[] { 1 }]), null),
            TestContext.Current.CancellationToken);
        var rejected = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == 3);
        Assert.False(rejected.Ok);
        Assert.Equal(MelangeErrorCodes.RateLimited, rejected.ErrorCode);
        Assert.Equal(headAfterBurst, host.Engine.Log.HeadLsn);

        // One second of refill buys exactly one more call.
        host.Time!.Advance(TimeSpan.FromSeconds(1));
        await raw.SendAsync(
            new CallReducerFrame(4, "SetChunk", ReducerArgs.Encode([4L, 1L, new byte[] { 1 }]), null),
            TestContext.Current.CancellationToken);
        var refilled = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == 4);
        Assert.True(refilled.Ok, refilled.Message);
    }

    [Fact]
    public async Task A_per_reducer_override_gets_its_own_rate()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:RateLimit:ReducerCallsPerSecond"] = "1",
            ["MelangeDb:RateLimit:BurstCapacity"] = "1",
            ["MelangeDb:RateLimit:PerReducer:Noop"] = "1000",
            ["MelangeDb:Transport:HeartbeatTimeoutMs"] = "10000000",
        }, manualTime: true);

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);

        // Noop's override refills fast enough that repeated calls keep passing...
        for (uint i = 1; i <= 5; i++)
        {
            host.Time!.Advance(TimeSpan.FromMilliseconds(10));
            await raw.SendAsync(new CallReducerFrame(i, "Noop", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
            var ok = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == i);
            Assert.True(ok.Ok, ok.Message);
        }

        // ...while the global 1/s bucket refuses a second SetChunk in the same window.
        await raw.SendAsync(new CallReducerFrame(10, "SetChunk", ReducerArgs.Encode([1L, 1L, new byte[] { 1 }]), null), TestContext.Current.CancellationToken);
        Assert.True((await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == 10)).Ok);
        await raw.SendAsync(new CallReducerFrame(11, "SetChunk", ReducerArgs.Encode([2L, 1L, new byte[] { 1 }]), null), TestContext.Current.CancellationToken);
        var limited = await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == 11);
        Assert.Equal(MelangeErrorCodes.RateLimited, limited.ErrorCode);
    }

    [Fact]
    public async Task In_process_dispatch_is_never_throttled()
    {
        // Rate limiting defends against clients; the host's own code (schedulers, workers,
        // tests) is not a client.
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:RateLimit:ReducerCallsPerSecond"] = "1",
            ["MelangeDb:RateLimit:BurstCapacity"] = "1",
        });
        for (var i = 0; i < 20; i++)
            host.Call("Noop");
    }

    [Fact]
    public async Task Disabling_the_limiter_lets_everything_through()
    {
        await using var host = await TransportTestHost.StartAsync(new Dictionary<string, string?>
        {
            ["MelangeDb:RateLimit:Enabled"] = "false",
            ["MelangeDb:RateLimit:ReducerCallsPerSecond"] = "1",
            ["MelangeDb:RateLimit:BurstCapacity"] = "1",
        });

        await using var raw = new RawSocketClient();
        await raw.ConnectAsync(host.WsUri, TestContext.Current.CancellationToken);
        for (uint i = 1; i <= 10; i++)
        {
            await raw.SendAsync(new CallReducerFrame(i, "Noop", ReducerArgs.Encode([]), null), TestContext.Current.CancellationToken);
            Assert.True((await raw.ReceiveUntilAsync<ReducerResultFrame>(TestContext.Current.CancellationToken, f => f.RequestId == i)).Ok);
        }
    }
}
