using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The limiter kept one bucket per (identity, reducer) forever, so a long-lived server holding
/// many identities grew a dictionary it never shrank. A refilled bucket is evictable precisely
/// because buckets are created full: dropping one changes no decision it would ever make.
/// </summary>
public class RateLimiterEvictionTests
{
    private static readonly RateLimitOptions Options = new() { ReducerCallsPerSecond = 10, BurstCapacity = 5 };

    [Fact]
    public void A_bucket_that_has_refilled_is_dropped()
    {
        var time = new SweepClock();
        var limiter = new ReducerRateLimiter(time);
        for (var i = 0; i < 50; i++)
            Assert.True(limiter.TryAcquire(Identity.Hash($"caller-{i}"), "Move", Options));
        Assert.Equal(50, limiter.BucketCount);

        // Long enough for a burst of five at ten per second to be fully back.
        time.Advance(TimeSpan.FromSeconds(10));
        limiter.Sweep(Options.BurstCapacity);

        Assert.Equal(0, limiter.BucketCount);
    }

    [Fact]
    public void A_bucket_still_paying_off_a_burst_survives_the_sweep()
    {
        var time = new SweepClock();
        var limiter = new ReducerRateLimiter(time);
        var spender = Identity.Hash("spender");
        for (var i = 0; i < Options.BurstCapacity; i++)
            Assert.True(limiter.TryAcquire(spender, "Move", Options));

        limiter.Sweep(Options.BurstCapacity);

        Assert.Equal(1, limiter.BucketCount);
    }

    [Fact]
    public void Eviction_does_not_hand_a_rate_limited_caller_a_fresh_burst()
    {
        // The property that makes eviction safe. A caller who has spent their burst must not be
        // able to get it back by being swept: only a bucket that has already refilled is dropped,
        // so the sweep can never be a way through the limit.
        var time = new SweepClock();
        var limiter = new ReducerRateLimiter(time);
        var spender = Identity.Hash("spender");
        for (var i = 0; i < Options.BurstCapacity; i++)
            Assert.True(limiter.TryAcquire(spender, "Move", Options));
        Assert.False(limiter.TryAcquire(spender, "Move", Options));

        limiter.Sweep(Options.BurstCapacity);

        Assert.False(limiter.TryAcquire(spender, "Move", Options));
    }

    [Fact]
    public void Buckets_for_different_reducers_are_evicted_independently()
    {
        var time = new SweepClock();
        var limiter = new ReducerRateLimiter(time);
        var caller = Identity.Hash("caller");
        Assert.True(limiter.TryAcquire(caller, "Move", Options));

        time.Advance(TimeSpan.FromSeconds(10));
        for (var i = 0; i < Options.BurstCapacity; i++)
            Assert.True(limiter.TryAcquire(caller, "Attack", Options));

        limiter.Sweep(Options.BurstCapacity);

        // Move refilled while Attack was being spent, so exactly one bucket should remain.
        Assert.Equal(1, limiter.BucketCount);
        Assert.False(limiter.TryAcquire(caller, "Attack", Options));
    }

    /// <summary>A hand-cranked monotonic clock; the limiter reads only timestamps.</summary>
    private sealed class SweepClock : TimeProvider
    {
        private long _timestamp = 1_000_000;

        public override long TimestampFrequency => 1_000_000;

        public override long GetTimestamp() => _timestamp;

        public void Advance(TimeSpan by) => _timestamp += (long)(by.TotalSeconds * TimestampFrequency);
    }
}
