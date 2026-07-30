using System.Collections.Concurrent;

namespace MelangeDB.Core;

/// <summary>
/// Token buckets per (identity, reducer). Refill is computed from elapsed time on each attempt —
/// no timers — through <see cref="TimeProvider"/>, so tests drive it deterministically. Buckets
/// are created full: bursts up to <c>RateLimit:BurstCapacity</c> pass at human click speed, and
/// the sustained rate is what stops macros.
/// </summary>
internal sealed class ReducerRateLimiter(TimeProvider time)
{
    private readonly ConcurrentDictionary<(Identity Caller, string Reducer), Bucket> _buckets = new();

    /// <summary>Takes one token from the caller's bucket for <paramref name="reducer"/>, or refuses.</summary>
    public bool TryAcquire(Identity caller, string reducer, RateLimitOptions options)
    {
        var rate = options.PerReducer.TryGetValue(reducer, out var perReducer)
            ? perReducer
            : options.ReducerCallsPerSecond;
        if (rate <= 0)
            return false;

        var bucket = _buckets.GetOrAdd((caller, reducer), _ => new Bucket(options.BurstCapacity, time.GetTimestamp()));
        return bucket.TryTake(rate, options.BurstCapacity, time);
    }

    private sealed class Bucket(double tokens, long refilledAt)
    {
        private double _tokens = tokens;
        private long _refilledAt = refilledAt;

        public bool TryTake(int ratePerSecond, int capacity, TimeProvider time)
        {
            lock (this)
            {
                var now = time.GetTimestamp();
                var elapsedSeconds = (double)(now - _refilledAt) / time.TimestampFrequency;
                if (elapsedSeconds > 0)
                    _tokens = Math.Min(capacity, _tokens + (elapsedSeconds * ratePerSecond));
                _refilledAt = now;
                if (_tokens < 1)
                    return false;
                _tokens -= 1;
                return true;
            }
        }
    }
}
