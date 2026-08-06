using System.Collections.Concurrent;

namespace MelangeDB.Core;

/// <summary>
/// Token buckets per (identity, reducer). Refill is computed from elapsed time on each attempt —
/// no timers — through <see cref="TimeProvider"/>, so tests drive it deterministically. Buckets
/// are created full: bursts up to <c>RateLimit:BurstCapacity</c> pass at human click speed, and
/// the sustained rate is what stops macros.
/// <para>
/// Refilled buckets are evicted. A bucket is created full, so a bucket that has refilled to
/// capacity is indistinguishable from one that does not exist — dropping it changes no decision it
/// would ever make, and it stops a long-lived server from holding one entry for every (identity,
/// reducer) pair it has ever seen.
/// </para>
/// </summary>
internal sealed class ReducerRateLimiter(TimeProvider time)
{
    /// <summary>Buckets to accumulate before a sweep is worth its walk.</summary>
    private const int SweepThreshold = 1024;

    /// <summary>Minimum seconds between sweeps, so a busy server does not walk the map per call.</summary>
    private const double SweepIntervalSeconds = 30;

    private readonly ConcurrentDictionary<(Identity Caller, string Reducer), Bucket> _buckets = new();
    private long _lastSweep = time.GetTimestamp();

    /// <summary>Takes one token from the caller's bucket for <paramref name="reducer"/>, or refuses.</summary>
    public bool TryAcquire(Identity caller, string reducer, RateLimitOptions options)
    {
        var rate = options.PerReducer.TryGetValue(reducer, out var perReducer)
            ? perReducer
            : options.ReducerCallsPerSecond;
        if (rate <= 0)
            return false;

        var key = (caller, reducer);
        while (true)
        {
            var bucket = _buckets.GetOrAdd(key, _ => new Bucket(options.BurstCapacity, time.GetTimestamp()));
            var taken = bucket.TryTake(rate, options.BurstCapacity, time);
            if (taken is null)
            {
                // A sweep evicted this bucket between the lookup and the take. It was full when it
                // went, so a fresh one is the same bucket; go around and get it.
                _buckets.TryRemove(new KeyValuePair<(Identity, string), Bucket>(key, bucket));
                continue;
            }

            MaybeSweep(options.BurstCapacity);
            return taken.Value;
        }
    }

    /// <summary>Current bucket count — the eviction is only observable through this.</summary>
    internal int BucketCount => _buckets.Count;

    /// <summary>Sweeps unconditionally. Test seam; production reaches this through the thresholds.</summary>
    internal void Sweep(int capacity)
    {
        foreach (var (key, bucket) in _buckets)
        {
            if (bucket.TryEvict(capacity, time))
                _buckets.TryRemove(new KeyValuePair<(Identity, string), Bucket>(key, bucket));
        }
    }

    private void MaybeSweep(int capacity)
    {
        if (_buckets.Count < SweepThreshold)
            return;

        var now = time.GetTimestamp();
        var last = Interlocked.Read(ref _lastSweep);
        if ((double)(now - last) / time.TimestampFrequency < SweepIntervalSeconds)
            return;

        // One sweeper at a time; a caller that loses the exchange has nothing to do.
        if (Interlocked.CompareExchange(ref _lastSweep, now, last) != last)
            return;

        Sweep(capacity);
    }

    private sealed class Bucket(double tokens, long refilledAt)
    {
        private double _tokens = tokens;
        private long _refilledAt = refilledAt;
        private bool _evicted;

        /// <summary>
        /// The rate the last take used. The sweep has to refill before it can tell a full bucket
        /// from an idle one — an idle bucket's token count is stale and low, which is precisely the
        /// bucket worth evicting — and the rate is per reducer, so the bucket has to carry it.
        /// </summary>
        private int _ratePerSecond;

        /// <summary>
        /// Takes a token, or null when this bucket has been evicted and the caller must fetch the
        /// replacement. Marking under the same lock the take uses is what keeps eviction free of
        /// consequence: a bucket cannot be swept out from under a take that already consumed from it.
        /// </summary>
        public bool? TryTake(int ratePerSecond, int capacity, TimeProvider time)
        {
            lock (this)
            {
                if (_evicted)
                    return null;
                _ratePerSecond = ratePerSecond;
                Refill(ratePerSecond, capacity, time);
                if (_tokens < 1)
                    return false;
                _tokens -= 1;
                return true;
            }
        }

        /// <summary>Retires this bucket if it has refilled to capacity, so it carries no state to lose.</summary>
        public bool TryEvict(int capacity, TimeProvider time)
        {
            lock (this)
            {
                if (_evicted || _ratePerSecond <= 0)
                    return false;

                // Refill first. A bucket that has sat untouched holds the token count it had when
                // it was last used, which is the low one that made it interesting; what decides
                // eviction is what it would hand out now.
                Refill(_ratePerSecond, capacity, time);
                if (_tokens < capacity)
                    return false;
                _evicted = true;
                return true;
            }
        }

        private void Refill(int ratePerSecond, int capacity, TimeProvider time)
        {
            var now = time.GetTimestamp();
            var elapsedSeconds = (double)(now - _refilledAt) / time.TimestampFrequency;
            if (elapsedSeconds > 0)
                _tokens = Math.Min(capacity, _tokens + (elapsedSeconds * ratePerSecond));
            _refilledAt = now;
        }
    }
}
