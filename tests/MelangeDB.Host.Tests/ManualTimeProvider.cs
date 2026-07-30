namespace MelangeDB.Host.Tests;

/// <summary>
/// A hand-cranked <see cref="TimeProvider"/>: time moves only when a test calls
/// <see cref="Advance"/>, and due timers fire synchronously on the advancing thread. This is what
/// keeps the scheduler tests deterministic — no wall-clock sleeps anywhere.
/// </summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private readonly Lock _lock = new();
    private readonly List<ManualTimer> _timers = [];
    private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow()
    {
        lock (_lock)
        {
            return _now;
        }
    }

    public override long GetTimestamp() => GetUtcNow().UtcTicks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(this, callback, state);
        timer.Change(dueTime, period);
        lock (_lock)
        {
            _timers.Add(timer);
        }

        return timer;
    }

    public void Advance(TimeSpan by)
    {
        DateTimeOffset target;
        lock (_lock)
        {
            target = _now + by;
        }

        while (true)
        {
            ManualTimer? next = null;
            lock (_lock)
            {
                foreach (var timer in _timers)
                {
                    if (timer.DueAt is { } due && due <= target && (next?.DueAt is not { } bestDue || due < bestDue))
                        next = timer;
                }

                // Monotonic on purpose: a timer callback may itself advance the clock (the
                // overrun tests simulate a slow tick that way), and the outer advance must not
                // rewind past what the nested one established.
                var candidate = next?.DueAt ?? target;
                if (candidate > _now)
                    _now = candidate;
            }

            if (next is null)
                return;
            next.Fire();
        }
    }

    private void Remove(ManualTimer timer)
    {
        lock (_lock)
        {
            _timers.Remove(timer);
        }
    }

    private sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        private TimeSpan _period = Timeout.InfiniteTimeSpan;

        public DateTimeOffset? DueAt { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._lock)
            {
                _period = period;
                DueAt = dueTime == Timeout.InfiniteTimeSpan ? null : owner._now + dueTime;
            }

            return true;
        }

        public void Fire()
        {
            lock (owner._lock)
            {
                DueAt = _period == Timeout.InfiniteTimeSpan ? null : owner._now + _period;
            }

            callback(state);
        }

        public void Dispose() => owner.Remove(this);

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
