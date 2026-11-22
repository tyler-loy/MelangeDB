namespace MelangeDB.Testing;

/// <summary>
/// A hand-cranked <see cref="TimeProvider"/>: time moves only when a test moves it, and every timer
/// that comes due fires <b>synchronously, on the advancing thread, in due order</b>, before
/// <see cref="Advance"/> returns. That is the guarantee scheduled-reducer tests rest on: after
/// <c>Advance(TimeSpan.FromSeconds(60))</c> every tick due in that minute has fired and committed,
/// with no sleeps and no races. <see cref="MelangeWorld"/> runs on one; it is public so a test that
/// composes its own host can use it too.
/// </summary>
public sealed class ManualClock : TimeProvider
{
    /// <summary>The default start: a fixed instant, so a test's timestamps do not depend on when it ran.</summary>
    public static readonly DateTimeOffset DefaultStart = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly Lock _lock = new();
    private readonly List<ManualTimer> _timers = [];
    private readonly List<TimeSpan> _armedDelays = [];
    private DateTimeOffset _now;

    public ManualClock()
        : this(DefaultStart)
    {
    }

    public ManualClock(DateTimeOffset start) => _now = start;

    /// <summary>
    /// Every finite delay a timer was armed or re-armed with, in order: a diagnostic for tests
    /// that assert on when something chose to wake up, not only on what happened.
    /// </summary>
    public IReadOnlyList<TimeSpan> ArmedDelays
    {
        get
        {
            lock (_lock)
            {
                return [.. _armedDelays];
            }
        }
    }

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
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ManualTimer(this, callback, state);
        timer.Change(dueTime, period);
        lock (_lock)
        {
            _timers.Add(timer);
        }

        return timer;
    }

    /// <summary>
    /// Moves time forward by <paramref name="by"/>, firing every timer that comes due on the way,
    /// earliest first, each at its own due time (so a callback reads the clock it was due at).
    /// A repeating timer fires as often as its period fits.
    /// </summary>
    public void Advance(TimeSpan by)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(by, TimeSpan.Zero);
        DateTimeOffset target;
        lock (_lock)
        {
            target = _now + by;
        }

        AdvanceToCore(target);
    }

    /// <summary>Moves time forward to <paramref name="instant"/>, as <see cref="Advance"/> does. Never moves it back.</summary>
    public void AdvanceTo(DateTimeOffset instant) => AdvanceToCore(instant);

    /// <summary>
    /// Moves the clock without firing anything: a stalled timer thread, a long GC pause, a frozen
    /// VM. The next <see cref="Advance"/>, even by zero, fires the overdue timers at the jumped-to
    /// time, as a resumed process would.
    /// </summary>
    public void Jump(TimeSpan by)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(by, TimeSpan.Zero);
        lock (_lock)
        {
            _now += by;
        }
    }

    private void AdvanceToCore(DateTimeOffset target)
    {
        while (true)
        {
            ManualTimer? next = null;
            lock (_lock)
            {
                foreach (var timer in _timers)
                {
                    if (timer.DueAt is { } due && due <= target && (next?.DueAt is not { } best || due < best))
                        next = timer;
                }

                // Never rewinds: a callback may itself advance the clock (a slow tick, simulated),
                // and the outer advance must not undo it; a timer left overdue by Jump fires at
                // the jumped-to now.
                var to = next?.DueAt ?? target;
                if (to > _now)
                    _now = to;
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

    private sealed class ManualTimer(ManualClock owner, TimerCallback callback, object? state) : ITimer
    {
        private TimeSpan _period = Timeout.InfiniteTimeSpan;

        public DateTimeOffset? DueAt { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._lock)
            {
                _period = period;
                DueAt = dueTime == Timeout.InfiniteTimeSpan ? null : owner._now + dueTime;
                if (dueTime != Timeout.InfiniteTimeSpan)
                    owner._armedDelays.Add(dueTime);
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
