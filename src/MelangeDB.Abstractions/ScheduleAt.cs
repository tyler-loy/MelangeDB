namespace MelangeDB;

/// <summary>
/// When a timer row fires: a one-shot instant, or a repeating interval. The discriminated column
/// type a table declaring <c>Scheduled</c> carries exactly one of. A one-shot timer's row is
/// deleted transactionally with its fire — the row <em>is</em> the schedule; a repeating timer's
/// row is written only when created, changed, or deleted, and its next fire is derived from the
/// interval rather than persisted per fire (see docs/road-to-0.1/plan-phase-05.md on write amplification).
/// </summary>
public readonly record struct ScheduleAt
{
    private readonly long _microseconds;
    private readonly bool _interval;

    private ScheduleAt(long microseconds, bool interval)
    {
        _microseconds = microseconds;
        _interval = interval;
    }

    /// <summary>A one-shot schedule: fire once at <paramref name="at"/>, then delete the row.</summary>
    public static ScheduleAt Instant(Timestamp at) => new(at.UnixTimeMicroseconds, interval: false);

    /// <summary>
    /// A repeating schedule: fire every <paramref name="every"/>, truncated to microsecond
    /// precision, starting one interval after the row commits.
    /// </summary>
    public static ScheduleAt Interval(TimeSpan every)
    {
        var microseconds = every.Ticks / TimeSpan.TicksPerMicrosecond;
        ArgumentOutOfRangeException.ThrowIfLessThan(microseconds, 1, nameof(every));
        return new ScheduleAt(microseconds, interval: true);
    }

    /// <summary>Whether this is a repeating interval rather than a one-shot instant.</summary>
    public bool IsInterval => _interval;

    /// <summary>The one-shot fire time. Meaningful only when <see cref="IsInterval"/> is false.</summary>
    public Timestamp DueAt => new(_interval ? 0 : _microseconds);

    /// <summary>The repeat interval. Meaningful only when <see cref="IsInterval"/> is true.</summary>
    public TimeSpan Every => _interval ? TimeSpan.FromTicks(_microseconds * TimeSpan.TicksPerMicrosecond) : TimeSpan.Zero;

    /// <summary>Reconstructs a value from its stored wire form — serializer plumbing, unvalidated.</summary>
    public static ScheduleAt FromMicroseconds(bool interval, long microseconds) => new(microseconds, interval);

    /// <summary>The stored discriminant payload — serializer plumbing.</summary>
    public long Microseconds => _microseconds;

    public static implicit operator ScheduleAt(Timestamp at) => Instant(at);

    public static implicit operator ScheduleAt(TimeSpan every) => Interval(every);

    public override string ToString() =>
        _interval ? $"every {Every}" : $"at {DueAt}";
}
