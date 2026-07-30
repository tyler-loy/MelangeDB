namespace MelangeDB;

/// <summary>
/// A point in time as microseconds since the Unix epoch (UTC). The clock a reducer must use —
/// supplied via <see cref="ReducerContext.Timestamp"/> so transactions stay deterministic and replayable.
/// </summary>
public readonly record struct Timestamp(long UnixTimeMicroseconds) : IComparable<Timestamp>
{
    private const long TicksPerMicrosecond = TimeSpan.TicksPerMicrosecond;

    /// <summary>Converts a <see cref="DateTimeOffset"/> to a timestamp, truncating below microsecond precision.</summary>
    public static Timestamp FromDateTimeOffset(DateTimeOffset value) =>
        new((value.UtcTicks - DateTimeOffset.UnixEpoch.Ticks) / TicksPerMicrosecond);

    /// <summary>Converts this timestamp back to a <see cref="DateTimeOffset"/>.</summary>
    public DateTimeOffset ToDateTimeOffset() =>
        DateTimeOffset.UnixEpoch.AddTicks(UnixTimeMicroseconds * TicksPerMicrosecond);

    public int CompareTo(Timestamp other) => UnixTimeMicroseconds.CompareTo(other.UnixTimeMicroseconds);

    public static bool operator <(Timestamp left, Timestamp right) => left.CompareTo(right) < 0;

    public static bool operator >(Timestamp left, Timestamp right) => left.CompareTo(right) > 0;

    public static bool operator <=(Timestamp left, Timestamp right) => left.CompareTo(right) <= 0;

    public static bool operator >=(Timestamp left, Timestamp right) => left.CompareTo(right) >= 0;

    public override string ToString() => ToDateTimeOffset().ToString("O");
}
