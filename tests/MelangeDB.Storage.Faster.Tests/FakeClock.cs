namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// A hand-cranked clock for the retention-window tests: time moves only when a test says so, which
/// makes "records younger than the window are pinned" an exact assertion rather than a race.
/// </summary>
internal sealed class FakeClock : TimeProvider
{
    private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
}
