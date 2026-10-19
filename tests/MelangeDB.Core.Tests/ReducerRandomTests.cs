using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// <see cref="ReducerContext.Random"/> (issue #156). It was seeded from
/// <c>(int)micros ^ caller.GetHashCode()</c>, and <see cref="Identity.GetHashCode"/> is randomized
/// per process — so the same call drew different numbers in every process, and two calls by one
/// caller in one microsecond drew identical ones. The default provider must be reproducible across
/// processes and distinct per attempt; a registered provider must be what the body sees.
/// </summary>
public class ReducerRandomTests
{
    private static readonly DateTimeOffset Frozen = new(2026, 10, 19, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Two_calls_in_the_same_microsecond_draw_different_streams()
    {
        using var harness = new EngineHarness(timeProvider: new FrozenClock(Frozen));
        var first = Draw(harness, isolation: Isolation.Serialized);
        var second = Draw(harness, isolation: Isolation.Serialized);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void Snapshot_bodies_in_the_same_microsecond_draw_different_streams()
    {
        using var harness = new EngineHarness(timeProvider: new FrozenClock(Frozen));
        var first = Draw(harness, isolation: Isolation.Snapshot);
        var second = Draw(harness, isolation: Isolation.Snapshot);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void Two_engines_making_the_same_calls_draw_the_same_numbers()
    {
        using var one = new EngineHarness(timeProvider: new FrozenClock(Frozen));
        using var two = new EngineHarness(timeProvider: new FrozenClock(Frozen));
        long[][] fromOne = [Draw(one, Isolation.Serialized), Draw(one, Isolation.Snapshot)];
        long[][] fromTwo = [Draw(two, Isolation.Serialized), Draw(two, Isolation.Snapshot)];
        Assert.Equal(fromOne, fromTwo);
    }

    /// <summary>
    /// The cross-process half of reproducibility, which two engines in one process cannot show:
    /// <see cref="Identity.GetHashCode"/> is the same for both of them. Pinned values can — they hold
    /// in every process or in none. Changing the algorithm changes every module's test outcomes,
    /// so a failure here is a breaking change to call out, not a constant to update quietly.
    /// </summary>
    [Fact]
    public void The_default_stream_is_pinned_for_a_given_seed()
    {
        var random = SeededReducerRandomProvider.Instance.Create(new ReducerRandomSeed(
            "Roll", Identity.Hash("pinned-caller"), new Timestamp(1_760_875_200_000_000), Sequence: 7));
        long[] expected = [PinnedFirst, PinnedSecond, PinnedThird];
        long[] actual = [random.NextInt64(), random.NextInt64(), random.NextInt64()];
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Every_seed_field_separates_the_stream()
    {
        var provider = SeededReducerRandomProvider.Instance;
        var baseline = new ReducerRandomSeed("Roll", Identity.Hash("a"), new Timestamp(1), 1);
        var first = provider.Create(baseline).NextInt64();
        Assert.NotEqual(first, provider.Create(baseline with { Caller = Identity.Hash("b") }).NextInt64());
        Assert.NotEqual(first, provider.Create(baseline with { Timestamp = new Timestamp(2) }).NextInt64());
        Assert.NotEqual(first, provider.Create(baseline with { Sequence = 2 }).NextInt64());

        // The high half of the timestamp counts: the old (int) cast threw it away.
        Assert.NotEqual(first, provider.Create(baseline with { Timestamp = new Timestamp(1 + (1L << 32)) }).NextInt64());
    }

    [Fact]
    public void A_registered_provider_is_what_the_body_sees()
    {
        var provider = new RecordingProvider();
        using var harness = new EngineHarness(timeProvider: new FrozenClock(Frozen), randomProvider: provider);

        harness.Invoke("Serialized", ctx => Assert.Same(provider.Created[^1], ctx.Random));
        harness.InvokeSnapshot("Snapshot", ctx => Assert.Same(provider.Created[^1], ctx.Random));

        Assert.Collection(
            provider.Seeds,
            seed => Assert.Equal(new ReducerRandomSeed("Serialized", EngineHarness.Caller, Timestamp.FromDateTimeOffset(Frozen), 1), seed),
            seed => Assert.Equal(new ReducerRandomSeed("Snapshot", EngineHarness.Caller, Timestamp.FromDateTimeOffset(Frozen), 2), seed));
    }

    [Fact]
    public void An_aborted_attempt_consumes_a_sequence_so_a_retry_draws_anew()
    {
        var provider = new RecordingProvider();
        using var harness = new EngineHarness(randomProvider: provider);

        Assert.Throws<InvalidOperationException>(() => harness.Invoke("Retry", _ => throw new InvalidOperationException("boom")));
        harness.Invoke("Retry", _ => { });

        Assert.Equal([1UL, 2UL], provider.Seeds.Select(s => s.Sequence));
    }

    [Fact]
    public void A_provider_returning_null_fails_the_call_by_name()
    {
        using var harness = new EngineHarness(randomProvider: new NullProvider());
        var exception = Assert.Throws<InvalidOperationException>(() => harness.Invoke("Roll", _ => { }));
        Assert.Contains(nameof(NullProvider), exception.Message);
        Assert.Contains("Roll", exception.Message);
    }

    [Fact]
    public void Bounded_draws_honor_the_Random_contract()
    {
        var random = SeededReducerRandomProvider.Instance.Create(new ReducerRandomSeed("Contract", Identity.Hash("c"), new Timestamp(1), 1));
        for (var i = 0; i < 10_000; i++)
        {
            Assert.InRange(random.Next(), 0, int.MaxValue - 1);
            Assert.InRange(random.Next(6), 0, 5);
            Assert.InRange(random.Next(-3, 3), -3, 2);
            Assert.InRange(random.Next(int.MinValue, int.MaxValue), int.MinValue, int.MaxValue - 1);
            Assert.InRange(random.NextInt64(), 0, long.MaxValue - 1);
            Assert.InRange(random.NextInt64(long.MinValue, long.MaxValue), long.MinValue, long.MaxValue - 1);
            Assert.InRange(random.NextDouble(), 0.0, Math.BitDecrement(1.0));
            Assert.InRange(random.NextSingle(), 0f, MathF.BitDecrement(1f));
        }

        Assert.Equal(0, random.Next(0));
        Assert.Equal(0, random.Next(1));
        Assert.Equal(4, random.Next(4, 4));
        Assert.Equal(4, random.Next(4, 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => random.Next(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => random.Next(5, 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => random.NextInt64(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => random.NextInt64(5, 4));
    }

    [Fact]
    public void Every_face_of_a_die_comes_up()
    {
        var random = SeededReducerRandomProvider.Instance.Create(new ReducerRandomSeed("Die", Identity.Hash("d"), new Timestamp(1), 1));
        var counts = new int[6];
        for (var i = 0; i < 60_000; i++)
            counts[random.Next(6)]++;

        // Each face expects 10,000; ±600 is beyond six standard deviations.
        Assert.All(counts, count => Assert.InRange(count, 9_400, 10_600));
    }

    [Fact]
    public void Odd_length_byte_fills_are_written_through_and_shuffle_draws_from_the_stream()
    {
        var seed = new ReducerRandomSeed("Bytes", Identity.Hash("e"), new Timestamp(1), 1);
        var bytes = new byte[13];
        SeededReducerRandomProvider.Instance.Create(seed).NextBytes(bytes);
        Assert.Contains(bytes[8..], b => b != 0);

        int[] first = [.. Enumerable.Range(0, 20)];
        int[] second = [.. Enumerable.Range(0, 20)];
        SeededReducerRandomProvider.Instance.Create(seed).Shuffle(first);
        SeededReducerRandomProvider.Instance.Create(seed).Shuffle(second);
        Assert.Equal(first, second);
        Assert.NotEqual(Enumerable.Range(0, 20), first);
    }

    private const long PinnedFirst = 1_879_108_640_154_064_111;
    private const long PinnedSecond = 9_149_702_920_651_480_105;
    private const long PinnedThird = 7_184_603_220_055_053_951;

    private static long[] Draw(EngineHarness harness, Isolation isolation)
    {
        long[] drawn = [];
        void Body(ReducerContext ctx) => drawn = [ctx.Random.NextInt64(), ctx.Random.NextInt64(), ctx.Random.NextInt64()];
        if (isolation == Isolation.Snapshot)
            harness.InvokeSnapshot("Roll", Body);
        else
            harness.Invoke("Roll", Body);
        return drawn;
    }

    private sealed class FrozenClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private sealed class RecordingProvider : IReducerRandomProvider
    {
        public List<ReducerRandomSeed> Seeds { get; } = [];

        public List<Random> Created { get; } = [];

        public Random Create(ReducerRandomSeed seed)
        {
            Seeds.Add(seed);
            var random = new Random(0);
            Created.Add(random);
            return random;
        }
    }

    private sealed class NullProvider : IReducerRandomProvider
    {
        public Random Create(ReducerRandomSeed seed) => null!;
    }
}
