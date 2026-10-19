using System.Buffers.Binary;

namespace MelangeDB;

/// <summary>
/// Supplies <see cref="ReducerContext.Random"/>: called once per transaction attempt, before the
/// body runs. The engine resolves it from DI and falls back to
/// <see cref="SeededReducerRandomProvider"/>, so registering one is how a test forces a roll —
/// a crit, a loot drop — through the real dispatch path rather than around it.
/// </summary>
public interface IReducerRandomProvider
{
    /// <summary>
    /// Creates the random source one transaction attempt draws from. The returned instance is the
    /// body's alone and is never shared across threads, so it need not be thread-safe.
    /// </summary>
    Random Create(ReducerRandomSeed seed);
}

/// <summary>
/// What a transaction attempt is known by before its body runs. The LSN is not among them: it is
/// assigned at append, after the body, and a snapshot-isolated body has no lock to reserve one under.
/// </summary>
/// <param name="ReducerName">The reducer being invoked, so a provider can script one and leave the rest alone.</param>
/// <param name="Caller">Who is calling.</param>
/// <param name="Timestamp">The transaction's timestamp — the same value as <see cref="ReducerContext.Timestamp"/>.</param>
/// <param name="Sequence">
/// The engine's count of transaction attempts, 1 for the first after it opens. Aborted attempts
/// count too. It is what separates two calls by one caller in one microsecond; it is not persisted,
/// so it restarts with the process.
/// </param>
public readonly record struct ReducerRandomSeed(string ReducerName, Identity Caller, Timestamp Timestamp, ulong Sequence);

/// <summary>
/// The default <see cref="IReducerRandomProvider"/>: a 64-bit generator seeded from a stable hash
/// of the caller, the full timestamp, and the sequence.
/// <para>
/// Two promises follow, and only two. <b>Reproducible across processes:</b> the same calls in the
/// same order against a frozen clock draw the same numbers in every process, on every machine —
/// nothing feeds the seed that .NET randomizes per process, which <see cref="Identity.GetHashCode"/>
/// is (issue #156). <b>Distinct per attempt:</b> the sequence differs between any two attempts on one
/// engine, so two calls in the same microsecond no longer draw identical streams.
/// </para>
/// <para>
/// Recovery does not depend on either: the log records row operations, not reducer calls, so a
/// restart never re-runs a body. And the generator is not cryptographic — a value that must be
/// unguessable or globally unique, such as an idempotency key handed to another system, should not
/// come from it.
/// </para>
/// </summary>
public sealed class SeededReducerRandomProvider : IReducerRandomProvider
{
    /// <summary>The shared instance; the provider holds no state.</summary>
    public static SeededReducerRandomProvider Instance { get; } = new();

    public Random Create(ReducerRandomSeed seed)
    {
        Span<byte> caller = stackalloc byte[Identity.Size];
        seed.Caller.WriteTo(caller);
        var hash = 0x9E3779B97F4A7C15UL;
        hash = SplitMix64Random.Mix(hash ^ BinaryPrimitives.ReadUInt64BigEndian(caller));
        hash = SplitMix64Random.Mix(hash ^ BinaryPrimitives.ReadUInt64BigEndian(caller[8..]));
        hash = SplitMix64Random.Mix(hash ^ BinaryPrimitives.ReadUInt64BigEndian(caller[16..]));
        hash = SplitMix64Random.Mix(hash ^ BinaryPrimitives.ReadUInt64BigEndian(caller[24..]));
        hash = SplitMix64Random.Mix(hash ^ unchecked((ulong)seed.Timestamp.UnixTimeMicroseconds));
        hash = SplitMix64Random.Mix(hash ^ seed.Sequence);
        return new SplitMix64Random(hash);
    }
}

/// <summary>
/// SplitMix64 behind the <see cref="Random"/> surface. <see cref="Random"/>'s own constructor takes
/// an <see cref="int"/> seed, and 32 bits of seed is a birthday collision every ~77,000
/// transactions — fine for dice, not for "distinct per attempt". Every virtual member is overridden,
/// so the non-virtual helpers (<c>Shuffle</c>, <c>GetItems</c>) draw from this stream too.
/// </summary>
internal sealed class SplitMix64Random(ulong seed) : Random
{
    private ulong _state = seed;

    /// <summary>The SplitMix64 finalizer: a bijective 64-bit mix, also used to fold the seed.</summary>
    internal static ulong Mix(ulong z)
    {
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    private ulong NextUInt64() => Mix(_state += 0x9E3779B97F4A7C15UL);

    /// <summary>Uniform in [0, <paramref name="range"/>), by Lemire's multiply-and-reject — no modulo bias.</summary>
    private ulong NextBelow(ulong range)
    {
        var high = Math.BigMul(NextUInt64(), range, out var low);
        if (low < range)
        {
            var threshold = (0UL - range) % range;
            while (low < threshold)
                high = Math.BigMul(NextUInt64(), range, out low);
        }

        return high;
    }

    public override int Next()
    {
        // 31 bits spans [0, int.MaxValue]; the contract excludes MaxValue.
        while (true)
        {
            var result = (int)(NextUInt64() >> 33);
            if (result != int.MaxValue)
                return result;
        }
    }

    public override int Next(int maxValue)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxValue);
        return maxValue <= 1 ? 0 : (int)NextBelow((ulong)maxValue);
    }

    public override int Next(int minValue, int maxValue)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minValue, maxValue);
        var range = (ulong)((long)maxValue - minValue);
        return range <= 1 ? minValue : (int)(minValue + (long)NextBelow(range));
    }

    public override long NextInt64()
    {
        while (true)
        {
            var result = (long)(NextUInt64() >> 1);
            if (result != long.MaxValue)
                return result;
        }
    }

    public override long NextInt64(long maxValue)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxValue);
        return maxValue <= 1 ? 0 : (long)NextBelow((ulong)maxValue);
    }

    public override long NextInt64(long minValue, long maxValue)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minValue, maxValue);
        var range = unchecked((ulong)(maxValue - minValue));
        return range <= 1 ? minValue : unchecked(minValue + (long)NextBelow(range));
    }

    public override double NextDouble() => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

    public override float NextSingle() => (NextUInt64() >> 40) * (1.0f / (1U << 24));

    protected override double Sample() => NextDouble();

    public override void NextBytes(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        NextBytes(buffer.AsSpan());
    }

    public override void NextBytes(Span<byte> buffer)
    {
        while (buffer.Length >= sizeof(ulong))
        {
            BinaryPrimitives.WriteUInt64LittleEndian(buffer, NextUInt64());
            buffer = buffer[sizeof(ulong)..];
        }

        if (!buffer.IsEmpty)
        {
            Span<byte> tail = stackalloc byte[sizeof(ulong)];
            BinaryPrimitives.WriteUInt64LittleEndian(tail, NextUInt64());
            tail[..buffer.Length].CopyTo(buffer);
        }
    }
}
