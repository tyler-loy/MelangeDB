namespace MelangeDB;

/// <summary>
/// The uniform, order-preserving encoded byte form of a row's primary key (or an indexed column
/// value). Comparing two keys byte-wise compares the underlying values, which is what lets the log,
/// the store, and range indexes all share one key shape.
/// </summary>
public readonly struct RowKey : IEquatable<RowKey>, IComparable<RowKey>
{
    private readonly byte[]? _bytes;
    private readonly int _hash;

    /// <summary>Creates a key from already-encoded bytes. The bytes are copied.</summary>
    public RowKey(ReadOnlySpan<byte> encoded)
    {
        _bytes = encoded.ToArray();
        _hash = ComputeHash(encoded);
    }

    /// <summary>The encoded bytes.</summary>
    public ReadOnlySpan<byte> Span => _bytes ?? [];

    /// <summary>The encoded length in bytes.</summary>
    public int Length => _bytes?.Length ?? 0;

    /// <summary>Returns a copy of the encoded bytes.</summary>
    public byte[] ToArray() => Span.ToArray();

    public bool Equals(RowKey other) => Span.SequenceEqual(other.Span);

    public override bool Equals(object? obj) => obj is RowKey other && Equals(other);

    public override int GetHashCode() => _hash;

    public int CompareTo(RowKey other) => Span.SequenceCompareTo(other.Span);

    public override string ToString() => Convert.ToHexStringLower(Span);

    public static bool operator ==(RowKey left, RowKey right) => left.Equals(right);

    public static bool operator !=(RowKey left, RowKey right) => !left.Equals(right);

    private static int ComputeHash(ReadOnlySpan<byte> bytes)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;
        var hash = offsetBasis;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((int)hash);
    }
}
