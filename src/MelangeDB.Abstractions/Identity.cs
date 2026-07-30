using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace MelangeDB;

/// <summary>
/// The stable identifier for who is acting: a 256-bit hash derived from a token's issuer and subject.
/// Stable across reconnects and restarts; distinct from <see cref="ConnectionId"/>, which identifies a socket.
/// </summary>
public readonly struct Identity : IEquatable<Identity>, IComparable<Identity>
{
    /// <summary>The size of an identity in bytes.</summary>
    public const int Size = 32;

    // Stored as big-endian segments so numeric comparison matches byte-wise (memcmp) ordering.
    private readonly ulong _a;
    private readonly ulong _b;
    private readonly ulong _c;
    private readonly ulong _d;

    /// <summary>Creates an identity from its 32-byte representation.</summary>
    public Identity(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Size)
            throw new ArgumentException($"An Identity is exactly {Size} bytes; got {bytes.Length}.", nameof(bytes));
        _a = BinaryPrimitives.ReadUInt64BigEndian(bytes);
        _b = BinaryPrimitives.ReadUInt64BigEndian(bytes[8..]);
        _c = BinaryPrimitives.ReadUInt64BigEndian(bytes[16..]);
        _d = BinaryPrimitives.ReadUInt64BigEndian(bytes[24..]);
    }

    /// <summary>The all-zero identity, meaning "no caller".</summary>
    public static Identity None => default;

    /// <summary>Whether this is the all-zero identity.</summary>
    public bool IsNone => (_a | _b | _c | _d) == 0;

    /// <summary>
    /// The authoritative identity derivation: SHA-256 over a token's issuer <em>and</em> subject.
    /// Issuer included deliberately — hashing the subject alone would let a subject from one token
    /// source collide into another source's identity, bypassing the entire policy layer without
    /// triggering any of it. The canonical form (<c>issuer '\n' subject</c>) is contract: it is
    /// what makes an identity stable across reconnects, restarts, and token refreshes.
    /// </summary>
    public static Identity FromIssuerSubject(string issuer, string subject)
    {
        ArgumentException.ThrowIfNullOrEmpty(issuer);
        ArgumentException.ThrowIfNullOrEmpty(subject);
        return Hash(issuer + "\n" + subject);
    }

    /// <summary>
    /// Derives an identity by hashing an arbitrary string with SHA-256. The stable primitive
    /// beneath <see cref="FromIssuerSubject"/>; also used for fixed in-process identities.
    /// </summary>
    public static Identity Hash(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Span<byte> digest = stackalloc byte[Size];
        SHA256.HashData(Encoding.UTF8.GetBytes(value), digest);
        return new Identity(digest);
    }

    /// <summary>Writes the 32-byte representation into <paramref name="destination"/>.</summary>
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Size)
            throw new ArgumentException($"Destination must be at least {Size} bytes.", nameof(destination));
        BinaryPrimitives.WriteUInt64BigEndian(destination, _a);
        BinaryPrimitives.WriteUInt64BigEndian(destination[8..], _b);
        BinaryPrimitives.WriteUInt64BigEndian(destination[16..], _c);
        BinaryPrimitives.WriteUInt64BigEndian(destination[24..], _d);
    }

    /// <summary>Returns the 32-byte representation.</summary>
    public byte[] ToByteArray()
    {
        var bytes = new byte[Size];
        WriteTo(bytes);
        return bytes;
    }

    public bool Equals(Identity other) => _a == other._a && _b == other._b && _c == other._c && _d == other._d;

    public override bool Equals(object? obj) => obj is Identity other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(_a, _b, _c, _d);

    public int CompareTo(Identity other)
    {
        var c = _a.CompareTo(other._a);
        if (c != 0) return c;
        c = _b.CompareTo(other._b);
        if (c != 0) return c;
        c = _c.CompareTo(other._c);
        return c != 0 ? c : _d.CompareTo(other._d);
    }

    public override string ToString() => Convert.ToHexStringLower(ToByteArray());

    public static bool operator ==(Identity left, Identity right) => left.Equals(right);

    public static bool operator !=(Identity left, Identity right) => !left.Equals(right);
}
