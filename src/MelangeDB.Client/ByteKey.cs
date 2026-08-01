namespace MelangeDB.Client;

/// <summary>
/// An encoded primary key as a dictionary key: value equality over the bytes. The one key shape
/// the whole client shares — subscription caches and typed table caches address rows with it.
/// </summary>
internal readonly struct ByteKey(byte[] bytes) : IEquatable<ByteKey>
{
    private readonly byte[] _bytes = bytes;

    public byte[] Bytes => _bytes;

    public bool Equals(ByteKey other) => _bytes.AsSpan().SequenceEqual(other._bytes);

    public override bool Equals(object? obj) => obj is ByteKey other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(_bytes);
        return hash.ToHashCode();
    }
}
