using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Protocol;

/// <summary>Thrown when a producer result does not match the shape the client expected.</summary>
public sealed class ProducerResultFormatException(string message) : Exception(message);

/// <summary>
/// Reads a producer's result — one tagged value in the reducer-argument encoding (see
/// <see cref="ReducerArgs"/>), with no count header. A record is an array of its fields in
/// declaration order and a list an array of its elements; the schema manifest says which is which,
/// so generated bindings read positionally. Mirrors the server's <c>ProducerResultWriter</c>; a
/// compatibility test holds the two together.
/// </summary>
public ref struct ProducerResultReader
{
    private enum Tag : byte
    {
        Null = 0,
        Bool = 1,
        Int64 = 2,
        UInt64 = 3,
        Float64 = 4,
        String = 5,
        Bytes = 6,
        Identity = 7,
        Timestamp = 8,
        Array = 9,
    }

    private readonly ReadOnlySpan<byte> _data;
    private int _position;

    public ProducerResultReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
    }

    /// <summary>Consumes a null and returns true, or leaves the next value in place and returns false.</summary>
    public bool TryReadNull()
    {
        if (Peek() != Tag.Null)
            return false;
        _position++;
        return true;
    }

    public bool ReadBool()
    {
        Expect(Tag.Bool, "bool");
        return Take(1)[0] != 0;
    }

    public long ReadInt64()
    {
        var tag = ReadTag();
        if (tag == Tag.Int64)
            return BinaryPrimitives.ReadInt64LittleEndian(Take(8));
        if (tag == Tag.UInt64 && BinaryPrimitives.ReadUInt64LittleEndian(Take(8)) is var unsigned && unsigned <= long.MaxValue)
            return (long)unsigned;
        throw Mismatch(tag, "signed integer");
    }

    public ulong ReadUInt64()
    {
        var tag = ReadTag();
        if (tag == Tag.UInt64)
            return BinaryPrimitives.ReadUInt64LittleEndian(Take(8));
        if (tag == Tag.Int64 && BinaryPrimitives.ReadInt64LittleEndian(Take(8)) is var signed && signed >= 0)
            return (ulong)signed;
        throw Mismatch(tag, "unsigned integer");
    }

    public int ReadInt32() => checked((int)ReadInt64());

    public short ReadInt16() => checked((short)ReadInt64());

    public sbyte ReadInt8() => checked((sbyte)ReadInt64());

    public uint ReadUInt32() => checked((uint)ReadUInt64());

    public ushort ReadUInt16() => checked((ushort)ReadUInt64());

    public byte ReadUInt8() => checked((byte)ReadUInt64());

    public double ReadFloat64()
    {
        Expect(Tag.Float64, "float");
        return BinaryPrimitives.ReadDoubleLittleEndian(Take(8));
    }

    public float ReadFloat32() => (float)ReadFloat64();

    /// <summary>A string, or null.</summary>
    public string? ReadString()
    {
        if (TryReadNull())
            return null;
        Expect(Tag.String, "string");
        return Encoding.UTF8.GetString(Take(ReadLength()));
    }

    /// <summary>Bytes, or null.</summary>
    public byte[]? ReadBytes()
    {
        if (TryReadNull())
            return null;
        Expect(Tag.Bytes, "bytes");
        return Take(ReadLength()).ToArray();
    }

    public Identity ReadIdentity()
    {
        Expect(Tag.Identity, "Identity");
        return new Identity(Take(Identity.Size));
    }

    public Timestamp ReadTimestamp()
    {
        Expect(Tag.Timestamp, "Timestamp");
        return new Timestamp(BinaryPrimitives.ReadInt64LittleEndian(Take(8)));
    }

    /// <summary>Starts a list or a record: its element (or field) count, or -1 for null.</summary>
    public int BeginArray()
    {
        if (TryReadNull())
            return -1;
        Expect(Tag.Array, "list or record");
        return ReadLength();
    }

    /// <summary>Throws unless the whole result was read — a shape mismatch rather than silently ignored bytes.</summary>
    public readonly void End()
    {
        if (_position != _data.Length)
            throw new ProducerResultFormatException($"The producer result has {_data.Length - _position} unread trailing byte(s); the bindings and the server disagree about its shape.");
    }

    private readonly Tag Peek() =>
        _position < _data.Length ? (Tag)_data[_position] : throw new ProducerResultFormatException("The producer result ended early.");

    private Tag ReadTag()
    {
        var tag = Peek();
        _position++;
        return tag;
    }

    private void Expect(Tag expected, string declared)
    {
        var tag = ReadTag();
        if (tag != expected)
            throw Mismatch(tag, declared);
    }

    /// <summary>A string's or byte array's byte length, or an array's value count; <see cref="Take"/> bounds the bytes.</summary>
    private int ReadLength()
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(Take(4));
        if (length < 0)
            throw new ProducerResultFormatException("A length in the producer result is negative.");
        return length;
    }

    private ReadOnlySpan<byte> Take(int count)
    {
        if (count < 0 || _data.Length - _position < count)
            throw new ProducerResultFormatException("The producer result ended early.");
        var slice = _data.Slice(_position, count);
        _position += count;
        return slice;
    }

    private static ProducerResultFormatException Mismatch(Tag tag, string declared) =>
        new($"The producer result holds a {tag} where the bindings expect a {declared}; regenerate them against the server's manifest.");
}
