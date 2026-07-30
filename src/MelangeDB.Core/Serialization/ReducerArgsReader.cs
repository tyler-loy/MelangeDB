using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Thrown when a reducer call's arguments fail decoding or validation. Raised before any
/// transaction opens, so a rejected call changes nothing and appends nothing.
/// </summary>
public sealed class ReducerArgumentException : Exception
{
    public ReducerArgumentException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// Decodes and validates reducer arguments in one pass, against the declared parameter types the
/// generated dispatcher encodes as a sequence of typed reads. Rejects non-finite floats, over-long
/// strings and collections, and integers outside the declared type's range — the inputs that
/// corrupt state regardless of game rules. Limits come from <see cref="ValidationOptions"/>.
/// </summary>
public ref struct ReducerArgsReader
{
    private readonly ReadOnlySpan<byte> _data;
    private readonly ValidationOptions _limits;
    private int _position;

    public ReducerArgsReader(ReadOnlySpan<byte> data, ValidationOptions limits)
    {
        ArgumentNullException.ThrowIfNull(limits);
        _data = data;
        _limits = limits;
        Count = data.IsEmpty ? 0 : ReadHeaderCount();
    }

    /// <summary>The argument count the payload declares.</summary>
    public int Count { get; }

    /// <summary>Rejects the call unless the payload carries exactly the declared argument count.</summary>
    public void ExpectCount(int declared)
    {
        if (Count != declared)
            throw new ReducerArgumentException($"Expected {declared} argument(s); the call carries {Count}.");
    }

    /// <summary>Rejects the call unless every argument byte was consumed.</summary>
    public void End()
    {
        if (_position != _data.Length)
            throw new ReducerArgumentException($"Argument payload has {_data.Length - _position} undecoded trailing byte(s).");
    }

    public bool ReadBool()
    {
        Expect(ArgsCodec.Tag.Bool, "bool");
        EnsureRemaining(1);
        return ReadRawByte() != 0;
    }

    public sbyte ReadInt8() => (sbyte)ReadIntegerInRange(sbyte.MinValue, sbyte.MaxValue, "sbyte");

    public byte ReadUInt8() => (byte)ReadIntegerInRange(byte.MinValue, byte.MaxValue, "byte");

    public short ReadInt16() => (short)ReadIntegerInRange(short.MinValue, short.MaxValue, "short");

    public ushort ReadUInt16() => (ushort)ReadIntegerInRange(ushort.MinValue, ushort.MaxValue, "ushort");

    public int ReadInt32() => (int)ReadIntegerInRange(int.MinValue, int.MaxValue, "int");

    public uint ReadUInt32() => (uint)ReadIntegerInRange(uint.MinValue, uint.MaxValue, "uint");

    public long ReadInt64() => ReadIntegerInRange(long.MinValue, long.MaxValue, "long");

    public ulong ReadUInt64()
    {
        var tag = ReadTag();
        switch (tag)
        {
            case ArgsCodec.Tag.UInt64:
                return ReadRawUInt64();
            case ArgsCodec.Tag.Int64:
                var signed = ReadRawInt64();
                if (signed < 0)
                    throw new ReducerArgumentException($"Value {signed} is out of range for a ulong argument.");
                return (ulong)signed;
            default:
                throw RejectTag(tag, "ulong");
        }
    }

    public float ReadFloat32()
    {
        var value = ReadFloat64();
        return (float)value;
    }

    public double ReadFloat64()
    {
        Expect(ArgsCodec.Tag.Float64, "float");
        var value = BitConverter.UInt64BitsToDouble(ReadRawUInt64());
        if (_limits.RejectNonFiniteFloats && !double.IsFinite(value))
            throw new ReducerArgumentException($"Non-finite float argument ({value}) rejected; see Validation:RejectNonFiniteFloats.");
        return value;
    }

    public string? ReadString()
    {
        var tag = ReadTag();
        if (tag == ArgsCodec.Tag.Null)
            return null;
        if (tag != ArgsCodec.Tag.String)
            throw RejectTag(tag, "string");
        var byteLength = ReadRawInt32();
        if (byteLength < 0 || byteLength > _data.Length - _position)
            throw new ReducerArgumentException("String argument length is malformed.");

        // A UTF-8 char is at least one byte, so the byte length bounds the char count — checked
        // before decoding so an over-long payload never allocates.
        if (byteLength > (long)_limits.MaxStringLength * 4)
            throw StringTooLong(byteLength);
        var value = Encoding.UTF8.GetString(_data.Slice(_position, byteLength));
        _position += byteLength;
        if (value.Length > _limits.MaxStringLength)
            throw StringTooLong(value.Length);
        return value;
    }

    public byte[]? ReadByteArray()
    {
        var tag = ReadTag();
        if (tag == ArgsCodec.Tag.Null)
            return null;
        if (tag != ArgsCodec.Tag.Bytes)
            throw RejectTag(tag, "byte[]");
        var length = ReadRawInt32();
        if (length < 0 || length > _data.Length - _position)
            throw new ReducerArgumentException("Blob argument length is malformed.");
        if (length > _limits.MaxCollectionLength)
            throw CollectionTooLong(length);
        var value = _data.Slice(_position, length).ToArray();
        _position += length;
        return value;
    }

    public Identity ReadIdentity()
    {
        Expect(ArgsCodec.Tag.Identity, "Identity");
        EnsureRemaining(Identity.Size);
        var value = new Identity(_data.Slice(_position, Identity.Size));
        _position += Identity.Size;
        return value;
    }

    public Timestamp ReadTimestamp()
    {
        Expect(ArgsCodec.Tag.Timestamp, "Timestamp");
        return new Timestamp(ReadRawInt64());
    }

    /// <summary>
    /// Begins an array argument, returning its length-checked element count, or -1 for null.
    /// The caller reads exactly that many elements with the typed reads.
    /// </summary>
    public int BeginArray()
    {
        var tag = ReadTag();
        if (tag == ArgsCodec.Tag.Null)
            return -1;
        if (tag != ArgsCodec.Tag.Array)
            throw RejectTag(tag, "array");
        var count = ReadRawInt32();
        if (count < 0 || count > _data.Length - _position)
            throw new ReducerArgumentException("Array argument length is malformed.");
        if (count > _limits.MaxCollectionLength)
            throw CollectionTooLong(count);
        return count;
    }

    private long ReadIntegerInRange(long min, long max, string declared)
    {
        var tag = ReadTag();
        switch (tag)
        {
            case ArgsCodec.Tag.Int64:
                var signed = ReadRawInt64();
                if (signed < min || signed > max)
                    throw new ReducerArgumentException($"Value {signed} is out of range for a {declared} argument.");
                return signed;
            case ArgsCodec.Tag.UInt64:
                var unsigned = ReadRawUInt64();
                if (unsigned > (ulong)max)
                    throw new ReducerArgumentException($"Value {unsigned} is out of range for a {declared} argument.");
                return (long)unsigned;
            default:
                throw RejectTag(tag, declared);
        }
    }

    private int ReadHeaderCount()
    {
        var count = BinaryPrimitives.ReadUInt16LittleEndian(_data);
        _position = 2;
        return count;
    }

    private ArgsCodec.Tag ReadTag()
    {
        if (_position >= _data.Length)
            throw new ReducerArgumentException("Argument payload ends before all declared parameters were decoded.");
        return (ArgsCodec.Tag)ReadRawByte();
    }

    private void Expect(ArgsCodec.Tag expected, string declared)
    {
        var tag = ReadTag();
        if (tag != expected)
            throw RejectTag(tag, declared);
    }

    private byte ReadRawByte() => _data[_position++];

    private int ReadRawInt32()
    {
        EnsureRemaining(4);
        var value = BinaryPrimitives.ReadInt32LittleEndian(_data[_position..]);
        _position += 4;
        return value;
    }

    private long ReadRawInt64() => unchecked((long)ReadRawUInt64());

    private ulong ReadRawUInt64()
    {
        EnsureRemaining(8);
        var value = BinaryPrimitives.ReadUInt64LittleEndian(_data[_position..]);
        _position += 8;
        return value;
    }

    private readonly void EnsureRemaining(int count)
    {
        if (_data.Length - _position < count)
            throw new ReducerArgumentException("Argument payload is truncated.");
    }

    private ReducerArgumentException StringTooLong(int length) =>
        new($"String argument of length {length} exceeds Validation:MaxStringLength ({_limits.MaxStringLength}).");

    private ReducerArgumentException CollectionTooLong(int length) =>
        new($"Collection argument of length {length} exceeds Validation:MaxCollectionLength ({_limits.MaxCollectionLength}).");

    private static ReducerArgumentException RejectTag(ArgsCodec.Tag tag, string declared) =>
        new($"Argument of wire kind {tag} cannot bind to a declared {declared} parameter.");
}
