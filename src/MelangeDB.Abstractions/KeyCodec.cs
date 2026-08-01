using System.Buffers.Binary;
using System.Text;

namespace MelangeDB;

/// <summary>
/// Encodes a column value into its uniform, order-preserving <see cref="RowKey"/> byte form:
/// big-endian for unsigned integers, big-endian with the sign bit flipped for signed ones, UTF-8
/// for strings, raw bytes for <see cref="Identity"/> and blobs. Floats are not key-encodable.
/// Lives in Abstractions because both sides of the wire need it: server-side generated codecs
/// encode primary keys and index values with these overloads, and client-side generated bindings
/// encode the same keys to address their local caches. The schema-interpreting overloads — which
/// need a column schema and therefore the engine — live in <c>MelangeDB.Core.SchemaKeyCodec</c>.
/// </summary>
public static class KeyCodec
{
    public static RowKey EncodeBool(bool value) => new([value ? (byte)1 : (byte)0]);

    public static RowKey EncodeInt8(sbyte value) => new([(byte)(value ^ unchecked((sbyte)0x80))]);

    public static RowKey EncodeUInt8(byte value) => new([value]);

    public static RowKey EncodeInt16(short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(buffer, (ushort)(value ^ short.MinValue));
        return new RowKey(buffer);
    }

    public static RowKey EncodeUInt16(ushort value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(buffer, value);
        return new RowKey(buffer);
    }

    public static RowKey EncodeInt32(int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)(value ^ int.MinValue));
        return new RowKey(buffer);
    }

    public static RowKey EncodeUInt32(uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        return new RowKey(buffer);
    }

    public static RowKey EncodeInt64(long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, (ulong)(value ^ long.MinValue));
        return new RowKey(buffer);
    }

    public static RowKey EncodeUInt64(ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        return new RowKey(buffer);
    }

    public static RowKey EncodeString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new RowKey(Encoding.UTF8.GetBytes(value));
    }

    public static RowKey EncodeBytes(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new RowKey(value);
    }

    public static RowKey EncodeIdentity(Identity value)
    {
        Span<byte> buffer = stackalloc byte[Identity.Size];
        value.WriteTo(buffer);
        return new RowKey(buffer);
    }

    public static RowKey EncodeTimestamp(Timestamp value) => EncodeInt64(value.UnixTimeMicroseconds);
}
