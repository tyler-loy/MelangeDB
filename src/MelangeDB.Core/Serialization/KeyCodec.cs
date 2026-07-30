using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Encodes a column value into its uniform, order-preserving <see cref="RowKey"/> byte form:
/// big-endian for unsigned integers, big-endian with the sign bit flipped for signed ones, UTF-8
/// for strings, raw bytes for <see cref="Identity"/> and blobs. Floats are not key-encodable.
/// The typed overloads are the allocation-honest path generated codecs call directly; the boxed
/// <see cref="Encode(ColumnSchema, object)"/> overload serves the reflection path.
/// </summary>
public static class KeyCodec
{
    /// <summary>Whether values of this kind may serve as primary keys or index values.</summary>
    public static bool IsKeyEncodable(ColumnKind kind) =>
        kind is not (ColumnKind.Float32 or ColumnKind.Float64 or ColumnKind.ScheduleAt);

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

    /// <summary>Boxed encoding against a column schema — the reflection path.</summary>
    public static RowKey Encode(ColumnSchema column, object? value)
    {
        ArgumentNullException.ThrowIfNull(column);
        ArgumentNullException.ThrowIfNull(value);
        if (column.IsEnum)
            value = Convert.ChangeType(value, Enum.GetUnderlyingType(column.ClrType));

        return column.Kind switch
        {
            ColumnKind.Bool => EncodeBool((bool)value),
            ColumnKind.Int8 => EncodeInt8((sbyte)value),
            ColumnKind.UInt8 => EncodeUInt8((byte)value),
            ColumnKind.Int16 => EncodeInt16((short)value),
            ColumnKind.UInt16 => EncodeUInt16((ushort)value),
            ColumnKind.Int32 => EncodeInt32((int)value),
            ColumnKind.UInt32 => EncodeUInt32((uint)value),
            ColumnKind.Int64 => EncodeInt64((long)value),
            ColumnKind.UInt64 => EncodeUInt64((ulong)value),
            ColumnKind.String => EncodeString((string)value),
            ColumnKind.Bytes => EncodeBytes((byte[])value),
            ColumnKind.Identity => EncodeIdentity((Identity)value),
            ColumnKind.Timestamp => EncodeTimestamp((Timestamp)value),
            _ => throw new NotSupportedException($"Column kind {column.Kind} is not key-encodable."),
        };
    }

    /// <summary>
    /// Boxed decoding: the inverse of <see cref="Encode(ColumnSchema, object)"/>, recovering the
    /// column value from its order-preserving key form. What lets a delete op — which carries only
    /// the encoded primary key, no row — be applied to a projection keyed by natural values, the
    /// relational tier being the case in point.
    /// </summary>
    public static object Decode(ColumnSchema column, in RowKey key)
    {
        ArgumentNullException.ThrowIfNull(column);
        var span = key.Span;
        object value = column.Kind switch
        {
            ColumnKind.Bool => span[0] != 0,
            ColumnKind.Int8 => (sbyte)(span[0] ^ 0x80),
            ColumnKind.UInt8 => span[0],
            ColumnKind.Int16 => (short)(BinaryPrimitives.ReadUInt16BigEndian(span) ^ unchecked((ushort)short.MinValue)),
            ColumnKind.UInt16 => BinaryPrimitives.ReadUInt16BigEndian(span),
            ColumnKind.Int32 => (int)BinaryPrimitives.ReadUInt32BigEndian(span) ^ int.MinValue,
            ColumnKind.UInt32 => BinaryPrimitives.ReadUInt32BigEndian(span),
            ColumnKind.Int64 => (long)BinaryPrimitives.ReadUInt64BigEndian(span) ^ long.MinValue,
            ColumnKind.UInt64 => BinaryPrimitives.ReadUInt64BigEndian(span),
            ColumnKind.String => Encoding.UTF8.GetString(span),
            ColumnKind.Bytes => span.ToArray(),
            ColumnKind.Identity => new Identity(span),
            ColumnKind.Timestamp => new Timestamp((long)BinaryPrimitives.ReadUInt64BigEndian(span) ^ long.MinValue),
            _ => throw new NotSupportedException($"Column kind {column.Kind} is not key-encodable."),
        };
        return column.IsEnum ? Enum.ToObject(column.ClrType, value) : value;
    }
}
