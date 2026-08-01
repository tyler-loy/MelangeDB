using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// The schema-interpreting half of key encoding: boxed encode and decode against a
/// <see cref="ColumnSchema"/>, serving the reflection path and every site that holds a schema
/// rather than a typed row. The typed, allocation-honest overloads generated codecs call directly
/// live in <see cref="KeyCodec"/> in Abstractions, where both the server and the client bindings
/// can reach them; this class delegates every byte decision there so the two can never drift.
/// </summary>
public static class SchemaKeyCodec
{
    /// <summary>Whether values of this kind may serve as primary keys or index values.</summary>
    public static bool IsKeyEncodable(ColumnKind kind) =>
        kind is not (ColumnKind.Float32 or ColumnKind.Float64 or ColumnKind.ScheduleAt);

    /// <summary>Boxed encoding against a column schema — the reflection path.</summary>
    public static RowKey Encode(ColumnSchema column, object? value)
    {
        ArgumentNullException.ThrowIfNull(column);
        ArgumentNullException.ThrowIfNull(value);
        if (column.IsEnum)
            value = Convert.ChangeType(value, Enum.GetUnderlyingType(column.ClrType));

        return column.Kind switch
        {
            ColumnKind.Bool => KeyCodec.EncodeBool((bool)value),
            ColumnKind.Int8 => KeyCodec.EncodeInt8((sbyte)value),
            ColumnKind.UInt8 => KeyCodec.EncodeUInt8((byte)value),
            ColumnKind.Int16 => KeyCodec.EncodeInt16((short)value),
            ColumnKind.UInt16 => KeyCodec.EncodeUInt16((ushort)value),
            ColumnKind.Int32 => KeyCodec.EncodeInt32((int)value),
            ColumnKind.UInt32 => KeyCodec.EncodeUInt32((uint)value),
            ColumnKind.Int64 => KeyCodec.EncodeInt64((long)value),
            ColumnKind.UInt64 => KeyCodec.EncodeUInt64((ulong)value),
            ColumnKind.String => KeyCodec.EncodeString((string)value),
            ColumnKind.Bytes => KeyCodec.EncodeBytes((byte[])value),
            ColumnKind.Identity => KeyCodec.EncodeIdentity((Identity)value),
            ColumnKind.Timestamp => KeyCodec.EncodeTimestamp((Timestamp)value),
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
