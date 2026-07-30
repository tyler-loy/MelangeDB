using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Encodes a column value into its uniform, order-preserving <see cref="RowKey"/> byte form:
/// big-endian for unsigned integers, big-endian with the sign bit flipped for signed ones, UTF-8
/// for strings, raw bytes for <see cref="Identity"/> and blobs. Floats are not key-encodable.
/// </summary>
public static class KeyCodec
{
    /// <summary>Whether values of this kind may serve as primary keys or index values.</summary>
    public static bool IsKeyEncodable(ColumnKind kind) =>
        kind is not (ColumnKind.Float32 or ColumnKind.Float64);

    public static RowKey Encode(ColumnSchema column, object? value)
    {
        ArgumentNullException.ThrowIfNull(column);
        ArgumentNullException.ThrowIfNull(value);
        if (column.IsEnum)
            value = Convert.ChangeType(value, Enum.GetUnderlyingType(column.ClrType));

        switch (column.Kind)
        {
            case ColumnKind.Bool:
                return new RowKey([(bool)value ? (byte)1 : (byte)0]);
            case ColumnKind.Int8:
                return new RowKey([(byte)((sbyte)value ^ unchecked((sbyte)0x80))]);
            case ColumnKind.UInt8:
                return new RowKey([(byte)value]);
            case ColumnKind.Int16:
            {
                Span<byte> buffer = stackalloc byte[2];
                BinaryPrimitives.WriteUInt16BigEndian(buffer, (ushort)((short)value ^ short.MinValue));
                return new RowKey(buffer);
            }

            case ColumnKind.UInt16:
            {
                Span<byte> buffer = stackalloc byte[2];
                BinaryPrimitives.WriteUInt16BigEndian(buffer, (ushort)value);
                return new RowKey(buffer);
            }

            case ColumnKind.Int32:
            {
                Span<byte> buffer = stackalloc byte[4];
                BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)((int)value ^ int.MinValue));
                return new RowKey(buffer);
            }

            case ColumnKind.UInt32:
            {
                Span<byte> buffer = stackalloc byte[4];
                BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)value);
                return new RowKey(buffer);
            }

            case ColumnKind.Int64:
            {
                Span<byte> buffer = stackalloc byte[8];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, (ulong)((long)value ^ long.MinValue));
                return new RowKey(buffer);
            }

            case ColumnKind.UInt64:
            {
                Span<byte> buffer = stackalloc byte[8];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, (ulong)value);
                return new RowKey(buffer);
            }

            case ColumnKind.String:
                return new RowKey(Encoding.UTF8.GetBytes((string)value));
            case ColumnKind.Bytes:
                return new RowKey((byte[])value);
            case ColumnKind.Identity:
            {
                Span<byte> buffer = stackalloc byte[Identity.Size];
                ((Identity)value).WriteTo(buffer);
                return new RowKey(buffer);
            }

            case ColumnKind.Timestamp:
            {
                Span<byte> buffer = stackalloc byte[8];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, (ulong)(((Timestamp)value).UnixTimeMicroseconds ^ long.MinValue));
                return new RowKey(buffer);
            }

            default:
                throw new NotSupportedException($"Column kind {column.Kind} is not key-encodable.");
        }
    }
}
