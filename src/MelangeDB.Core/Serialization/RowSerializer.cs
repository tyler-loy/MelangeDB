using System.Runtime.InteropServices;

namespace MelangeDB.Core;

/// <summary>
/// Serializes rows against a <see cref="TableSchema"/>'s declared column order. Format v1: fixed-width
/// little-endian primitives; strings and blobs are null-flagged and length-prefixed; enums as their
/// underlying integer. The format version rides in every log record, so phase 02's generated
/// serializers can supersede this reflection path while existing logs still read.
/// </summary>
public static class RowSerializer
{
    /// <summary>The current row and record format version.</summary>
    public const ushort FormatVersion = 1;

    public static byte[] Serialize(TableSchema table, object row)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(row);
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        foreach (var column in table.Columns)
            WriteValue(writer, column, column.GetValue(row));
        writer.Flush();
        return stream.ToArray();
    }

    public static object Deserialize(TableSchema table, ReadOnlyMemory<byte> data)
    {
        ArgumentNullException.ThrowIfNull(table);
        var array = MemoryMarshal.TryGetArray(data, out var segment) && segment.Array is not null
            ? segment
            : new ArraySegment<byte>(data.ToArray());
        using var stream = new MemoryStream(array.Array!, array.Offset, array.Count, writable: false);
        using var reader = new BinaryReader(stream);
        var row = Activator.CreateInstance(table.RowType)!;
        foreach (var column in table.Columns)
            column.SetValue(row, ReadValue(reader, column));
        return row;
    }

    private static void WriteValue(BinaryWriter writer, ColumnSchema column, object? value)
    {
        if (column.IsEnum && value is not null)
            value = Convert.ChangeType(value, Enum.GetUnderlyingType(column.ClrType));

        switch (column.Kind)
        {
            case ColumnKind.Bool:
                writer.Write((bool)value!);
                break;
            case ColumnKind.Int8:
                writer.Write((sbyte)value!);
                break;
            case ColumnKind.UInt8:
                writer.Write((byte)value!);
                break;
            case ColumnKind.Int16:
                writer.Write((short)value!);
                break;
            case ColumnKind.UInt16:
                writer.Write((ushort)value!);
                break;
            case ColumnKind.Int32:
                writer.Write((int)value!);
                break;
            case ColumnKind.UInt32:
                writer.Write((uint)value!);
                break;
            case ColumnKind.Int64:
                writer.Write((long)value!);
                break;
            case ColumnKind.UInt64:
                writer.Write((ulong)value!);
                break;
            case ColumnKind.Float32:
                writer.Write((float)value!);
                break;
            case ColumnKind.Float64:
                writer.Write((double)value!);
                break;
            case ColumnKind.String:
                if (value is null)
                {
                    writer.Write((byte)0);
                }
                else
                {
                    writer.Write((byte)1);
                    var bytes = System.Text.Encoding.UTF8.GetBytes((string)value);
                    writer.Write(bytes.Length);
                    writer.Write(bytes);
                }

                break;
            case ColumnKind.Bytes:
                if (value is null)
                {
                    writer.Write((byte)0);
                }
                else
                {
                    var bytes = (byte[])value;
                    writer.Write((byte)1);
                    writer.Write(bytes.Length);
                    writer.Write(bytes);
                }

                break;
            case ColumnKind.Identity:
                Span<byte> identity = stackalloc byte[Identity.Size];
                ((Identity)value!).WriteTo(identity);
                writer.Write(identity);
                break;
            case ColumnKind.Timestamp:
                writer.Write(((Timestamp)value!).UnixTimeMicroseconds);
                break;
            default:
                throw new NotSupportedException($"Unknown column kind {column.Kind}.");
        }
    }

    private static object? ReadValue(BinaryReader reader, ColumnSchema column)
    {
        object? value = column.Kind switch
        {
            ColumnKind.Bool => reader.ReadBoolean(),
            ColumnKind.Int8 => reader.ReadSByte(),
            ColumnKind.UInt8 => reader.ReadByte(),
            ColumnKind.Int16 => reader.ReadInt16(),
            ColumnKind.UInt16 => reader.ReadUInt16(),
            ColumnKind.Int32 => reader.ReadInt32(),
            ColumnKind.UInt32 => reader.ReadUInt32(),
            ColumnKind.Int64 => reader.ReadInt64(),
            ColumnKind.UInt64 => reader.ReadUInt64(),
            ColumnKind.Float32 => reader.ReadSingle(),
            ColumnKind.Float64 => reader.ReadDouble(),
            ColumnKind.String => reader.ReadByte() == 0 ? null : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32())),
            ColumnKind.Bytes => reader.ReadByte() == 0 ? null : reader.ReadBytes(reader.ReadInt32()),
            ColumnKind.Identity => new Identity(reader.ReadBytes(Identity.Size)),
            ColumnKind.Timestamp => new Timestamp(reader.ReadInt64()),
            _ => throw new NotSupportedException($"Unknown column kind {column.Kind}."),
        };
        if (column.IsEnum && value is not null)
            value = Enum.ToObject(column.ClrType, value);
        return value;
    }
}
