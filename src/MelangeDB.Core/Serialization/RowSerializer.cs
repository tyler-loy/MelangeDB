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

    /// <summary>
    /// Serializes a row from boxed column values keyed by column name — the schema-driven path
    /// bulk ingestion uses, no row type instance and no reflection accessors involved. Missing
    /// columns serialize as their default; a value that cannot coerce to its column's kind throws.
    /// </summary>
    public static byte[] SerializeValues(TableSchema table, IReadOnlyDictionary<string, object?> values)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(values);
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        foreach (var column in table.Columns)
        {
            values.TryGetValue(column.Name, out var value);
            WriteValue(writer, column, CoerceValue(table, column, value));
        }

        writer.Flush();
        return stream.ToArray();
    }

    /// <summary>
    /// Coerces a loosely typed value (as JSON or a wire map delivers it) to the boxed form
    /// <see cref="Serialize"/> expects for the column's kind, range-checked.
    /// </summary>
    public static object? CoerceValue(TableSchema table, ColumnSchema column, object? value)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(column);
        try
        {
            return column.Kind switch
            {
                ColumnKind.Bool => value is null ? false : (bool)value,
                ColumnKind.Int8 => checked((sbyte)ToInt64(value)),
                ColumnKind.UInt8 => checked((byte)ToUInt64(value)),
                ColumnKind.Int16 => checked((short)ToInt64(value)),
                ColumnKind.UInt16 => checked((ushort)ToUInt64(value)),
                ColumnKind.Int32 => checked((int)ToInt64(value)),
                ColumnKind.UInt32 => checked((uint)ToUInt64(value)),
                ColumnKind.Int64 => ToInt64(value),
                ColumnKind.UInt64 => ToUInt64(value),
                ColumnKind.Float32 => value is null ? 0f : Convert.ToSingle(value, System.Globalization.CultureInfo.InvariantCulture),
                ColumnKind.Float64 => value is null ? 0d : Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture),
                ColumnKind.String => value is null ? null : (string)value,
                ColumnKind.Bytes => value switch
                {
                    null => null,
                    byte[] bytes => bytes,
                    string base64 => Convert.FromBase64String(base64),
                    _ => throw new InvalidCastException(),
                },
                ColumnKind.Identity => ToIdentity(value),
                ColumnKind.Timestamp => value switch
                {
                    null => new Timestamp(0),
                    Timestamp timestamp => timestamp,
                    _ => new Timestamp(ToInt64(value)),
                },
                _ => throw new NotSupportedException($"Unknown column kind {column.Kind}."),
            };
        }
        catch (Exception exception) when (exception is InvalidCastException or FormatException or OverflowException)
        {
            throw new ArgumentException(
                $"Table '{table.Name}': value '{value}' cannot coerce to column '{column.Name}' of kind {column.Kind}.",
                nameof(value));
        }
    }

    private static long ToInt64(object? value) => value switch
    {
        null => 0L,
        ulong u => checked((long)u),
        _ => Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture),
    };

    private static ulong ToUInt64(object? value) => value switch
    {
        null => 0UL,
        ulong u => u,
        _ => checked((ulong)Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture)),
    };

    private static Identity ToIdentity(object? value) => value switch
    {
        null => Identity.None,
        Identity identity => identity,
        byte[] bytes => new Identity(bytes),
        string hex => new Identity(Convert.FromHexString(hex)),
        _ => throw new InvalidCastException(),
    };

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
