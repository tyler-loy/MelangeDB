using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Core;

/// <summary>One decoded column: its name and wire kind from the shape, and its value.</summary>
/// <param name="Value">
/// The column's kind's own CLR value: <see cref="bool"/>, the sized integers, <see cref="float"/>,
/// <see cref="double"/>, <see cref="string"/> or <see cref="byte"/>[] (either may be null),
/// <see cref="Identity"/>, <see cref="Timestamp"/> or <see cref="ScheduleAt"/>. An enum column is
/// its underlying integer: a shape records the wire kind, not the enum.
/// </param>
public sealed record DecodedColumn(string Name, ColumnKind Kind, object? Value);

/// <summary>
/// A row decoded against a recorded shape. <see cref="Error"/> is set when the bytes and the shape
/// disagree; <see cref="Columns"/> then holds the columns that decoded before they did.
/// </summary>
public sealed record DecodedRow(IReadOnlyList<DecodedColumn> Columns, string? Error)
{
    public bool Succeeded => Error is null;
}

/// <summary>A decoded primary key: its column, kind and value, or the reason it could not be decoded.</summary>
public sealed record DecodedKey(string? Column, ColumnKind? Kind, object? Value, string? Error)
{
    public bool Succeeded => Error is null;
}

/// <summary>
/// Reducer arguments decoded from their self-describing tags. The values are <c>null</c>,
/// <see cref="bool"/>, <see cref="long"/>, <see cref="ulong"/>, <see cref="double"/>,
/// <see cref="string"/>, <see cref="byte"/>[], <see cref="Identity"/>, <see cref="Timestamp"/>, or an
/// <see cref="object"/>[] of those. Parameter names are not in the bytes: an integer argument is
/// <see cref="long"/> or <see cref="ulong"/> whatever the parameter declared, and an enum is its integer.
/// </summary>
public sealed record DecodedArguments(IReadOnlyList<object?> Values, string? Error)
{
    public bool Succeeded => Error is null;
}

/// <summary>
/// Decodes stored bytes with no schema and no application assemblies: rows and keys against a
/// <see cref="TableShape"/> from the shape sidecar, and reducer arguments from their tags. Row
/// format v1 is positional, so a shape is required to read a row at all, and it has to be the shape
/// that governed the row's LSN; <see cref="InspectSource.DecodeRow"/> picks it.
/// </summary>
public static class RowDecoder
{
    /// <summary>Decodes <paramref name="row"/> as written under <paramref name="shape"/>.</summary>
    public static DecodedRow DecodeRow(TableShape shape, ReadOnlySpan<byte> row)
    {
        ArgumentNullException.ThrowIfNull(shape);
        var columns = new List<DecodedColumn>(shape.Columns.Count);
        var reader = new RowReader(row);
        foreach (var column in shape.Columns)
        {
            object? value;
            try
            {
                value = ReadColumn(ref reader, column.Kind);
            }
            catch (Exception exception) when (exception is IndexOutOfRangeException or ArgumentOutOfRangeException or ArgumentException)
            {
                return new DecodedRow(
                    columns,
                    $"The row is {row.Length} bytes and ends inside column '{column.Name}' ({column.Kind}): the bytes and the recorded shape disagree.");
            }

            columns.Add(new DecodedColumn(column.Name, column.Kind, value));
        }

        return reader.Position == row.Length
            ? new DecodedRow(columns, null)
            : new DecodedRow(
                columns,
                $"The row is {row.Length} bytes but its recorded shape reads {reader.Position}: the bytes and the recorded shape disagree.");
    }

    /// <summary>Decodes a primary key under <paramref name="shape"/>'s key column.</summary>
    public static DecodedKey DecodeKey(TableShape shape, in RowKey key)
    {
        ArgumentNullException.ThrowIfNull(shape);
        var column = shape.Columns.FirstOrDefault(c => c.Name == shape.Key);
        if (column is null)
            return new DecodedKey(shape.Key, null, null, $"The recorded shape names key column '{shape.Key}' but does not list it.");
        try
        {
            return new DecodedKey(column.Name, column.Kind, SchemaKeyCodec.Decode(column.Kind, key), null);
        }
        catch (Exception exception) when (exception is IndexOutOfRangeException or ArgumentOutOfRangeException or ArgumentException or NotSupportedException)
        {
            return new DecodedKey(column.Name, column.Kind, null, $"A {key.Length}-byte key does not decode as {column.Kind}.");
        }
    }

    /// <summary>
    /// Decodes a commit record's <see cref="CommitRecord.Arguments"/>: a count, then that many
    /// tagged values. Empty bytes are no arguments.
    /// </summary>
    public static DecodedArguments DecodeArguments(ReadOnlySpan<byte> arguments)
    {
        if (arguments.IsEmpty)
            return new DecodedArguments([], null);
        var values = new List<object?>();
        var position = 0;
        try
        {
            var count = BinaryPrimitives.ReadUInt16LittleEndian(arguments);
            position = 2;
            for (var i = 0; i < count; i++)
                values.Add(ReadArgument(arguments, ref position, depth: 0));
        }
        catch (Exception exception) when (exception is IndexOutOfRangeException or ArgumentOutOfRangeException or InvalidDataException)
        {
            return new DecodedArguments(values, $"The arguments are {arguments.Length} bytes and stop decoding at byte {position}: {exception.Message}");
        }

        return position == arguments.Length
            ? new DecodedArguments(values, null)
            : new DecodedArguments(values, $"{arguments.Length - position} byte(s) follow the {values.Count} declared argument(s).");
    }

    private static object? ReadColumn(ref RowReader reader, ColumnKind kind) => kind switch
    {
        ColumnKind.Bool => reader.ReadBool(),
        ColumnKind.Int8 => reader.ReadInt8(),
        ColumnKind.UInt8 => reader.ReadUInt8(),
        ColumnKind.Int16 => reader.ReadInt16(),
        ColumnKind.UInt16 => reader.ReadUInt16(),
        ColumnKind.Int32 => reader.ReadInt32(),
        ColumnKind.UInt32 => reader.ReadUInt32(),
        ColumnKind.Int64 => reader.ReadInt64(),
        ColumnKind.UInt64 => reader.ReadUInt64(),
        ColumnKind.Float32 => reader.ReadFloat32(),
        ColumnKind.Float64 => reader.ReadFloat64(),
        ColumnKind.String => reader.ReadString(),
        ColumnKind.Bytes => reader.ReadBytes(),
        ColumnKind.Identity => reader.ReadIdentity(),
        ColumnKind.Timestamp => reader.ReadTimestamp(),
        ColumnKind.ScheduleAt => reader.ReadScheduleAt(),
        _ => throw new ArgumentException($"Unknown column kind {kind}."),
    };

    private static object? ReadArgument(ReadOnlySpan<byte> data, ref int position, int depth)
    {
        if (depth > 32)
            throw new InvalidDataException("arrays nest more than 32 deep.");
        var tag = (ArgsCodec.Tag)data[position++];
        switch (tag)
        {
            case ArgsCodec.Tag.Null:
                return null;
            case ArgsCodec.Tag.Bool:
                return data[position++] != 0;
            case ArgsCodec.Tag.Int64:
                var signed = BinaryPrimitives.ReadInt64LittleEndian(data[position..]);
                position += 8;
                return signed;
            case ArgsCodec.Tag.UInt64:
                var unsigned = BinaryPrimitives.ReadUInt64LittleEndian(data[position..]);
                position += 8;
                return unsigned;
            case ArgsCodec.Tag.Float64:
                var real = BinaryPrimitives.ReadDoubleLittleEndian(data[position..]);
                position += 8;
                return real;
            case ArgsCodec.Tag.String:
                var text = Encoding.UTF8.GetString(Counted(data, ref position));
                return text;
            case ArgsCodec.Tag.Bytes:
                return Counted(data, ref position).ToArray();
            case ArgsCodec.Tag.Identity:
                var identity = new Identity(data.Slice(position, Identity.Size));
                position += Identity.Size;
                return identity;
            case ArgsCodec.Tag.Timestamp:
                var micros = BinaryPrimitives.ReadInt64LittleEndian(data[position..]);
                position += 8;
                return new Timestamp(micros);
            case ArgsCodec.Tag.Array:
                var length = BinaryPrimitives.ReadInt32LittleEndian(data[position..]);
                position += 4;
                if (length < 0 || length > data.Length - position)
                    throw new InvalidDataException($"an array declares {length} elements.");
                var elements = new object?[length];
                for (var i = 0; i < length; i++)
                    elements[i] = ReadArgument(data, ref position, depth + 1);
                return elements;
            default:
                throw new InvalidDataException($"unknown argument tag {(byte)tag}.");
        }
    }

    private static ReadOnlySpan<byte> Counted(ReadOnlySpan<byte> data, ref int position)
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(data[position..]);
        position += 4;
        if (length < 0 || length > data.Length - position)
            throw new InvalidDataException($"a value declares {length} bytes.");
        var slice = data.Slice(position, length);
        position += length;
        return slice;
    }
}
