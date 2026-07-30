using System.Text;

namespace MelangeDB.Core;

/// <summary>
/// Serializes reducer arguments: the format <see cref="ReducerArgsReader"/> decodes for dispatch
/// and the log stores as audit metadata. Self-describing tagged values; enums as their underlying
/// integer; arrays as a count plus tagged elements. Values outside the supported kinds fall back
/// to their string form (they are then audit-only — no reducer parameter can declare them).
/// </summary>
internal static class ArgsCodec
{
    internal enum Tag : byte
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

    public static byte[] Encode(IReadOnlyList<object?>? arguments)
    {
        if (arguments is null || arguments.Count == 0)
            return [];
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write((ushort)arguments.Count);
        foreach (var argument in arguments)
            WriteArgument(writer, argument);
        writer.Flush();
        return stream.ToArray();
    }

    private static void WriteArgument(BinaryWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.Write((byte)Tag.Null);
                break;
            case bool b:
                writer.Write((byte)Tag.Bool);
                writer.Write(b);
                break;
            case sbyte or short or int or long:
                writer.Write((byte)Tag.Int64);
                writer.Write(Convert.ToInt64(value));
                break;
            case byte or ushort or uint or ulong:
                writer.Write((byte)Tag.UInt64);
                writer.Write(Convert.ToUInt64(value));
                break;
            case float or double:
                writer.Write((byte)Tag.Float64);
                writer.Write(Convert.ToDouble(value));
                break;
            case Enum e:
                if (Enum.GetUnderlyingType(e.GetType()) == typeof(ulong))
                {
                    writer.Write((byte)Tag.UInt64);
                    writer.Write(Convert.ToUInt64(e));
                }
                else
                {
                    writer.Write((byte)Tag.Int64);
                    writer.Write(Convert.ToInt64(e));
                }

                break;
            case byte[] bytes:
                writer.Write((byte)Tag.Bytes);
                writer.Write(bytes.Length);
                writer.Write(bytes);
                break;
            case Identity identity:
                writer.Write((byte)Tag.Identity);
                Span<byte> buffer = stackalloc byte[Identity.Size];
                identity.WriteTo(buffer);
                writer.Write(buffer);
                break;
            case Timestamp timestamp:
                writer.Write((byte)Tag.Timestamp);
                writer.Write(timestamp.UnixTimeMicroseconds);
                break;
            case Array array:
                writer.Write((byte)Tag.Array);
                writer.Write(array.Length);
                foreach (var element in array)
                    WriteArgument(writer, element);
                break;
            default:
                writer.Write((byte)Tag.String);
                var text = Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty);
                writer.Write(text.Length);
                writer.Write(text);
                break;
        }
    }
}
