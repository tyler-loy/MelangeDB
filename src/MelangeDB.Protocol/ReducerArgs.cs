using System.Buffers.Binary;
using System.Text;

namespace MelangeDB.Protocol;

/// <summary>
/// Encodes reducer arguments into the payload the server's generated dispatchers decode: a
/// little-endian argument count, then self-describing tagged values. Byte-compatible with the
/// server-side codec — a compatibility test holds the two together.
/// </summary>
public static class ReducerArgs
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

    /// <summary>Encodes arguments for a <see cref="CallReducerFrame"/>.</summary>
    public static byte[] Encode(params object?[] arguments)
    {
        if (arguments.Length == 0)
            return [];
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write((ushort)arguments.Length);
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
                writer.Write(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case byte or ushort or uint or ulong:
                writer.Write((byte)Tag.UInt64);
                writer.Write(Convert.ToUInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case float or double:
                writer.Write((byte)Tag.Float64);
                writer.Write(Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case Enum e:
                if (Enum.GetUnderlyingType(e.GetType()) == typeof(ulong))
                {
                    writer.Write((byte)Tag.UInt64);
                    writer.Write(Convert.ToUInt64(e, System.Globalization.CultureInfo.InvariantCulture));
                }
                else
                {
                    writer.Write((byte)Tag.Int64);
                    writer.Write(Convert.ToInt64(e, System.Globalization.CultureInfo.InvariantCulture));
                }

                break;
            case string s:
                writer.Write((byte)Tag.String);
                var text = Encoding.UTF8.GetBytes(s);
                writer.Write(text.Length);
                writer.Write(text);
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
                throw new NotSupportedException(
                    $"Argument of type {value.GetType()} is not wire-encodable; supported kinds are " +
                    "null, bool, integers, floats, string, byte[], Identity, Timestamp, and arrays of those.");
        }
    }
}
