using System.Text.Json.Nodes;
using MelangeDB.Protocol;

namespace MelangeDB.Conformance;

/// <summary>
/// The corpus's pure-function vectors: encodings with exactly one right answer, produced by the
/// same codecs the server and the C# client run. A client must reproduce these bytes exactly —
/// unlike MessagePack framing, where any well-formed encoding is accepted, these are compared
/// byte for byte by the server (keys address rows; arguments are decoded positionally).
/// </summary>
public static class Vectors
{
    private delegate void WriteColumn(ref RowWriter writer);

    /// <summary>
    /// One column value per vector, as row-format bytes: what a column of that kind occupies inside
    /// a row. The frames cover every kind a public table can carry; these add <c>ScheduleAt</c>,
    /// which only scheduled tables may hold and scheduled tables are never public, so no frame
    /// carries it today — a decoder implements it anyway, because the kind is in the descriptor's
    /// vocabulary and a server may start sending it without a protocol bump.
    /// </summary>
    public static JsonArray Columns()
    {
        var vectors = new JsonArray();

        void Add(ColumnKind kind, WriteColumn write)
        {
            var writer = new RowWriter(16);
            write(ref writer);
            var bytes = writer.ToArray();
            var reader = new RowReader(bytes);
            vectors.Add(new JsonObject
            {
                ["kind"] = (int)kind,
                ["kindName"] = kind.ToString(),
                ["value"] = CorpusJson.Column(ref reader, kind),
                ["bytes"] = CorpusJson.Hex(bytes),
            });
        }

        Add(ColumnKind.Bool, static (ref RowWriter w) => w.WriteBool(true));
        Add(ColumnKind.Int8, static (ref RowWriter w) => w.WriteInt8(-2));
        Add(ColumnKind.UInt8, static (ref RowWriter w) => w.WriteUInt8(254));
        Add(ColumnKind.Int16, static (ref RowWriter w) => w.WriteInt16(-2));
        Add(ColumnKind.UInt16, static (ref RowWriter w) => w.WriteUInt16(0x1234));
        Add(ColumnKind.Int32, static (ref RowWriter w) => w.WriteInt32(-2));
        Add(ColumnKind.UInt32, static (ref RowWriter w) => w.WriteUInt32(0x12345678));
        Add(ColumnKind.Int64, static (ref RowWriter w) => w.WriteInt64(-2));
        Add(ColumnKind.UInt64, static (ref RowWriter w) => w.WriteUInt64(0x0123456789abcdef));
        Add(ColumnKind.Float32, static (ref RowWriter w) => w.WriteFloat32(-1.5f));
        Add(ColumnKind.Float64, static (ref RowWriter w) => w.WriteFloat64(-1.5));
        Add(ColumnKind.String, static (ref RowWriter w) => w.WriteString("hé"));
        Add(ColumnKind.String, static (ref RowWriter w) => w.WriteString(null));
        Add(ColumnKind.Bytes, static (ref RowWriter w) => w.WriteBytes([0xde, 0xad]));
        Add(ColumnKind.Bytes, static (ref RowWriter w) => w.WriteBytes(null));
        Add(ColumnKind.Identity, static (ref RowWriter w) => w.WriteIdentity(SampleRows.OtherIdentity));
        Add(ColumnKind.Timestamp, static (ref RowWriter w) => w.WriteTimestamp(new Timestamp(1_767_225_600_000_000)));
        Add(ColumnKind.ScheduleAt, static (ref RowWriter w) => w.WriteScheduleAt(ScheduleAt.FromMicroseconds(interval: false, 1_767_225_600_000_000)));
        Add(ColumnKind.ScheduleAt, static (ref RowWriter w) => w.WriteScheduleAt(ScheduleAt.FromMicroseconds(interval: true, 250_000)));
        return vectors;
    }

    /// <summary>
    /// Encoded primary keys, per key-encodable column kind. <c>value</c> is in the corpus's
    /// canonical form (see <see cref="CorpusJson"/>); <c>key</c> is the exact bytes.
    /// </summary>
    public static JsonArray Keys()
    {
        var vectors = new JsonArray();

        void Add(ColumnKind kind, JsonNode? value, RowKey key) =>
            vectors.Add(new JsonObject { ["kind"] = (int)kind, ["kindName"] = kind.ToString(), ["value"] = value, ["key"] = CorpusJson.Hex(key.Span) });

        foreach (var value in new[] { false, true })
            Add(ColumnKind.Bool, value, KeyCodec.EncodeBool(value));
        foreach (var value in new sbyte[] { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue })
            Add(ColumnKind.Int8, value, KeyCodec.EncodeInt8(value));
        foreach (var value in new byte[] { 0, 1, byte.MaxValue })
            Add(ColumnKind.UInt8, value, KeyCodec.EncodeUInt8(value));
        foreach (var value in new short[] { short.MinValue, -1, 0, 1, short.MaxValue })
            Add(ColumnKind.Int16, value, KeyCodec.EncodeInt16(value));
        foreach (var value in new ushort[] { 0, 1, 0x1234, ushort.MaxValue })
            Add(ColumnKind.UInt16, value, KeyCodec.EncodeUInt16(value));
        foreach (var value in new[] { int.MinValue, -1, 0, 1, 0x12345678, int.MaxValue })
            Add(ColumnKind.Int32, value, KeyCodec.EncodeInt32(value));
        foreach (var value in new uint[] { 0, 1, 0x12345678, uint.MaxValue })
            Add(ColumnKind.UInt32, value, KeyCodec.EncodeUInt32(value));
        foreach (var value in new[] { long.MinValue, -1, 0, 1, 0x0123456789abcdef, long.MaxValue })
            Add(ColumnKind.Int64, CorpusJson.I64(value), KeyCodec.EncodeInt64(value));
        foreach (var value in new ulong[] { 0, 1, 0x0123456789abcdef, ulong.MaxValue })
            Add(ColumnKind.UInt64, CorpusJson.U64(value), KeyCodec.EncodeUInt64(value));
        foreach (var value in new[] { string.Empty, "a", "plain", "été ☃ \U0001F600" })
            Add(ColumnKind.String, value, KeyCodec.EncodeString(value));
        foreach (var value in new byte[][] { [], [0x00], [0xde, 0xad, 0xbe, 0xef] })
            Add(ColumnKind.Bytes, CorpusJson.Hex(value), KeyCodec.EncodeBytes(value));
        foreach (var value in new[] { Identity.None, SampleRows.OtherIdentity })
            Add(ColumnKind.Identity, CorpusJson.Hex(value.ToByteArray()), KeyCodec.EncodeIdentity(value));
        foreach (var value in new[] { long.MinValue, -1, 0, 1_767_225_600_000_000 })
            Add(ColumnKind.Timestamp, CorpusJson.I64(value), KeyCodec.EncodeTimestamp(new Timestamp(value)));
        return vectors;
    }

    /// <summary>
    /// Reducer- and producer-argument payloads: the decoded arguments in canonical tagged form,
    /// and the exact bytes. The empty argument list is the empty payload, not a zero count.
    /// </summary>
    public static JsonArray Arguments()
    {
        var vectors = new JsonArray();

        void Add(string about, params object?[] arguments)
        {
            var bytes = ReducerArgs.Encode(arguments);
            vectors.Add(new JsonObject { ["about"] = about, ["arguments"] = CorpusJson.Arguments(bytes), ["bytes"] = CorpusJson.Hex(bytes) });
        }

        Add("no arguments: an empty payload");
        Add("null", [null]);
        Add("bools", true, false);
        Add("signed integers of every width widen to Int64", (sbyte)-1, (short)-2, -3, -4L, long.MinValue, long.MaxValue);
        Add("unsigned integers of every width widen to UInt64", (byte)1, (ushort)2, 3u, 4ul, ulong.MaxValue);
        Add("floats widen to Float64", 1.5f, -0.0, double.NaN, double.PositiveInfinity);
        Add("strings are UTF-8 with an Int32 byte length", string.Empty, "plain", "été ☃ \U0001F600");
        Add("bytes", Array.Empty<byte>(), new byte[] { 0xde, 0xad, 0xbe, 0xef });
        Add("identity and timestamp", SampleRows.OtherIdentity, new Timestamp(1_767_225_600_000_000));
        Add("an enum travels as its underlying integer", Mood.Furious);
        Add("arrays, nested", new[] { 1L, 2L, 3L }, new object?[] { "a", null, new[] { true } });
        return vectors;
    }
}
