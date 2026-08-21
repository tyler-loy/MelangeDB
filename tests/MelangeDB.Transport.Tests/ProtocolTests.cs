using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The wire format itself: every frame type round-trips, MessagePack integer boundaries encode
/// correctly, and the client-side argument encoder is byte-compatible with the server-side
/// decoder the generated dispatchers use.
/// </summary>
public class ProtocolTests
{
    private readonly MessagePackFrameSerializer _serializer = new();

    public static TheoryData<Frame> Frames() => new((Frame[])
    [
        new HelloFrame(1, 3, "token") { Channel = MelangeChannels.Control },
        new WelcomeFrame(1, Guid.NewGuid(), Guid.NewGuid(), 42UL, "HTTP/2", Identity.FromIssuerSubject("iss", "sub")),
        new CallReducerFrame(7, "Greet", [1, 2, 3], "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01") { Channel = MelangeChannels.Calls },
        new ReducerResultFrame(7, true, 9UL, null, null) { Channel = MelangeChannels.Calls },
        new ReducerResultFrame(8, false, 0UL, "rejected", "PvP is off"),
        new SubscribeFrame(3, "SELECT * FROM t WHERE a = :p", new Dictionary<string, object?> { ["p"] = 5L }) { Channel = MelangeChannels.Data },
        new SubscribeFrame(4, "SELECT * FROM t", null),
        new UnsubscribeFrame(3),
        new UnsubscribedFrame(3),
        new SubscriptionAppliedFrame(
            3, 10UL, 0, false,
            [new WireRow([1, 2], new byte[] { 7, 0, 0, 0, 0, 0, 0, 0 }, default)],
            new WireDescriptor("t", [new WireColumn("Id", ColumnKind.Int64), new WireColumn("Name", ColumnKind.String)])) { Channel = 19 },
        new SubscriptionAppliedFrame(3, 10UL, 2, false, [new WireRow([1, 2], new byte[] { 9 }, new byte[] { 0b01 })]) { Channel = 19 },
        new SubscriptionAppliedFrame(3, 10UL, 3, true, []),
        new TransactionUpdateFrame(11UL, [new SubscriptionUpdate(3, [
            new WireRowOp(RowOpKind.Insert, [1], new byte[] { 1, 0, 0, 0 }, default),
            new WireRowOp(RowOpKind.Update, [3], new byte[] { 2 }, new byte[] { 0b10 }),
            new WireRowOp(RowOpKind.Delete, [2], default, default),
        ])])
        { Channel = MelangeChannels.Data },
        new ErrorFrame("parse", "Expected FROM", 0, 3),
        new PingFrame(1),
        new PongFrame(1),
        new ResumeFrame(Guid.NewGuid(), 100UL, [new ResumeSubscription(3, "SELECT * FROM t", null)]),
        new ResumeResultFrame(false, "retention exceeded"),
        new ReauthenticateFrame("fresh-token"),
        new ReauthenticateResultFrame(true, null),
    ]);

    [Theory]
    [MemberData(nameof(Frames))]
    public void Every_frame_type_round_trips_with_its_channel_tag(Frame frame)
    {
        var decoded = _serializer.Deserialize(_serializer.Serialize(frame));
        Assert.Equal(frame.Type, decoded.Type);
        Assert.Equal(frame.Channel, decoded.Channel);
        Assert.Equal(Convert.ToHexString(_serializer.Serialize(frame)), Convert.ToHexString(_serializer.Serialize(decoded)));
    }

    [Theory]
    [MemberData(nameof(Frames))]
    public void Measure_reports_exactly_what_serialize_produces(Frame frame)
    {
        // The delta path measures a frame under the engine's write lock and encodes it later, on
        // the sender. If these two ever disagree, the backpressure ledger drifts from the bytes
        // actually queued and Subscriptions:MaxBufferedBytes quietly stops meaning what it says.
        Assert.Equal(_serializer.Serialize(frame).Length, _serializer.Measure(frame));
    }

    [Fact]
    public void Measure_agrees_with_serialize_at_every_length_boundary()
    {
        // MessagePack picks its length prefix by size, so the sizes worth checking are the ones
        // either side of each prefix change — and multibyte text, where character count and byte
        // count part ways. Parameter maps are where values still ride under protocol v2.
        foreach (var value in BoundaryValues())
        {
            var frame = new SubscribeFrame(1, "SELECT * FROM t WHERE a = :v", new Dictionary<string, object?> { ["v"] = value });
            Assert.Equal(_serializer.Serialize(frame).Length, _serializer.Measure(frame));
        }

        // Rows ride as binary now, and binary has its own prefix boundaries.
        foreach (var length in (int[])[0, 1, 254, 255, 256, 65_535, 65_536])
        {
            var frame = new TransactionUpdateFrame(1UL, [new SubscriptionUpdate(1, [
                new WireRowOp(RowOpKind.Insert, [1], new byte[length], new byte[length == 0 ? 0 : 1]),
            ])]);
            Assert.Equal(_serializer.Serialize(frame).Length, _serializer.Measure(frame));
        }
    }

    [Fact]
    public void Measuring_a_frame_never_produces_its_bytes()
    {
        // Measure exists to avoid the encode, so a counting writer that quietly allocated and
        // filled a buffer would defeat the point while still passing every equality check above.
        var writer = MsgPackWriter.Counting();
        MessagePackFrameSerializer.WriteValue(ref writer, new string('m', 5_000));

        Assert.Empty(writer.ToArray());
        Assert.True(writer.Length > 5_000);
    }

    private static IEnumerable<object?> BoundaryValues()
    {
        foreach (var length in (int[])[0, 1, 31, 32, 255, 256, 65_535, 65_536])
        {
            yield return new string('a', length);
            yield return new string('é', length); // Two bytes per char: byte count is not char count.
            yield return new byte[length];
        }

        foreach (var number in (long[])[0, 127, 128, 255, 256, 65_535, 65_536, long.MaxValue, -1, -32, -33, -128, -129, long.MinValue])
            yield return number;

        yield return null;
        yield return true;
        yield return 3.5d;
        yield return 2.25f;
        yield return ulong.MaxValue;
    }

    [Fact]
    public void Wire_values_round_trip_across_messagepack_boundaries()
    {
        object?[] values =
        [
            null, true, false,
            0L, 1L, 127L, 128L, 255L, 256L, 65535L, 65536L, (long)uint.MaxValue, (long)uint.MaxValue + 1,
            long.MaxValue, -1L, -31L, -32L, -33L, -127L, -128L, -129L, -32768L, -32769L, long.MinValue,
            ulong.MaxValue, 3.5d, 2.25f, "", "short", new string('x', 31), new string('y', 32), new string('z', 300),
            Array.Empty<byte>(), new byte[] { 1, 2, 3 }, new byte[300],
        ];
        foreach (var value in values)
        {
            var writer = new MsgPackWriter(16);
            MessagePackFrameSerializer.WriteValue(ref writer, value);
            var reader = new MsgPackReader(writer.ToArray());
            var decoded = MessagePackFrameSerializer.ReadValue(ref reader);
            var expected = value switch
            {
                ulong big when big <= long.MaxValue => (long)big,
                _ => value,
            };
            Assert.Equal(expected, decoded);
        }
    }

    [Fact]
    public void Client_encoded_arguments_decode_through_the_server_side_reader()
    {
        var identity = Identity.Hash("alice");
        var payload = ReducerArgs.Encode(
            true, (sbyte)-5, (byte)200, (short)-1000, (ushort)50000, -123456, 3000000000U,
            long.MinValue, ulong.MaxValue, 2.5f, 3.25d, "hello", (string?)null,
            new byte[] { 9, 8 }, identity, new Timestamp(1234567), new[] { 1, 2, 3 });

        var reader = new ReducerArgsReader(payload, new ValidationOptions());
        reader.ExpectCount(17);
        Assert.True(reader.ReadBool());
        Assert.Equal((sbyte)-5, reader.ReadInt8());
        Assert.Equal((byte)200, reader.ReadUInt8());
        Assert.Equal((short)-1000, reader.ReadInt16());
        Assert.Equal((ushort)50000, reader.ReadUInt16());
        Assert.Equal(-123456, reader.ReadInt32());
        Assert.Equal(3000000000U, reader.ReadUInt32());
        Assert.Equal(long.MinValue, reader.ReadInt64());
        Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
        Assert.Equal(2.5f, reader.ReadFloat32());
        Assert.Equal(3.25d, reader.ReadFloat64());
        Assert.Equal("hello", reader.ReadString());
        Assert.Null(reader.ReadString());
        Assert.Equal(new byte[] { 9, 8 }, reader.ReadByteArray());
        Assert.Equal(identity, reader.ReadIdentity());
        Assert.Equal(new Timestamp(1234567), reader.ReadTimestamp());
        Assert.Equal(3, reader.BeginArray());
        Assert.Equal(1, reader.ReadInt32());
        Assert.Equal(2, reader.ReadInt32());
        Assert.Equal(3, reader.ReadInt32());
        reader.End();
    }

    [Fact]
    public void Malformed_frames_throw_rather_than_tear()
    {
        Assert.Throws<MelangeProtocolException>(() => _serializer.Deserialize([0x99, 0x01]));
        Assert.Throws<MelangeProtocolException>(() => _serializer.Deserialize([]));
        Assert.Throws<MelangeProtocolException>(() => _serializer.Deserialize([0x93, 0x63, 0x00, 0x00]));

        // A truncated valid frame:
        var bytes = _serializer.Serialize(new CallReducerFrame(1, "Greet", [1, 2, 3], null));
        Assert.Throws<MelangeProtocolException>(() => _serializer.Deserialize(bytes.AsSpan(0, bytes.Length - 2)));
    }
}

/// <summary>The SQL subset, parsed: only the supported shapes are valid, and everything else says why not.</summary>
public class SqlSubsetParserTests
{
    [Fact]
    public void Parses_the_supported_shapes()
    {
        var wholeTable = SqlSubsetParser.Parse("SELECT * FROM recipe", null);
        Assert.Equal("recipe", wholeTable.Table);
        Assert.Null(wholeTable.Projection);
        Assert.Equal(PredicateKind.None, wholeTable.Predicate);

        var equality = SqlSubsetParser.Parse(
            "SELECT * FROM inventory_item WHERE owner_id = :id",
            new Dictionary<string, object?> { ["id"] = 7L });
        Assert.Equal(PredicateKind.Equality, equality.Predicate);
        Assert.Equal("owner_id", equality.Column);
        Assert.Equal(7L, equality.EqualsValue);

        var range = SqlSubsetParser.Parse(
            "select * from terrain_chunk_data where chunk_id between :lo and :hi",
            new Dictionary<string, object?> { ["lo"] = 1L, ["hi"] = 9L });
        Assert.Equal(PredicateKind.Range, range.Predicate);
        Assert.Equal(1L, range.RangeLow);
        Assert.Equal(9L, range.RangeHigh);

        var notDefault = SqlSubsetParser.Parse("SELECT * FROM terrain_chunk_data WHERE edit_count <> 0", null);
        Assert.Equal(PredicateKind.NotDefault, notDefault.Predicate);
        Assert.Equal("edit_count", notDefault.Column);
        Assert.Equal(0L, notDefault.EqualsValue);

        var projection = SqlSubsetParser.Parse(
            "SELECT skill_id, total_xp, level FROM player_skill WHERE player_num = :id",
            new Dictionary<string, object?> { ["id"] = 3L });
        Assert.Equal(["skill_id", "total_xp", "level"], projection.Projection);
    }

    /// <summary>
    /// Both SQL spellings of inequality parse to the same shape. A generator in another language
    /// should not have to know which one this parser happened to pick.
    /// </summary>
    [Theory]
    [InlineData("SELECT * FROM t WHERE a <> 0")]
    [InlineData("SELECT * FROM t WHERE a != 0")]
    [InlineData("select * from t where a<>0")]
    [InlineData("SELECT * FROM t WHERE a <> :zero")]
    public void Both_spellings_of_inequality_parse_to_not_default(string query)
    {
        var parsed = SqlSubsetParser.Parse(query, new Dictionary<string, object?> { ["zero"] = 0L });
        Assert.Equal(PredicateKind.NotDefault, parsed.Predicate);
        Assert.Equal("a", parsed.Column);
        Assert.Equal(0L, parsed.EqualsValue);
    }

    [Fact]
    public void Literals_are_accepted_where_parameters_are()
    {
        var query = SqlSubsetParser.Parse("SELECT * FROM t WHERE a BETWEEN -5 AND 12", null);
        Assert.Equal(-5L, query.RangeLow);
        Assert.Equal(12L, query.RangeHigh);
        Assert.Equal("x y", SqlSubsetParser.Parse("SELECT * FROM t WHERE a = 'x y'", null).EqualsValue);
        Assert.Equal(2.5d, SqlSubsetParser.Parse("SELECT * FROM t WHERE a = 2.5", null).EqualsValue);
    }

    [Theory]
    [InlineData("DELETE FROM t")]
    [InlineData("SELECT * FROM t WHERE a > 5")]
    [InlineData("SELECT * FROM t WHERE a = :p AND b = :q")]
    [InlineData("SELECT * FROM t JOIN u ON t.a = u.a")]
    [InlineData("SELECT * FROM t WHERE a = 'unterminated")]
    [InlineData("SELECT FROM t")]
    [InlineData("SELECT *")]
    [InlineData("")]
    public void Everything_outside_the_subset_is_rejected(string query) =>
        Assert.ThrowsAny<Exception>(() => SqlSubsetParser.Parse(query, null));

    [Fact]
    public void A_named_parameter_without_a_value_names_itself_in_the_error()
    {
        var error = Assert.Throws<SqlParseException>(() =>
            SqlSubsetParser.Parse("SELECT * FROM t WHERE a = :missing", new Dictionary<string, object?>()));
        Assert.Contains(":missing", error.Message);
    }
}
