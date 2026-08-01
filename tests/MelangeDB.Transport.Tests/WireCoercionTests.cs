using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// The one coercion table, proven against the real serializer: every client-visible
/// <c>ColumnKind</c> is written with <see cref="MessagePackFrameSerializer"/> exactly as the
/// server frames rows, decoded back, and read through <see cref="ClientWireValues"/>. Hand-built
/// dictionaries would prove nothing — MessagePack's lossiness (ints as long, floats kept distinct
/// from doubles, Identity as raw bytes) is precisely what is under test.
/// </summary>
public class WireCoercionTests
{
    /// <summary>Round-trips a column map through a real delta frame, byte-for-byte the server's path.</summary>
    private static IReadOnlyDictionary<string, object?> RoundTrip(Dictionary<string, object?> columns)
    {
        var serializer = new MessagePackFrameSerializer();
        var frame = new TransactionUpdateFrame(1, [new SubscriptionUpdate(1, [new WireRowOp(RowOpKind.Insert, [1], columns)])]);
        var decoded = (TransactionUpdateFrame)serializer.Deserialize(serializer.Serialize(frame));
        return decoded.Updates[0].Ops[0].Columns!;
    }

    [Fact]
    public void Every_client_visible_kind_coerces_through_the_real_wire()
    {
        var identity = Identity.Hash("coercion");
        var columns = RoundTrip(new Dictionary<string, object?>
        {
            ["Bool"] = true,
            ["Int8"] = sbyte.MinValue,
            ["UInt8"] = byte.MaxValue,
            ["Int16"] = short.MinValue,
            ["UInt16"] = ushort.MaxValue,
            ["Int32"] = int.MinValue,
            ["UInt32"] = uint.MaxValue,
            ["Int64"] = long.MinValue,
            ["UInt64"] = ulong.MaxValue,
            ["Float32"] = 1.5f,
            ["Float64"] = Math.PI,
            ["String"] = "melange",
            ["Bytes"] = new byte[] { 1, 2, 3 },
            ["Identity"] = identity,
            ["Timestamp"] = new Timestamp(1_234_567_890_123_456),
        });

        Assert.True(ClientWireValues.ReadBool(columns, "Bool", "T"));
        Assert.Equal(sbyte.MinValue, ClientWireValues.ReadInt8(columns, "Int8", "T"));
        Assert.Equal(byte.MaxValue, ClientWireValues.ReadUInt8(columns, "UInt8", "T"));
        Assert.Equal(short.MinValue, ClientWireValues.ReadInt16(columns, "Int16", "T"));
        Assert.Equal(ushort.MaxValue, ClientWireValues.ReadUInt16(columns, "UInt16", "T"));
        Assert.Equal(int.MinValue, ClientWireValues.ReadInt32(columns, "Int32", "T"));
        Assert.Equal(uint.MaxValue, ClientWireValues.ReadUInt32(columns, "UInt32", "T"));
        Assert.Equal(long.MinValue, ClientWireValues.ReadInt64(columns, "Int64", "T"));
        Assert.Equal(ulong.MaxValue, ClientWireValues.ReadUInt64(columns, "UInt64", "T"));
        Assert.Equal(1.5f, ClientWireValues.ReadFloat32(columns, "Float32", "T"));
        Assert.Equal(Math.PI, ClientWireValues.ReadFloat64(columns, "Float64", "T"));
        Assert.Equal("melange", ClientWireValues.ReadString(columns, "String", "T"));
        Assert.Equal(new byte[] { 1, 2, 3 }, ClientWireValues.ReadBytes(columns, "Bytes", "T"));
        Assert.Equal(identity, ClientWireValues.ReadIdentity(columns, "Identity", "T"));
        Assert.Equal(1_234_567_890_123_456, ClientWireValues.ReadTimestamp(columns, "Timestamp", "T").UnixTimeMicroseconds);
    }

    [Fact]
    public void Boundary_values_survive_the_integer_lossiness()
    {
        // The wire's soft spots: a ulong beyond long.MaxValue surfaces as ulong, at or below it
        // as long; zero and the extremes of every narrower kind ride as long. The coercion table
        // must accept each without a Convert call in sight.
        var columns = RoundTrip(new Dictionary<string, object?>
        {
            ["UInt64Small"] = 7UL,
            ["UInt64Edge"] = (ulong)long.MaxValue,
            ["UInt64Big"] = (ulong)long.MaxValue + 1,
            ["Int64Max"] = long.MaxValue,
            ["Zero"] = 0,
            ["NegOne"] = -1,
        });

        Assert.Equal(7UL, ClientWireValues.ReadUInt64(columns, "UInt64Small", "T"));
        Assert.Equal((ulong)long.MaxValue, ClientWireValues.ReadUInt64(columns, "UInt64Edge", "T"));
        Assert.Equal((ulong)long.MaxValue + 1, ClientWireValues.ReadUInt64(columns, "UInt64Big", "T"));
        Assert.Equal(long.MaxValue, ClientWireValues.ReadInt64(columns, "Int64Max", "T"));
        Assert.Equal(0, ClientWireValues.ReadInt32(columns, "Zero", "T"));
        Assert.Equal(-1, ClientWireValues.ReadInt32(columns, "NegOne", "T"));
    }

    [Fact]
    public void Nullable_kinds_pass_null_through()
    {
        var columns = RoundTrip(new Dictionary<string, object?>
        {
            ["String"] = null,
            ["Bytes"] = null,
        });

        Assert.Null(ClientWireValues.ReadString(columns, "String", "T"));
        Assert.Null(ClientWireValues.ReadBytes(columns, "Bytes", "T"));
    }

    [Fact]
    public void Floats_stay_distinct_from_doubles_on_the_wire()
    {
        // float.MaxValue as a float must come back as a float; the same magnitude written as a
        // double must refuse to bind to a Float32 column — silent narrowing is the bug class.
        var columns = RoundTrip(new Dictionary<string, object?>
        {
            ["F"] = float.MaxValue,
            ["D"] = (double)float.MaxValue,
        });

        Assert.Equal(float.MaxValue, ClientWireValues.ReadFloat32(columns, "F", "T"));
        Assert.Throws<MelangeSchemaMismatchException>(() => ClientWireValues.ReadFloat32(columns, "D", "T"));
        Assert.Throws<MelangeSchemaMismatchException>(() => ClientWireValues.ReadFloat64(columns, "F", "T"));
    }

    [Fact]
    public void Drift_fails_loudly_not_as_defaults()
    {
        var columns = RoundTrip(new Dictionary<string, object?> { ["Present"] = 1 });

        var missing = Assert.Throws<MelangeSchemaMismatchException>(() => ClientWireValues.ReadInt32(columns, "Absent", "Player"));
        Assert.Contains("Player", missing.Message);
        Assert.Contains("Absent", missing.Message);

        var wrongKind = Assert.Throws<MelangeSchemaMismatchException>(() => ClientWireValues.ReadString(columns, "Present", "Player"));
        Assert.Contains("Present", wrongKind.Message);

        var outOfRange = RoundTrip(new Dictionary<string, object?> { ["Small"] = 300 });
        Assert.Throws<MelangeSchemaMismatchException>(() => ClientWireValues.ReadUInt8(outOfRange, "Small", "Player"));
    }
}
