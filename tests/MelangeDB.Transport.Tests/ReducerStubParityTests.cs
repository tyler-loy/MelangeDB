using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Generated reducer stubs encode by boxing typed arguments into <see cref="ReducerArgs"/> — so
/// these tests hold the stub-side encoding against the exact reader pass a generated server
/// descriptor performs, kind by kind, arrays included. The asymmetries under proof: unsigned
/// integers ride as UInt64, enums narrow to their underlying kind, and the reader range-checks
/// on the way in. Timer rows are deliberately absent — the manifest never advertises a scheduled
/// reducer, TypedBindingsTests pins that no stub exists, and the server tells a client naming one
/// "unknown".
/// </summary>
public class ReducerStubParityTests
{
    private static readonly ValidationOptions Limits = new();

    [Fact]
    public void Every_scalar_kind_decodes_as_the_descriptor_reads_it()
    {
        var identity = Identity.Hash("parity");
        var timestamp = new Timestamp(777_000_111);

        // Exactly what a generated stub sends: typed values boxed into the args array.
        var payload = ReducerArgs.Encode(
            true,
            (sbyte)-8, (byte)8,
            (short)-16, (ushort)16,
            -32, 32u,
            -64L, ulong.MaxValue,
            1.25f, Math.E,
            "melange", (string?)null,
            new byte[] { 9, 8 },
            identity, timestamp);

        var reader = new ReducerArgsReader(payload, Limits);
        reader.ExpectCount(16);
        Assert.True(reader.ReadBool());
        Assert.Equal((sbyte)-8, reader.ReadInt8());
        Assert.Equal((byte)8, reader.ReadUInt8());
        Assert.Equal((short)-16, reader.ReadInt16());
        Assert.Equal((ushort)16, reader.ReadUInt16());
        Assert.Equal(-32, reader.ReadInt32());
        Assert.Equal(32u, reader.ReadUInt32());
        Assert.Equal(-64L, reader.ReadInt64());
        Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
        Assert.Equal(1.25f, reader.ReadFloat32());
        Assert.Equal(Math.E, reader.ReadFloat64());
        Assert.Equal("melange", reader.ReadString());
        Assert.Null(reader.ReadString());
        Assert.Equal(new byte[] { 9, 8 }, reader.ReadByteArray());
        Assert.Equal(identity, reader.ReadIdentity());
        Assert.Equal(timestamp, reader.ReadTimestamp());
        reader.End();
    }

    [Fact]
    public void Client_side_enums_narrow_to_what_the_server_descriptor_reads()
    {
        // The stub passes the generated MelangeDB.Types enum; the server's descriptor reads the
        // underlying kind and casts to its own enum type. Underlying value identity is the
        // contract — the two enum TYPES never meet.
        var payload = ReducerArgs.Encode(MelangeDB.Types.ContainerKind.WorldContainer);
        var reader = new ReducerArgsReader(payload, Limits);
        reader.ExpectCount(1);
        Assert.Equal(ContainerKind.WorldContainer, (ContainerKind)reader.ReadInt32());
        reader.End();
    }

    [Fact]
    public void Arrays_of_every_element_shape_decode_elementwise()
    {
        var payload = ReducerArgs.Encode(
            new[] { 1, -2, 3 },
            new[] { "a", "b" },
            new ulong[] { ulong.MaxValue, 0 },
            new[] { MelangeDB.Types.ContainerKind.PlayerPack, MelangeDB.Types.ContainerKind.WorldContainer },
            (int[]?)null);

        var reader = new ReducerArgsReader(payload, Limits);
        reader.ExpectCount(5);

        Assert.Equal(3, reader.BeginArray());
        Assert.Equal(1, reader.ReadInt32());
        Assert.Equal(-2, reader.ReadInt32());
        Assert.Equal(3, reader.ReadInt32());

        Assert.Equal(2, reader.BeginArray());
        Assert.Equal("a", reader.ReadString());
        Assert.Equal("b", reader.ReadString());

        Assert.Equal(2, reader.BeginArray());
        Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
        Assert.Equal(0UL, reader.ReadUInt64());

        Assert.Equal(2, reader.BeginArray());
        Assert.Equal(ContainerKind.PlayerPack, (ContainerKind)reader.ReadInt32());
        Assert.Equal(ContainerKind.WorldContainer, (ContainerKind)reader.ReadInt32());

        Assert.Equal(-1, reader.BeginArray());
        reader.End();
    }

    [Fact]
    public void The_emitted_query_templates_are_exactly_what_the_real_parser_accepts()
    {
        // The three SQL strings the client generator emits, parsed by the server's own parser —
        // not a lookalike. The end-to-end binding tests prove acceptance through a live server;
        // this pins the shapes at the parser seam, where a template typo would first exist.
        var all = SqlSubsetParser.Parse("SELECT * FROM Chunk", null);
        Assert.Equal(PredicateKind.None, all.Predicate);
        Assert.Null(all.Projection);

        var equality = SqlSubsetParser.Parse(
            "SELECT * FROM Chunk WHERE X = :p",
            new Dictionary<string, object?> { ["p"] = 7L });
        Assert.Equal(PredicateKind.Equality, equality.Predicate);
        Assert.Equal("X", equality.Column);
        Assert.Equal(7L, equality.EqualsValue);

        var range = SqlSubsetParser.Parse(
            "SELECT * FROM Chunk WHERE X BETWEEN :lo AND :hi",
            new Dictionary<string, object?> { ["lo"] = 0L, ["hi"] = 10L });
        Assert.Equal(PredicateKind.Range, range.Predicate);
        Assert.Equal(0L, range.RangeLow);
        Assert.Equal(10L, range.RangeHigh);
    }
}
