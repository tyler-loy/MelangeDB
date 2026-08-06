using MelangeDB.Core;
using MelangeDB.Protocol;
using MelangeDB.Server;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Wire fidelity under protocol v2: the bytes the server's row serializer writes are the bytes the
/// wire carries and the bytes a client decodes, for every client-visible column kind.
/// <para>
/// This replaced a coercion-table test, and the replacement is doing different work. Version 1
/// sent MessagePack maps, so it was <em>lossy on purpose</em> — every integer arrived as
/// <c>long</c>, an <c>Identity</c> as raw bytes, a <c>float</c> indistinguishable from a
/// <c>double</c> if you were careless — and the old test pinned the coercion table that undid the
/// damage. Version 2 has no damage to undo. What is at risk instead is <b>drift</b>: two halves of
/// one format, a writer in Core and a reader reached from the client, which must agree byte for
/// byte forever. So the assertion runs the real writer, puts its output through the real frame
/// serializer, and reads it back the way a client does.
/// </para>
/// </summary>
public class WireRowTests
{
    private static readonly TableSchema Schema = SchemaRegistry.FromTypes(typeof(AllKinds)).Tables.Single();

    private static readonly WireDescriptor Descriptor =
        new("AllKinds", [.. Schema.Columns.Select(c => new WireColumn(c.Name, c.Kind))]);

    [Fact]
    public void Every_client_visible_kind_survives_the_wire_exactly()
    {
        var who = Identity.Hash("fidelity");
        var row = new AllKinds
        {
            Id = 42,
            Flag = true,
            Int8 = sbyte.MinValue,
            UInt8 = byte.MaxValue,
            Int16 = short.MinValue,
            UInt16 = ushort.MaxValue,
            Int32 = int.MinValue,
            UInt32 = uint.MaxValue,
            Int64 = long.MinValue,
            UInt64 = ulong.MaxValue,
            Float32 = 1.5f,
            Float64 = Math.PI,
            Text = "melange",
            Blob = [1, 2, 3],
            Who = who,
            At = new Timestamp(1_234_567_890_123_456),
        };

        var columns = RoundTrip(row);

        Assert.Equal(42L, columns["Id"]);
        Assert.Equal(true, columns["Flag"]);
        Assert.Equal(sbyte.MinValue, columns["Int8"]);
        Assert.Equal(byte.MaxValue, columns["UInt8"]);
        Assert.Equal(short.MinValue, columns["Int16"]);
        Assert.Equal(ushort.MaxValue, columns["UInt16"]);
        Assert.Equal(int.MinValue, columns["Int32"]);
        Assert.Equal(uint.MaxValue, columns["UInt32"]);
        Assert.Equal(long.MinValue, columns["Int64"]);
        Assert.Equal(ulong.MaxValue, columns["UInt64"]);
        Assert.Equal(1.5f, columns["Float32"]);
        Assert.Equal(Math.PI, columns["Float64"]);
        Assert.Equal("melange", columns["Text"]);
        Assert.Equal(new byte[] { 1, 2, 3 }, columns["Blob"]);
        Assert.Equal(who, columns["Who"]);
        Assert.Equal(new Timestamp(1_234_567_890_123_456), columns["At"]);
    }

    [Fact]
    public void Every_kind_keeps_its_own_CLR_type_rather_than_widening()
    {
        // The whole class of bug protocol v1's coercion table existed to catch: a UInt8 that
        // arrives as long, a Float32 that arrives as double. Under v2 the format is fixed-width
        // and self-describing through the descriptor, so the types must come back exact — and a
        // test that only compared values would pass even if every one of them widened.
        var columns = RoundTrip(new AllKinds { Text = "x", Blob = [] });

        Assert.IsType<long>(columns["Id"]);
        Assert.IsType<bool>(columns["Flag"]);
        Assert.IsType<sbyte>(columns["Int8"]);
        Assert.IsType<byte>(columns["UInt8"]);
        Assert.IsType<short>(columns["Int16"]);
        Assert.IsType<ushort>(columns["UInt16"]);
        Assert.IsType<int>(columns["Int32"]);
        Assert.IsType<uint>(columns["UInt32"]);
        Assert.IsType<long>(columns["Int64"]);
        Assert.IsType<ulong>(columns["UInt64"]);
        Assert.IsType<float>(columns["Float32"]);
        Assert.IsType<double>(columns["Float64"]);
        Assert.IsType<string>(columns["Text"]);
        Assert.IsType<byte[]>(columns["Blob"]);
        Assert.IsType<Identity>(columns["Who"]);
        Assert.IsType<Timestamp>(columns["At"]);
    }

    [Fact]
    public void Nullable_kinds_pass_null_through()
    {
        var columns = RoundTrip(new AllKinds { Text = null!, Blob = null! });

        Assert.Null(columns["Text"]);
        Assert.Null(columns["Blob"]);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(254)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(65_535)]
    [InlineData(65_536)]
    public void Payload_lengths_survive_every_prefix_boundary(int length)
    {
        // Both length prefixes are in play at once: row format v1's int32 inside the row, and
        // MessagePack's variable binary header outside it. A frame is only correct if they agree
        // at every size where either one changes width.
        var columns = RoundTrip(new AllKinds
        {
            Text = new string('é', length), // Two bytes per char: byte count is not char count.
            Blob = new byte[length],
        });

        Assert.Equal(new string('é', length), columns["Text"]);
        Assert.Equal(length, ((byte[])columns["Blob"]!).Length);
    }

    [Fact]
    public void A_masked_row_carries_only_the_columns_its_mask_names()
    {
        // The column-policy shape: the row bytes hold a subset, in descriptor order, and the mask
        // says which. Decoding without honouring the mask would read the next column's bytes as
        // this one's — plausible garbage rather than a failure, which is exactly why the mask
        // travels with the row instead of being inferred.
        var writer = new RowWriter(16);
        writer.WriteInt64(7);
        writer.WriteString("visible");
        var mask = new byte[WireRowValues.MaskLength(Descriptor.Columns.Count)];
        SetBit(mask, IndexOf("Id"));
        SetBit(mask, IndexOf("Text"));

        var columns = WireRowValues.ToColumns(Descriptor, writer.ToArray(), mask);

        Assert.Equal(["Id", "Text"], columns.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(7L, columns["Id"]);
        Assert.Equal("visible", columns["Text"]);
    }

    [Fact]
    public void An_empty_mask_means_every_column_not_no_columns()
    {
        // The one-byte encoding of the common case. Reading it as "nothing is present" would make
        // every ordinary row arrive empty, so it is worth an assertion of its own.
        Assert.True(WireRowValues.IsPresent(default, 0));
        Assert.True(WireRowValues.IsPresent(default, 63));
        Assert.Equal(Descriptor.Columns.Count, RoundTrip(new AllKinds { Text = "x", Blob = [] }).Count);
    }

    [Fact]
    public void Projecting_every_column_hands_back_the_original_bytes_rather_than_a_copy()
    {
        // The initial-set and re-scope paths reach the projector directly, without the fan-out's
        // memo in front of it, so the no-copy shortcut has to live here too. Both spellings of
        // "everything" take it: a null projection, and a set that happens to name every column —
        // which is what an explicit `SELECT a, b, c` and a fully-permissive column policy produce.
        var row = RowSerializer.Serialize(Schema, new AllKinds { Text = "x", Blob = [1] }).AsMemory();
        var everything = new HashSet<string>(Schema.Columns.Select(c => c.Name), StringComparer.Ordinal);

        AssertSameMemory(row, RowWire.Project(Schema, row, null));
        AssertSameMemory(row, RowWire.Project(Schema, row, everything));
    }

    [Fact]
    public void A_projection_emits_exactly_the_kept_columns_slices_in_schema_order()
    {
        // The projector copies raw slices rather than decoding and re-encoding, so the proof is
        // that its output equals a row containing only those columns, written from scratch.
        var row = RowSerializer.Serialize(Schema, new AllKinds { Id = 9, Text = "kept", Blob = [7, 7] }).AsMemory();

        var projected = RowWire.Project(Schema, row, new HashSet<string>(["Id", "Text"], StringComparer.Ordinal));

        var expected = new RowWriter(16);
        expected.WriteInt64(9);
        expected.WriteString("kept");
        Assert.Equal(expected.ToArray(), projected.ToArray());
    }

    private static void AssertSameMemory(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right) =>
        Assert.True(
            left.Length == right.Length && !left.IsEmpty && left.Span.Overlaps(right.Span, out var offset) && offset == 0,
            "the projector copied a row it did not need to copy");

    private static Dictionary<string, object?> RoundTrip(AllKinds row)
    {
        // The server's own writer, the real frame serializer, and the client's reader — the three
        // pieces that have to agree, exercised as one.
        var serializer = new MessagePackFrameSerializer();
        var bytes = RowSerializer.Serialize(Schema, row);
        var frame = new TransactionUpdateFrame(1, [new SubscriptionUpdate(1, [
            new WireRowOp(RowOpKind.Insert, [1], bytes, default),
        ])]);
        var decoded = (TransactionUpdateFrame)serializer.Deserialize(serializer.Serialize(frame));
        var op = decoded.Updates[0].Ops[0];
        return WireRowValues.ToColumns(Descriptor, op.Row.Span, op.ColumnMask.Span);
    }

    private static int IndexOf(string column)
    {
        for (var i = 0; i < Descriptor.Columns.Count; i++)
        {
            if (Descriptor.Columns[i].Name == column)
                return i;
        }

        throw new InvalidOperationException($"No column '{column}'.");
    }

    private static void SetBit(byte[] mask, int ordinal) => mask[ordinal >> 3] |= (byte)(1 << (ordinal & 7));
}
