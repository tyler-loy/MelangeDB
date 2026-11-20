using MelangeDB.Client;
using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// Schema drift, caught structurally.
/// <para>
/// This is the safety net protocol v2 had to build before it could remove the old one. Version 1
/// sent named maps, so a client that asked for a column the server no longer sent got a clean
/// "no such column" from the coercion table. Ordered row bytes have no names in them: a schema
/// that drifted by one column decodes into <em>plausible garbage</em> — an int read from the wrong
/// offset is still an int — and a client would carry on with silently wrong data. So the
/// comparison happens once, up front, against the descriptor, and everything below is the
/// enumeration of what "drifted" can mean.
/// </para>
/// </summary>
public class ClientRowShapeTests
{
    private static readonly WireColumn[] Expected =
    [
        new("Id", ColumnKind.Int64),
        new("X", ColumnKind.Int64),
        new("Data", ColumnKind.Bytes),
    ];

    [Fact]
    public void An_exactly_matching_descriptor_verifies()
    {
        ClientRowShape.Verify("Chunk", Expected, new WireDescriptor("Chunk", Expected));
    }

    [Fact]
    public void A_renamed_column_is_caught_with_both_names_in_the_message()
    {
        var drifted = Descriptor(new WireColumn("PosX", ColumnKind.Int64), at: 1);

        var failure = Assert.Throws<MelangeSchemaMismatchException>(
            () => ClientRowShape.Verify("Chunk", Expected, drifted));

        Assert.Contains("PosX", failure.Message);
        Assert.Contains("X", failure.Message);
    }

    [Fact]
    public void A_column_whose_kind_changed_is_caught_even_though_its_name_did_not()
    {
        // The one the map wire could miss most easily and the bytes wire cannot survive at all:
        // Int64 to Int32 keeps the name and halves the width, so every column after it would be
        // read four bytes early.
        var drifted = Descriptor(new WireColumn("X", ColumnKind.Int32), at: 1);

        var failure = Assert.Throws<MelangeSchemaMismatchException>(
            () => ClientRowShape.Verify("Chunk", Expected, drifted));

        Assert.Contains("Int32", failure.Message);
        Assert.Contains("Int64", failure.Message);
    }

    [Fact]
    public void A_column_that_went_missing_is_caught_by_name()
    {
        // A removal — and a rename, which is a removal plus an addition — leaves the bindings
        // reading a column the server no longer sends.
        var removed = new WireDescriptor("Chunk", [Expected[0], Expected[2]]);

        var failure = Assert.Throws<MelangeSchemaMismatchException>(
            () => ClientRowShape.Verify("Chunk", Expected, removed));

        Assert.Contains("'X'", failure.Message);
    }

    // -- Additive drift: tolerated, mapped by name (road-to-0.4 phase 28) -------------------------
    // Position used to be contract: any difference from the bindings' column list refused the
    // subscription, so every additive server deploy broke every client not yet rebuilt. The
    // descriptor names every column, so a client can find its own columns wherever they are.

    [Fact]
    public void An_exact_match_needs_no_projection()
    {
        Assert.Null(ClientRowShape.Plan("Chunk", Expected, new WireDescriptor("Chunk", Expected)));
    }

    [Fact]
    public void Reordered_columns_are_mapped_by_name()
    {
        var reordered = new WireDescriptor("Chunk", [Expected[2], Expected[0], Expected[1]]);
        var row = Row(writer =>
        {
            writer.WriteBytes([9, 8]);
            writer.WriteInt64(7);
            writer.WriteInt64(-3);
        });

        var projected = ClientRowShape.Plan("Chunk", Expected, reordered)!.Project(row);

        var (id, x, data) = Read(projected);
        Assert.Equal((7L, -3L), (id, x));
        Assert.Equal([9, 8], data);
    }

    [Fact]
    public void Added_columns_anywhere_are_skipped()
    {
        // A string and an Identity in the middle, a nullable string and a ScheduleAt at the end:
        // variable-width, fixed-width, null, and the nine-byte kind, all walked past by kind.
        var widened = new WireDescriptor("Chunk",
        [
            Expected[0],
            new WireColumn("Label", ColumnKind.String),
            new WireColumn("Owner", ColumnKind.Identity),
            Expected[1],
            Expected[2],
            new WireColumn("Note", ColumnKind.String),
            new WireColumn("Due", ColumnKind.ScheduleAt),
        ]);
        var row = Row(writer =>
        {
            writer.WriteInt64(7);
            writer.WriteString("ignored");
            writer.WriteIdentity(Identity.Hash("someone"));
            writer.WriteInt64(-3);
            writer.WriteBytes(null);
            writer.WriteString(null);
            writer.WriteScheduleAt(ScheduleAt.FromMicroseconds(interval: true, 5));
        });

        var projected = ClientRowShape.Plan("Chunk", Expected, widened)!.Project(row);

        Assert.Equal((7L, -3L, (byte[]?)null), Read(projected));
    }

    private static byte[] Row(Action<RowWriterBox> write)
    {
        var box = new RowWriterBox();
        write(box);
        return box.ToArray();
    }

    private static (long Id, long X, byte[]? Data) Read(byte[] projected)
    {
        var reader = new RowReader(projected);
        var result = (reader.ReadInt64(), reader.ReadInt64(), reader.ReadBytes());
        Assert.Equal(projected.Length, reader.Position);
        return result;
    }

    /// <summary>A class wrapper so a lambda can write to the mutable <see cref="RowWriter"/> struct.</summary>
    private sealed class RowWriterBox
    {
        private RowWriter _writer = new(64);

        public void WriteInt64(long value) => _writer.WriteInt64(value);

        public void WriteString(string? value) => _writer.WriteString(value);

        public void WriteBytes(byte[]? value) => _writer.WriteBytes(value);

        public void WriteIdentity(Identity value) => _writer.WriteIdentity(value);

        public void WriteScheduleAt(ScheduleAt value) => _writer.WriteScheduleAt(value);

        public byte[] ToArray() => _writer.ToArray();
    }

    [Fact]
    public void A_projected_subscription_is_refused_by_name_rather_than_decoded_partially()
    {
        var projected = new WireDescriptor("Chunk", [Expected[0], Expected[1]]);

        var failure = Assert.Throws<MelangeSchemaMismatchException>(
            () => ClientRowShape.Verify("Chunk", Expected, projected));

        Assert.Contains("untyped", failure.Message);
    }

    [Fact]
    public void Rows_shaped_for_another_table_are_refused()
    {
        var failure = Assert.Throws<MelangeSchemaMismatchException>(
            () => ClientRowShape.Verify("Chunk", Expected, new WireDescriptor("Skill", Expected)));

        Assert.Contains("Skill", failure.Message);
    }

    private static WireDescriptor Descriptor(WireColumn replacement, int at)
    {
        var columns = Expected.ToArray();
        columns[at] = replacement;
        return new WireDescriptor("Chunk", columns);
    }
}
