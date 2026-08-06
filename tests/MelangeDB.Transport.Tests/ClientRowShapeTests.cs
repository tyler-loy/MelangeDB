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
    public void Reordered_columns_are_caught_although_the_set_is_identical()
    {
        // Position is contract under v2 — the same names in a different order is a different
        // format. A set comparison would call this equal, which is exactly why it is not one.
        var reordered = new WireDescriptor("Chunk", [Expected[1], Expected[0], Expected[2]]);

        Assert.Throws<MelangeSchemaMismatchException>(
            () => ClientRowShape.Verify("Chunk", Expected, reordered));
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
