namespace MelangeDB.Protocol;

/// <summary>One column a subscription's rows carry, named and kinded, in row-byte order.</summary>
public readonly record struct WireColumn(string Name, ColumnKind Kind);

/// <summary>
/// The shape of every row bytes blob one subscription sends: the table, and the ordered columns the
/// bytes carry. Sent once — on chunk 0 of the initial set — and held by the client for the life of
/// the subscription, which is what lets every row after it be nothing but its values.
/// <para>
/// A subscription's shape cannot change while it lives: re-scoping is forbidden from changing the
/// projection, and a schema change means a new epoch, which means a full re-establishment and a new
/// descriptor. A resumed subscription keeps the descriptor it already holds, correctly — a resume
/// is only accepted against the same log epoch.
/// </para>
/// <para>
/// Rows may still carry <em>fewer</em> columns than the descriptor names, when a column policy
/// masks per row. That is what <see cref="WireRow.ColumnMask"/> reports; an empty mask means every
/// descriptor column is present, which is the case that matters and the one that costs one byte.
/// </para>
/// </summary>
public sealed record WireDescriptor(string Table, IReadOnlyList<WireColumn> Columns);

/// <summary>
/// Decodes row bytes against a <see cref="WireDescriptor"/>. This is the untyped client's path;
/// generated bindings read the same bytes column by column with no dictionary at all.
/// </summary>
public static class WireRowValues
{
    /// <summary>Whether descriptor column <paramref name="ordinal"/> is present under <paramref name="mask"/>.</summary>
    public static bool IsPresent(ReadOnlySpan<byte> mask, int ordinal) =>
        mask.IsEmpty || (mask[ordinal >> 3] & (1 << (ordinal & 7))) != 0;

    /// <summary>The byte length a mask over <paramref name="columnCount"/> columns occupies.</summary>
    public static int MaskLength(int columnCount) => (columnCount + 7) / 8;

    /// <summary>
    /// Decodes the row into a name→value map, skipping columns the mask excludes. Values use the
    /// same CLR shapes the v1 map wire used, so an untyped consumer reads exactly what it did
    /// before — <see cref="Identity"/> as itself rather than as raw bytes is the one deliberate
    /// improvement, since the bytes are no longer routed through MessagePack's lossy value set.
    /// </summary>
    public static Dictionary<string, object?> ToColumns(WireDescriptor descriptor, ReadOnlySpan<byte> row, ReadOnlySpan<byte> mask)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        var reader = new RowReader(row);
        var columns = new Dictionary<string, object?>(descriptor.Columns.Count, StringComparer.Ordinal);
        for (var i = 0; i < descriptor.Columns.Count; i++)
        {
            if (!IsPresent(mask, i))
                continue;
            var column = descriptor.Columns[i];
            columns[column.Name] = ReadValue(ref reader, column.Kind);
        }

        return columns;
    }

    private static object? ReadValue(ref RowReader reader, ColumnKind kind) => kind switch
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
        _ => throw new MelangeProtocolException($"Unknown column kind {kind}."),
    };
}
