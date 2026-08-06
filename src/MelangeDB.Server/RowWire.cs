using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>
/// Schema-interpreted access to serialized v1 rows: wire projection for frames, JSON column maps
/// for the HTTP endpoints, column-slice comparison for projection masking, and predicate encoding.
/// Walks the row bytes by column kind — no reflection, no row type instance — so it works
/// identically for generated and reflection-built schemas on the fan-out hot path.
/// </summary>
internal static class RowWire
{
    /// <summary>
    /// The row as this projection puts it on the wire: the store's own bytes when the projection
    /// keeps every column, else the kept columns' raw slices concatenated in schema order.
    /// <para>
    /// This is the server half of protocol v2, and the reason it is a win: the store already holds
    /// the row in exactly the format the wire wants, so the common case — no projection, no
    /// <c>[ServerOnly]</c> column — is a reference copy and nothing else. A row leaves the fan-out
    /// path without being decoded, without a dictionary, and without a string per column.
    /// </para>
    /// </summary>
    public static ReadOnlyMemory<byte> Project(TableSchema schema, ReadOnlyMemory<byte> row, IReadOnlySet<string>? projection)
    {
        if (projection is null)
            return row;

        var span = row.Span;
        var measure = new RowReader(span);
        var total = 0;
        foreach (var column in schema.Columns)
        {
            var length = measure.SkipColumn(column.Kind);
            if (projection.Contains(column.Name))
                total += length;
        }

        // A projection that names every column is still no projection on the wire. Worth the check:
        // it turns an explicit `select *` and a fully-permissive column policy back into the free
        // path instead of copying a row to reproduce it.
        if (total == span.Length)
            return row;

        var projected = new byte[total];
        var reader = new RowReader(span);
        var offset = 0;
        foreach (var column in schema.Columns)
        {
            var start = reader.Position;
            var length = reader.SkipColumn(column.Kind);
            if (!projection.Contains(column.Name))
                continue;
            span.Slice(start, length).CopyTo(projected.AsSpan(offset));
            offset += length;
        }

        return projected;
    }

    /// <summary>
    /// The bitset of <paramref name="descriptor"/> positions <paramref name="visible"/> keeps, or
    /// empty when it keeps all of them. Callers only reach this when a column policy produced a
    /// per-row set; <paramref name="visible"/> is always a subset of the descriptor, because
    /// <c>VisibleColumns</c> intersects into the static wire columns the descriptor was built from
    /// — which is what makes an equal count an equal set.
    /// </summary>
    public static ReadOnlyMemory<byte> Mask(IReadOnlyList<WireColumn> descriptor, IReadOnlySet<string> visible)
    {
        if (visible.Count == descriptor.Count)
            return default;

        var mask = new byte[WireRowValues.MaskLength(descriptor.Count)];
        for (var i = 0; i < descriptor.Count; i++)
        {
            if (visible.Contains(descriptor[i].Name))
                mask[i >> 3] |= (byte)(1 << (i & 7));
        }

        return mask;
    }

    /// <summary>
    /// Decodes a row into named column values, restricted to <paramref name="projection"/> when it
    /// is non-null. Frames no longer travel this way — this is the HTTP endpoints' path, which
    /// genuinely wants a JSON object rather than bytes.
    /// </summary>
    public static Dictionary<string, object?> ToColumns(TableSchema schema, ReadOnlySpan<byte> row, IReadOnlySet<string>? projection)
    {
        var reader = new RowReader(row);
        var columns = new Dictionary<string, object?>(projection?.Count ?? schema.Columns.Count, StringComparer.Ordinal);
        foreach (var column in schema.Columns)
        {
            var value = ReadValue(ref reader, column.Kind);
            if (projection is null || projection.Contains(column.Name))
                columns[column.Name] = value;
        }

        return columns;
    }

    /// <summary>
    /// Whether two versions of a row are byte-identical on every projected column — the test that
    /// keeps a projected subscription silent when only non-projected columns changed.
    /// </summary>
    public static bool ProjectedEqual(TableSchema schema, ReadOnlySpan<byte> oldRow, ReadOnlySpan<byte> newRow, IReadOnlySet<string> projection)
    {
        var oldReader = new RowReader(oldRow);
        var newReader = new RowReader(newRow);
        foreach (var column in schema.Columns)
        {
            var oldStart = oldReader.Position;
            var newStart = newReader.Position;
            var oldLength = oldReader.SkipColumn(column.Kind);
            var newLength = newReader.SkipColumn(column.Kind);
            if (!projection.Contains(column.Name))
                continue;
            if (!oldRow.Slice(oldStart, oldLength).SequenceEqual(newRow.Slice(newStart, newLength)))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Encodes one column's value from a serialized row into its order-preserving key form, or
    /// null when the value is null. Dispatches through the generated codec when the schema has one.
    /// </summary>
    public static RowKey? EncodeColumn(TableSchema schema, string column, ReadOnlySpan<byte> row)
    {
        if (schema.Codec is { } codec)
            return codec.EncodeColumnFromBytes(column, row);

        var boxed = RowSerializer.Deserialize(schema, row.ToArray());
        var columnSchema = schema.Column(column);
        var value = columnSchema.GetValue(boxed);
        return value is null ? null : SchemaKeyCodec.Encode(columnSchema, value);
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
        _ => throw new NotSupportedException($"Unknown column kind {kind}."),
    };
}
