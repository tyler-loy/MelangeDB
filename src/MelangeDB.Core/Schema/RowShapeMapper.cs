namespace MelangeDB.Core;

/// <summary>
/// Re-encodes a row written under one <see cref="TableShape"/> into another, matching columns
/// <em>by name</em>: each target column takes its raw byte slice from the source column of the
/// same name, or zero-fills when the source has no such column. By name because declaration
/// order is the byte order — a column added mid-class is byte-wise a reorder, and position-based
/// tolerance would make "I added a field to my class" destructive depending on where the cursor
/// was.
/// <para>
/// The mapper assumes the shapes passed an additive <see cref="ShapeCompatibility"/> check: every
/// source column the target keeps has the same kind, so a slice is a slice — no value ever
/// converts. Zero-fill is each kind's all-zero encoding, which is exactly what serializing a
/// <c>new T()</c> would produce for the column: numeric zero, null string, null bytes, the zero
/// identity, the zero timestamp.
/// </para>
/// </summary>
internal sealed class RowShapeMapper
{
    private readonly TableShape _source;
    private readonly int[] _sourceIndexOf; // per target column: index into source, or -1 to zero-fill
    private readonly int[] _zeroWidth;     // per target column: bytes to zero-fill when -1

    public RowShapeMapper(TableShape source, TableShape target)
    {
        _source = source;
        _sourceIndexOf = new int[target.Columns.Count];
        _zeroWidth = new int[target.Columns.Count];
        for (var t = 0; t < target.Columns.Count; t++)
        {
            var column = target.Columns[t];
            _sourceIndexOf[t] = -1;
            for (var s = 0; s < source.Columns.Count; s++)
            {
                if (source.Columns[s].Name == column.Name)
                {
                    if (source.Columns[s].Kind != column.Kind)
                    {
                        throw new InvalidOperationException(
                            $"Column '{column.Name}' changed kind {source.Columns[s].Kind} -> {column.Kind}; " +
                            "a kind change is destructive and must be refused before a mapper is built.");
                    }

                    _sourceIndexOf[t] = s;
                    break;
                }
            }

            if (_sourceIndexOf[t] < 0)
                _zeroWidth[t] = ZeroWidthOf(column.Kind);
        }
    }

    /// <summary>Maps one row's bytes from the source shape to the target shape.</summary>
    public byte[] Map(ReadOnlySpan<byte> row)
    {
        // One pass slices the source row by kind; the second concatenates slices in target order.
        Span<int> starts = stackalloc int[_source.Columns.Count];
        Span<int> lengths = stackalloc int[_source.Columns.Count];
        var reader = new RowReader(row);
        for (var s = 0; s < _source.Columns.Count; s++)
        {
            starts[s] = reader.Position;
            lengths[s] = reader.SkipColumn(_source.Columns[s].Kind);
        }

        if (reader.Position != row.Length)
        {
            throw new InvalidDataException(
                $"A row is {row.Length} bytes but its recorded shape reads {reader.Position}. The shape " +
                "sidecar disagrees with the stored bytes; the directory or its sidecar is damaged — restore from backup.");
        }

        var size = 0;
        for (var t = 0; t < _sourceIndexOf.Length; t++)
            size += _sourceIndexOf[t] >= 0 ? lengths[_sourceIndexOf[t]] : _zeroWidth[t];

        var mapped = new byte[size]; // zero-initialized: zero-filled columns need no writes at all
        var position = 0;
        for (var t = 0; t < _sourceIndexOf.Length; t++)
        {
            var s = _sourceIndexOf[t];
            if (s >= 0)
            {
                row.Slice(starts[s], lengths[s]).CopyTo(mapped.AsSpan(position));
                position += lengths[s];
            }
            else
            {
                position += _zeroWidth[t];
            }
        }

        return mapped;
    }

    /// <summary>
    /// The width of a kind's all-zero encoding — for the variable-width kinds, the one-byte null
    /// flag. <see cref="ColumnKind.ScheduleAt"/> is discriminant byte plus microseconds.
    /// </summary>
    private static int ZeroWidthOf(ColumnKind kind) => kind switch
    {
        ColumnKind.Bool or ColumnKind.Int8 or ColumnKind.UInt8 => 1,
        ColumnKind.Int16 or ColumnKind.UInt16 => 2,
        ColumnKind.Int32 or ColumnKind.UInt32 or ColumnKind.Float32 => 4,
        ColumnKind.Int64 or ColumnKind.UInt64 or ColumnKind.Float64 or ColumnKind.Timestamp => 8,
        ColumnKind.Identity => Identity.Size,
        ColumnKind.ScheduleAt => 9,
        ColumnKind.String or ColumnKind.Bytes => 1,
        _ => throw new NotSupportedException($"Unknown column kind {kind}."),
    };
}
