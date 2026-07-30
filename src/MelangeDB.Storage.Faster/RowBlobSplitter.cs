using System.Buffers.Binary;
using MelangeDB.Core;

namespace MelangeDB.Storage.Faster;

/// <summary>
/// Splits large <c>byte[]</c> column payloads out of a serialized row and splices them back —
/// byte-exactly, because the serialized bytes are the identity of a row's state. The main record
/// keeps the column's null flag and length prefix; only the payload moves out of line, so a walk
/// of the main record still sees every column's framing. This is what lets a blob table's main
/// records stay small: scanning by key or filtering on an indexed column touches main-record pages
/// only, and a blob pages in exactly when its row is materialized.
/// </summary>
internal static class RowBlobSplitter
{
    /// <summary>
    /// Payloads at or above this size go out of line. Below it, the indirection costs more than it
    /// saves. Deliberately a constant, not configuration — see docs/plan-phase-07.md.
    /// </summary>
    public const int InlineThreshold = 256;

    /// <summary>
    /// Splits a row: returns the main bytes (payloads excised), the excised payloads keyed by
    /// bytes-column ordinal, and the mask of which bytes-columns went out of line. A row with no
    /// qualifying payload returns the original memory and an empty mask, zero-copy.
    /// </summary>
    public static (ReadOnlyMemory<byte> Main, uint Mask, List<(int Ordinal, ReadOnlyMemory<byte> Payload)>? Blobs) Split(
        TableSchema schema, ReadOnlyMemory<byte> row)
    {
        var span = row.Span;
        List<(int Start, int Length)>? excised = null;
        List<(int Ordinal, ReadOnlyMemory<byte> Payload)>? blobs = null;
        uint mask = 0;
        var position = 0;
        var bytesOrdinal = 0;
        foreach (var column in schema.Columns)
        {
            if (column.Kind == ColumnKind.Bytes)
            {
                var flag = span[position++];
                if (flag != 0)
                {
                    var length = BinaryPrimitives.ReadInt32LittleEndian(span[position..]);
                    position += 4;
                    if (length >= InlineThreshold)
                    {
                        mask |= 1u << bytesOrdinal;
                        (excised ??= []).Add((position, length));
                        (blobs ??= []).Add((bytesOrdinal, row.Slice(position, length)));
                    }

                    position += length;
                }

                bytesOrdinal++;
            }
            else
            {
                position += SkipColumn(span, position, column.Kind);
            }
        }

        if (excised is null)
            return (row, 0, null);

        var mainLength = span.Length;
        foreach (var (_, length) in excised)
            mainLength -= length;
        var main = new byte[mainLength];
        var write = 0;
        var read = 0;
        foreach (var (start, length) in excised)
        {
            span[read..start].CopyTo(main.AsSpan(write));
            write += start - read;
            read = start + length;
        }

        span[read..].CopyTo(main.AsSpan(write));
        return (main, mask, blobs);
    }

    /// <summary>
    /// Rebuilds the original row bytes from a main record and its out-of-line payloads.
    /// <paramref name="fetch"/> is called once per excised bytes-column ordinal, in column order.
    /// </summary>
    public static byte[] Join(TableSchema schema, ReadOnlySpan<byte> main, uint mask, Func<int, byte[]> fetch)
    {
        var insertions = new List<(int MainOffset, byte[] Payload)>();
        var position = 0;
        var bytesOrdinal = 0;
        var totalPayload = 0;
        foreach (var column in schema.Columns)
        {
            if (column.Kind == ColumnKind.Bytes)
            {
                var flag = main[position++];
                if (flag != 0)
                {
                    var length = BinaryPrimitives.ReadInt32LittleEndian(main[position..]);
                    position += 4;
                    if ((mask & (1u << bytesOrdinal)) != 0)
                    {
                        var payload = fetch(bytesOrdinal);
                        if (payload.Length != length)
                        {
                            throw new InvalidDataException(
                                $"Table '{schema.Name}': out-of-line payload for bytes-column ordinal {bytesOrdinal} " +
                                $"is {payload.Length} bytes but the main record declares {length}.");
                        }

                        insertions.Add((position, payload));
                        totalPayload += length;
                    }
                    else
                    {
                        position += length;
                    }
                }

                bytesOrdinal++;
            }
            else
            {
                position += SkipColumn(main, position, column.Kind);
            }
        }

        if (insertions.Count == 0)
            return main.ToArray();

        var joined = new byte[main.Length + totalPayload];
        var write = 0;
        var read = 0;
        foreach (var (offset, payload) in insertions)
        {
            main[read..offset].CopyTo(joined.AsSpan(write));
            write += offset - read;
            payload.CopyTo(joined.AsSpan(write));
            write += payload.Length;
            read = offset;
        }

        main[read..].CopyTo(joined.AsSpan(write));
        return joined;
    }

    /// <summary>Whether any column of the table can carry an out-of-line payload.</summary>
    public static bool HasBytesColumns(TableSchema schema)
    {
        foreach (var column in schema.Columns)
        {
            if (column.Kind == ColumnKind.Bytes)
                return true;
        }

        return false;
    }

    private static int SkipColumn(ReadOnlySpan<byte> span, int position, ColumnKind kind) => kind switch
    {
        ColumnKind.Bool or ColumnKind.Int8 or ColumnKind.UInt8 => 1,
        ColumnKind.Int16 or ColumnKind.UInt16 => 2,
        ColumnKind.Int32 or ColumnKind.UInt32 or ColumnKind.Float32 => 4,
        ColumnKind.Int64 or ColumnKind.UInt64 or ColumnKind.Float64 or ColumnKind.Timestamp => 8,
        ColumnKind.Identity => Identity.Size,
        ColumnKind.ScheduleAt => 9,
        ColumnKind.String => span[position] == 0 ? 1 : 5 + BinaryPrimitives.ReadInt32LittleEndian(span[(position + 1)..]),
        _ => throw new NotSupportedException($"Unknown column kind {kind}."),
    };
}
