namespace MelangeDB.Core;

/// <summary>
/// The non-generic face of a generated per-table serializer, carried on <see cref="TableSchema.Codec"/>.
/// A codec implements the same versioned v1 row format as <see cref="RowSerializer"/> — column order,
/// little-endian fixed-width primitives, null-flagged length-prefixed strings and blobs — so logs
/// written by either path read through the other.
/// </summary>
public abstract class RowCodec
{
    /// <summary>The row struct this codec serializes.</summary>
    public abstract Type RowType { get; }

    /// <summary>
    /// Encodes one indexed column's value from a serialized row, or null when the value is null.
    /// The non-generic bridge the hot store uses for index maintenance without reflection.
    /// </summary>
    public abstract RowKey? EncodeColumnFromBytes(string column, ReadOnlySpan<byte> row);

    /// <summary>
    /// Encodes each of <paramref name="columns"/> from a serialized row into the matching slot of
    /// <paramref name="destination"/>, reading the row once for the whole set. A default
    /// <see cref="RowKey"/> — zero length — means the column was null, which is not indexed.
    /// <para>
    /// Index maintenance wants every indexed column of the row it is about to write, and asking
    /// for them one at a time through <see cref="EncodeColumnFromBytes"/> deserialized the whole
    /// row per column: a table with three indexes paid three full deserializes per put, each one
    /// re-allocating that row's string and byte columns.
    /// </para>
    /// </summary>
    public abstract void EncodeColumnsFromBytes(ReadOnlySpan<byte> row, IReadOnlyList<string> columns, RowKey[] destination);
}

/// <summary>
/// The typed face of a generated per-table serializer: no reflection, no boxing. The generator
/// emits one subclass per <c>[Table]</c> struct; the engine's typed paths dispatch through it.
/// </summary>
public abstract class RowCodec<TRow> : RowCodec
    where TRow : struct
{
    public sealed override Type RowType => typeof(TRow);

    /// <summary>Serializes a row in the schema's declared column order, format v1.</summary>
    public abstract byte[] Serialize(in TRow row);

    /// <summary>Deserializes a row written by this codec or by the reflection serializer.</summary>
    public abstract TRow Deserialize(ReadOnlySpan<byte> data);

    /// <summary>Encodes the row's primary key into its order-preserving byte form.</summary>
    public abstract RowKey EncodePrimaryKey(in TRow row);

    /// <summary>
    /// Encodes one key-encodable column's value, or null when the value is null. Only the primary
    /// key and indexed columns are supported; other names throw.
    /// </summary>
    public abstract RowKey? EncodeColumn(string column, in TRow row);

    /// <summary>
    /// Assigns zero-valued <c>[AutoInc]</c> columns from the transaction's allocation stage and
    /// observes explicitly supplied values, mirroring the reflection path's insert behaviour.
    /// </summary>
    public abstract void AssignAutoInc(ref TRow row, AutoIncStage stage, TableId table);

    /// <summary>
    /// The bridges decode before they encode, and index maintenance reaches them on the store's
    /// apply path — which is where a row whose stored bytes no longer match this build's schema is
    /// very often decoded for the first time, ahead of any read. Naming what failed here is what
    /// keeps that first failure legible; see <see cref="RowSerializer.DecodeFailed"/>.
    /// </summary>
    public sealed override RowKey? EncodeColumnFromBytes(string column, ReadOnlySpan<byte> row)
    {
        TRow typed;
        try
        {
            typed = Deserialize(row);
        }
        catch (Exception exception) when (RowSerializer.IsDecodeFault(exception))
        {
            throw RowSerializer.DecodeFailed($"Row type '{typeof(TRow).Name}'", row.Length, column, exception);
        }

        return EncodeColumn(column, in typed);
    }

    /// <inheritdoc cref="EncodeColumnFromBytes"/>
    public sealed override void EncodeColumnsFromBytes(ReadOnlySpan<byte> row, IReadOnlyList<string> columns, RowKey[] destination)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(destination);
        TRow typed;
        try
        {
            typed = Deserialize(row);
        }
        catch (Exception exception) when (RowSerializer.IsDecodeFault(exception))
        {
            throw RowSerializer.DecodeFailed($"Row type '{typeof(TRow).Name}'", row.Length, column: null, exception);
        }

        for (var i = 0; i < columns.Count; i++)
            destination[i] = EncodeColumn(columns[i], in typed) ?? default;
    }
}
