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

    public sealed override RowKey? EncodeColumnFromBytes(string column, ReadOnlySpan<byte> row)
    {
        var typed = Deserialize(row);
        return EncodeColumn(column, in typed);
    }
}
