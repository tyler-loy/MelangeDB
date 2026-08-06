using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>
/// What the client generator implements per table: the row-bytes decoding into the row struct, the
/// column shape those bindings were generated from, and primary-key encoding so the typed cache and
/// lookups address rows by the same encoded bytes the server keys deltas with. Implementations are
/// stateless singletons; the emitted code stays thin and all cache mechanics live in the library
/// behind this seam.
/// </summary>
public interface IClientRowCodec<TRow>
    where TRow : struct
{
    /// <summary>The wire table name, as subscriptions and the manifest spell it.</summary>
    string TableName { get; }

    /// <summary>
    /// The columns these bindings expect, in row-byte order — checked once per subscription against
    /// the server's <see cref="WireDescriptor"/>. This is where schema drift is caught now, and it
    /// is caught more completely than the map wire caught it: a renamed column, a reordered one, or
    /// one whose kind changed all fail the same structural comparison, before a single row decodes.
    /// </summary>
    IReadOnlyList<WireColumn> Columns { get; }

    /// <summary>
    /// Decodes one row's schema-ordered v1 bytes into the row struct. The bytes are exactly what
    /// the server's store holds, so this is a positional read with no dictionary, no name lookup,
    /// and no boxing — a projected subscription's partial rows are the untyped API's business,
    /// never this one's, and the caller rejects them before reaching here.
    /// </summary>
    TRow DecodeRow(ReadOnlySpan<byte> row);

    /// <summary>Encodes the row's primary key into its order-preserving byte form.</summary>
    byte[] EncodePrimaryKey(in TRow row);
}
