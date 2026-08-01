namespace MelangeDB.Client;

/// <summary>
/// What the client generator implements per table: the name→CLR decoding of a wire column map
/// into the row struct, and primary-key encoding so the typed cache and lookups address rows by
/// the same encoded bytes the server keys deltas with. Implementations are stateless singletons;
/// the emitted code stays thin and all cache mechanics live in the library behind this seam.
/// </summary>
public interface IClientRowCodec<TRow>
    where TRow : struct
{
    /// <summary>The wire table name, as subscriptions and the manifest spell it.</summary>
    string TableName { get; }

    /// <summary>
    /// Decodes one wire column map into the row struct, through the
    /// <see cref="ClientWireValues"/> coercion table. Throws
    /// <see cref="MelangeSchemaMismatchException"/> when the map does not match the schema the
    /// bindings were generated from — a projected subscription's partial rows are the untyped
    /// API's business, never this one's.
    /// </summary>
    TRow DecodeRow(IReadOnlyDictionary<string, object?> columns);

    /// <summary>Encodes the row's primary key into its order-preserving byte form.</summary>
    byte[] EncodePrimaryKey(in TRow row);
}
