using MelangeDB.Core;

namespace MelangeDB.Server;

/// <summary>
/// One row, decoded at most once however many readers ask. The fan-out evaluates each committed
/// row for every subscriber on its table — the predicate, then the caller's row policies, then its
/// column policies — and each of those used to decode the row for itself, so a table with N
/// subscribers paid N full deserializations per op (2N with the pre-image, 4N with column
/// policies), under the engine's write lock. The row is the same for all of them; only the verdict
/// differs by caller. So this holds the bytes, materializes the typed row on first demand and hands
/// the same instance to every reader, and memoizes each predicate column's encoded value.
/// <para>
/// Reused across ops — <see cref="Reset"/> clears everything — because it lives on the fan-out
/// path, where an allocation per op per sink is the kind of cost this exists to remove. Not thread
/// safe; one per caller.
/// </para>
/// </summary>
internal sealed class DecodedRow
{
    private TableSchema _schema = null!;
    private ReadOnlyMemory<byte> _bytes;
    private object? _typed;
    private Dictionary<string, RowKey?>? _columns;

    /// <summary>Whether there is a row at all — false for a pre-image that does not exist.</summary>
    public bool Present { get; private set; }

    public ReadOnlyMemory<byte> Bytes => _bytes;

    public ReadOnlySpan<byte> Span => _bytes.Span;

    /// <summary>The row's boxed struct, decoded on first access and shared thereafter.</summary>
    public object Typed => _typed ??= _schema.DecodeBoxed(_bytes);

    public DecodedRow Reset(TableSchema schema, ReadOnlyMemory<byte> bytes, bool present = true)
    {
        _schema = schema;
        _bytes = bytes;
        Present = present;
        _typed = null;
        _columns?.Clear();
        return this;
    }

    /// <summary>
    /// One indexed or primary-key column's order-preserving encoding, or null for a null value.
    /// Memoized by column name, so a hundred subscribers on the same predicate column cost one
    /// encode and ninety-nine dictionary hits.
    /// </summary>
    public RowKey? EncodeColumn(string column)
    {
        _columns ??= new Dictionary<string, RowKey?>(StringComparer.Ordinal);
        if (_columns.TryGetValue(column, out var cached))
            return cached;

        RowKey? encoded;
        if (_schema.Codec is { } codec)
        {
            encoded = codec.EncodeColumnBoxed(column, Typed);
        }
        else
        {
            var columnSchema = _schema.Column(column);
            var value = columnSchema.GetValue(Typed);
            encoded = value is null ? null : SchemaKeyCodec.Encode(columnSchema, value);
        }

        _columns[column] = encoded;
        return encoded;
    }
}
