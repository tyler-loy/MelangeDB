using MelangeDB.Protocol;

namespace MelangeDB.Client;

/// <summary>
/// Thrown when a subscription's wire shape does not match the schema the bindings were generated
/// from — a renamed, reordered, added, removed, or re-kinded column, or a row narrowed by a column
/// policy arriving at a typed cache. This is the loud form of schema drift; compare the bindings'
/// schema hash against the server module's manifest to find the stale side.
/// </summary>
public sealed class MelangeSchemaMismatchException : Exception
{
    public MelangeSchemaMismatchException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The one place a typed client checks that the server's rows are shaped the way its bindings
/// expect, and maps them when they are shaped differently but compatibly.
/// <para>
/// Protocol v1 checked this per column per row, because a map of names was all it had. Ordered row
/// bytes cannot be checked that way — a wrong shape decodes into plausible garbage rather than
/// failing — so the check runs once, against the descriptor, when a subscription's first rows
/// arrive and before any row is read.
/// </para>
/// <para>
/// Since road-to-0.4 phase 28 the check is a <em>mapping</em> rather than an equality: every column
/// the bindings know is found in the descriptor <b>by name</b> and must have the same kind; columns
/// the bindings do not know are skipped. So a client built against an older schema survives an
/// additive server change — an added column at any position, or a reorder — instead of refusing
/// every subscription until it is rebuilt, which a game client on a player's machine cannot be in
/// lockstep with a server deploy. What it does not survive is a column it needs going missing or
/// changing kind: a removal, a rename (a removal plus an addition), or a retype still throws.
/// </para>
/// </summary>
public static class ClientRowShape
{
    /// <summary>
    /// Verifies the server's descriptor can be read by bindings expecting <paramref name="expected"/>:
    /// every expected column present by name with the same kind. Extra and reordered server columns
    /// are accepted; <see cref="Plan"/> builds the mapping that reads past them.
    /// </summary>
    public static void Verify(string table, IReadOnlyList<WireColumn> expected, WireDescriptor actual) =>
        _ = Plan(table, expected, actual);

    /// <summary>
    /// The mapping from the server's row bytes to the bindings' layout, or null when the two are
    /// identical and rows can be decoded as they arrive — the case that matters, and the one that
    /// costs nothing. Throws <see cref="MelangeSchemaMismatchException"/> when a column the
    /// bindings need is missing or has changed kind.
    /// </summary>
    internal static ClientRowProjection? Plan(string table, IReadOnlyList<WireColumn> expected, WireDescriptor actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);

        if (!string.Equals(actual.Table, table, StringComparison.Ordinal))
        {
            throw new MelangeSchemaMismatchException(
                $"A subscription on '{table}' is receiving rows shaped for '{actual.Table}'.");
        }

        if (actual.Columns.Count == expected.Count && actual.Columns.SequenceEqual(expected))
            return null;

        var sources = new int[expected.Count];
        for (var i = 0; i < expected.Count; i++)
        {
            var wanted = expected[i];
            var found = -1;
            for (var j = 0; j < actual.Columns.Count; j++)
            {
                if (string.Equals(actual.Columns[j].Name, wanted.Name, StringComparison.Ordinal))
                {
                    found = j;
                    break;
                }
            }

            if (found < 0)
            {
                throw new MelangeSchemaMismatchException(
                    $"Table '{table}': these bindings read column '{wanted.Name}' ({wanted.Kind}), which the server does not send " +
                    $"(it sends {Describe(actual.Columns)}). Either the column was removed or renamed on the server — regenerate " +
                    "the bindings from the server's manifest — or this is a projected subscription, which is the untyped API's " +
                    "business, not a typed cache's.");
            }

            if (actual.Columns[found].Kind != wanted.Kind)
            {
                throw new MelangeSchemaMismatchException(
                    $"Table '{table}': column '{wanted.Name}' is {actual.Columns[found].Kind} on the server where these bindings " +
                    $"expect {wanted.Kind} — a changed kind is not readable by older bindings; regenerate them from the server's manifest.");
            }

            sources[i] = found;
        }

        return new ClientRowProjection(actual.Columns.Select(static c => c.Kind).ToArray(), sources);
    }

    private static string Describe(IReadOnlyList<WireColumn> columns) =>
        string.Join(", ", columns.Select(static c => c.Name));
}

/// <summary>
/// Rewrites one subscription's server row bytes into the layout older bindings decode: the columns
/// the bindings know, in the bindings' order, each copied as its raw slice. The bytes are already
/// in row format v1, so a projection is one walk of the server row and a set of copies — no decode
/// and re-encode — and the bindings' generated <c>DecodeRow</c> runs unchanged on the result.
/// </summary>
internal sealed class ClientRowProjection
{
    private readonly ColumnKind[] _serverKinds;
    private readonly int[] _sources;
    private readonly int _walk;

    internal ClientRowProjection(ColumnKind[] serverKinds, int[] sources)
    {
        _serverKinds = serverKinds;
        _sources = sources;
        _walk = sources.Length == 0 ? 0 : sources.Max() + 1;
    }

    /// <summary>The row in the bindings' layout.</summary>
    public byte[] Project(ReadOnlySpan<byte> row)
    {
        Span<int> starts = _walk <= 64 ? stackalloc int[_walk] : new int[_walk];
        Span<int> lengths = _walk <= 64 ? stackalloc int[_walk] : new int[_walk];
        var reader = new RowReader(row);
        for (var j = 0; j < _walk; j++)
        {
            starts[j] = reader.Position;
            lengths[j] = reader.SkipColumn(_serverKinds[j]);
        }

        var total = 0;
        foreach (var source in _sources)
            total += lengths[source];

        var projected = new byte[total];
        var position = 0;
        foreach (var source in _sources)
        {
            row.Slice(starts[source], lengths[source]).CopyTo(projected.AsSpan(position));
            position += lengths[source];
        }

        return projected;
    }
}
