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
/// expect.
/// <para>
/// Protocol v1 checked this per column per row, because a map of names was all it had: every
/// decoded field looked itself up and complained if it was missing or the wrong CLR type. Ordered
/// row bytes cannot be checked that way — a wrong shape decodes into plausible garbage rather than
/// failing — so the check moved to where it belongs, and got stronger for the move: the whole
/// column list is compared once, by name, kind, and position, when the subscription's descriptor
/// arrives and before any row is read.
/// </para>
/// </summary>
public static class ClientRowShape
{
    /// <summary>
    /// Verifies the server's descriptor is exactly the shape <paramref name="expected"/> describes.
    /// </summary>
    public static void Verify(string table, IReadOnlyList<WireColumn> expected, WireDescriptor actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);

        if (!string.Equals(actual.Table, table, StringComparison.Ordinal))
        {
            throw new MelangeSchemaMismatchException(
                $"A subscription on '{table}' is receiving rows shaped for '{actual.Table}'.");
        }

        if (actual.Columns.Count != expected.Count)
        {
            throw new MelangeSchemaMismatchException(
                $"Table '{table}': the server sends {actual.Columns.Count} columns ({Describe(actual.Columns)}) where these bindings expect {expected.Count} ({Describe(expected)}) — "
                + "the bindings were generated from a different schema than the server is running. A projected subscription is the untyped API's business, not a typed cache's.");
        }

        for (var i = 0; i < expected.Count; i++)
        {
            if (actual.Columns[i] == expected[i])
                continue;
            throw new MelangeSchemaMismatchException(
                $"Table '{table}': column {i} is '{actual.Columns[i].Name}' ({actual.Columns[i].Kind}) on the server where these bindings expect '{expected[i].Name}' ({expected[i].Kind}) — "
                + "the bindings were generated from a different schema than the server is running.");
        }
    }

    private static string Describe(IReadOnlyList<WireColumn> columns) =>
        string.Join(", ", columns.Select(static c => c.Name));
}
