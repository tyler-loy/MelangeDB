using System.Collections.Concurrent;
using MelangeDB.Core;

namespace MelangeDB.Server;

/// <summary>
/// The client contract's closed set of subscription parameter shapes (C15.2), and the check for a
/// value outside it. The server still coerces wider shapes (C15.3: a float rounded into an integer
/// column, a decimal string read as an integer, base64 read as bytes, hex read as an identity), so
/// a client leaning on one works today. 0.4 warns; 0.5 refuses. The warning is how a deployment
/// finds out before the refusal does.
/// </summary>
internal static class ParameterShapes
{
    /// <summary>
    /// The first parameter the query bound whose value is outside the closed set for the column it
    /// is compared against, as <c>(name, shape)</c>; null when every one is inside it. Literals in
    /// the query text are not parameters and are not checked.
    /// </summary>
    public static (string Name, string Shape)? FindWidened(
        TableSchema schema, SubscriptionQuery query, IReadOnlyDictionary<string, object?>? parameters)
    {
        if (query.ParameterNames.Count == 0 || parameters is null || query.Column is null)
            return null;
        var column = schema.Columns.FirstOrDefault(c => string.Equals(c.Name, query.Column, StringComparison.Ordinal));
        if (column is null)
            return null;

        foreach (var name in query.ParameterNames)
        {
            if (!parameters.TryGetValue(name, out var value) || value is null)
                continue;
            if (!InClosedSet(column.Kind, value))
                return (name, ShapeOf(value));
        }

        return null;
    }

    /// <summary>
    /// Whether this (table, column, shape) is being reported for the first time, recorded in
    /// <paramref name="reported"/> (one per transport). One warning per combination is enough to
    /// find the caller; one per subscribe would bury it.
    /// </summary>
    public static bool FirstReport(
        ConcurrentDictionary<(string Table, string Column, string Shape), byte> reported, TableSchema schema, string column, string shape) =>
        reported.TryAdd((schema.Name, column, shape), 0);

    internal static bool InClosedSet(ColumnKind kind, object value) => kind switch
    {
        ColumnKind.Bool => value is bool,
        ColumnKind.Int8 or ColumnKind.Int16 or ColumnKind.Int32 or ColumnKind.Int64
            or ColumnKind.UInt8 or ColumnKind.UInt16 or ColumnKind.UInt32 or ColumnKind.UInt64
            or ColumnKind.Timestamp => value is long or ulong,
        ColumnKind.String => value is string,
        ColumnKind.Bytes => value is byte[],
        ColumnKind.Identity => value is byte[] { Length: 32 },

        // Floats and ScheduleAt cannot be indexed, so no predicate compares against them; the
        // compiler refuses those queries on its own terms.
        _ => true,
    };

    private static string ShapeOf(object value) => value switch
    {
        bool => "bool",
        long or ulong => "integer",
        float or double => "float",
        string => "string",
        byte[] bytes => $"bin of {bytes.Length} bytes",
        _ => value.GetType().Name,
    };
}
