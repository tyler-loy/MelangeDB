using System.Globalization;

namespace MelangeDB.Core;

/// <summary>
/// The boot-time check that a <c>[Unique]</c> column's existing rows actually are unique, run after
/// recovery for the columns <see cref="ShapeDiff.UniqueToCheck"/> names (road-to-0.4 phase 27,
/// issue #192). Write-time enforcement only ever sees new writes; a constraint declared over rows
/// that already break it would otherwise be enforced against a table that violates it, and every
/// reader promised one row per value would get several.
/// </summary>
/// <remarks>
/// One sequential <see cref="IHotStoreReader.Scan"/> per table, however many of its columns need
/// checking, holding each column's values seen so far. That is one copy of what the column's
/// secondary index already holds in memory, for the duration of a check that runs once per
/// constraint change — and it keeps the check on the store's public read surface rather than on an
/// index walk that would need a new member on <see cref="IHotStore"/>.
/// </remarks>
internal static class UniqueConstraintCheck
{
    /// <summary>How many duplicated values a refusal names per column before summarizing.</summary>
    internal const int ValuesNamed = 10;

    /// <summary>How many holding keys a refusal names per duplicated value.</summary>
    internal const int KeysNamed = 5;

    private const int ValueTextLimit = 64;

    /// <summary>
    /// One reason per column whose rows hold a value more than once, in the order the columns were
    /// given; empty when every column is unique over its rows. A null value is never a duplicate,
    /// as at write time.
    /// </summary>
    public static IReadOnlyList<string> FindViolations(
        IHotStoreReader store, SchemaRegistry schema, IReadOnlyList<ShapeUniqueColumn> columns)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(columns);
        List<string> reasons = [];
        foreach (var group in columns.GroupBy(static c => c.Table, StringComparer.Ordinal))
        {
            if (!schema.TryGetByName(group.Key, out var table))
                continue;
            var checks = group.Select(c => new ColumnCheck(table.Column(c.Column), c.ColumnIsNew)).ToArray();
            Scan(store, table, checks);
            foreach (var check in checks)
            {
                if (check.Duplicates.Count > 0)
                    reasons.Add(Describe(table, check));
            }
        }

        return reasons;
    }

    private static void Scan(IHotStoreReader store, TableSchema table, ColumnCheck[] checks)
    {
        foreach (var (key, row) in store.Scan(table.Id))
        {
            object? boxed = null;
            foreach (var check in checks)
            {
                RowKey? encoded;
                if (table.Codec is { } codec)
                {
                    encoded = codec.EncodeColumnFromBytes(check.Column.Name, row.Span);
                }
                else
                {
                    boxed ??= RowSerializer.Deserialize(table, row);
                    encoded = check.Column.GetValue(boxed) is { } value ? SchemaKeyCodec.Encode(check.Column, value) : null;
                }

                if (encoded is { } found)
                    check.Observe(found, key);
            }
        }
    }

    private static string Describe(TableSchema table, ColumnCheck check)
    {
        var named = check.Duplicates
            .Take(ValuesNamed)
            .Select(pair =>
            {
                var keys = pair.Value.Take(KeysNamed).Select(k => Format(SchemaKeyCodec.Decode(table.PrimaryKey, k)));
                var more = pair.Value.Count > KeysNamed ? $", and {pair.Value.Count - KeysNamed} more" : string.Empty;
                return $"{Format(SchemaKeyCodec.Decode(check.Column, pair.Key))} (held by {table.PrimaryKey.Name} {string.Join(", ", keys)}{more})";
            });
        var rest = check.Duplicates.Count > ValuesNamed ? $"; and {check.Duplicates.Count - ValuesNamed} more duplicated value(s)" : string.Empty;
        var reason =
            $"table '{table.Name}': column '{check.Column.Name}' is declared [Unique] but {check.Duplicates.Count} value(s) " +
            $"are held by more than one row: {string.Join("; ", named)}{rest}";
        return check.ColumnIsNew
            ? reason + ". The column is new in this deploy, so every existing row holds its default value: declare it " +
              "nullable (null is never a duplicate), or add it without [Unique], fill it with distinct values, then declare it"
            : reason;
    }

    private static string Format(object? value) => value switch
    {
        null => "null",
        string s => "'" + (s.Length > ValueTextLimit ? s[..ValueTextLimit] + "…" : s) + "'",
        byte[] bytes => $"<{bytes.Length} bytes>",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };

    private sealed class ColumnCheck(ColumnSchema column, bool columnIsNew)
    {
        private readonly Dictionary<RowKey, RowKey> _firstHolder = [];

        public ColumnSchema Column { get; } = column;

        public bool ColumnIsNew { get; } = columnIsNew;

        /// <summary>Each duplicated value and every key holding it, in the order the scan found them.</summary>
        public Dictionary<RowKey, List<RowKey>> Duplicates { get; } = [];

        public void Observe(RowKey value, RowKey key)
        {
            if (_firstHolder.TryAdd(value, key))
                return;
            if (!Duplicates.TryGetValue(value, out var holders))
                Duplicates[value] = holders = [_firstHolder[value]];
            holders.Add(key);
        }
    }
}
