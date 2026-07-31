namespace MelangeDB.Storage.Postgres;

/// <summary>
/// Validates and quotes SQL identifiers. Every identifier the tier emits comes from a
/// <see cref="Core.TableSchema"/> — C# type and field names — or from configuration, never from a
/// client; this class is the belt to that suspenders: an identifier outside the strict shape is
/// refused, and everything that passes is double-quoted so Postgres treats it as exact text.
/// </summary>
internal static class PostgresIdentifier
{
    /// <summary>Validates and quotes one identifier (<c>name</c> → <c>"name"</c>).</summary>
    public static string Quote(string identifier)
    {
        ArgumentException.ThrowIfNullOrEmpty(identifier);
        if (identifier.Length > 63)
            throw new ArgumentException($"Identifier '{identifier}' exceeds Postgres's 63-byte limit.", nameof(identifier));
        if (char.IsAsciiDigit(identifier[0]))
            throw new ArgumentException($"Identifier '{identifier}' cannot start with a digit.", nameof(identifier));
        foreach (var c in identifier)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '_')
                throw new ArgumentException($"Identifier '{identifier}' contains '{c}'; only ASCII letters, digits, and '_' are valid.", nameof(identifier));
        }

        return $"\"{identifier}\"";
    }

    /// <summary>Quotes a schema-qualified table reference (<c>schema.table</c>).</summary>
    public static string Qualify(string schema, string table) => $"{Quote(schema)}.{Quote(table)}";
}
