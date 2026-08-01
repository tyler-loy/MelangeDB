using System.Text;

namespace MelangeDB.CodeGen;

/// <summary>Thrown for a manifest the client generator cannot accept; the message says why.</summary>
internal sealed class ManifestException : Exception
{
    public ManifestException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// Parses <c>melange-schema.json</c> into the client model. The JSON reader is hand-rolled for
/// the same reason the SQL parser is: the format is small and closed, the generator targets
/// netstandard2.0 with no dependencies, and "valid manifest" should stay unambiguous. Anything
/// structurally off — unknown format version, missing field, unknown kind name — throws
/// <see cref="ManifestException"/> with the reason; the generator turns that into a diagnostic
/// rather than emitting half a binding tree.
/// </summary>
internal static class ManifestParser
{
    public static ClientSchemaModel Parse(string json)
    {
        var root = JsonReader.Read(json) as Dictionary<string, object?>
            ?? throw new ManifestException("The manifest root is not a JSON object.");

        var format = GetNumber(root, "format");
        if (format != ManifestEmitter.FormatVersion)
        {
            throw new ManifestException(
                $"Manifest format {format} is not supported; this generator reads format {ManifestEmitter.FormatVersion}. " +
                "Re-export the manifest with a matching MelangeDB.CodeGen.");
        }

        var enums = new List<ClientEnumModel>();
        foreach (var entry in GetArray(root, "enums"))
        {
            var declaration = AsObject(entry, "enums[]");
            var members = new List<EnumMemberModel>();
            foreach (var memberEntry in GetArray(declaration, "members"))
            {
                var member = AsObject(memberEntry, "members[]");
                members.Add(new EnumMemberModel(GetString(member, "name"), GetString(member, "value")));
            }

            enums.Add(new ClientEnumModel(
                GetString(declaration, "name"),
                GetKind(declaration, "underlying"),
                new EquatableArray<EnumMemberModel>([.. members])));
        }

        var enumNames = new HashSet<string>(enums.Select(static e => e.Name), StringComparer.Ordinal);

        var tables = new List<ClientTableModel>();
        foreach (var entry in GetArray(root, "tables"))
        {
            var table = AsObject(entry, "tables[]");
            var tableName = GetString(table, "name");
            var columns = new List<ClientColumnModel>();
            foreach (var columnEntry in GetArray(table, "columns"))
            {
                var column = AsObject(columnEntry, "columns[]");
                columns.Add(new ClientColumnModel(
                    GetString(column, "name"),
                    GetKind(column, "kind"),
                    GetEnumReference(column, enumNames, $"table '{tableName}'"),
                    GetBool(column, "primaryKey"),
                    GetBool(column, "autoInc"),
                    GetBool(column, "unique"),
                    GetBool(column, "indexed")));
            }

            if (columns.Count(static c => c.IsPrimaryKey) != 1)
                throw new ManifestException($"Table '{tableName}' does not declare exactly one primary-key column.");
            tables.Add(new ClientTableModel(tableName, GetString(table, "type"), new EquatableArray<ClientColumnModel>([.. columns])));
        }

        var reducers = new List<ClientReducerModel>();
        foreach (var entry in GetArray(root, "reducers"))
        {
            var reducer = AsObject(entry, "reducers[]");
            var reducerName = GetString(reducer, "name");
            var parameters = new List<ClientParameterModel>();
            foreach (var parameterEntry in GetArray(reducer, "params"))
            {
                var parameter = AsObject(parameterEntry, "params[]");
                parameters.Add(new ClientParameterModel(
                    GetString(parameter, "name"),
                    GetKind(parameter, "kind"),
                    GetBool(parameter, "isArray"),
                    GetEnumReference(parameter, enumNames, $"reducer '{reducerName}'")));
            }

            reducers.Add(new ClientReducerModel(reducerName, new EquatableArray<ClientParameterModel>([.. parameters])));
        }

        return new ClientSchemaModel(
            GetString(root, "schemaHash"),
            GetString(root, "module"),
            new EquatableArray<ClientEnumModel>([.. enums]),
            new EquatableArray<ClientTableModel>([.. tables]),
            new EquatableArray<ClientReducerModel>([.. reducers]));
    }

    private static string? GetEnumReference(Dictionary<string, object?> node, HashSet<string> enumNames, string where)
    {
        if (!node.TryGetValue("enum", out var value) || value is null)
            return null;
        var name = value as string ?? throw new ManifestException($"An enum reference in {where} is not a string.");
        if (!enumNames.Contains(name))
            throw new ManifestException($"{where} references enum '{name}', which the manifest does not declare.");
        return name;
    }

    private static List<object?> GetArray(Dictionary<string, object?> node, string name) =>
        node.TryGetValue(name, out var value) && value is List<object?> array
            ? array
            : throw new ManifestException($"The manifest is missing array field '{name}'.");

    private static Dictionary<string, object?> AsObject(object? value, string where) =>
        value as Dictionary<string, object?> ?? throw new ManifestException($"Expected an object in {where}.");

    private static string GetString(Dictionary<string, object?> node, string name) =>
        node.TryGetValue(name, out var value) && value is string text
            ? text
            : throw new ManifestException($"The manifest is missing string field '{name}'.");

    private static bool GetBool(Dictionary<string, object?> node, string name) =>
        node.TryGetValue(name, out var value) && value is bool flag
            ? flag
            : throw new ManifestException($"The manifest is missing boolean field '{name}'.");

    private static long GetNumber(Dictionary<string, object?> node, string name) =>
        node.TryGetValue(name, out var value) && value is long number
            ? number
            : throw new ManifestException($"The manifest is missing numeric field '{name}'.");

    private static WireKind GetKind(Dictionary<string, object?> node, string name)
    {
        var text = GetString(node, name);
        return text switch
        {
            "Bool" => WireKind.Bool,
            "Int8" => WireKind.Int8,
            "UInt8" => WireKind.UInt8,
            "Int16" => WireKind.Int16,
            "UInt16" => WireKind.UInt16,
            "Int32" => WireKind.Int32,
            "UInt32" => WireKind.UInt32,
            "Int64" => WireKind.Int64,
            "UInt64" => WireKind.UInt64,
            "Float32" => WireKind.Float32,
            "Float64" => WireKind.Float64,
            "String" => WireKind.String,
            "Bytes" => WireKind.Bytes,
            "Identity" => WireKind.Identity,
            "Timestamp" => WireKind.Timestamp,
            _ => throw new ManifestException($"'{text}' is not a client-visible column kind."),
        };
    }

    /// <summary>
    /// A deliberately small JSON reader: objects, arrays, strings with escapes, integer numbers,
    /// booleans, null. Everything the manifest writer emits, nothing it doesn't — floats, for
    /// instance, never appear (enum values ride as strings for exactly that reason).
    /// </summary>
    private static class JsonReader
    {
        public static object? Read(string text)
        {
            var position = 0;
            var value = ReadValue(text, ref position);
            SkipWhitespace(text, ref position);
            if (position != text.Length)
                throw new ManifestException($"Unexpected trailing JSON at position {position}.");
            return value;
        }

        private static object? ReadValue(string text, ref int position)
        {
            SkipWhitespace(text, ref position);
            if (position >= text.Length)
                throw new ManifestException("Unexpected end of JSON.");
            var c = text[position];
            return c switch
            {
                '{' => ReadObject(text, ref position),
                '[' => ReadArray(text, ref position),
                '"' => ReadString(text, ref position),
                't' or 'f' => ReadBool(text, ref position),
                'n' => ReadNull(text, ref position),
                '-' or (>= '0' and <= '9') => ReadNumber(text, ref position),
                _ => throw new ManifestException($"Unexpected character '{c}' at position {position}."),
            };
        }

        private static Dictionary<string, object?> ReadObject(string text, ref int position)
        {
            position++; // {
            var result = new Dictionary<string, object?>(StringComparer.Ordinal);
            SkipWhitespace(text, ref position);
            if (Peek(text, position) == '}')
            {
                position++;
                return result;
            }

            while (true)
            {
                SkipWhitespace(text, ref position);
                var name = ReadString(text, ref position);
                SkipWhitespace(text, ref position);
                Expect(text, ref position, ':');
                result[name] = ReadValue(text, ref position);
                SkipWhitespace(text, ref position);
                var next = Next(text, ref position);
                if (next == '}')
                    return result;
                if (next != ',')
                    throw new ManifestException($"Expected ',' or '}}' at position {position - 1}.");
            }
        }

        private static List<object?> ReadArray(string text, ref int position)
        {
            position++; // [
            var result = new List<object?>();
            SkipWhitespace(text, ref position);
            if (Peek(text, position) == ']')
            {
                position++;
                return result;
            }

            while (true)
            {
                result.Add(ReadValue(text, ref position));
                SkipWhitespace(text, ref position);
                var next = Next(text, ref position);
                if (next == ']')
                    return result;
                if (next != ',')
                    throw new ManifestException($"Expected ',' or ']' at position {position - 1}.");
            }
        }

        private static string ReadString(string text, ref int position)
        {
            if (Peek(text, position) != '"')
                throw new ManifestException($"Expected a string at position {position}.");
            position++;
            var builder = new StringBuilder();
            while (true)
            {
                if (position >= text.Length)
                    throw new ManifestException("Unterminated JSON string.");
                var c = text[position++];
                if (c == '"')
                    return builder.ToString();
                if (c != '\\')
                {
                    builder.Append(c);
                    continue;
                }

                if (position >= text.Length)
                    throw new ManifestException("Unterminated JSON escape.");
                var escape = text[position++];
                switch (escape)
                {
                    case '"' or '\\' or '/':
                        builder.Append(escape);
                        break;
                    case 'b':
                        builder.Append('\b');
                        break;
                    case 'f':
                        builder.Append('\f');
                        break;
                    case 'n':
                        builder.Append('\n');
                        break;
                    case 'r':
                        builder.Append('\r');
                        break;
                    case 't':
                        builder.Append('\t');
                        break;
                    case 'u':
                        if (position + 4 > text.Length)
                            throw new ManifestException("Truncated \\u escape.");
                        builder.Append((char)Convert.ToInt32(text.Substring(position, 4), 16));
                        position += 4;
                        break;
                    default:
                        throw new ManifestException($"Unknown JSON escape '\\{escape}'.");
                }
            }
        }

        private static object ReadBool(string text, ref int position)
        {
            if (Matches(text, position, "true"))
            {
                position += 4;
                return true;
            }

            if (Matches(text, position, "false"))
            {
                position += 5;
                return false;
            }

            throw new ManifestException($"Malformed literal at position {position}.");
        }

        private static object? ReadNull(string text, ref int position)
        {
            if (!Matches(text, position, "null"))
                throw new ManifestException($"Malformed literal at position {position}.");
            position += 4;
            return null;
        }

        private static object ReadNumber(string text, ref int position)
        {
            var start = position;
            if (Peek(text, position) == '-')
                position++;
            while (position < text.Length && text[position] is >= '0' and <= '9')
                position++;
            if (position < text.Length && text[position] is '.' or 'e' or 'E')
                throw new ManifestException("The manifest carries no non-integer numbers; enum values ride as strings.");
            return long.Parse(text.Substring(start, position - start), System.Globalization.CultureInfo.InvariantCulture);
        }

        private static bool Matches(string text, int position, string literal) =>
            position + literal.Length <= text.Length && text.Substring(position, literal.Length) == literal;

        private static void SkipWhitespace(string text, ref int position)
        {
            while (position < text.Length && char.IsWhiteSpace(text[position]))
                position++;
        }

        private static char Peek(string text, int position) =>
            position < text.Length ? text[position] : throw new ManifestException("Unexpected end of JSON.");

        private static char Next(string text, ref int position) =>
            position < text.Length ? text[position++] : throw new ManifestException("Unexpected end of JSON.");

        private static void Expect(string text, ref int position, char expected)
        {
            if (Next(text, ref position) != expected)
                throw new ManifestException($"Expected '{expected}' at position {position - 1}.");
        }
    }
}
