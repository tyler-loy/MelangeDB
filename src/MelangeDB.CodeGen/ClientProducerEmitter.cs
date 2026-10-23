using System.Text;

namespace MelangeDB.CodeGen;

/// <summary>
/// The client half of producers: one record per result type, one typed stub per producer on
/// <c>conn.Producers</c>, and the decoders that read a result positionally against the manifest's
/// type references — the inverse of the server's generated encoders.
/// </summary>
internal static class ClientProducerEmitter
{
    private const string Reader = "global::MelangeDB.Protocol.ProducerResultReader";

    public static void Emit(StringBuilder builder, ClientSchemaModel schema)
    {
        var types = schema.ResultTypes.Items.ToDictionary(static t => t.Name, StringComparer.Ordinal);
        var enums = schema.Enums.Items.ToDictionary(static e => e.Name, StringComparer.Ordinal);

        foreach (var type in schema.ResultTypes.Items)
            EmitRecord(builder, type, types);

        builder.AppendLine("    /// <summary>One typed stub per producer: a read-only server call answered with a value.</summary>");
        builder.AppendLine("    public sealed class MelangeProducers");
        builder.AppendLine("    {");
        builder.AppendLine("        private readonly global::MelangeDB.Client.MelangeClient _client;");
        builder.AppendLine();
        builder.AppendLine("        internal MelangeProducers(global::MelangeDB.Client.MelangeClient client) => _client = client;");
        foreach (var producer in schema.Producers.Items)
        {
            var parameters = string.Concat(producer.Parameters.Items.Select(p => $"{ClientEmitter.ParameterClrType(p)} {ClientEmitter.ParameterName(p.Name)}{DefaultClause(p)}, "));
            var arguments = string.Join(", ", producer.Parameters.Items.Select(static p => ClientEmitter.ParameterName(p.Name)));
            var result = ClrTypeOf(producer.Returns, types);
            builder.AppendLine();
            builder.AppendLine($"        public async global::System.Threading.Tasks.Task<{result}> {producer.Name}Async({parameters}global::System.Threading.CancellationToken cancellationToken = default)");
            builder.AppendLine("        {");
            builder.AppendLine(producer.Parameters.Length == 0
                ? $"            var reply = await _client.CallProducerAsync({ClientEmitter.Literal(producer.Name)}, null, cancellationToken).ConfigureAwait(false);"
                : $"            var reply = await _client.CallProducerAsync({ClientEmitter.Literal(producer.Name)}, new object[] {{ {arguments} }}, cancellationToken).ConfigureAwait(false);");
            builder.AppendLine($"            return MelangeProducerDecoders.Decode_{producer.Name}(reply.Payload);");
            builder.AppendLine("        }");
        }

        builder.AppendLine("    }");
        builder.AppendLine();

        var lists = new SortedSet<string>(StringComparer.Ordinal);
        var decoders = new StringBuilder();
        foreach (var producer in schema.Producers.Items)
        {
            decoders.AppendLine();
            decoders.AppendLine($"        public static {ClrTypeOf(producer.Returns, types)} Decode_{producer.Name}(global::System.ReadOnlyMemory<byte> payload)");
            decoders.AppendLine("        {");
            decoders.AppendLine($"            var reader = new {Reader}(payload.Span);");
            decoders.AppendLine($"            var value = {ReadExpression(producer.Returns, types, enums, lists)};");
            decoders.AppendLine("            reader.End();");
            decoders.AppendLine("            return value;");
            decoders.AppendLine("        }");
        }

        foreach (var type in schema.ResultTypes.Items)
            EmitRecordReader(decoders, type, types, enums, lists);

        // List readers are collected while the expressions above are built, including the lists
        // a list reader itself needs; a work queue keeps nested lists from being missed.
        var emitted = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Queue<string>(lists);
        while (pending.Count > 0)
        {
            var list = pending.Dequeue();
            if (!emitted.Add(list))
                continue;
            var inner = list.Substring(0, list.Length - 2);
            var nested = new SortedSet<string>(StringComparer.Ordinal);
            var element = ReadExpression(inner, types, enums, nested);
            foreach (var more in nested)
                pending.Enqueue(more);
            var listType = ClrTypeOf(list, types);
            decoders.AppendLine();
            decoders.AppendLine($"        public static {listType} {ListReaderName(list)}(ref {Reader} reader)");
            decoders.AppendLine("        {");
            decoders.AppendLine("            var count = reader.BeginArray();");
            decoders.AppendLine("            if (count < 0)");
            decoders.AppendLine("                return null;");
            decoders.AppendLine($"            var list = new {listType}(count);");
            decoders.AppendLine("            for (var i = 0; i < count; i++)");
            decoders.AppendLine($"                list.Add({element});");
            decoders.AppendLine("            return list;");
            decoders.AppendLine("        }");
        }

        builder.AppendLine("    /// <summary>Reads producer results positionally, against the manifest's type references.</summary>");
        builder.AppendLine("    internal static class MelangeProducerDecoders");
        builder.AppendLine("    {");
        builder.Append(decoders.ToString().TrimStart('\r', '\n'));
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private static void EmitRecord(StringBuilder builder, ClientResultTypeModel type, IReadOnlyDictionary<string, ClientResultTypeModel> types)
    {
        builder.AppendLine($"    /// <summary>Mirrors the server's '{type.Name}', a producer result type, per the schema manifest.</summary>");
        builder.AppendLine(type.IsValueType
            ? $"    public partial record struct {type.Name}"
            : $"    public sealed partial record {type.Name}");
        builder.AppendLine("    {");
        foreach (var field in type.Fields.Items)
            builder.AppendLine($"        public {ClrTypeOf(field.Type, types)} {ClientEmitter.ParameterName(field.Name)} {{ get; init; }}");
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private static void EmitRecordReader(
        StringBuilder decoders,
        ClientResultTypeModel type,
        IReadOnlyDictionary<string, ClientResultTypeModel> types,
        IReadOnlyDictionary<string, ClientEnumModel> enums,
        ISet<string> lists)
    {
        decoders.AppendLine();
        decoders.AppendLine($"        public static {type.Name} Read_{type.Name}(ref {Reader} reader)");
        decoders.AppendLine("        {");
        decoders.AppendLine("            var count = reader.BeginArray();");
        decoders.AppendLine("            if (count < 0)");
        decoders.AppendLine(type.IsValueType
            ? $"                throw new global::MelangeDB.Protocol.ProducerResultFormatException(\"A null where the bindings expect a {type.Name}.\");"
            : "                return null;");
        decoders.AppendLine($"            if (count != {type.Fields.Length})");
        decoders.AppendLine($"                throw new global::MelangeDB.Protocol.ProducerResultFormatException($\"A {type.Name} with {{count}} fields where the bindings expect {type.Fields.Length}; regenerate them against the server's manifest.\");");
        for (var i = 0; i < type.Fields.Length; i++)
            decoders.AppendLine($"            var f{i} = {ReadExpression(type.Fields[i].Type, types, enums, lists)};");
        var assignments = string.Join(", ", type.Fields.Items.Select((field, i) => $"{ClientEmitter.ParameterName(field.Name)} = f{i}"));
        decoders.AppendLine($"            return new {type.Name} {{ {assignments} }};");
        decoders.AppendLine("        }");
    }

    /// <summary>The expression reading one value of <paramref name="reference"/> from <c>reader</c>.</summary>
    private static string ReadExpression(
        string reference,
        IReadOnlyDictionary<string, ClientResultTypeModel> types,
        IReadOnlyDictionary<string, ClientEnumModel> enums,
        ISet<string> lists)
    {
        if (reference.EndsWith("[]", StringComparison.Ordinal))
        {
            lists.Add(reference);
            return $"{ListReaderName(reference)}(ref reader)";
        }

        if (reference.EndsWith("?", StringComparison.Ordinal))
        {
            var inner = reference.Substring(0, reference.Length - 1);
            return $"reader.TryReadNull() ? ({ClrTypeOf(reference, types)})null : {ReadExpression(inner, types, enums, lists)}";
        }

        if (ResultTypeReference.ScalarKind(reference) is { } kind)
            return kind == WireKind.Bytes ? "reader.ReadBytes()" : $"reader.Read{kind}()";
        if (enums.TryGetValue(reference, out var declaration))
        {
            return declaration.Underlying is WireKind.UInt8 or WireKind.UInt16 or WireKind.UInt32 or WireKind.UInt64
                ? $"({declaration.Name})reader.ReadUInt64()"
                : $"({declaration.Name})reader.ReadInt64()";
        }

        return $"Read_{reference}(ref reader)";
    }

    /// <summary>The client CLR type of a manifest reference.</summary>
    private static string ClrTypeOf(string reference, IReadOnlyDictionary<string, ClientResultTypeModel> types)
    {
        if (reference.EndsWith("[]", StringComparison.Ordinal))
            return $"global::System.Collections.Generic.List<{ClrTypeOf(reference.Substring(0, reference.Length - 2), types)}>";
        if (reference.EndsWith("?", StringComparison.Ordinal))
            return ClrTypeOf(reference.Substring(0, reference.Length - 1), types) + "?";
        return ResultTypeReference.ScalarKind(reference) is { } kind ? ClientEmitter.ClrType(kind, null) : reference;
    }

    private static string ListReaderName(string reference)
    {
        var name = new StringBuilder("ReadList_");
        foreach (var c in reference)
            name.Append(c switch { '[' => "L", ']' => string.Empty, '?' => "N", _ => c.ToString() });
        return name.ToString();
    }

    /// <summary>
    /// A parameter default as C#, from the manifest's literal text: an enum value is cast, a float
    /// gets its suffix, and <c>null</c> becomes <c>default</c> so it suits value types too.
    /// </summary>
    private static string DefaultClause(ClientParameterModel parameter)
    {
        if (parameter.Default is not { } value)
            return string.Empty;
        if (value == "null")
            return " = default";
        if (parameter.EnumName is { } enumName && !parameter.IsArray)
            return $" = ({enumName})({value})";
        return parameter.Kind switch
        {
            WireKind.Float32 => $" = {value}F",
            WireKind.Float64 => $" = {value}D",
            _ => $" = {value}",
        };
    }
}
