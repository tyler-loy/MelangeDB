using System.Text;

namespace MelangeDB.CodeGen;

/// <summary>Producer emission: descriptors, argument reads with defaults, and the result encoders.</summary>
internal static partial class Emitter
{
    private static void EmitProducers(
        StringBuilder builder,
        ProducerModel[] producers,
        IReadOnlyDictionary<string, string> placementByTypeName)
    {
        builder.AppendLine();
        builder.AppendLine("        public global::System.Collections.Generic.IReadOnlyList<global::MelangeDB.Core.ProducerDescriptor> Producers() =>");
        builder.AppendLine("            new global::MelangeDB.Core.ProducerDescriptor[]");
        builder.AppendLine("            {");
        foreach (var producer in producers)
            EmitProducerDescriptor(builder, producer, producer.ResolveSite(placementByTypeName));
        builder.AppendLine("            };");
    }

    private static void EmitProducerDescriptor(StringBuilder builder, ProducerModel producer, string site)
    {
        const string Indent = "                        ";
        var parameters = producer.Parameters.Items;
        var count = $"reader.ExpectCount({producer.RequiredParameters}, {parameters.Length});";
        builder.AppendLine("                new global::MelangeDB.Core.ProducerDescriptor(");
        builder.AppendLine($"                    {Literal(producer.ProducerName)},");
        builder.AppendLine($"                    typeof({producer.ContainingTypeFqn}),");
        builder.AppendLine("                    static (ref global::MelangeDB.Core.ReducerArgsReader reader) =>");
        builder.AppendLine("                    {");
        builder.AppendLine(Indent + count);
        for (var i = 0; i < parameters.Length; i++)
            EmitProducerArgumentRead(builder, parameters[i], i, Indent, bind: false);
        builder.AppendLine(Indent + "reader.End();");
        builder.AppendLine("                    },");
        builder.AppendLine("                    static (object instance, global::MelangeDB.ProducerContext context, global::System.ReadOnlyMemory<byte> arguments, global::MelangeDB.Core.ValidationOptions limits) =>");
        builder.AppendLine("                    {");
        builder.AppendLine(Indent + "var reader = new global::MelangeDB.Core.ReducerArgsReader(arguments.Span, limits);");
        builder.AppendLine(Indent + count);
        for (var i = 0; i < parameters.Length; i++)
            EmitProducerArgumentRead(builder, parameters[i], i, Indent, bind: true);
        builder.AppendLine(Indent + "reader.End();");
        var arguments = string.Concat(Enumerable.Range(0, parameters.Length).Select(static i => $", arg{i}"));
        builder.AppendLine($"{Indent}return global::MelangeDB.Core.ProducerResults.Box((({producer.ContainingTypeFqn})instance).{producer.MethodName}(context{arguments}));");
        builder.AppendLine("                    },");
        builder.AppendLine("                    static (global::MelangeDB.Core.ProducerResultWriter writer, object value) =>");
        builder.AppendLine("                    {");
        var counter = 0;
        EmitWrite(builder, producer.ResultRef, $"(({producer.ResultFqn})value)", Indent, ref counter);
        builder.AppendLine("                    },");
        if (producer.PolicyFqn is not null)
            builder.AppendLine($"                    policy: typeof({producer.PolicyFqn}),");
        builder.AppendLine($"                    site: global::MelangeDB.ReducerSite.{site}),");
    }

    /// <summary>
    /// Reads one argument. A parameter with a default reads only when the call carries it, so a
    /// caller — an HTTP client posting a short array, an older binding — may leave trailing ones off.
    /// </summary>
    private static void EmitProducerArgumentRead(StringBuilder builder, ParameterModel parameter, int index, string indent, bool bind)
    {
        var inner = indent;
        if (parameter.HasDefault)
        {
            if (bind)
                builder.AppendLine($"{indent}{parameter.ClrFqn} arg{index} = {parameter.DefaultLiteral};");
            builder.AppendLine($"{indent}if (reader.Count > {index})");
            builder.AppendLine($"{indent}{{");
            inner = indent + "    ";
        }
        else if (bind)
        {
            builder.AppendLine($"{indent}{parameter.ClrFqn} arg{index};");
        }

        if (!parameter.IsArray)
        {
            var read = ArgRead(parameter.Kind);
            var expression = parameter.IsEnum ? $"({parameter.ClrFqn})reader.{read}()" : $"reader.{read}()";
            builder.AppendLine(bind ? $"{inner}arg{index} = {expression};" : $"{inner}_ = {expression};");
        }
        else
        {
            var elementRead = ArgRead(parameter.ElementKind);
            var element = parameter.ElementIsEnum
                ? $"({parameter.ElementClrFqn})reader.{elementRead}()"
                : $"reader.{elementRead}()";
            builder.AppendLine($"{inner}var count{index} = reader.BeginArray();");
            if (bind)
            {
                builder.AppendLine($"{inner}arg{index} = count{index} < 0 ? null : new {parameter.ElementClrFqn}[count{index}];");
                builder.AppendLine($"{inner}for (var i{index} = 0; i{index} < count{index}; i{index}++)");
                builder.AppendLine($"{inner}    arg{index}[i{index}] = {element};");
            }
            else
            {
                builder.AppendLine($"{inner}for (var i{index} = 0; i{index} < count{index}; i{index}++)");
                builder.AppendLine($"{inner}    _ = {element};");
            }
        }

        if (parameter.HasDefault)
            builder.AppendLine($"{indent}}}");
    }

    /// <summary>One encoder per result type, shared by every producer that reaches it.</summary>
    private static void EmitResultEncoders(StringBuilder builder, ProducerModel[] producers)
    {
        var types = producers
            .SelectMany(static p => p.ResultTypes.Items)
            .GroupBy(static t => t.Fqn, StringComparer.Ordinal)
            .Select(static g => g.First())
            .OrderBy(static t => t.Fqn, StringComparer.Ordinal)
            .ToArray();
        if (types.Length == 0)
            return;

        builder.AppendLine();
        builder.AppendLine("    /// <summary>Encoders for the types producers return; see MelangeDB.Core.ProducerResultWriter.</summary>");
        builder.AppendLine("    internal static class MelangeProducerCodecs");
        builder.AppendLine("    {");
        foreach (var type in types)
        {
            builder.AppendLine($"        public static void {type.EncoderName}(global::MelangeDB.Core.ProducerResultWriter writer, {type.Fqn} value)");
            builder.AppendLine("        {");
            if (!type.IsValueType)
            {
                builder.AppendLine("            if (value is null)");
                builder.AppendLine("            {");
                builder.AppendLine("                writer.WriteNull();");
                builder.AppendLine("                return;");
                builder.AppendLine("            }");
                builder.AppendLine();
            }

            builder.AppendLine($"            writer.WriteArrayHeader({type.Fields.Length});");
            var counter = 0;
            foreach (var field in type.Fields.Items)
                EmitWrite(builder, field.TypeRef, $"value.{field.Name}", "            ", ref counter);
            builder.AppendLine("        }");
            builder.AppendLine();
        }

        builder.AppendLine("    }");
    }

    /// <summary>Writes <paramref name="expression"/>, of the type <paramref name="typeRef"/> names.</summary>
    private static void EmitWrite(StringBuilder builder, string typeRef, string expression, string indent, ref int counter)
    {
        switch (typeRef[0])
        {
            case 's':
                builder.AppendLine($"{indent}{ScalarWrite((WireKind)Enum.Parse(typeof(WireKind), typeRef.Substring(2)), expression)};");
                return;
            case 'e':
            {
                var kind = (WireKind)Enum.Parse(typeof(WireKind), typeRef.Substring(2, typeRef.IndexOf(':', 2) - 2));
                builder.AppendLine(IsUnsigned(kind)
                    ? $"{indent}writer.WriteUInt64((ulong){expression});"
                    : $"{indent}writer.WriteInt64((long){expression});");
                return;
            }

            case 'r':
            {
                var fqn = typeRef.Substring(2);
                builder.AppendLine($"{indent}global::MelangeDB.Generated.MelangeProducerCodecs.Write_{ResultTypeRefs.Safe(fqn)}(writer, {expression});");
                return;
            }

            case 'n':
            {
                var value = $"value{counter++}";
                builder.AppendLine($"{indent}if ({expression} is {{ }} {value})");
                builder.AppendLine($"{indent}{{");
                EmitWrite(builder, typeRef.Substring(2), value, indent + "    ", ref counter);
                builder.AppendLine($"{indent}}}");
                builder.AppendLine($"{indent}else");
                builder.AppendLine($"{indent}{{");
                builder.AppendLine($"{indent}    writer.WriteNull();");
                builder.AppendLine($"{indent}}}");
                return;
            }

            case 'l':
            {
                var list = $"list{counter}";
                var item = $"item{counter++}";
                builder.AppendLine($"{indent}if ({expression} is null)");
                builder.AppendLine($"{indent}{{");
                builder.AppendLine($"{indent}    writer.WriteNull();");
                builder.AppendLine($"{indent}}}");
                builder.AppendLine($"{indent}else");
                builder.AppendLine($"{indent}{{");
                builder.AppendLine($"{indent}    var {list} = global::MelangeDB.Core.ProducerResults.Materialize({expression});");
                builder.AppendLine($"{indent}    writer.WriteArrayHeader({list}.Count);");
                builder.AppendLine($"{indent}    foreach (var {item} in {list})");
                builder.AppendLine($"{indent}    {{");
                EmitWrite(builder, typeRef.Substring(2), item, indent + "        ", ref counter);
                builder.AppendLine($"{indent}    }}");
                builder.AppendLine($"{indent}}}");
                return;
            }

            default:
                throw new InvalidOperationException($"Unknown result type reference '{typeRef}'.");
        }
    }

    private static string ScalarWrite(WireKind kind, string expression) => kind switch
    {
        WireKind.Bool => $"writer.WriteBool({expression})",
        WireKind.Int8 or WireKind.Int16 or WireKind.Int32 or WireKind.Int64 => $"writer.WriteInt64({expression})",
        WireKind.UInt8 or WireKind.UInt16 or WireKind.UInt32 or WireKind.UInt64 => $"writer.WriteUInt64({expression})",
        WireKind.Float32 or WireKind.Float64 => $"writer.WriteFloat64({expression})",
        WireKind.String => $"writer.WriteString({expression})",
        WireKind.Bytes => $"writer.WriteBytes({expression})",
        WireKind.Identity => $"writer.WriteIdentity({expression})",
        WireKind.Timestamp => $"writer.WriteTimestamp({expression})",
        _ => throw new InvalidOperationException($"Wire kind {kind} has no result encoding."),
    };

    private static bool IsUnsigned(WireKind kind) =>
        kind is WireKind.UInt8 or WireKind.UInt16 or WireKind.UInt32 or WireKind.UInt64;
}
