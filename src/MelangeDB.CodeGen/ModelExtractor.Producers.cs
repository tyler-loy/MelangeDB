using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MelangeDB.CodeGen;

/// <summary>Producer discovery: the signature, the parameters, and the result type's whole shape.</summary>
internal static partial class ModelExtractor
{
    private static readonly string[] SequenceTypes =
    [
        "System.Collections.Generic.List<T>",
        "System.Collections.Generic.IList<T>",
        "System.Collections.Generic.IReadOnlyList<T>",
        "System.Collections.Generic.ICollection<T>",
        "System.Collections.Generic.IReadOnlyCollection<T>",
        "System.Collections.Generic.IEnumerable<T>",
    ];

    public static ProducerModel? ExtractProducer(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not IMethodSymbol method || context.Attributes.Length == 0)
            return null;

        var attribute = context.Attributes[0];
        var producerName = method.Name;
        string? policyFqn = null;
        var declaredSite = "Auto";
        foreach (var named in attribute.NamedArguments)
        {
            if (named.Key == "Name" && named.Value.Value is string explicitName)
                producerName = explicitName;
            if (named.Key == "Policy" && named.Value.Value is INamedTypeSymbol policyType)
                policyFqn = Fqn(policyType);
            if (named.Key == "Site" && named.Value.Value is int site)
                declaredSite = site switch { 1 => "Hub", 2 => "Shard", _ => "Auto" };
        }

        var location = LocationInfo.From(
            (context.TargetNode as MethodDeclarationSyntax)?.Identifier.GetLocation() ?? context.TargetNode.GetLocation());
        var diagnostics = new List<DiagnosticInfo>();
        void Signature(string reason) =>
            diagnostics.Add(new DiagnosticInfo("MELANGE0028", location, new EquatableArray<string>([producerName, reason])));

        if (method.IsStatic)
            Signature("is static; producers are instance methods on a DI-resolved class");
        if (method.IsGenericMethod)
            Signature("is generic");
        if (method.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Internal))
            Signature("is not public or internal, so the generated dispatcher cannot call it");
        if (method.ContainingType is not { TypeKind: TypeKind.Class } containing || containing.IsGenericType || !IsAccessible(containing))
            Signature("must be declared on a non-generic, public-or-internal class");

        var parameters = new List<ParameterModel>();
        var enums = new List<EnumModel>();
        if (method.Parameters.Length == 0 || method.Parameters[0].Type.ToDisplayString() != "MelangeDB.ProducerContext")
        {
            Signature("must take ProducerContext as its first parameter");
        }
        else
        {
            var defaulted = false;
            for (var i = 1; i < method.Parameters.Length; i++)
            {
                var parameter = method.Parameters[i];
                if (parameter.RefKind != RefKind.None)
                {
                    Signature($"passes parameter '{parameter.Name}' by reference");
                    continue;
                }

                var model = ClassifyParameter(parameter);
                if (model is null || IsTableStruct(parameter.Type))
                {
                    Signature(
                        $"has parameter '{parameter.Name}' of type {parameter.Type.ToDisplayString()}, which a client call cannot carry " +
                        "(supported: bool, integers, floats, string, byte[], Identity, Timestamp, enums, and single-dimension arrays of these)");
                    continue;
                }

                if (parameter.HasExplicitDefaultValue)
                {
                    var (literal, json) = DefaultValue(parameter);
                    model = model with { DefaultLiteral = literal, DefaultJson = json };
                    defaulted = true;
                }
                else if (defaulted)
                {
                    Signature($"has parameter '{parameter.Name}' with no default after one with a default");
                }

                if (model.IsEnum)
                    AddEnum(enums, parameter.Type);
                else if (model.ElementIsEnum && parameter.Type is IArrayTypeSymbol enumArray)
                    AddEnum(enums, enumArray.ElementType);
                parameters.Add(model);
            }
        }

        var resultTypes = new List<ResultTypeModel>();
        var resultRef = string.Empty;
        var resultFqn = string.Empty;
        if (UnwrapResult(method.ReturnType) is not { } result)
        {
            Signature($"returns {method.ReturnType.ToDisplayString()}; a producer returns a value — T, Task<T>, or ValueTask<T>");
        }
        else
        {
            resultFqn = Fqn(result);
            resultRef = ResultRef(result, "its result", producerName, resultTypes, enums, diagnostics, location, []) ?? string.Empty;
        }

        var (touched, opaque) = AnalyzeTableTouches(method, context.TargetNode as MethodDeclarationSyntax);
        return new ProducerModel(
            ContainingTypeFqn: method.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            MethodName: method.Name,
            ProducerName: producerName,
            PolicyFqn: policyFqn,
            Location: location,
            Parameters: new EquatableArray<ParameterModel>([.. parameters]),
            ResultRef: resultRef,
            ResultFqn: resultFqn,
            ResultTypes: new EquatableArray<ResultTypeModel>([.. resultTypes]),
            Enums: new EquatableArray<EnumModel>([.. enums]),
            Diagnostics: new EquatableArray<DiagnosticInfo>([.. diagnostics]),
            DeclaredSite: declaredSite,
            TouchedTables: touched,
            OpaqueBody: opaque);
    }

    /// <summary>The value a producer returns: <c>T</c> from <c>Task&lt;T&gt;</c>, <c>ValueTask&lt;T&gt;</c>, or <c>T</c> itself; null for no value.</summary>
    private static ITypeSymbol? UnwrapResult(ITypeSymbol returnType)
    {
        if (returnType.SpecialType == SpecialType.System_Void)
            return null;
        if (returnType is INamedTypeSymbol named && named.ContainingNamespace?.ToDisplayString() == "System.Threading.Tasks")
        {
            if (named.Name is "Task" or "ValueTask")
                return named.TypeArguments.Length == 1 ? named.TypeArguments[0] : null;
        }

        return returnType;
    }

    /// <summary>
    /// The <see cref="ResultTypeRefs"/> reference for <paramref name="type"/>, registering every
    /// result type and enum it reaches. Null, with MELANGE0029 naming the member path, when some
    /// part of it cannot be encoded.
    /// </summary>
    private static string? ResultRef(
        ITypeSymbol type,
        string path,
        string producerName,
        List<ResultTypeModel> types,
        List<EnumModel> enums,
        List<DiagnosticInfo> diagnostics,
        LocationInfo location,
        HashSet<string> inProgress)
    {
        string? Refuse(string reason)
        {
            diagnostics.Add(new DiagnosticInfo(
                "MELANGE0029",
                location,
                new EquatableArray<string>([producerName, path, type.ToDisplayString(), reason])));
            return null;
        }

        if (type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } nullable)
        {
            var inner = ResultRef(nullable.TypeArguments[0], path, producerName, types, enums, diagnostics, location, inProgress);
            return inner is null ? null : ResultTypeRefs.Nullable(inner);
        }

        var (kind, isEnum, _) = Classify(type);
        if (kind == WireKind.ScheduleAt)
            return Refuse("is a schedule, which only a table column carries");
        if (isEnum)
        {
            AddEnum(enums, type);
            return ResultTypeRefs.Enum(kind, Fqn(type));
        }

        if (kind != WireKind.None)
            return ResultTypeRefs.Scalar(kind);

        if (type is IArrayTypeSymbol array)
        {
            if (array.Rank != 1)
                return Refuse("is a multi-dimensional array; use an array of arrays or a list of lists");
            var element = ResultRef(array.ElementType, path + "[]", producerName, types, enums, diagnostics, location, inProgress);
            return element is null ? null : ResultTypeRefs.List(element);
        }

        if (type is INamedTypeSymbol { IsGenericType: true } generic
            && SequenceTypes.Contains(generic.OriginalDefinition.ToDisplayString(), StringComparer.Ordinal))
        {
            var element = ResultRef(generic.TypeArguments[0], path + "[]", producerName, types, enums, diagnostics, location, inProgress);
            return element is null ? null : ResultTypeRefs.List(element);
        }

        if (IsTableStruct(type))
            return Refuse("a [Table] struct — a producer's result is its contract with the client, so map the row to a record that says what the client may see");
        if (type is not INamedTypeSymbol { TypeKind: TypeKind.Class or TypeKind.Struct } record)
            return Refuse("is not a scalar, a list, or a record");
        if (record.ContainingNamespace?.ToDisplayString() is { } ns && (ns == "System" || ns.StartsWith("System.", StringComparison.Ordinal)))
            return Refuse("is a framework type with no wire form");
        if (record.IsGenericType)
            return Refuse("is generic, and a client binding needs one name per result type");
        if (record.IsAbstract)
            return Refuse("is abstract, so the client could not tell which shape it holds");
        if (!IsAccessible(record))
            return Refuse("is not public or internal, so the generated encoder cannot read it");

        var fqn = Fqn(record);
        if (types.Any(t => t.Fqn == fqn) || !inProgress.Add(fqn))
            return ResultTypeRefs.Record(fqn);

        var fields = new List<ResultFieldModel>();
        var valid = true;
        foreach (var member in ResultMembers(record))
        {
            var memberType = member is IPropertySymbol property ? property.Type : ((IFieldSymbol)member).Type;
            var reference = ResultRef(memberType, $"{path}.{member.Name}", producerName, types, enums, diagnostics, location, inProgress);
            if (reference is null)
            {
                valid = false;
                continue;
            }

            fields.Add(new ResultFieldModel(member.Name, reference));
        }

        inProgress.Remove(fqn);
        if (!valid)
            return null;
        types.Add(new ResultTypeModel(record.Name, fqn, record.IsValueType, new EquatableArray<ResultFieldModel>([.. fields])));
        return ResultTypeRefs.Record(fqn);
    }

    /// <summary>
    /// The members a result type carries, in wire order: public instance fields and readable
    /// properties, base type's first, each in declaration order. Indexers and static members are not
    /// data; a record's compiler-generated <c>EqualityContract</c> is not public, so it never appears.
    /// </summary>
    private static IEnumerable<ISymbol> ResultMembers(INamedTypeSymbol type)
    {
        if (type.BaseType is { SpecialType: not (SpecialType.System_Object or SpecialType.System_ValueType) } baseType)
        {
            foreach (var inherited in ResultMembers(baseType))
                yield return inherited;
        }

        foreach (var member in type.GetMembers())
        {
            if (member.IsStatic || member.DeclaredAccessibility != Accessibility.Public)
                continue;
            if (member is IFieldSymbol { IsConst: false })
                yield return member;
            else if (member is IPropertySymbol { IsIndexer: false, GetMethod.DeclaredAccessibility: Accessibility.Public } && !IsOverride(member))
                yield return member;
        }
    }

    private static bool IsOverride(ISymbol member) => member is IPropertySymbol { IsOverride: true };

    /// <summary>A parameter default as a C# literal for the generated reader, and as JSON for the manifest.</summary>
    private static (string Literal, string Json) DefaultValue(IParameterSymbol parameter)
    {
        var value = parameter.ExplicitDefaultValue;
        if (value is null)
            return ("default", "null");
        if (parameter.Type.TypeKind == TypeKind.Enum)
        {
            var text = Convert.ToString(value, CultureInfo.InvariantCulture)!;
            return ($"({Fqn(parameter.Type)})({text})", text);
        }

        return value switch
        {
            bool b => (b ? "true" : "false", b ? "true" : "false"),
            string s => (SymbolDisplay.FormatLiteral(s, quote: true), SymbolDisplay.FormatLiteral(s, quote: true)),
            float f => (f.ToString("R", CultureInfo.InvariantCulture) + "F", f.ToString("R", CultureInfo.InvariantCulture)),
            double d => (d.ToString("R", CultureInfo.InvariantCulture) + "D", d.ToString("R", CultureInfo.InvariantCulture)),
            _ => (Convert.ToString(value, CultureInfo.InvariantCulture)!, Convert.ToString(value, CultureInfo.InvariantCulture)!),
        };
    }
}
