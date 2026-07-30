using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MelangeDB.CodeGen;

/// <summary>Turns attributed symbols into equatable models, collecting structural diagnostics.</summary>
internal static class ModelExtractor
{
    public static TableModel? ExtractTable(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not INamedTypeSymbol type || context.Attributes.Length == 0)
            return null;

        var attribute = context.Attributes[0];
        var tableName = type.Name;
        var isPublic = false;
        var tier = "Hot";
        var residency = "Paged";
        var placement = "Partitioned";
        string? shardBy = null;
        string? scheduled = null;
        foreach (var named in attribute.NamedArguments)
        {
            switch (named.Key)
            {
                case "Name":
                    tableName = named.Value.Value as string ?? tableName;
                    break;
                case "Public":
                    isPublic = named.Value.Value is true;
                    break;
                case "Tier":
                    tier = named.Value.Value is int t && t == 1 ? "Relational" : "Hot";
                    break;
                case "Residency":
                    residency = named.Value.Value is int r ? r switch { 1 => "Resident", 2 => "Auto", _ => "Paged" } : "Paged";
                    break;
                case "Placement":
                    placement = named.Value.Value is int p ? p switch { 1 => "Replicated", 2 => "Global", 3 => "Local", _ => "Partitioned" } : "Partitioned";
                    break;
                case "ShardBy":
                    shardBy = named.Value.Value as string;
                    break;
                case "Scheduled":
                    scheduled = named.Value.Value as string;
                    break;
            }
        }

        var typeLocation = LocationInfo.From(
            (context.TargetNode as StructDeclarationSyntax)?.Identifier.GetLocation() ?? context.TargetNode.GetLocation());
        var diagnostics = new List<DiagnosticInfo>();
        var columns = new List<ColumnModel>();

        // Fields first, then read-write properties — matching the reflection path's
        // MetadataToken ordering, so both serializers agree on column order.
        foreach (var member in type.GetMembers())
        {
            if (member is IFieldSymbol { IsStatic: false, IsConst: false, DeclaredAccessibility: Accessibility.Public, IsImplicitlyDeclared: false } field)
                AddColumn(columns, diagnostics, tableName, isPublic, placement, field, field.Type, isProperty: false);
        }

        foreach (var member in type.GetMembers())
        {
            if (member is IPropertySymbol { IsStatic: false, DeclaredAccessibility: Accessibility.Public, GetMethod: not null, SetMethod: not null, IsIndexer: false } property)
                AddColumn(columns, diagnostics, tableName, isPublic, placement, property, property.Type, isProperty: true);
        }

        var primaryKeys = columns.Count(static c => c.IsPrimaryKey);
        if (primaryKeys != 1)
        {
            diagnostics.Add(new DiagnosticInfo(
                "MELANGE0001",
                typeLocation,
                new EquatableArray<string>([tableName, primaryKeys.ToString()])));
        }

        return new TableModel(
            TypeFqn: type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            TypeName: type.Name,
            SafeName: SafeName(type),
            TableName: tableName,
            Location: typeLocation,
            IsPublic: isPublic,
            Tier: tier,
            Residency: residency,
            Placement: placement,
            ShardBy: shardBy,
            Scheduled: scheduled,
            Columns: new EquatableArray<ColumnModel>([.. columns]),
            Diagnostics: new EquatableArray<DiagnosticInfo>([.. diagnostics]));
    }

    public static ReducerModel? ExtractReducer(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not IMethodSymbol method || context.Attributes.Length == 0)
            return null;

        var attribute = context.Attributes[0];
        var kind = attribute.ConstructorArguments.Length > 0 && attribute.ConstructorArguments[0].Value is int k
            ? k switch { 1 => "ClientConnected", 2 => "ClientDisconnected", _ => "Standard" }
            : "Standard";
        var reducerName = method.Name;
        string? policyFqn = null;
        foreach (var named in attribute.NamedArguments)
        {
            if (named.Key == "Name" && named.Value.Value is string explicitName)
                reducerName = explicitName;
            if (named.Key == "Policy" && named.Value.Value is INamedTypeSymbol policyType)
                policyFqn = Fqn(policyType);
        }

        var location = LocationInfo.From(
            (context.TargetNode as MethodDeclarationSyntax)?.Identifier.GetLocation() ?? context.TargetNode.GetLocation());
        var diagnostics = new List<DiagnosticInfo>();

        if (method.IsAsync)
            diagnostics.Add(new DiagnosticInfo("MELANGE0008", location, new EquatableArray<string>([reducerName])));
        else if (!method.ReturnsVoid)
            AddSignature(diagnostics, location, reducerName, $"returns {method.ReturnType.ToDisplayString()} instead of void");

        if (method.IsStatic)
            AddSignature(diagnostics, location, reducerName, "is static; reducers are instance methods on a DI-resolved class");
        if (method.IsGenericMethod)
            AddSignature(diagnostics, location, reducerName, "is generic");
        if (method.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Internal))
            AddSignature(diagnostics, location, reducerName, "is not public or internal, so the generated dispatcher cannot call it");
        if (method.ContainingType is not { TypeKind: TypeKind.Class } containing || containing.IsGenericType || !IsAccessible(containing))
            AddSignature(diagnostics, location, reducerName, "must be declared on a non-generic, public-or-internal class");

        var parameters = new List<ParameterModel>();
        if (method.Parameters.Length == 0 || method.Parameters[0].Type.ToDisplayString() != "MelangeDB.ReducerContext")
        {
            AddSignature(diagnostics, location, reducerName, "must take ReducerContext as its first parameter");
        }
        else
        {
            for (var i = 1; i < method.Parameters.Length; i++)
            {
                var parameter = method.Parameters[i];
                if (parameter.RefKind != RefKind.None)
                {
                    AddSignature(diagnostics, location, reducerName, $"passes parameter '{parameter.Name}' by reference");
                    continue;
                }

                var model = ClassifyParameter(parameter);
                if (model is null)
                {
                    diagnostics.Add(new DiagnosticInfo(
                        "MELANGE0004",
                        LocationInfo.From(parameter.Locations.FirstOrDefault() ?? Location.None),
                        new EquatableArray<string>([reducerName, parameter.Name, parameter.Type.ToDisplayString()])));
                    continue;
                }

                parameters.Add(model);
            }
        }

        return new ReducerModel(
            ContainingTypeFqn: method.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            MethodName: method.Name,
            ReducerName: reducerName,
            Kind: kind,
            PolicyFqn: policyFqn,
            Parameters: new EquatableArray<ParameterModel>([.. parameters]),
            Diagnostics: new EquatableArray<DiagnosticInfo>([.. diagnostics]));
    }

    internal static (WireKind Kind, bool IsEnum, string EnumUnderlyingFqn) Classify(ITypeSymbol type)
    {
        if (type.TypeKind == TypeKind.Enum && type is INamedTypeSymbol { EnumUnderlyingType: { } underlying })
            return (FromSpecialType(underlying.SpecialType), true, Fqn(underlying));
        if (type is IArrayTypeSymbol { Rank: 1, ElementType.SpecialType: SpecialType.System_Byte })
            return (WireKind.Bytes, false, string.Empty);
        var byName = type.ToDisplayString() switch
        {
            "MelangeDB.Identity" => WireKind.Identity,
            "MelangeDB.Timestamp" => WireKind.Timestamp,
            _ => WireKind.None,
        };
        return byName != WireKind.None
            ? (byName, false, string.Empty)
            : (FromSpecialType(type.SpecialType), false, string.Empty);
    }

    private static WireKind FromSpecialType(SpecialType type) => type switch
    {
        SpecialType.System_Boolean => WireKind.Bool,
        SpecialType.System_SByte => WireKind.Int8,
        SpecialType.System_Byte => WireKind.UInt8,
        SpecialType.System_Int16 => WireKind.Int16,
        SpecialType.System_UInt16 => WireKind.UInt16,
        SpecialType.System_Int32 => WireKind.Int32,
        SpecialType.System_UInt32 => WireKind.UInt32,
        SpecialType.System_Int64 => WireKind.Int64,
        SpecialType.System_UInt64 => WireKind.UInt64,
        SpecialType.System_Single => WireKind.Float32,
        SpecialType.System_Double => WireKind.Float64,
        SpecialType.System_String => WireKind.String,
        _ => WireKind.None,
    };

    private static void AddColumn(
        List<ColumnModel> columns,
        List<DiagnosticInfo> diagnostics,
        string tableName,
        bool isPublic,
        string placement,
        ISymbol member,
        ITypeSymbol memberType,
        bool isProperty)
    {
        var location = LocationInfo.From(member.Locations.FirstOrDefault() ?? Location.None);
        var (kind, isEnum, _) = Classify(memberType);
        var isPrimaryKey = HasAttribute(member, "PrimaryKeyAttribute");
        var isAutoInc = HasAttribute(member, "AutoIncAttribute");
        var isUnique = HasAttribute(member, "UniqueAttribute");
        var isIndexed = HasAttribute(member, "IndexAttribute");
        var isServerOnly = HasAttribute(member, "ServerOnlyAttribute");

        if (kind == WireKind.None)
        {
            diagnostics.Add(new DiagnosticInfo(
                "MELANGE0011",
                location,
                new EquatableArray<string>([tableName, member.Name, memberType.ToDisplayString()])));
            return;
        }

        var column = new ColumnModel(member.Name, kind, Fqn(memberType), isEnum, isPrimaryKey, isAutoInc, isUnique, isIndexed, isServerOnly, isProperty);
        columns.Add(column);

        if (isAutoInc && kind is not (WireKind.Int64 or WireKind.UInt64))
        {
            diagnostics.Add(new DiagnosticInfo(
                "MELANGE0002",
                location,
                new EquatableArray<string>([tableName, member.Name, memberType.ToDisplayString()])));
        }

        if ((isPrimaryKey || isUnique || isIndexed) && !column.IsKeyEncodable)
        {
            diagnostics.Add(new DiagnosticInfo(
                "MELANGE0012",
                location,
                new EquatableArray<string>([tableName, member.Name, memberType.ToDisplayString()])));
        }

        if (isUnique && placement == "Partitioned")
            diagnostics.Add(new DiagnosticInfo("MELANGE0003", location, new EquatableArray<string>([tableName, member.Name])));

        if (isServerOnly && !isPublic)
            diagnostics.Add(new DiagnosticInfo("MELANGE0007", location, new EquatableArray<string>([tableName, member.Name])));
    }

    private static ParameterModel? ClassifyParameter(IParameterSymbol parameter)
    {
        if (parameter.Type is IArrayTypeSymbol { Rank: 1 } array && array.ElementType.SpecialType != SpecialType.System_Byte)
        {
            var (elementKind, elementIsEnum, elementUnderlying) = Classify(array.ElementType);
            if (elementKind == WireKind.None || array.ElementType is IArrayTypeSymbol)
                return null;
            return new ParameterModel(
                parameter.Name,
                WireKind.None,
                Fqn(parameter.Type),
                IsEnum: false,
                EnumUnderlyingFqn: string.Empty,
                IsArray: true,
                ElementKind: elementKind,
                ElementClrFqn: Fqn(array.ElementType),
                ElementIsEnum: elementIsEnum,
                ElementEnumUnderlyingFqn: elementUnderlying);
        }

        var (kind, isEnum, underlying) = Classify(parameter.Type);
        if (kind == WireKind.None)
            return null;
        return new ParameterModel(
            parameter.Name,
            kind,
            Fqn(parameter.Type),
            isEnum,
            underlying,
            IsArray: false,
            ElementKind: WireKind.None,
            ElementClrFqn: string.Empty,
            ElementIsEnum: false,
            ElementEnumUnderlyingFqn: string.Empty);
    }

    private static void AddSignature(List<DiagnosticInfo> diagnostics, LocationInfo location, string reducerName, string reason) =>
        diagnostics.Add(new DiagnosticInfo("MELANGE0009", location, new EquatableArray<string>([reducerName, reason])));

    private static bool IsAccessible(INamedTypeSymbol type) =>
        type.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal
        && (type.ContainingType is null || IsAccessible(type.ContainingType));

    private static bool HasAttribute(ISymbol member, string attributeName) =>
        member.GetAttributes().Any(a =>
            a.AttributeClass is { Name: var name, ContainingNamespace.Name: "MelangeDB" } && name == attributeName);

    private static string Fqn(ITypeSymbol type) => type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

    private static string SafeName(INamedTypeSymbol type)
    {
        var display = type.ToDisplayString();
        var safe = new char[display.Length];
        for (var i = 0; i < display.Length; i++)
        {
            var c = display[i];
            safe[i] = char.IsLetterOrDigit(c) ? c : '_';
        }

        return new string(safe);
    }
}
