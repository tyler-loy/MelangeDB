using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace MelangeDB.CodeGen;

/// <summary>
/// An array with value equality, so records holding one stay cacheable in the incremental pipeline.
/// </summary>
internal readonly struct EquatableArray<T>(T[] items) : IEquatable<EquatableArray<T>>
    where T : IEquatable<T>
{
    private readonly T[]? _items = items;

    public T[] Items => _items ?? [];

    public int Length => Items.Length;

    public T this[int index] => Items[index];

    public bool Equals(EquatableArray<T> other)
    {
        var a = Items;
        var b = other.Items;
        if (a.Length != b.Length)
            return false;
        for (var i = 0; i < a.Length; i++)
        {
            if (!a[i].Equals(b[i]))
                return false;
        }

        return true;
    }

    public override bool Equals(object? obj) => obj is EquatableArray<T> other && Equals(other);

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            foreach (var item in Items)
                hash = (hash * 31) + item.GetHashCode();
            return hash;
        }
    }
}

/// <summary>A diagnostic captured during model extraction, in an equatable, tree-free form.</summary>
internal sealed record DiagnosticInfo(string DescriptorId, LocationInfo Location, EquatableArray<string> Args)
{
    public Diagnostic ToDiagnostic() => Diagnostic.Create(
        DescriptorById(DescriptorId),
        Location.ToLocation(),
        Args.Items.Cast<object?>().ToArray());

    private static DiagnosticDescriptor DescriptorById(string id) => id switch
    {
        "MELANGE0001" => Diagnostics.NoPrimaryKey,
        "MELANGE0002" => Diagnostics.AutoIncNotInteger,
        "MELANGE0003" => Diagnostics.UniqueOnPartitionedTable,
        "MELANGE0004" => Diagnostics.UnserializableReducerParameter,
        "MELANGE0007" => Diagnostics.ServerOnlyOnPrivateTable,
        "MELANGE0008" => Diagnostics.AsyncReducer,
        "MELANGE0009" => Diagnostics.InvalidReducerSignature,
        "MELANGE0011" => Diagnostics.UnsupportedColumnType,
        "MELANGE0012" => Diagnostics.KeyColumnNotEncodable,
        "MELANGE0016" => Diagnostics.ScheduleAtColumnMisplaced,
        "MELANGE0018" => Diagnostics.ShardByIsPrimaryKey,
        "MELANGE0022" => Diagnostics.ScheduledTablePlacement,
        "MELANGE0024" => Diagnostics.ArchiveTablePublic,
        "MELANGE0025" => Diagnostics.UniqueOnArchiveTable,
        "MELANGE0026" => Diagnostics.ArchiveTableDeclaration,
        "MELANGE0027" => Diagnostics.KeepKeysOnNonArchiveTable,
        "MELANGE0028" => Diagnostics.InvalidProducerSignature,
        "MELANGE0029" => Diagnostics.UnserializableProducerResult,
        _ => throw new InvalidOperationException($"Unknown diagnostic id {id}."),
    };
}

/// <summary>A source location in an equatable, tree-free form.</summary>
internal readonly record struct LocationInfo(string FilePath, TextSpan Span, LinePositionSpan LineSpan)
{
    public static LocationInfo From(Location location) =>
        new(location.SourceTree?.FilePath ?? string.Empty, location.SourceSpan, location.GetLineSpan().Span);

    public Location ToLocation() =>
        FilePath.Length == 0 ? Location.None : Location.Create(FilePath, Span, LineSpan);
}

/// <summary>The wire kind of a column or argument; names match MelangeDB.Core.ColumnKind.</summary>
internal enum WireKind
{
    None = 0,
    Bool,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    String,
    Bytes,
    Identity,
    Timestamp,
    ScheduleAt,
}

/// <summary>One member of a client-visible enum; the value rides as invariant decimal text.</summary>
internal sealed record EnumMemberModel(string Name, string Value);

/// <summary>
/// An enum referenced by a column or reducer parameter, captured whole so the schema manifest can
/// carry the definition to clients that never see the server compilation.
/// </summary>
internal sealed record EnumModel(
    string Name,
    string Fqn,
    WireKind Underlying,
    EquatableArray<EnumMemberModel> Members);

internal sealed record ColumnModel(
    string Name,
    WireKind Kind,
    string ClrFqn,
    bool IsEnum,
    bool IsPrimaryKey,
    bool IsAutoInc,
    bool IsUnique,
    bool IsIndexed,
    bool IsServerOnly,
    bool IsProperty)
{
    public bool IsKeyEncodable => Kind is not (WireKind.Float32 or WireKind.Float64 or WireKind.ScheduleAt);

    public bool HasIndexAccessor => IsUnique || IsIndexed;
}

internal sealed record TableModel(
    string TypeFqn,
    string TypeName,
    string SafeName,
    string TableName,
    LocationInfo Location,
    bool IsPublic,
    string Tier,
    string Residency,
    string Placement,
    string? ShardBy,
    string? Scheduled,
    EquatableArray<ColumnModel> Columns,
    EquatableArray<DiagnosticInfo> Diagnostics,
    EquatableArray<EnumModel> Enums = default,
    bool KeepKeys = false)
{
    public bool IsValid => Diagnostics.Items.All(d => d.DescriptorId is "MELANGE0003" or "MELANGE0007");
}

internal sealed record ParameterModel(
    string Name,
    WireKind Kind,
    string ClrFqn,
    bool IsEnum,
    string EnumUnderlyingFqn,
    bool IsArray,
    WireKind ElementKind,
    string ElementClrFqn,
    bool ElementIsEnum,
    string ElementEnumUnderlyingFqn,
    bool IsTimerRow = false,
    string? DefaultLiteral = null,
    string? DefaultJson = null)
{
    /// <summary>Whether the parameter declares a default value, which a caller may then leave off (producers only).</summary>
    public bool HasDefault => DefaultLiteral is not null;
}

internal sealed record ReducerModel(
    string ContainingTypeFqn,
    string MethodName,
    string ReducerName,
    string Kind,
    string? PolicyFqn,
    LocationInfo Location,
    EquatableArray<ParameterModel> Parameters,
    EquatableArray<DiagnosticInfo> Diagnostics,
    string DeclaredSite = "Auto",
    EquatableArray<string> TouchedTables = default,
    bool OpaqueBody = false,
    EquatableArray<EnumModel> Enums = default,
    string Isolation = "Serialized")
{
    public bool IsValid => Diagnostics.Length == 0;

    /// <summary>Whether any parameter is a <c>[Table]</c> struct — the scheduled-reducer shape.</summary>
    public bool HasTimerRowParameter => Parameters.Items.Any(static p => p.IsTimerRow);

    /// <summary>
    /// Resolves the reducer's execution site against the compilation's tables. Lifecycle reducers
    /// are hub-executed (a session start is a hub-attachment event); an explicit
    /// <c>[Reducer(Site = ...)]</c> wins; otherwise the body's table touches decide — only Global
    /// and Replicated touches means hub, and a body the analysis cannot see through (it passes
    /// <c>ctx</c> or <c>ctx.Db</c> to a helper, or names a table this compilation does not know)
    /// resolves to the shard, where a Global read fails loudly with the fix in the message.
    /// </summary>
    public string ResolveSite(IReadOnlyDictionary<string, string> placementByTypeName) =>
        Kind is "ClientConnected" or "ClientDisconnected"
            ? "Hub"
            : Sites.Resolve(DeclaredSite, OpaqueBody, TouchedTables, placementByTypeName);
}

/// <summary>Execution-site resolution shared by reducers and producers.</summary>
internal static class Sites
{
    /// <summary>
    /// An explicit <c>Site</c> wins; otherwise only Global and Replicated touches means the hub,
    /// and an opaque body or any other touch means the shard.
    /// </summary>
    public static string Resolve(
        string declaredSite,
        bool opaqueBody,
        EquatableArray<string> touchedTables,
        IReadOnlyDictionary<string, string> placementByTypeName)
    {
        if (declaredSite is "Hub" or "Shard")
            return declaredSite;
        if (opaqueBody)
            return "Shard";
        foreach (var table in touchedTables.Items)
        {
            if (!placementByTypeName.TryGetValue(table, out var placement)
                || placement is not ("Global" or "Replicated"))
            {
                return "Shard";
            }
        }

        return "Hub";
    }
}

/// <summary>
/// One member of a producer result type, in declaration order — the order it rides on the wire.
/// <paramref name="TypeRef"/> is a <see cref="ResultTypeRefs"/> reference.
/// </summary>
internal sealed record ResultFieldModel(string Name, string TypeRef);

/// <summary>
/// A record, class, or struct a producer returns (or that one it returns contains), encoded as an
/// array of its public fields and properties. <paramref name="Name"/> is the simple name the client
/// declares it under.
/// </summary>
internal sealed record ResultTypeModel(
    string Name,
    string Fqn,
    bool IsValueType,
    EquatableArray<ResultFieldModel> Fields)
{
    /// <summary>The generated encoder method's name: unique per CLR type, not per simple name.</summary>
    public string EncoderName => "Write_" + ResultTypeRefs.Safe(Fqn);
}

/// <summary>
/// The compact, equatable form a producer result's type travels in through the generator:
/// <c>s:Kind</c> a scalar (a <see cref="WireKind"/> name), <c>e:Kind:Fqn</c> an enum with its
/// underlying kind, <c>r:Fqn</c> a result type, <c>l:inner</c> a list or array, <c>n:inner</c> a
/// nullable value type.
/// </summary>
internal static class ResultTypeRefs
{
    public static string Scalar(WireKind kind) => "s:" + kind;

    public static string Enum(WireKind underlying, string fqn) => $"e:{underlying}:{fqn}";

    public static string Record(string fqn) => "r:" + fqn;

    public static string List(string inner) => "l:" + inner;

    public static string Nullable(string inner) => "n:" + inner;

    public static string Safe(string fqn)
    {
        var builder = new System.Text.StringBuilder(fqn.Length);
        foreach (var c in fqn.Replace("global::", string.Empty))
            builder.Append(char.IsLetterOrDigit(c) ? c : '_');
        return builder.ToString();
    }
}

internal sealed record ProducerModel(
    string ContainingTypeFqn,
    string MethodName,
    string ProducerName,
    string? PolicyFqn,
    LocationInfo Location,
    EquatableArray<ParameterModel> Parameters,
    string ResultRef,
    string ResultFqn,
    EquatableArray<ResultTypeModel> ResultTypes,
    EquatableArray<EnumModel> Enums,
    EquatableArray<DiagnosticInfo> Diagnostics,
    string DeclaredSite,
    EquatableArray<string> TouchedTables,
    bool OpaqueBody)
{
    public bool IsValid => Diagnostics.Length == 0;

    /// <summary>How many leading parameters a caller must supply: those before the first default.</summary>
    public int RequiredParameters
    {
        get
        {
            var required = 0;
            foreach (var parameter in Parameters.Items)
            {
                if (parameter.HasDefault)
                    break;
                required++;
            }

            return required;
        }
    }

    public string ResolveSite(IReadOnlyDictionary<string, string> placementByTypeName) =>
        Sites.Resolve(DeclaredSite, OpaqueBody, TouchedTables, placementByTypeName);
}
