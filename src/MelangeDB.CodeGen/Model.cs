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
    EquatableArray<EnumModel> Enums = default)
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
    bool IsTimerRow = false);

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
    EquatableArray<EnumModel> Enums = default)
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
    public string ResolveSite(IReadOnlyDictionary<string, string> placementByTypeName)
    {
        if (Kind is "ClientConnected" or "ClientDisconnected")
            return "Hub";
        if (DeclaredSite is "Hub" or "Shard")
            return DeclaredSite;
        if (OpaqueBody)
            return "Shard";
        foreach (var table in TouchedTables.Items)
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
