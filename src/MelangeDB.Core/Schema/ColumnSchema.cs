namespace MelangeDB.Core;

/// <summary>
/// One column of a table: its declared CLR type, wire kind, constraints, and boxed accessors.
/// Built by reflection in phase 01; phase 02's generator constructs these directly.
/// </summary>
public sealed class ColumnSchema
{
    public required string Name { get; init; }

    /// <summary>The declared CLR type, which for enum columns is the enum type itself.</summary>
    public required Type ClrType { get; init; }

    /// <summary>The wire kind; for enum columns, the kind of the underlying integer type.</summary>
    public required ColumnKind Kind { get; init; }

    public bool IsEnum { get; init; }

    public bool IsPrimaryKey { get; init; }

    public bool IsAutoInc { get; init; }

    public bool IsUnique { get; init; }

    public bool IsIndexed { get; init; }

    /// <summary>
    /// <c>[ServerOnly]</c>: the column never leaves the process — excluded from every frame for
    /// every client, admin included, and an explicit request for it is an error.
    /// </summary>
    public bool IsServerOnly { get; init; }

    /// <summary>Reads this column from a boxed row.</summary>
    public required Func<object, object?> GetValue { get; init; }

    /// <summary>Writes this column on a boxed row.</summary>
    public required Action<object, object?> SetValue { get; init; }
}
