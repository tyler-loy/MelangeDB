namespace MelangeDB.Core;

/// <summary>The wire kind of a column value. Enums serialize as their underlying integer kind.</summary>
public enum ColumnKind : byte
{
    Bool = 1,
    Int8 = 2,
    UInt8 = 3,
    Int16 = 4,
    UInt16 = 5,
    Int32 = 6,
    UInt32 = 7,
    Int64 = 8,
    UInt64 = 9,
    Float32 = 10,
    Float64 = 11,
    String = 12,
    Bytes = 13,
    Identity = 14,
    Timestamp = 15,
    ScheduleAt = 16,
}

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
