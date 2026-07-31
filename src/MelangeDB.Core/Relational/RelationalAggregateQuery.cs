namespace MelangeDB.Core;

/// <summary>The aggregate functions ad-hoc SQL supports.</summary>
public enum AggregateFunction
{
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// <summary>The <c>DATE_TRUNC</c> units ad-hoc SQL supports — a closed set, never client text.</summary>
public enum TimeBucketUnit
{
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

/// <summary>What one output column of an aggregate query is.</summary>
public enum RelationalSelectionKind
{
    /// <summary>A plain column; must also appear in the grouping.</summary>
    Column,

    /// <summary>A <c>DATE_TRUNC</c> bucket over a timestamp column.</summary>
    Bucket,

    /// <summary>An aggregate over the group — <c>COUNT(*)</c> carries no column.</summary>
    Aggregate,
}

/// <summary>
/// One output column of an aggregate query: a grouping column, a time bucket, or an aggregate.
/// Columns are always schema columns validated by the caller — the executor quotes identifiers it
/// was handed from a <see cref="TableSchema"/>, never client text.
/// </summary>
public sealed record RelationalSelection
{
    public required RelationalSelectionKind Kind { get; init; }

    /// <summary>The result column's name: the column name, or a derived aggregate name.</summary>
    public required string OutputName { get; init; }

    /// <summary>The schema column, or null for <c>COUNT(*)</c>.</summary>
    public ColumnSchema? Column { get; init; }

    public AggregateFunction? Function { get; init; }

    public TimeBucketUnit? Bucket { get; init; }
}

/// <summary>The predicate shapes an aggregate query supports — the same two the row shapes have.</summary>
public sealed record RelationalPredicate
{
    public required ColumnSchema Column { get; init; }

    /// <summary>The equality operand, coerced to the column's CLR form; null for a range.</summary>
    public object? EqualsValue { get; init; }

    public object? RangeLow { get; init; }

    public object? RangeHigh { get; init; }
}

/// <summary>
/// A validated one-shot aggregate query over a relational-tier table, ready for the relational
/// executor: selections, grouping, and an optional predicate, every identifier already resolved
/// against the <see cref="TableSchema"/>. Results are ordered by the grouping selections
/// ascending, so output is deterministic without an ORDER BY in the subset.
/// </summary>
public sealed record RelationalAggregateQuery
{
    public required TableSchema Table { get; init; }

    /// <summary>Output columns, in declaration order.</summary>
    public required IReadOnlyList<RelationalSelection> Selections { get; init; }

    /// <summary>
    /// The grouping selections (kind <see cref="RelationalSelectionKind.Column"/> or
    /// <see cref="RelationalSelectionKind.Bucket"/>); empty means one row over the whole table.
    /// </summary>
    public required IReadOnlyList<RelationalSelection> GroupBy { get; init; }

    public RelationalPredicate? Predicate { get; init; }
}

/// <summary>An aggregate query's result: column names and boxed row values, in grouping order.</summary>
public sealed record RelationalQueryResult(IReadOnlyList<string> Columns, IReadOnlyList<object?[]> Rows);

/// <summary>
/// The seam between ad-hoc SQL and the relational tier's engine: executes a validated aggregate
/// query against the tier. Registered by the storage package (<c>AddPostgres</c>); absent when no
/// relational tier is configured, which the SQL endpoint reports as an explicit error rather than
/// an empty result. Results reflect the tier at its applier's checkpoint — the documented,
/// deliberate lag of the two-backend design.
/// </summary>
public interface IRelationalQueryExecutor
{
    /// <summary>Executes one aggregate query and materializes its result.</summary>
    Task<RelationalQueryResult> ExecuteAsync(RelationalAggregateQuery query, CancellationToken cancellationToken = default);
}
