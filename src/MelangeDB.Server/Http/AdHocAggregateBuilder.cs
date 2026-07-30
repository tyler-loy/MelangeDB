using MelangeDB.Core;
using MelangeDB.Protocol;

namespace MelangeDB.Server;

/// <summary>
/// Validates a parsed aggregate query against the schema and builds the executor's form. All
/// identifier resolution happens here, against <see cref="TableSchema"/> — the executor quotes
/// what this produced and parameterizes every operand, so no client text ever reaches Postgres.
/// Aggregates are relational-tier only and owner-mode only; the endpoint enforces the mode, this
/// class enforces the tier and the columns.
/// </summary>
internal static class AdHocAggregateBuilder
{
    public static RelationalAggregateQuery Build(AggregateQuery parsed, SchemaRegistry registry)
    {
        if (!registry.TryGetByName(parsed.Table, out var schema))
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.UnknownTable, $"No table named '{parsed.Table}' is registered.");
        }

        if (schema.Tier != StorageTier.Relational)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.NotRelationalTier,
                $"Table '{schema.Name}' is hot-tier; aggregates run against the relational tier only. " +
                "The four row shapes remain available for hot tables.");
        }

        var groupBy = parsed.GroupBy.Select(item => Resolve(schema, item)).ToList();
        var selections = new List<RelationalSelection>(parsed.Items.Count);
        foreach (var item in parsed.Items)
        {
            var selection = Resolve(schema, item);
            if (selection.Kind != RelationalSelectionKind.Aggregate && !groupBy.Contains(selection))
            {
                throw new SubscriptionRejectedException(
                    MelangeErrorCodes.InvalidAggregate,
                    $"Selected {(selection.Kind == RelationalSelectionKind.Bucket ? "bucket over" : "column")} " +
                    $"'{selection.Column!.Name}' must also appear in GROUP BY.");
            }

            selections.Add(selection);
        }

        return new RelationalAggregateQuery
        {
            Table = schema,
            Selections = selections,
            GroupBy = groupBy,
            Predicate = BuildPredicate(schema, parsed),
        };
    }

    private static RelationalSelection Resolve(TableSchema schema, AggregateItem item)
    {
        var column = item.Column is null ? null : Require(schema, item.Column);
        switch (item.Kind)
        {
            case RelationalSelectionKind.Column:
                return new RelationalSelection
                {
                    Kind = RelationalSelectionKind.Column,
                    OutputName = column!.Name,
                    Column = column,
                };
            case RelationalSelectionKind.Bucket:
                if (column!.Kind != ColumnKind.Timestamp)
                {
                    throw new SubscriptionRejectedException(
                        MelangeErrorCodes.InvalidAggregate,
                        $"DATE_TRUNC needs a Timestamp column; '{column.Name}' is {column.Kind}.");
                }

                return new RelationalSelection
                {
                    Kind = RelationalSelectionKind.Bucket,
                    OutputName = column.Name,
                    Column = column,
                    Bucket = item.Bucket,
                };
            default:
                ValidateAggregateArgument(item.Function!.Value, column);
                return new RelationalSelection
                {
                    Kind = RelationalSelectionKind.Aggregate,
                    OutputName = column is null
                        ? item.Function.Value.ToString().ToLowerInvariant()
                        : $"{item.Function.Value.ToString().ToLowerInvariant()}_{column.Name}",
                    Column = column,
                    Function = item.Function,
                };
        }
    }

    private static void ValidateAggregateArgument(AggregateFunction function, ColumnSchema? column)
    {
        if (column is null)
        {
            if (function != AggregateFunction.Count)
                throw new SubscriptionRejectedException(MelangeErrorCodes.InvalidAggregate, $"{function} requires a column.");
            return;
        }

        var valid = function switch
        {
            AggregateFunction.Count => true,
            AggregateFunction.Sum or AggregateFunction.Avg => IsNumeric(column.Kind),
            _ => IsNumeric(column.Kind) || column.Kind is ColumnKind.Timestamp or ColumnKind.String,
        };
        if (!valid)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.InvalidAggregate,
                $"{function.ToString().ToUpperInvariant()} cannot aggregate column '{column.Name}' of kind {column.Kind}.");
        }
    }

    private static bool IsNumeric(ColumnKind kind) => kind is
        ColumnKind.Int8 or ColumnKind.UInt8 or ColumnKind.Int16 or ColumnKind.UInt16 or
        ColumnKind.Int32 or ColumnKind.UInt32 or ColumnKind.Int64 or ColumnKind.UInt64 or
        ColumnKind.Float32 or ColumnKind.Float64;

    private static RelationalPredicate? BuildPredicate(TableSchema schema, AggregateQuery parsed)
    {
        if (parsed.Predicate == PredicateKind.None)
            return null;
        var column = Require(schema, parsed.Column!);
        if (column.Kind == ColumnKind.ScheduleAt)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.InvalidAggregate, $"Column '{column.Name}' of kind ScheduleAt cannot carry a predicate.");
        }

        try
        {
            return parsed.Predicate == PredicateKind.Equality
                ? new RelationalPredicate
                {
                    Column = column,
                    EqualsValue = RequireOperand(column, RowSerializer.CoerceValue(schema, column, parsed.EqualsValue)),
                }
                : new RelationalPredicate
                {
                    Column = column,
                    RangeLow = RequireOperand(column, RowSerializer.CoerceValue(schema, column, parsed.RangeLow)),
                    RangeHigh = RequireOperand(column, RowSerializer.CoerceValue(schema, column, parsed.RangeHigh)),
                };
        }
        catch (ArgumentException exception)
        {
            throw new SubscriptionRejectedException(MelangeErrorCodes.InvalidArguments, exception.Message);
        }
    }

    private static object RequireOperand(ColumnSchema column, object? value)
    {
        return value ?? throw new SubscriptionRejectedException(
            MelangeErrorCodes.InvalidArguments, $"Predicate operand for column '{column.Name}' cannot be null.");
    }

    private static ColumnSchema Require(TableSchema schema, string name)
    {
        var column = schema.Columns.FirstOrDefault(c => c.Name == name)
            ?? throw new SubscriptionRejectedException(
                MelangeErrorCodes.UnknownColumn, $"Table '{schema.Name}' has no column '{name}'.");
        if (column.IsServerOnly)
        {
            throw new SubscriptionRejectedException(
                MelangeErrorCodes.ServerOnlyColumn,
                $"Column '{column.Name}' is [ServerOnly] and never leaves the process; an explicit request for it is an error.");
        }

        return column;
    }
}
