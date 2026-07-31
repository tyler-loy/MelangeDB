using System.Text;
using MelangeDB.Core;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MelangeDB.Storage.Postgres;

/// <summary>
/// Executes validated aggregate queries against the relational tier. The SQL is assembled entirely
/// from schema-derived, quoted identifiers and closed keyword sets — <c>DATE_TRUNC</c> units come
/// from an enum, functions from an enum — with every operand a typed parameter, so client text
/// never reaches Postgres. Results reflect the tier at the applier's checkpoint; grouped rows come
/// back ordered by their grouping keys so output is deterministic.
/// </summary>
internal sealed class PostgresQueryExecutor : IRelationalQueryExecutor
{
    private readonly PostgresConnectionSource _connections;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;

    public PostgresQueryExecutor(PostgresConnectionSource connections, IOptionsMonitor<MelangeDbOptions> options)
    {
        _connections = connections;
        _options = options;
    }

    public async Task<RelationalQueryResult> ExecuteAsync(RelationalAggregateQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var schema = _options.CurrentValue.Postgres.Schema;
        var sql = new StringBuilder("SELECT ");
        sql.AppendJoin(", ", query.Selections.Select(s => $"{Expression(s)} AS {PostgresIdentifier.Quote(s.OutputName)}"));
        sql.Append($" FROM {PostgresIdentifier.Qualify(schema, query.Table.Name)}");

        var parameters = new List<NpgsqlParameter>();
        if (query.Predicate is { } predicate)
        {
            if (predicate.EqualsValue is not null)
            {
                parameters.Add(PostgresTypeMap.Parameter(predicate.Column, predicate.EqualsValue));
                sql.Append($" WHERE {PostgresIdentifier.Quote(predicate.Column.Name)} = $1");
            }
            else
            {
                parameters.Add(PostgresTypeMap.Parameter(predicate.Column, predicate.RangeLow));
                parameters.Add(PostgresTypeMap.Parameter(predicate.Column, predicate.RangeHigh));
                sql.Append($" WHERE {PostgresIdentifier.Quote(predicate.Column.Name)} BETWEEN $1 AND $2");
            }
        }

        if (query.GroupBy.Count > 0)
        {
            var groupExpressions = query.GroupBy.Select(Expression).ToList();
            sql.Append(" GROUP BY ").AppendJoin(", ", groupExpressions);
            sql.Append(" ORDER BY ").AppendJoin(", ", groupExpressions);
        }

        await using var connection = await _connections.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql.ToString(), connection);
        foreach (var parameter in parameters)
            command.Parameters.Add(parameter);

        var rows = new List<object?[]>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new object?[query.Selections.Count];
            for (var i = 0; i < row.Length; i++)
            {
                var selection = query.Selections[i];
                var column = selection.Kind == RelationalSelectionKind.Aggregate ? null : selection.Column;
                row[i] = PostgresTypeMap.FromReader(reader.GetValue(i), column);
            }

            rows.Add(row);
        }

        return new RelationalQueryResult(query.Selections.Select(s => s.OutputName).ToList(), rows);
    }

    private static string Expression(RelationalSelection selection) => selection.Kind switch
    {
        RelationalSelectionKind.Column => PostgresIdentifier.Quote(selection.Column!.Name),
        RelationalSelectionKind.Bucket =>
            $"date_trunc('{BucketName(selection.Bucket!.Value)}', {PostgresIdentifier.Quote(selection.Column!.Name)})",
        _ => selection.Function switch
        {
            AggregateFunction.Count when selection.Column is null => "count(*)",
            AggregateFunction.Count => $"count({PostgresIdentifier.Quote(selection.Column!.Name)})",
            AggregateFunction.Sum => $"sum({PostgresIdentifier.Quote(selection.Column!.Name)})",
            AggregateFunction.Avg => $"avg({PostgresIdentifier.Quote(selection.Column!.Name)})",
            AggregateFunction.Min => $"min({PostgresIdentifier.Quote(selection.Column!.Name)})",
            AggregateFunction.Max => $"max({PostgresIdentifier.Quote(selection.Column!.Name)})",
            _ => throw new NotSupportedException($"Unknown aggregate {selection.Function}."),
        },
    };

    /// <summary>The unit literal, from the enum — a closed set, never client text.</summary>
    private static string BucketName(TimeBucketUnit unit) => unit switch
    {
        TimeBucketUnit.Minute => "minute",
        TimeBucketUnit.Hour => "hour",
        TimeBucketUnit.Day => "day",
        TimeBucketUnit.Week => "week",
        TimeBucketUnit.Month => "month",
        TimeBucketUnit.Year => "year",
        _ => throw new NotSupportedException($"Unknown bucket unit {unit}."),
    };
}
