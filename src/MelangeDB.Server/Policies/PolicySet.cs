using System.Runtime.CompilerServices;
using MelangeDB.Core;
using Microsoft.Extensions.DependencyInjection;

namespace MelangeDB.Server;

/// <summary>
/// The DI-resolved row and column policies, bridged from their typed interfaces to the serialized
/// rows the fan-out path holds. Built once at transport start: for each public table the closed
/// <c>IRowPolicy&lt;T&gt;</c> / <c>IColumnPolicy&lt;T&gt;</c> services are resolved from the root
/// provider — policies are singletons in effect and must be thread-safe. Policies registered for
/// a private table are ignored: no policy can make a private table visible.
/// </summary>
internal sealed class PolicySet
{
    private readonly Dictionary<TableId, TablePolicyEvaluator> _byTable = [];

    public PolicySet(IServiceProvider services, SchemaRegistry schema)
    {
        foreach (var table in schema.Tables)
        {
            if (!table.IsPublic)
                continue;
            var rowPolicies = services.GetServices(typeof(IRowPolicy<>).MakeGenericType(table.RowType));
            var columnPolicies = services.GetServices(typeof(IColumnPolicy<>).MakeGenericType(table.RowType));
            var rows = rowPolicies.Where(p => p is not null).Cast<object>().ToArray();
            var columns = columnPolicies.Where(p => p is not null).Cast<object>().ToArray();
            if (rows.Length == 0 && columns.Length == 0)
                continue;
            var evaluator = (TablePolicyEvaluator)Activator.CreateInstance(
                typeof(TablePolicyEvaluator<>).MakeGenericType(table.RowType), table, rows, columns)!;
            _byTable[table.Id] = evaluator;
        }
    }

    /// <summary>The evaluator for a table, or null when it has no policies.</summary>
    public TablePolicyEvaluator? For(TableId table) => _byTable.GetValueOrDefault(table);
}

/// <summary>
/// Evaluates one table's policies against a serialized row. Rows compose as a UNION (any row
/// policy admitting the row is enough); columns compose as an INTERSECTION (every mask must admit
/// a column). Evaluation deserializes the row once and shares it across all policies.
/// </summary>
internal abstract class TablePolicyEvaluator
{
    /// <summary>Whether any row policy exists. With none, <c>Public = true</c> means fully visible.</summary>
    public abstract bool HasRowPolicies { get; }

    /// <summary>Whether any column policy exists — the per-row mask path is skipped entirely otherwise.</summary>
    public abstract bool HasColumnPolicies { get; }

    /// <summary>Union across row policies: whether any policy admits the row to this caller.</summary>
    public abstract bool IsRowVisible(ReadOnlySpan<byte> row, PolicyContext ctx);

    /// <summary>Intersection across column policies: removes every column some mask hides.</summary>
    public abstract void IntersectColumns(ReadOnlySpan<byte> row, PolicyContext ctx, HashSet<string> visible);

    /// <summary>
    /// <see cref="IsRowVisible(ReadOnlySpan{byte}, PolicyContext)"/> over a row decoded once and
    /// shared — the fan-out's form, where the same row is judged for every subscriber in turn.
    /// </summary>
    public abstract bool IsRowVisible(DecodedRow row, PolicyContext ctx);

    /// <summary><see cref="IntersectColumns(ReadOnlySpan{byte}, PolicyContext, HashSet{string})"/> over a shared decode.</summary>
    public abstract void IntersectColumns(DecodedRow row, PolicyContext ctx, HashSet<string> visible);
}

internal sealed class TablePolicyEvaluator<TRow> : TablePolicyEvaluator
    where TRow : struct
{
    private readonly TableSchema _schema;
    private readonly IRowPolicy<TRow>[] _rowPolicies;
    private readonly IColumnPolicy<TRow>[] _columnPolicies;

    public TablePolicyEvaluator(TableSchema schema, object[] rowPolicies, object[] columnPolicies)
    {
        _schema = schema;
        _rowPolicies = [.. rowPolicies.Cast<IRowPolicy<TRow>>()];
        _columnPolicies = [.. columnPolicies.Cast<IColumnPolicy<TRow>>()];
    }

    public override bool HasRowPolicies => _rowPolicies.Length > 0;

    public override bool HasColumnPolicies => _columnPolicies.Length > 0;

    public override bool IsRowVisible(ReadOnlySpan<byte> row, PolicyContext ctx)
    {
        if (_rowPolicies.Length == 0)
            return true;
        var typed = Materialize(row);
        return AnyAdmits(in typed, ctx);
    }

    public override bool IsRowVisible(DecodedRow row, PolicyContext ctx)
    {
        if (_rowPolicies.Length == 0)
            return true;
        return AnyAdmits(in Unsafe.Unbox<TRow>(row.Typed), ctx);
    }

    public override void IntersectColumns(ReadOnlySpan<byte> row, PolicyContext ctx, HashSet<string> visible)
    {
        if (_columnPolicies.Length == 0)
            return;
        var typed = Materialize(row);
        Intersect(in typed, ctx, visible);
    }

    public override void IntersectColumns(DecodedRow row, PolicyContext ctx, HashSet<string> visible)
    {
        if (_columnPolicies.Length == 0)
            return;
        Intersect(in Unsafe.Unbox<TRow>(row.Typed), ctx, visible);
    }

    private bool AnyAdmits(in TRow typed, PolicyContext ctx)
    {
        foreach (var policy in _rowPolicies)
        {
            if (policy.IsVisibleTo(in typed, ctx))
                return true;
        }

        return false;
    }

    private void Intersect(in TRow typed, PolicyContext ctx, HashSet<string> visible)
    {
        foreach (var policy in _columnPolicies)
        {
            var mask = policy.VisibleTo(in typed, ctx);
            visible.RemoveWhere(column => !mask.Admits(column));
        }
    }

    private TRow Materialize(ReadOnlySpan<byte> row) =>
        _schema.Codec is RowCodec<TRow> codec
            ? codec.Deserialize(row)
            : (TRow)RowSerializer.Deserialize(_schema, row.ToArray());
}
