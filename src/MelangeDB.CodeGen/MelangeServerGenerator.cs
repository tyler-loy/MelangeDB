using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MelangeDB.CodeGen;

/// <summary>
/// The server-side incremental generator: discovers <c>[Table]</c> structs and <c>[Reducer]</c>
/// methods and emits schema registration, per-table codecs, typed accessors, and the
/// argument-decoding reducer dispatcher — plus the structural MELANGE diagnostics. Client-side
/// binding generation is deliberately a separate future generator; see docs/plan-phase-02.md.
/// </summary>
[Generator]
public sealed class MelangeServerGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var tables = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                "MelangeDB.TableAttribute",
                static (node, _) => node is StructDeclarationSyntax,
                static (ctx, _) => ModelExtractor.ExtractTable(ctx))
            .Where(static table => table is not null)
            .Select(static (table, _) => table!);

        var reducers = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                "MelangeDB.ReducerAttribute",
                static (node, _) => node is MethodDeclarationSyntax,
                static (ctx, _) => ModelExtractor.ExtractReducer(ctx))
            .Where(static reducer => reducer is not null)
            .Select(static (reducer, _) => reducer!);

        context.RegisterSourceOutput(tables, static (production, table) =>
        {
            foreach (var diagnostic in table.Diagnostics.Items)
                production.ReportDiagnostic(diagnostic.ToDiagnostic());
            if (table.IsValid)
                production.AddSource($"{table.SafeName}.Table.g.cs", Emitter.EmitTable(table));
        });

        context.RegisterSourceOutput(
            tables.Collect().Combine(reducers.Collect()),
            static (production, source) => EmitModel(production, source.Left, source.Right));
    }

    private static void EmitModel(
        SourceProductionContext production,
        ImmutableArray<TableModel> tables,
        ImmutableArray<ReducerModel> reducers)
    {
        foreach (var reducer in reducers)
        {
            foreach (var diagnostic in reducer.Diagnostics.Items)
                production.ReportDiagnostic(diagnostic.ToDiagnostic());
        }

        var validTables = tables
            .Where(static t => t.IsValid)
            .OrderBy(static t => t.TableName, StringComparer.Ordinal)
            .ToArray();
        validTables = ReportNameCollisions(production, validTables);
        var validReducers = reducers
            .Where(static r => r.IsValid)
            .OrderBy(static r => r.ReducerName, StringComparer.Ordinal)
            .ThenBy(static r => r.ContainingTypeFqn, StringComparer.Ordinal)
            .ToArray();
        if (validTables.Length == 0 && validReducers.Length == 0)
            return;
        production.AddSource("MelangeModel.g.cs", Emitter.EmitModel(validTables, validReducers));
    }

    /// <summary>
    /// Reports MELANGE0013 for tables colliding on table name (the TableId axis) or struct name
    /// (the generated-type-name axis), and drops the colliding tables from the emitted model —
    /// turning a confusing runtime registration failure into a compile-time error.
    /// </summary>
    private static TableModel[] ReportNameCollisions(SourceProductionContext production, TableModel[] tables)
    {
        var colliding = new HashSet<TableModel>();
        foreach (var group in tables.GroupBy(static t => t.TableName, StringComparer.Ordinal))
        {
            if (group.Count() > 1)
                colliding.UnionWith(group);
        }

        foreach (var group in tables.GroupBy(static t => t.TypeName, StringComparer.Ordinal))
        {
            if (group.Count() > 1)
                colliding.UnionWith(group);
        }

        foreach (var table in colliding)
        {
            production.ReportDiagnostic(Diagnostic.Create(
                Diagnostics.DuplicateTableName,
                table.Location.ToLocation(),
                table.TableName,
                table.TypeFqn));
        }

        return colliding.Count == 0 ? tables : tables.Where(t => !colliding.Contains(t)).ToArray();
    }
}
