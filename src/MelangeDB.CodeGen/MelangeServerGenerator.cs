using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MelangeDB.CodeGen;

/// <summary>
/// The server-side incremental generator: discovers <c>[Table]</c> structs and <c>[Reducer]</c>
/// methods and emits schema registration, per-table codecs, typed accessors, and the
/// argument-decoding reducer dispatcher — plus the structural MELANGE diagnostics. Client-side
/// binding generation is deliberately a separate future generator; see docs/road-to-0.1/plan-phase-02.md.
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

        var moduleName = context.CompilationProvider.Select(static (compilation, _) => compilation.AssemblyName ?? "module");

        context.RegisterSourceOutput(
            tables.Collect().Combine(reducers.Collect()).Combine(moduleName),
            static (production, source) => EmitModel(production, source.Left.Left, source.Left.Right, source.Right));
    }

    private static void EmitModel(
        SourceProductionContext production,
        ImmutableArray<TableModel> tables,
        ImmutableArray<ReducerModel> reducers,
        string moduleName)
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
        validReducers = ValidateScheduling(production, reducers, validTables, validReducers);
        if (validTables.Length == 0 && validReducers.Length == 0)
            return;
        production.AddSource("MelangeModel.g.cs", Emitter.EmitModel(validTables, validReducers));
        EmitManifest(production, validTables, validReducers, moduleName);
    }

    /// <summary>
    /// Emits the client-visible schema manifest when the module has a client-visible surface.
    /// Enum name collisions abort the manifest with MELANGE0019 rather than shipping a contract
    /// whose enum names are ambiguous — the manifest keys enums by the simple name the client
    /// bindings will declare.
    /// </summary>
    private static void EmitManifest(
        SourceProductionContext production,
        TableModel[] validTables,
        ReducerModel[] validReducers,
        string moduleName)
    {
        if (!ManifestEmitter.HasClientSurface(validTables, validReducers))
            return;

        var collisions = ManifestEmitter.EnumNameCollisions(validTables, validReducers);
        if (collisions.Length > 0)
        {
            foreach (var collision in collisions)
                production.ReportDiagnostic(Diagnostic.Create(Diagnostics.AmbiguousClientEnumName, Location.None, collision));
            return;
        }

        var (json, hash) = ManifestEmitter.Build(moduleName, validTables, validReducers);
        production.AddSource("MelangeSchemaManifest.g.cs", ManifestEmitter.EmitHolder(json, hash));
    }

    /// <summary>
    /// The checks that need tables and reducers side by side: a <c>Scheduled</c> table must name
    /// a reducer that exists (MELANGE0014) with the timer-row signature
    /// <c>void R(ReducerContext, TTimer)</c> (MELANGE0015), and a timer-row parameter is only
    /// valid on the reducer its table schedules. Reducers failing the shape are dropped from the
    /// emitted model, turning a runtime dispatch failure into a compile error.
    /// </summary>
    private static ReducerModel[] ValidateScheduling(
        SourceProductionContext production,
        ImmutableArray<ReducerModel> allReducers,
        TableModel[] validTables,
        ReducerModel[] validReducers)
    {
        var invalid = new HashSet<ReducerModel>();
        var scheduledTables = validTables.Where(static t => t.Scheduled is not null).ToArray();
        foreach (var table in scheduledTables)
        {
            if (allReducers.All(r => r.ReducerName != table.Scheduled))
            {
                production.ReportDiagnostic(Diagnostic.Create(
                    Diagnostics.ScheduledReducerMissing,
                    table.Location.ToLocation(),
                    table.TableName,
                    table.Scheduled));
                continue;
            }

            // A reducer that exists but already carries its own diagnostics reports those instead.
            var reducer = validReducers.FirstOrDefault(r => r.ReducerName == table.Scheduled);
            if (reducer is null)
                continue;
            if (reducer.Kind != "Standard"
                || reducer.Parameters.Length != 1
                || !reducer.Parameters[0].IsTimerRow
                || reducer.Parameters[0].ClrFqn != table.TypeFqn)
            {
                production.ReportDiagnostic(Diagnostic.Create(
                    Diagnostics.ScheduledReducerSignature,
                    reducer.Location.ToLocation(),
                    reducer.ReducerName,
                    $"is scheduled by table '{table.TableName}' and must be declared void {reducer.ReducerName}(ReducerContext ctx, {table.TypeName} timer)"));
                invalid.Add(reducer);
            }
        }

        foreach (var reducer in validReducers)
        {
            if (!reducer.HasTimerRowParameter || invalid.Contains(reducer))
                continue;
            var wellFormed = reducer.Kind == "Standard"
                && reducer.Parameters.Length == 1
                && reducer.Parameters[0].IsTimerRow
                && scheduledTables.Any(t => t.Scheduled == reducer.ReducerName && t.TypeFqn == reducer.Parameters[0].ClrFqn);
            if (!wellFormed)
            {
                production.ReportDiagnostic(Diagnostic.Create(
                    Diagnostics.ScheduledReducerSignature,
                    reducer.Location.ToLocation(),
                    reducer.ReducerName,
                    "takes a [Table] struct parameter, which is only valid as the single timer-row parameter of the reducer that table's Scheduled declaration names"));
                invalid.Add(reducer);
            }
        }

        return invalid.Count == 0 ? validReducers : validReducers.Where(r => !invalid.Contains(r)).ToArray();
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
