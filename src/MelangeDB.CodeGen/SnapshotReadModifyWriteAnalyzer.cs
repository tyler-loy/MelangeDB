using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace MelangeDB.CodeGen;

/// <summary>
/// Flags the detectable read-modify-write shape inside a reducer declared
/// <c>Isolation.Snapshot</c>: a row obtained from a generated single-row <c>Find</c>, written back
/// through the table handle's <c>Update</c>. Under snapshot isolation the body reads a view pinned
/// at one LSN, so the write-back silently overwrites whatever committed to that row after the pin —
/// reconcile fixes op shape, never op value (docs/design/snapshot-isolation.md). A warning and
/// never an error: read-modify-write is undecidable in general, and a body that recomputes a row it
/// also read is legitimate. Deliberately narrow — only <c>Find</c> results are tracked, not rows
/// from <c>Iter</c>/<c>Filter</c>/<c>First</c>, because updating rows mid-sweep is exactly what the
/// legitimate recompute sweeps the isolation level exists for do every tick.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SnapshotReadModifyWriteAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [Diagnostics.SnapshotReadModifyWrite];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationBlockAction(AnalyzeBlocks);
    }

    private static void AnalyzeBlocks(OperationBlockAnalysisContext context)
    {
        if (context.OwningSymbol is not IMethodSymbol method || !IsSnapshotReducer(method))
            return;

        foreach (var block in context.OperationBlocks)
        {
            var found = CollectFindLocals(block);
            foreach (var operation in block.DescendantsAndSelf())
            {
                if (operation is IInvocationOperation { TargetMethod: { Name: "Update", Parameters.Length: 1 } target } invocation
                    && IsGeneratedType(target.ContainingType, "Handle")
                    && DerivesFromFind(invocation.Arguments[0].Value, found))
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        Diagnostics.SnapshotReadModifyWrite,
                        invocation.Syntax.GetLocation(),
                        method.Name,
                        TableName(target.Parameters[0].Type)));
                }
            }
        }
    }

    private static bool IsSnapshotReducer(IMethodSymbol method)
    {
        foreach (var attribute in method.GetAttributes())
        {
            if (attribute.AttributeClass is not { Name: "ReducerAttribute", ContainingNamespace.Name: "MelangeDB" })
                continue;
            foreach (var named in attribute.NamedArguments)
            {
                // Isolation.Snapshot is 1; see the Isolation enum in MelangeDB.Abstractions.
                if (named.Key == "Isolation" && named.Value.Value is int isolation && isolation == 1)
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The locals holding a row (or nullable row) that came from a <c>Find</c>, gathered to a
    /// fixpoint so a copy of a copy is still tracked. Taint is never removed on reassignment —
    /// over-approximating keeps this simple, and the cost of the imprecision is a warning on a
    /// local that once held a found row, which is a body worth a second look anyway.
    /// </summary>
    private static HashSet<ILocalSymbol> CollectFindLocals(IOperation block)
    {
        var found = new HashSet<ILocalSymbol>(SymbolEqualityComparer.Default);
        var changed = true;
        while (changed)
        {
            changed = false;
            foreach (var operation in block.DescendantsAndSelf())
            {
                switch (operation)
                {
                    case IVariableDeclaratorOperation { Initializer.Value: { } initializer } declarator
                        when DerivesFromFind(initializer, found):
                        changed |= found.Add(declarator.Symbol);
                        break;
                    case ISimpleAssignmentOperation { Target: ILocalReferenceOperation local, Value: { } value }
                        when DerivesFromFind(value, found):
                        changed |= found.Add(local.Local);
                        break;
                    case IIsPatternOperation { Value: { } value } isPattern
                        when DerivesFromFind(value, found) && DeclaredLocal(isPattern.Pattern) is { } declared:
                        changed |= found.Add(declared);
                        break;
                }
            }
        }

        return found;
    }

    /// <summary>The local a pattern binds the matched row to, if any: <c>is { } player</c>, <c>is Player player</c>.</summary>
    private static ILocalSymbol? DeclaredLocal(IPatternOperation pattern) => pattern switch
    {
        IDeclarationPatternOperation { DeclaredSymbol: ILocalSymbol local } => local,
        IRecursivePatternOperation { DeclaredSymbol: ILocalSymbol local } => local,
        INegatedPatternOperation negated => DeclaredLocal(negated.Pattern),
        _ => null,
    };

    /// <summary>
    /// Whether an expression's value is a row from a <c>Find</c>, seen through the wrappers the
    /// shape is written with in practice: conversions, <c>?? throw</c>, <c>.Value</c> and
    /// <c>GetValueOrDefault()</c> on the nullable result, and <c>row with { … }</c>.
    /// </summary>
    private static bool DerivesFromFind(IOperation operation, HashSet<ILocalSymbol> found)
    {
        while (true)
        {
            switch (operation)
            {
                case IConversionOperation conversion:
                    operation = conversion.Operand;
                    continue;
                case ICoalesceOperation coalesce:
                    operation = coalesce.Value;
                    continue;
                case IWithOperation with:
                    operation = with.Operand;
                    continue;
                case IPropertyReferenceOperation { Property.Name: "Value", Instance: { } instance }
                    when IsNullable(instance.Type):
                    operation = instance;
                    continue;
                case IInvocationOperation { TargetMethod.Name: "GetValueOrDefault", Instance: { } instance }
                    when IsNullable(instance.Type):
                    operation = instance;
                    continue;
                case IInvocationOperation invocation:
                    return invocation.TargetMethod.Name == "Find"
                        && IsGeneratedType(invocation.TargetMethod.ContainingType, "Accessor");
                case ILocalReferenceOperation local:
                    return found.Contains(local.Local);
                default:
                    return false;
            }
        }
    }

    private static bool IsNullable(ITypeSymbol? type) =>
        type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };

    private static bool IsGeneratedType(INamedTypeSymbol? type, string suffix) =>
        type is not null
        && type.Name.EndsWith(suffix, StringComparison.Ordinal)
        && type.ContainingNamespace?.ToDisplayString() == "MelangeDB.Generated";

    /// <summary>The table's declared name — the <c>[Table(Name = …)]</c> override, else the struct name.</summary>
    private static string TableName(ITypeSymbol rowType)
    {
        var tableAttribute = rowType.GetAttributes().FirstOrDefault(static a =>
            a.AttributeClass is { Name: "TableAttribute", ContainingNamespace.Name: "MelangeDB" });
        if (tableAttribute is not null)
        {
            foreach (var named in tableAttribute.NamedArguments)
            {
                if (named.Key == "Name" && named.Value.Value is string explicitName)
                    return explicitName;
            }
        }

        return rowType.Name;
    }
}
