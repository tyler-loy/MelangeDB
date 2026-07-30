using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace MelangeDB.CodeGen;

/// <summary>
/// Flags <c>Iter()</c> on a generated table handle when the table is not declared
/// <c>Residency.Resident</c> — the porting tool the residency decision ships with: port against
/// the paged default, compile, and the warnings are the exact list of scans that will become I/O,
/// instead of guesswork. Silent for <c>Resident</c> tables, whose scans are a memory walk by
/// declared contract; <c>Auto</c> tables are flagged, because a scan that is fast only until a
/// size threshold crosses is precisely the cliff the diagnostic exists to surface. Only the typed
/// accessor path is analyzed — <c>IDbView.Scan</c> is the infrastructure seam under it, and
/// framework plumbing scanning through the seam is not application code to be ported.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TableScanAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [Diagnostics.UnindexedScanOnPagedTable];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        var invocation = (IInvocationOperation)context.Operation;
        var method = invocation.TargetMethod;
        if (method.Name != "Iter" || method.Parameters.Length != 0)
            return;
        var containing = method.ContainingType;
        if (containing is null
            || !containing.Name.EndsWith("Handle", StringComparison.Ordinal)
            || containing.ContainingNamespace?.ToDisplayString() != "MelangeDB.Generated")
        {
            return;
        }

        if (method.ReturnType is not INamedTypeSymbol { TypeArguments.Length: 1 } enumerable
            || enumerable.TypeArguments[0] is not INamedTypeSymbol rowType)
        {
            return;
        }

        var tableAttribute = rowType.GetAttributes().FirstOrDefault(static a =>
            a.AttributeClass is { Name: "TableAttribute", ContainingNamespace.Name: "MelangeDB" });
        if (tableAttribute is null)
            return;

        foreach (var named in tableAttribute.NamedArguments)
        {
            // Residency.Resident is 1; see the Residency enum in MelangeDB.Abstractions.
            if (named.Key == "Residency" && named.Value.Value is int residency && residency == 1)
                return;
        }

        var tableName = rowType.Name;
        foreach (var named in tableAttribute.NamedArguments)
        {
            if (named.Key == "Name" && named.Value.Value is string explicitName)
                tableName = explicitName;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            Diagnostics.UnindexedScanOnPagedTable,
            invocation.Syntax.GetLocation(),
            tableName));
    }
}
