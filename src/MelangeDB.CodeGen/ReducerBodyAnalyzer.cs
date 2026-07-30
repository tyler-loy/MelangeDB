using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace MelangeDB.CodeGen;

/// <summary>
/// Flags non-determinism and I/O inside reducer bodies: ambient clocks (use <c>ctx.Timestamp</c>),
/// ambient randomness (use <c>ctx.Random</c>), and members of known I/O types. The async-reducer
/// rule itself is MELANGE0008, reported by the generator; this analyzer covers what a synchronous
/// body can still do wrong. Deliberately a fixed known-type list in this phase — flagging I/O
/// reached through injected services is a later phase's flow analysis.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ReducerBodyAnalyzer : DiagnosticAnalyzer
{
    private static readonly ImmutableHashSet<string> IoTypes = ImmutableHashSet.Create(
        StringComparer.Ordinal,
        "System.Net.Http.HttpClient",
        "System.IO.File",
        "System.IO.Directory",
        "System.Console");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [Diagnostics.AmbientTimeInReducer, Diagnostics.AmbientRandomInReducer, Diagnostics.IoTypeInReducer];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationBlockStartAction(static blockContext =>
        {
            if (blockContext.OwningSymbol is not IMethodSymbol method || !IsReducer(method))
                return;

            blockContext.RegisterOperationAction(
                operationContext => AnalyzePropertyReference(operationContext, method.Name),
                OperationKind.PropertyReference);
            blockContext.RegisterOperationAction(
                operationContext => AnalyzeObjectCreation(operationContext, method.Name),
                OperationKind.ObjectCreation);
            blockContext.RegisterOperationAction(
                operationContext => AnalyzeInvocation(operationContext, method.Name),
                OperationKind.Invocation);
        });
    }

    private static bool IsReducer(IMethodSymbol method) =>
        method.GetAttributes().Any(static a =>
            a.AttributeClass is { Name: "ReducerAttribute", ContainingNamespace.Name: "MelangeDB" });

    private static void AnalyzePropertyReference(OperationAnalysisContext context, string reducerName)
    {
        var property = ((IPropertyReferenceOperation)context.Operation).Property;
        var containing = property.ContainingType?.ToDisplayString();
        if (containing is "System.DateTime" or "System.DateTimeOffset" && property.Name is "Now" or "UtcNow" or "Today")
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Diagnostics.AmbientTimeInReducer,
                context.Operation.Syntax.GetLocation(),
                reducerName,
                $"{containing}.{property.Name}"));
            return;
        }

        if (containing is "System.Threading.Tasks.Task" || containing?.StartsWith("System.Threading.Tasks.Task<", StringComparison.Ordinal) is true)
        {
            if (property.Name == "Result")
                ReportIo(context, reducerName, $"{containing}.Result");
        }
    }

    private static void AnalyzeObjectCreation(OperationAnalysisContext context, string reducerName)
    {
        var creation = (IObjectCreationOperation)context.Operation;
        if (creation.Type?.ToDisplayString() == "System.Random")
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Diagnostics.AmbientRandomInReducer,
                context.Operation.Syntax.GetLocation(),
                reducerName));
        }
        else if (creation.Type?.ToDisplayString() is { } created && IoTypes.Contains(created))
        {
            ReportIo(context, reducerName, $"new {created}()");
        }
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context, string reducerName)
    {
        var target = ((IInvocationOperation)context.Operation).TargetMethod;
        var containing = target.ContainingType?.OriginalDefinition.ToDisplayString();
        if (containing is null)
            return;
        if (IoTypes.Contains(containing)
            || (containing is "System.Threading.Thread" && target.Name == "Sleep")
            || (containing is "System.Threading.Tasks.Task" or "System.Threading.Tasks.Task<TResult>" && target.Name is "Delay" or "Wait" or "GetAwaiter"))
        {
            ReportIo(context, reducerName, $"{containing}.{target.Name}");
        }
    }

    private static void ReportIo(OperationAnalysisContext context, string reducerName, string member) =>
        context.ReportDiagnostic(Diagnostic.Create(
            Diagnostics.IoTypeInReducer,
            context.Operation.Syntax.GetLocation(),
            reducerName,
            member));
}
