using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace MelangeDB.CodeGen;

/// <summary>
/// The client-side incremental generator: consumes a <c>melange-schema.json</c> AdditionalFile —
/// never the network, never server code — and emits typed bindings into <c>MelangeDB.Types</c>:
/// row structs and enums, per-table codecs and cache handles, subscription helpers for the
/// supported query shapes, reducer stubs, and the connection wrapper carrying the schema hash.
/// Lives in the same package as <see cref="MelangeServerGenerator"/>; the triggers are orthogonal
/// (attributed source vs. a manifest file), so neither fires in the other's world.
/// </summary>
[Generator]
public sealed class MelangeClientGenerator : IIncrementalGenerator
{
    /// <summary>The AdditionalFile name that triggers binding generation.</summary>
    public const string ManifestFileName = "melange-schema.json";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var manifests = context.AdditionalTextsProvider
            .Where(static text => System.IO.Path.GetFileName(text.Path).Equals(ManifestFileName, StringComparison.OrdinalIgnoreCase))
            .Select(static (text, cancellationToken) => (text.Path, Content: text.GetText(cancellationToken)?.ToString()))
            .Collect();

        context.RegisterSourceOutput(manifests, static (production, texts) => Emit(production, texts));
    }

    private static void Emit(SourceProductionContext production, ImmutableArray<(string Path, string? Content)> manifests)
    {
        if (manifests.Length == 0)
            return;
        if (manifests.Length > 1)
        {
            production.ReportDiagnostic(Diagnostic.Create(
                Diagnostics.MultipleManifests,
                Location.None,
                string.Join(", ", manifests.Select(static m => m.Path))));
            return;
        }

        var (path, content) = manifests[0];
        if (string.IsNullOrWhiteSpace(content))
        {
            production.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidManifest, Location.None, path, "the file is empty or unreadable"));
            return;
        }

        ClientSchemaModel schema;
        try
        {
            schema = ManifestParser.Parse(content!);
        }
        catch (ManifestException exception)
        {
            production.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidManifest, Location.None, path, exception.Message));
            return;
        }

        foreach (var table in schema.Tables.Items)
            production.AddSource($"{table.TypeName}.Client.g.cs", ClientEmitter.EmitTable(table));
        production.AddSource("MelangeClientModel.g.cs", ClientEmitter.EmitModel(schema));
    }
}
