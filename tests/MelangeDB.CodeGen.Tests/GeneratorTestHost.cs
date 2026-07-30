using System.Collections.Immutable;
using MelangeDB.CodeGen;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace MelangeDB.CodeGen.Tests;

/// <summary>Runs the generator and analyzer over in-memory compilations.</summary>
internal static class GeneratorTestHost
{
    public sealed record RunResult(
        Compilation Output,
        ImmutableArray<Diagnostic> GeneratorDiagnostics,
        IReadOnlyList<(string HintName, string Source)> GeneratedSources)
    {
        /// <summary>Diagnostics with a MELANGE id, generator and compilation combined.</summary>
        public IReadOnlyList<Diagnostic> MelangeDiagnostics =>
            GeneratorDiagnostics.Concat(Output.GetDiagnostics())
                .Where(d => d.Id.StartsWith("MELANGE", StringComparison.Ordinal))
                .ToList();

        /// <summary>Compilation errors of the output compilation, generated sources included.</summary>
        public IReadOnlyList<Diagnostic> Errors =>
            Output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
    }

    public static RunResult RunGenerator(string source)
    {
        var compilation = Compile(source);
        var driver = CSharpGeneratorDriver
            .Create(new MelangeServerGenerator())
            .WithUpdatedParseOptions(ParseOptions)
            .RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        var run = driver.GetRunResult();
        var sources = run.Results
            .SelectMany(r => r.GeneratedSources)
            .Select(s => (s.HintName, s.SourceText.ToString()))
            .ToList();
        return new RunResult(output, diagnostics, sources);
    }

    public static async Task<ImmutableArray<Diagnostic>> RunAnalyzerAsync(string source)
    {
        var compilation = Compile(source);
        var withAnalyzers = compilation.WithAnalyzers([new ReducerBodyAnalyzer()]);
        return await withAnalyzers.GetAnalyzerDiagnosticsAsync(CancellationToken.None);
    }

    /// <summary>
    /// Runs the scan analyzer over the compilation <em>after</em> the generator, since the typed
    /// accessors it analyzes are themselves generated.
    /// </summary>
    public static async Task<ImmutableArray<Diagnostic>> RunScanAnalyzerAsync(string source)
    {
        var compilation = Compile(source);
        CSharpGeneratorDriver
            .Create(new MelangeServerGenerator())
            .WithUpdatedParseOptions(ParseOptions)
            .RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
        var withAnalyzers = output.WithAnalyzers([new TableScanAnalyzer()]);
        return await withAnalyzers.GetAnalyzerDiagnosticsAsync(CancellationToken.None);
    }

    private static CSharpParseOptions ParseOptions => new(LanguageVersion.Preview);

    private static CSharpCompilation Compile(string source) =>
        CSharpCompilation.Create(
            "MelangeGeneratorTests",
            [CSharpSyntaxTree.ParseText(source, ParseOptions)],
            References(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

    private static IReadOnlyList<MetadataReference> References()
    {
        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator))
            paths.Add(path);
        paths.Add(typeof(MelangeDB.TableAttribute).Assembly.Location);
        paths.Add(typeof(MelangeDB.Core.MelangeEngine).Assembly.Location);
        return paths.Select(MetadataReference (p) => MetadataReference.CreateFromFile(p)).ToList();
    }
}
