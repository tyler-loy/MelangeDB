using System.Collections.Immutable;
using System.Text.RegularExpressions;
using MelangeDB.CodeGen;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;
using Xunit;

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

    /// <summary>
    /// Runs the client generator over manifest AdditionalFiles, the way a consuming client
    /// project triggers it. The compilation references MelangeDB.Client, so generated bindings
    /// compile for real, not just as text.
    /// </summary>
    public static RunResult RunClientGenerator(string manifestJson, string source = "") =>
        RunClientGenerator([("melange-schema.json", manifestJson)], source);

    public static RunResult RunClientGenerator(IReadOnlyList<(string Path, string Content)> manifests, string source = "")
    {
        var compilation = Compile(source);
        var additionalTexts = manifests
            .Select(AdditionalText (m) => new InMemoryAdditionalText(m.Path, m.Content))
            .ToArray();
        var driver = CSharpGeneratorDriver
            .Create([new MelangeClientGenerator().AsSourceGenerator()], additionalTexts, ParseOptions)
            .RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        var run = driver.GetRunResult();
        var sources = run.Results
            .SelectMany(r => r.GeneratedSources)
            .Select(s => (s.HintName, s.SourceText.ToString()))
            .ToList();
        return new RunResult(output, diagnostics, sources);
    }

    /// <summary>
    /// The manifest the server generator embedded for <paramref name="source"/> — the JSON pulled
    /// back out of the generated constant, so client-generator tests consume the real writer's
    /// output rather than a hand-maintained copy.
    /// </summary>
    public static string ExportManifest(string source)
    {
        var run = RunGenerator(source);
        var (_, holder) = Assert.Single(run.GeneratedSources, s => s.HintName == "MelangeSchemaManifest.g.cs");
        const string marker = "public const string Json = @\"";
        var start = holder.IndexOf(marker, StringComparison.Ordinal) + marker.Length;
        var end = holder.LastIndexOf("\";", StringComparison.Ordinal);
        return holder[start..end].Replace("\"\"", "\"");
    }

    /// <summary>
    /// Compares generated output against a checked-in snapshot; on drift a .actual lands next to
    /// the build output for diffing.
    /// </summary>
    public static void AssertSnapshot(RunResult result, string hintName, string expectedFile)
    {
        var (_, actual) = Assert.Single(result.GeneratedSources, s => s.HintName == hintName);
        AssertSnapshotText(actual, expectedFile);
    }

    /// <summary>Compares already-generated text against a checked-in snapshot.</summary>
    public static void AssertSnapshotText(string generated, string expectedFile)
    {
        var actual = Normalize(generated);

        var expectedPath = Path.Combine(AppContext.BaseDirectory, "Snapshots", expectedFile);
        Directory.CreateDirectory(Path.GetDirectoryName(expectedPath)!);
        if (!File.Exists(expectedPath))
        {
            File.WriteAllText(expectedPath + ".actual", actual);
            Assert.Fail($"Missing snapshot {expectedFile}; review and check in the .actual file written next to it.");
        }

        var expected = Normalize(File.ReadAllText(expectedPath));
        if (expected != actual)
            File.WriteAllText(expectedPath + ".actual", actual);
        Assert.Equal(expected, actual);
    }

    private static readonly Regex GeneratorVersion = new(
        "(\"{1,2}generator\"{1,2}:\\s*\"{1,2})[0-9][0-9.]*",
        RegexOptions.Compiled);

    /// <summary>
    /// Normalizes line endings, and pins the emitted generator version to 0.0.0.0.
    /// </summary>
    /// <remarks>
    /// The manifest records which MelangeDB built it — useful in a shipped artifact, useless in a
    /// snapshot. Left alone it makes every release break these two tests on the version bump alone,
    /// and a failure that always means "refresh the file" is how a real regression in generated
    /// output eventually gets waved through. The version is asserted by shape here, not by value;
    /// that it is emitted at all is what these snapshots are for.
    /// </remarks>
    private static string Normalize(string text) =>
        GeneratorVersion.Replace(text.Replace("\r\n", "\n"), "${1}0.0.0.0");

    private sealed class InMemoryAdditionalText(string path, string content) : AdditionalText
    {
        public override string Path { get; } = path;

        public override SourceText GetText(CancellationToken cancellationToken = default) =>
            SourceText.From(content, System.Text.Encoding.UTF8);
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
        paths.Add(typeof(MelangeDB.Protocol.Frame).Assembly.Location);
        paths.Add(typeof(MelangeDB.Client.MelangeClient).Assembly.Location);
        return paths.Select(MetadataReference (p) => MetadataReference.CreateFromFile(p)).ToList();
    }
}
