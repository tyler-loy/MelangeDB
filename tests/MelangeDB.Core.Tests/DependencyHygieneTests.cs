using System.Text.Json;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The core path must have no dependency on ASP.NET Core, FASTER, Npgsql, or any OpenTelemetry
/// package — including System.Diagnostics.DiagnosticSource, since ActivitySource and Meter are in
/// the net10.0 framework. Easy to violate accidentally, painful to walk back, hence a test.
/// </summary>
public class DependencyHygieneTests
{
    private static readonly string[] ForbiddenPrefixes =
    [
        "OpenTelemetry",
        "Npgsql",
        "FASTER",
        "Microsoft.FASTER",
        "Microsoft.AspNetCore",
        "System.Diagnostics.DiagnosticSource",
    ];

    [Theory]
    [InlineData("MelangeDB.Core")]
    [InlineData("MelangeDB.Abstractions")]
    public void Resolved_package_graph_contains_no_forbidden_dependency(string project)
    {
        var assetsPath = Path.Combine(RepoRoot(), "src", project, "obj", "project.assets.json");
        Assert.True(File.Exists(assetsPath), $"Missing {assetsPath}; restore the solution first.");

        using var assets = JsonDocument.Parse(File.ReadAllText(assetsPath));
        var resolved = new List<string>();
        foreach (var target in assets.RootElement.GetProperty("targets").EnumerateObject())
        {
            foreach (var library in target.Value.EnumerateObject())
                resolved.Add(library.Name.Split('/')[0]);
        }

        // Empty is legal (Abstractions is dependency-free); a forbidden package is not.
        var offending = resolved
            .Where(name => ForbiddenPrefixes.Any(p => name.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
            .ToList();
        Assert.Empty(offending);
    }

    [Fact]
    public void Core_assembly_references_no_forbidden_assembly()
    {
        // System.Diagnostics.DiagnosticSource appears here as a *framework* assembly — that is the
        // whole point: ActivitySource and Meter come from net10.0, not from a package. The package
        // ban is asserted by the assets-graph test above.
        var references = typeof(MelangeEngine).Assembly.GetReferencedAssemblies()
            .Concat(typeof(Identity).Assembly.GetReferencedAssemblies())
            .Select(a => a.Name ?? string.Empty)
            .ToList();
        Assert.NotEmpty(references);
        var offending = references
            .Where(name => ForbiddenPrefixes
                .Where(p => p != "System.Diagnostics.DiagnosticSource")
                .Any(p => name.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
            .ToList();
        Assert.Empty(offending);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "MelangeDB.slnx")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        return directory.FullName;
    }
}
