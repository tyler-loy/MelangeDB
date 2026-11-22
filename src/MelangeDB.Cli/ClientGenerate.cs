using MelangeDB.CodeGen;

namespace MelangeDB.Cli;

/// <summary>
/// <c>melange generate</c>: client bindings from a <c>melange-schema.json</c>, outside any compiler.
/// It runs the same manifest parser and the same emitters the Roslyn generator runs, so
/// <c>--lang cs</c> writes byte for byte what an MSBuild project gets from the analyzer — for a
/// consumer that cannot run one (a Unity or Godot project, a build that checks generated code in).
/// </summary>
internal static class ClientGenerate
{
    /// <summary>The languages <c>--lang</c> accepts.</summary>
    public static readonly IReadOnlyList<string> Languages = ["cs"];

    /// <summary>The files a manifest generates in <paramref name="language"/>; throws <see cref="ManifestException"/> on a bad manifest.</summary>
    public static IReadOnlyList<GeneratedFile> Generate(string manifestJson, string language)
    {
        var schema = ManifestParser.Parse(manifestJson);
        return language switch
        {
            "cs" => ClientBindings.CSharp(schema),
            _ => throw new ArgumentException($"Unknown language '{language}'; expected one of: {string.Join(", ", Languages)}.", nameof(language)),
        };
    }

    /// <summary>
    /// Writes the generated files into <paramref name="directory"/>, creating it. Files are written
    /// whole and only when their content changed, so a build watching the directory does not churn.
    /// </summary>
    public static IReadOnlyList<string> WriteTo(IReadOnlyList<GeneratedFile> files, string directory)
    {
        Directory.CreateDirectory(directory);
        var written = new List<string>();
        foreach (var file in files)
        {
            var path = Path.Combine(directory, file.Name);
            if (File.Exists(path) && File.ReadAllText(path) == file.Content)
                continue;
            File.WriteAllText(path, file.Content, new System.Text.UTF8Encoding(false));
            written.Add(path);
        }

        return written;
    }
}
