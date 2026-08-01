using System.Reflection;

namespace MelangeDB.Cli;

/// <summary>
/// The two ways to obtain a module's schema manifest, one writer: read the generated
/// <c>MelangeDB.Generated.MelangeSchemaManifest</c> constant out of a built assembly, or fetch
/// the same bytes from a running server's <c>/melange/schema</c> endpoint. Both paths yield the
/// generator's verbatim JSON, so exporting from a DLL and exporting from the dev server produce
/// byte-identical files — a property the transport tests hold.
/// </summary>
public static class SchemaExporter
{
    /// <summary>The well-known generated type holding a module's manifest.</summary>
    public const string HolderTypeName = "MelangeDB.Generated.MelangeSchemaManifest";

    /// <summary>The path the development schema endpoint serves under the default transport path.</summary>
    public const string DefaultEndpointPath = "/melange/schema";

    /// <summary>
    /// Reads the manifest constant from a built module assembly. Loads the assembly but resolves
    /// only the generated holder type — a static class with two string constants — so the
    /// module's own dependency graph never needs to load.
    /// </summary>
    public static string ReadFromAssembly(string assemblyPath)
    {
        var fullPath = Path.GetFullPath(assemblyPath);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException($"No assembly at '{fullPath}'.", fullPath);
        return ReadFromAssembly(Assembly.LoadFrom(fullPath));
    }

    /// <summary>Reads the manifest constant from an already-loaded module assembly.</summary>
    public static string ReadFromAssembly(Assembly assembly)
    {
        var holder = assembly.GetType(HolderTypeName, throwOnError: false)
            ?? throw new InvalidOperationException(
                $"Assembly '{assembly.GetName().Name}' carries no generated schema manifest ({HolderTypeName}). " +
                "Reference the MelangeDB.CodeGen analyzer and declare at least one public table or reducer.");
        var field = holder.GetField("Json", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"{HolderTypeName} in '{assembly.GetName().Name}' has no Json constant.");
        return (string)field.GetRawConstantValue()!;
    }

    /// <summary>
    /// Fetches the manifest from a running server. A bare base URL gets the default
    /// <c>/melange/schema</c> path appended; a URL that already names a path is used as given.
    /// </summary>
    public static async Task<string> FetchAsync(Uri url, CancellationToken cancellationToken = default)
    {
        var target = url.AbsolutePath is "" or "/"
            ? new Uri(url, DefaultEndpointPath)
            : url;
        using var http = new HttpClient();
        using var response = await http.GetAsync(target, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"The schema endpoint answered {(int)response.StatusCode} for {target}: {body}. " +
                "The endpoint is on in Development and follows MelangeDb:Transport:SchemaEndpointEnabled elsewhere.");
        }

        return body;
    }
}
