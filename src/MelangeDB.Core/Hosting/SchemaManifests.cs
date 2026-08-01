using System.Reflection;

namespace MelangeDB.Core;

/// <summary>
/// The client-visible schema manifests discovered from registered module assemblies — the JSON
/// the generator embedded as <c>MelangeDB.Generated.MelangeSchemaManifest</c>. Read once at
/// startup by reflection, like the model discovery it rides along with; nothing on the
/// invocation path touches it. The transport's development schema endpoint serves from here.
/// </summary>
public sealed class SchemaManifests
{
    /// <summary>The well-known generated type holding a module's manifest.</summary>
    public const string HolderTypeName = "MelangeDB.Generated.MelangeSchemaManifest";

    private readonly List<SchemaManifest> _manifests = [];

    /// <summary>Every discovered manifest, in registration order.</summary>
    public IReadOnlyList<SchemaManifest> All => _manifests;

    /// <summary>
    /// The manifest to serve, when the answer is unambiguous: exactly one module registered one.
    /// Null with zero (nothing generated a client-visible surface) or several (this endpoint does
    /// not merge modules; serving one of many silently would be a lie about the schema).
    /// </summary>
    public SchemaManifest? Single => _manifests.Count == 1 ? _manifests[0] : null;

    /// <summary>Reads the generated manifest out of <paramref name="assembly"/>, if it carries one.</summary>
    public void AddFrom(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        if (_manifests.Any(m => m.Assembly == assembly))
            return;
        if (ReadConst(assembly, "Json") is not { } json || ReadConst(assembly, "Hash") is not { } hash)
            return;
        _manifests.Add(new SchemaManifest(assembly, json, hash));
    }

    private static string? ReadConst(Assembly assembly, string field) =>
        assembly.GetType(HolderTypeName, throwOnError: false)
            ?.GetField(field, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
            ?.GetRawConstantValue() as string;
}

/// <summary>One module's exported client-visible schema.</summary>
public sealed record SchemaManifest(Assembly Assembly, string Json, string Hash);
