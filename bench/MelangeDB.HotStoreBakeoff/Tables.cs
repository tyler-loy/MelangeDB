using System.Reflection;
using MelangeDB.Core;

namespace MelangeDB.HotStoreBakeoff;

// Rows the bake-off drives through the store. Declared here so the source generator emits their
// codecs: the store decodes rows to maintain secondary indexes and to split out-of-line payloads,
// and the generated codec is the one the server runs.
#pragma warning disable CS0649

/// <summary>A game entity: the update-heavy shape (positions change every tick), with an indexed chunk.</summary>
[Table]
internal struct Entity
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public int ChunkId;

    public float X;
    public float Y;
    public float Z;
    public int Health;
    public string Name;
}

/// <summary>A terrain chunk: one large bytes column, which the store keeps out of line (the blob space).</summary>
[Table]
internal struct Terrain
{
    [PrimaryKey]
    public ulong Id;

    public byte[] Data;
}

#pragma warning restore CS0649

internal static class BakeoffSchema
{
    public static SchemaRegistry Load()
    {
        var attribute = typeof(BakeoffSchema).Assembly.GetCustomAttribute<MelangeGeneratedModelAttribute>()
            ?? throw new InvalidOperationException("No generated model: the CodeGen analyzer reference is missing.");
        return new SchemaRegistry(((IMelangeModel)Activator.CreateInstance(attribute.ModelType)!).Tables());
    }
}
