namespace MelangeDB.Core;

/// <summary>
/// The per-assembly registration the source generator emits: every <c>[Table]</c> schema with its
/// codec, and every <c>[Reducer]</c> descriptor. <c>AddTablesFrom</c>/<c>AddReducersFrom</c>
/// discover it through <see cref="MelangeGeneratedModelAttribute"/>, which is why a new table or
/// reducer needs no manual registration anywhere.
/// </summary>
public interface IMelangeModel
{
    /// <summary>Builds the assembly's table schemas, generated codecs attached.</summary>
    IReadOnlyList<TableSchema> Tables();

    /// <summary>Builds the assembly's reducer descriptors.</summary>
    IReadOnlyList<ReducerDescriptor> Reducers();
}

/// <summary>
/// Emitted by the source generator to name the assembly's <see cref="IMelangeModel"/>. Read once at
/// startup by <c>AddTablesFrom</c>/<c>AddReducersFrom</c>; nothing on the invocation path touches it.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class MelangeGeneratedModelAttribute : Attribute
{
    public MelangeGeneratedModelAttribute(Type modelType)
    {
        ArgumentNullException.ThrowIfNull(modelType);
        ModelType = modelType;
    }

    /// <summary>The generated model class; instantiable with a parameterless constructor.</summary>
    public Type ModelType { get; }
}
