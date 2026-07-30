namespace MelangeDB.Core;

/// <summary>The reducers a host knows, keyed by name — the generated dispatcher's lookup table.</summary>
public sealed class ReducerRegistry
{
    private readonly Dictionary<string, ReducerDescriptor> _byName;

    public ReducerRegistry(IEnumerable<ReducerDescriptor> descriptors)
    {
        ArgumentNullException.ThrowIfNull(descriptors);
        _byName = new Dictionary<string, ReducerDescriptor>(StringComparer.Ordinal);
        foreach (var descriptor in descriptors)
        {
            if (!_byName.TryAdd(descriptor.Name, descriptor))
                throw new ArgumentException($"Two reducers are named '{descriptor.Name}'. Reducer names must be unique.", nameof(descriptors));
        }
    }

    /// <summary>All registered reducers, ordered by name for determinism.</summary>
    public IReadOnlyList<ReducerDescriptor> Reducers =>
        _byName.Values.OrderBy(d => d.Name, StringComparer.Ordinal).ToArray();

    public ReducerDescriptor Get(string name) =>
        _byName.TryGetValue(name, out var descriptor)
            ? descriptor
            : throw new ArgumentException($"No reducer named '{name}' is registered.", nameof(name));
}
