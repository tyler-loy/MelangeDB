namespace MelangeDB.Core;

/// <summary>
/// Decodes a producer call's arguments and starts the body on a DI-resolved instance. Generated as
/// a plain (not async) lambda: the arguments are read synchronously from the ref-struct reader, and
/// only the body's own awaitable crosses the await.
/// </summary>
public delegate ValueTask<object?> ProducerInvoker(object instance, ProducerContext context, ReadOnlyMemory<byte> arguments, ValidationOptions limits);

/// <summary>Encodes a producer's result value; see <see cref="ProducerResultWriter"/>.</summary>
public delegate void ProducerResultEncoder(ProducerResultWriter writer, object? value);

/// <summary>
/// One generated producer registration: its name, the DI-resolved class it lives on, and the
/// generated decode, invoke, and encode delegates. Constructed by generated code only.
/// </summary>
public sealed class ProducerDescriptor
{
    public ProducerDescriptor(
        string name,
        Type producerClass,
        ReducerArgsValidator validate,
        ProducerInvoker invoke,
        ProducerResultEncoder encode,
        Type? policy = null,
        ReducerSite site = ReducerSite.Shard)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(producerClass);
        ArgumentNullException.ThrowIfNull(validate);
        ArgumentNullException.ThrowIfNull(invoke);
        ArgumentNullException.ThrowIfNull(encode);
        Name = name;
        ProducerClass = producerClass;
        Validate = validate;
        Invoke = invoke;
        Encode = encode;
        Policy = policy;
        ExecutionSite = site == ReducerSite.Auto ? ReducerSite.Shard : site;
    }

    /// <summary>The producer's public name, unique across producers and reducers.</summary>
    public string Name { get; }

    /// <summary>The class declaring the producer method, resolved from the per-call DI scope.</summary>
    public Type ProducerClass { get; }

    public ReducerArgsValidator Validate { get; }

    public ProducerInvoker Invoke { get; }

    public ProducerResultEncoder Encode { get; }

    /// <summary>The <c>IReducerPolicy</c> from <c>[Producer(Policy = ...)]</c>; null means unpoliced.</summary>
    public Type? Policy { get; }

    /// <summary>Where the producer runs in a cluster, resolved by the reducer rules; never <see cref="ReducerSite.Auto"/> here.</summary>
    public ReducerSite ExecutionSite { get; }
}

/// <summary>The producers a host knows, keyed by name.</summary>
public sealed class ProducerRegistry
{
    private readonly Dictionary<string, ProducerDescriptor> _byName;

    public ProducerRegistry(IEnumerable<ProducerDescriptor> descriptors)
    {
        ArgumentNullException.ThrowIfNull(descriptors);
        _byName = new Dictionary<string, ProducerDescriptor>(StringComparer.Ordinal);
        foreach (var descriptor in descriptors)
        {
            if (!_byName.TryAdd(descriptor.Name, descriptor))
                throw new ArgumentException($"Two producers are named '{descriptor.Name}'. Producer names must be unique.", nameof(descriptors));
        }
    }

    /// <summary>All registered producers, ordered by name.</summary>
    public IReadOnlyList<ProducerDescriptor> Producers =>
        _byName.Values.OrderBy(static d => d.Name, StringComparer.Ordinal).ToArray();

    public bool TryGet(string name, out ProducerDescriptor descriptor) =>
        _byName.TryGetValue(name, out descriptor!);
}
