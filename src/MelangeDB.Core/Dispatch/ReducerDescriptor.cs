namespace MelangeDB.Core;

/// <summary>Validates a call's encoded arguments by decoding and discarding them — the pre-transaction pass.</summary>
public delegate void ReducerArgsValidator(ref ReducerArgsReader reader);

/// <summary>Decodes a call's arguments and invokes the reducer body on a DI-resolved instance.</summary>
public delegate void ReducerBodyInvoker(object instance, ReducerContext context, ref ReducerArgsReader reader);

/// <summary>
/// One generated reducer registration: the name the dispatcher keys on, the DI-resolved class the
/// body lives on, and the generated decode delegates. Constructed by generated code only — there
/// is no reflection fallback for reducers.
/// </summary>
public sealed class ReducerDescriptor
{
    public ReducerDescriptor(
        string name,
        ReducerKind kind,
        Type reducerClass,
        ReducerArgsValidator validate,
        ReducerBodyInvoker invoke,
        Type? policy = null,
        ReducerSite site = ReducerSite.Shard)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(reducerClass);
        ArgumentNullException.ThrowIfNull(validate);
        ArgumentNullException.ThrowIfNull(invoke);
        Name = name;
        Kind = kind;
        ReducerClass = reducerClass;
        Validate = validate;
        Invoke = invoke;
        Policy = policy;
        ExecutionSite = site == ReducerSite.Auto ? ReducerSite.Shard : site;
    }

    /// <summary>The reducer's public name; the dispatcher's key.</summary>
    public string Name { get; }

    public ReducerKind Kind { get; }

    /// <summary>The class declaring the reducer method, resolved from the per-call DI scope.</summary>
    public Type ReducerClass { get; }

    public ReducerArgsValidator Validate { get; }

    public ReducerBodyInvoker Invoke { get; }

    /// <summary>
    /// The <c>IReducerPolicy</c> type from <c>[Reducer(Policy = ...)]</c>, resolved from the
    /// call's DI scope for client-originated calls; null means unpoliced (see
    /// <c>Policies:DefaultReducerPosture</c>).
    /// </summary>
    public Type? Policy { get; }

    /// <summary>
    /// Where the reducer executes in a cluster: <see cref="ReducerSite.Hub"/> when the body
    /// touches only Global and Replicated tables, else <see cref="ReducerSite.Shard"/> — resolved
    /// at compile time (or declared via <c>[Reducer(Site = ...)]</c>), never
    /// <see cref="ReducerSite.Auto"/> here. Single-node deployments ignore it.
    /// </summary>
    public ReducerSite ExecutionSite { get; }
}

/// <summary>Encodes reducer arguments into the wire form the generated dispatcher decodes.</summary>
public static class ReducerArguments
{
    public static byte[] Encode(params object?[] arguments) => ArgsCodec.Encode(arguments);
}
