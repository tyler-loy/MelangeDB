using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// Dispatches reducer calls through the generated registry: decode and validate the arguments
/// first — a rejected call opens no transaction and appends nothing — then resolve the reducer
/// class from a fresh DI scope and invoke the body as one transaction. One call, one scope, so
/// constructor-injected scoped services live exactly as long as the invocation.
/// </summary>
public sealed class MelangeReducerHost
{
    private readonly MelangeEngine _engine;
    private readonly ReducerRegistry _registry;
    private readonly IServiceScopeFactory _scopes;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private volatile bool _stopping;

    public MelangeReducerHost(
        MelangeEngine engine,
        ReducerRegistry registry,
        IServiceScopeFactory scopes,
        IOptionsMonitor<MelangeDbOptions> options)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _registry = registry;
        _scopes = scopes;
        _options = options;
    }

    /// <summary>The registered reducers.</summary>
    public IReadOnlyList<ReducerDescriptor> Reducers => _registry.Reducers;

    /// <summary>Encodes <paramref name="arguments"/> and dispatches — the in-process convenience path.</summary>
    public ulong Call(string reducerName, Identity caller, params object?[] arguments) =>
        Call(reducerName, caller, ConnectionId.None, ArgsCodec.Encode(arguments));

    /// <summary>
    /// Dispatches a reducer call from already-encoded arguments — the path a transport uses.
    /// Arguments are decoded and validated against the declared parameter types before any
    /// transaction opens; a <see cref="ReducerArgumentException"/> means nothing happened.
    /// <paramref name="parentContext"/> carries a caller-propagated trace context
    /// (<c>traceparent</c> on the wire) so the reducer span parents to the client's trace.
    /// </summary>
    public ulong Call(
        string reducerName,
        Identity caller,
        ConnectionId connectionId,
        ReadOnlyMemory<byte> encodedArguments,
        System.Diagnostics.ActivityContext parentContext = default)
    {
        if (_stopping)
            throw new InvalidOperationException("MelangeDB is shutting down; no further reducer calls are accepted.");

        var descriptor = _registry.Get(reducerName);
        var validation = _options.CurrentValue.Validation;

        // The pre-transaction pass: decode and discard. Anything malformed, non-finite, over-long,
        // or out of range is rejected here, before a scope, a transaction, or a log record exists.
        var validator = new ReducerArgsReader(encodedArguments.Span, validation);
        descriptor.Validate(ref validator);

        using var scope = _scopes.CreateScope();
        var instance = scope.ServiceProvider.GetRequiredService(descriptor.ReducerClass);
        return _engine.Invoke(
            descriptor.Name,
            caller,
            encodedArguments,
            context =>
            {
                var reader = new ReducerArgsReader(encodedArguments.Span, validation);
                descriptor.Invoke(instance, context, ref reader);
            },
            connectionId,
            parentContext);
    }

    internal void SignalStopping() => _stopping = true;
}
