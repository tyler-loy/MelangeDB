using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MelangeDB.Core;

/// <summary>
/// Dispatches reducer calls through the generated registry: decode and validate the arguments
/// first — a rejected call opens no transaction and appends nothing — then resolve the reducer
/// class from a fresh DI scope and invoke the body as one transaction. One call, one scope, so
/// constructor-injected scoped services live exactly as long as the invocation. Client-originated
/// calls (<see cref="CallSource.Client"/>) additionally pass the rate limiter and the reducer's
/// authorization policy, both before any transaction opens.
/// </summary>
public sealed class MelangeReducerHost
{
    private readonly MelangeEngine _engine;
    private readonly ReducerRegistry _registry;
    private readonly IServiceScopeFactory _scopes;
    private readonly IOptionsMonitor<MelangeDbOptions> _options;
    private readonly ReducerRateLimiter _rateLimiter;
    private volatile bool _stopping;

    public MelangeReducerHost(
        MelangeEngine engine,
        ReducerRegistry registry,
        IServiceScopeFactory scopes,
        IOptionsMonitor<MelangeDbOptions> options,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(options);
        _engine = engine;
        _registry = registry;
        _scopes = scopes;
        _options = options;
        _rateLimiter = new ReducerRateLimiter(timeProvider ?? TimeProvider.System);
    }

    /// <summary>The registered reducers.</summary>
    public IReadOnlyList<ReducerDescriptor> Reducers => _registry.Reducers;

    /// <summary>
    /// The unpoliced-reducer report: every client-callable reducer with no authorization policy.
    /// Empty is the goal; <c>Policies:UnpolicedReducerReport</c> decides what a non-empty report
    /// does at startup.
    /// </summary>
    public IReadOnlyList<string> UnpolicedReducers =>
        _registry.Reducers
            .Where(descriptor => descriptor.Kind == ReducerKind.Standard && descriptor.Policy is null)
            .Select(descriptor => descriptor.Name)
            .ToArray();

    /// <summary>Encodes <paramref name="arguments"/> and dispatches — the in-process convenience path.</summary>
    public ulong Call(string reducerName, Identity caller, params object?[] arguments) =>
        Call(reducerName, caller, ConnectionId.None, ArgsCodec.Encode(arguments));

    /// <summary>
    /// Dispatches a reducer call from already-encoded arguments — the path a transport uses.
    /// Arguments are decoded and validated against the declared parameter types before any
    /// transaction opens; a <see cref="ReducerArgumentException"/> means nothing happened.
    /// <paramref name="parentContext"/> carries a caller-propagated trace context
    /// (<c>traceparent</c> on the wire) so the reducer span parents to the client's trace.
    /// <paramref name="source"/> marks client-originated calls, which are rate limited, policy
    /// checked, and refused for non-<see cref="ReducerKind.Standard"/> reducers — each before a
    /// transaction opens.
    /// </summary>
    public ulong Call(
        string reducerName,
        Identity caller,
        ConnectionId connectionId,
        ReadOnlyMemory<byte> encodedArguments,
        System.Diagnostics.ActivityContext parentContext = default,
        CallSource source = default)
    {
        if (_stopping)
            throw new InvalidOperationException("MelangeDB is shutting down; no further reducer calls are accepted.");

        var descriptor = _registry.Get(reducerName);
        var options = _options.CurrentValue;
        if (source.ClientOriginated)
        {
            // Lifecycle and scheduled reducers are not client-callable; answering "unknown"
            // rather than "forbidden" keeps their existence unconfirmed.
            if (descriptor.Kind != ReducerKind.Standard)
                throw new ArgumentException($"No reducer named '{reducerName}' is registered.", nameof(reducerName));

            if (options.RateLimit.Enabled && !_rateLimiter.TryAcquire(caller, descriptor.Name, options.RateLimit))
            {
                _engine.Telemetry?.RecordRateLimited(descriptor.Name);
                throw new RateLimitedException(
                    $"Reducer '{descriptor.Name}' is over its rate limit for this identity (RateLimit:*); the call opened no transaction.");
            }
        }

        var validation = options.Validation;

        // The pre-transaction pass: decode and discard. Anything malformed, non-finite, over-long,
        // or out of range is rejected here, before a scope, a transaction, or a log record exists.
        var validator = new ReducerArgsReader(encodedArguments.Span, validation);
        descriptor.Validate(ref validator);

        using var scope = _scopes.CreateScope();
        if (source.ClientOriginated)
            Authorize(descriptor, caller, source, options.Policies, scope.ServiceProvider);
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

    private void Authorize(
        ReducerDescriptor descriptor,
        Identity caller,
        CallSource source,
        PoliciesOptions policies,
        IServiceProvider services)
    {
        if (descriptor.Policy is null)
        {
            if (policies.DefaultReducerPosture == ReducerPolicyPosture.Deny)
            {
                throw new ReducerDeniedException(
                    $"Reducer '{descriptor.Name}' declares no policy and Policies:DefaultReducerPosture is Deny.");
            }

            return;
        }

        if (services.GetRequiredService(descriptor.Policy) is not IReducerPolicy policy)
        {
            throw new InvalidOperationException(
                $"Reducer '{descriptor.Name}': policy type {descriptor.Policy} does not implement IReducerPolicy.");
        }

        var context = new PolicyContext(caller, source.CallerIsGuest, _engine.CommittedView);
        if (!policy.MayCall(descriptor.Name, context))
            throw new ReducerDeniedException($"Reducer '{descriptor.Name}' denied by {descriptor.Policy.Name}.");
    }
}
