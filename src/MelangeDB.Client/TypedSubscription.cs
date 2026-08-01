namespace MelangeDB.Client;

/// <summary>
/// A live subscription feeding a typed table cache. Row access and events live on
/// <see cref="Cache"/> — merged across every subscription over the table — while this object
/// owns the subscription's lifecycle: re-scoping its predicate parameters in place (the
/// terrain-streaming pattern) and unsubscribing, which removes only the rows no other
/// subscription still covers.
/// </summary>
public sealed class TypedSubscription<TRow>
    where TRow : struct
{
    private readonly ClientCacheRegistry _registry;
    private readonly TypedCacheBinding<TRow> _binding;

    internal TypedSubscription(
        ClientCacheRegistry registry,
        MelangeSubscription subscription,
        ClientCache<TRow> cache,
        TypedCacheBinding<TRow> binding)
    {
        _registry = registry;
        _binding = binding;
        Raw = subscription;
        Cache = cache;
    }

    /// <summary>The underlying untyped subscription — query text, anchor LSN, inconsistency counter.</summary>
    public MelangeSubscription Raw { get; }

    /// <summary>The table's merged typed cache this subscription feeds.</summary>
    public ClientCache<TRow> Cache { get; }

    /// <summary>
    /// Re-scopes the predicate parameters in place. The server answers with a precise diff on the
    /// data channel, and the cache reconciles it as inserts, updates, and deletes — never a
    /// flush. Parameter names are the query's <c>:name</c> placeholders; the generated helpers
    /// supply them typed.
    /// </summary>
    public Task RescopeAsync(IReadOnlyDictionary<string, object?> parameters, CancellationToken cancellationToken = default) =>
        _registry.Client.RescopeAsync(Raw, parameters, cancellationToken);

    /// <summary>
    /// Unsubscribes and drops this subscription's coverage: rows it alone held leave the cache
    /// with <c>OnDelete</c>; rows another subscription still covers stay.
    /// </summary>
    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        await _registry.Client.UnsubscribeAsync(Raw, cancellationToken).ConfigureAwait(false);
        _binding.Detach();
    }
}
