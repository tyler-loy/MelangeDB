namespace MelangeDB.Client;

/// <summary>
/// One connection's typed caches: one merged <see cref="ClientCache{TRow}"/> per table, created
/// on first use and shared by every handle and subscription over that table — which is what makes
/// overlapping and re-scoped subscriptions converge on one row set. The generated connection
/// wrapper owns one of these; the raw <see cref="MelangeClient"/> API is untouched underneath.
/// </summary>
public sealed class ClientCacheRegistry
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, object> _caches = new(StringComparer.Ordinal);

    public ClientCacheRegistry(MelangeClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        Client = client;
    }

    /// <summary>The connection these caches mirror.</summary>
    public MelangeClient Client { get; }

    /// <summary>The merged cache for <paramref name="codec"/>'s table, created on first use.</summary>
    public ClientCache<TRow> GetOrAdd<TRow>(IClientRowCodec<TRow> codec)
        where TRow : struct
    {
        ArgumentNullException.ThrowIfNull(codec);
        lock (_lock)
        {
            if (_caches.TryGetValue(codec.TableName, out var existing))
                return (ClientCache<TRow>)existing;
            var cache = new ClientCache<TRow>(codec, this);
            _caches[codec.TableName] = cache;
            return cache;
        }
    }

    /// <summary>
    /// Subscribes with a typed cache attached from birth: the sink rides the subscription into
    /// <see cref="MelangeClient.SubscribeAsync(string, IReadOnlyDictionary{string, object?}?, CancellationToken)"/>'s
    /// internals, so the initial set, every delta, and every rescope reach the cache with no
    /// window where rows could slip past. The query must select full rows — projected
    /// subscriptions stay on the untyped API by design.
    /// </summary>
    public async Task<TypedSubscription<TRow>> SubscribeAsync<TRow>(
        IClientRowCodec<TRow> codec,
        string query,
        IReadOnlyDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default)
        where TRow : struct
    {
        var cache = GetOrAdd(codec);
        var binding = cache.CreateBinding();
        var subscription = await Client.SubscribeAsync(query, parameters, binding, cancellationToken).ConfigureAwait(false);
        return new TypedSubscription<TRow>(this, subscription, cache, binding);
    }

    /// <summary>
    /// The one seam every typed event passes through, on the receive loop. A frame-tick pump —
    /// the Godot client wants handlers raised on its own thread — replaces this dispatch, and
    /// nothing else, when that lands as its own issue.
    /// </summary>
    internal void DispatchTypedEvent(Action fire) => fire();
}
