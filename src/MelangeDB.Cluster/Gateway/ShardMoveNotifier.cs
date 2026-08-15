namespace MelangeDB.Cluster;

/// <summary>
/// What a gateway connection does at each stage of a shard's planned drain. The contract mirrors
/// <see cref="IPlayerHandoffObserver"/>, with one structural difference: a drain concerns a
/// <em>shard</em>, so every connection hears every notification and each checks whether the shard
/// is its current attachment. Ordering: <see cref="OnMoveStarted"/> fires before the origin is
/// asked to quiesce — an implementation that mutes its origin attachment inside the callback is
/// guaranteed the origin's death rattle (its transport closing under the quiesce) never reaches
/// the client as a resync error. Implementations must return quickly and never throw
/// meaningfully; heavy work goes async.
/// </summary>
internal interface IShardMoveObserver
{
    /// <summary>The drain started: mute the origin attachment and start queueing the shard's calls.</summary>
    void OnMoveStarted(ShardKey shard);

    /// <summary>The destination owns the shard: reconnect there, re-scope subscriptions, flush the queue.</summary>
    void OnMoved(ShardKey shard);

    /// <summary>The drain failed and the origin keeps the shard: flush the queue back to it.</summary>
    void OnMoveFailed(ShardKey shard);
}

/// <summary>
/// The hub's registry of gateway connections interested in shard moves. Unlike
/// <see cref="HandoffNotifier"/> this is unkeyed — which connections a drain affects is a
/// property of their current attachment, which only they know. Observer exceptions are swallowed:
/// a broken client connection must never fail a drain.
/// </summary>
internal sealed class ShardMoveNotifier
{
    private readonly List<IShardMoveObserver> _observers = [];

    public IDisposable Register(IShardMoveObserver observer)
    {
        lock (_observers)
        {
            _observers.Add(observer);
        }

        return new Registration(this, observer);
    }

    public void NotifyStarted(ShardKey shard) => Notify(observer => observer.OnMoveStarted(shard));

    public void NotifyMoved(ShardKey shard) => Notify(observer => observer.OnMoved(shard));

    public void NotifyFailed(ShardKey shard) => Notify(observer => observer.OnMoveFailed(shard));

    private void Notify(Action<IShardMoveObserver> action)
    {
        IShardMoveObserver[] snapshot;
        lock (_observers)
        {
            snapshot = [.. _observers];
        }

        foreach (var observer in snapshot)
        {
            try
            {
                action(observer);
            }
            catch (Exception)
            {
                // A broken client connection must never fail a drain.
            }
        }
    }

    private sealed class Registration(ShardMoveNotifier notifier, IShardMoveObserver observer) : IDisposable
    {
        public void Dispose()
        {
            lock (notifier._observers)
            {
                notifier._observers.Remove(observer);
            }
        }
    }
}
