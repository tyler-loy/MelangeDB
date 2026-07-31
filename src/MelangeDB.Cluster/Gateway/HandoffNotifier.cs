using System.Collections.Concurrent;

namespace MelangeDB.Cluster;

/// <summary>
/// What a gateway connection does at each stage of its player's handoff. All callbacks are
/// invoked synchronously by the saga at precisely ordered moments — most importantly,
/// <see cref="OnDestinationAuthoritative"/> fires after the destination's import is durable and
/// <em>before</em> the origin's release is requested, so an implementation that mutes the origin
/// inside the callback is guaranteed the release's row deletions never reach the client.
/// Implementations must return quickly and never throw meaningfully; heavy work goes async.
/// </summary>
internal interface IPlayerHandoffObserver
{
    /// <summary>The player entered the border band toward <paramref name="to"/>; pre-open a session.</summary>
    void OnApproach(ShardKey from, ShardKey to);

    /// <summary>A transfer saga started; origin rows are about to freeze.</summary>
    void OnStarted(ShardKey from, ShardKey to);

    /// <summary>The destination owns the player now (import durable, release not yet sent).</summary>
    void OnDestinationAuthoritative(ShardKey from, ShardKey to);

    /// <summary>The saga closed: true means transferred, false means the player stayed on the origin.</summary>
    void OnClosed(ShardKey from, ShardKey to, bool success);
}

/// <summary>
/// The hub's registry of gateway connections interested in a player's handoffs. A saga notifies
/// every observer registered under the player's identity; connections register at handshake and
/// deregister at teardown. Observer exceptions are swallowed — a broken client connection must
/// never fail a transfer.
/// </summary>
internal sealed class HandoffNotifier
{
    private readonly ConcurrentDictionary<Identity, List<IPlayerHandoffObserver>> _observers = new();

    public IDisposable Register(Identity player, IPlayerHandoffObserver observer)
    {
        var list = _observers.GetOrAdd(player, static _ => []);
        lock (list)
        {
            list.Add(observer);
        }

        return new Registration(this, player, observer);
    }

    public void NotifyApproach(Identity player, ShardKey from, ShardKey to) =>
        Notify(player, observer => observer.OnApproach(from, to));

    public void NotifyStarted(Identity player, ShardKey from, ShardKey to) =>
        Notify(player, observer => observer.OnStarted(from, to));

    public void NotifyDestinationAuthoritative(Identity player, ShardKey from, ShardKey to) =>
        Notify(player, observer => observer.OnDestinationAuthoritative(from, to));

    public void NotifyClosed(Identity player, ShardKey from, ShardKey to, bool success) =>
        Notify(player, observer => observer.OnClosed(from, to, success));

    private void Notify(Identity player, Action<IPlayerHandoffObserver> action)
    {
        if (!_observers.TryGetValue(player, out var list))
            return;
        IPlayerHandoffObserver[] snapshot;
        lock (list)
        {
            snapshot = [.. list];
        }

        foreach (var observer in snapshot)
        {
            try
            {
                action(observer);
            }
            catch (Exception)
            {
                // A broken client connection must never fail a transfer.
            }
        }
    }

    private sealed class Registration(HandoffNotifier notifier, Identity player, IPlayerHandoffObserver observer) : IDisposable
    {
        public void Dispose()
        {
            if (!notifier._observers.TryGetValue(player, out var list))
                return;
            lock (list)
            {
                list.Remove(observer);
            }
        }
    }
}
