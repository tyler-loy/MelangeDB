using System.Collections.Concurrent;

namespace MelangeDB.Server;

/// <summary>
/// The moderation surface: terminate every live session for an identity, and keep a revocation
/// set that new connections and <c>Reauthenticate</c> are checked against — effective immediately,
/// no restart. Register it as a singleton (<c>services.AddSingleton&lt;MelangeSessions&gt;()</c>)
/// and <c>MapMelangeSocket</c> picks it up; without a registration the transport still enforces
/// its own instance, it is just unreachable from application code.
/// <para>
/// Revocation here is session-level and in-memory: it answers "the ban must take effect
/// <em>now</em>, not in 55 minutes when the token expires." The durable ban belongs at the IdP —
/// stop issuing the subject tokens — which is where identity decisions live.
/// </para>
/// </summary>
public sealed class MelangeSessions
{
    private readonly ConcurrentDictionary<Identity, byte> _revoked = new();
    private readonly List<Func<Identity, int>> _terminators = [];

    /// <summary>Whether an identity is currently revoked.</summary>
    public bool IsRevoked(Identity identity) => _revoked.ContainsKey(identity);

    /// <summary>
    /// Revokes an identity: terminates its live sessions, refuses its new connections, and fails
    /// its re-authentication until <see cref="Reinstate"/>. Returns the sessions closed.
    /// </summary>
    public int Revoke(Identity identity)
    {
        _revoked[identity] = 0;
        return Terminate(identity);
    }

    /// <summary>Clears a revocation. Live sessions are not restored; the client reconnects.</summary>
    public void Reinstate(Identity identity) => _revoked.TryRemove(identity, out _);

    /// <summary>Terminates the identity's live sessions without revoking it. Returns the count closed.</summary>
    public int Terminate(Identity identity)
    {
        Func<Identity, int>[] terminators;
        lock (_terminators)
        {
            terminators = [.. _terminators];
        }

        var closed = 0;
        foreach (var terminator in terminators)
            closed += terminator(identity);
        return closed;
    }

    internal IDisposable Attach(Func<Identity, int> terminator)
    {
        lock (_terminators)
        {
            _terminators.Add(terminator);
        }

        return new Detach(this, terminator);
    }

    private sealed class Detach(MelangeSessions owner, Func<Identity, int> terminator) : IDisposable
    {
        public void Dispose()
        {
            lock (owner._terminators)
            {
                owner._terminators.Remove(terminator);
            }
        }
    }
}
