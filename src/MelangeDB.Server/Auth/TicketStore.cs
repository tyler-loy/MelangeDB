using System.Collections.Concurrent;
using System.Security.Cryptography;

namespace MelangeDB.Server;

/// <summary>
/// Mints and redeems connect tickets: single-use, short-lived (<c>Auth:TicketTtlSeconds</c>)
/// stand-ins for a JWT already validated over HTTP, presented on the socket URL because the
/// browser WebSocket API cannot set headers. A ticket carries the validated identity and the
/// underlying token's expiry, so a ticket-authenticated session runs the same re-auth clock as a
/// header-authenticated one. Single-use and short-lived means a leaked ticket is near-worthless.
/// </summary>
internal sealed class TicketStore(TimeProvider time, Func<int> ttlSeconds)
{
    private readonly ConcurrentDictionary<string, Entry> _tickets = new(StringComparer.Ordinal);

    /// <summary>Mints a single-use ticket for an authenticated caller; returns it and its lifetime.</summary>
    public (string Ticket, int ExpiresInSeconds) Mint(AuthResult session)
    {
        Prune();
        var ttl = ttlSeconds();
        var ticket = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));
        _tickets[ticket] = new Entry(session, time.GetUtcNow().AddSeconds(ttl));
        return (ticket, ttl);
    }

    /// <summary>
    /// Redeems a ticket; each redeems at most once, and never after expiry. The removal is the
    /// single-use guarantee — a replayed ticket finds nothing.
    /// </summary>
    public bool TryRedeem(string ticket, out AuthResult session)
    {
        if (_tickets.TryRemove(ticket, out var entry) && entry.TicketExpiresAt >= time.GetUtcNow())
        {
            session = entry.Session;
            return true;
        }

        session = null!;
        return false;
    }

    private void Prune()
    {
        var now = time.GetUtcNow();
        foreach (var (ticket, entry) in _tickets)
        {
            if (entry.TicketExpiresAt < now)
                _tickets.TryRemove(ticket, out _);
        }
    }

    private readonly record struct Entry(AuthResult Session, DateTimeOffset TicketExpiresAt);
}
