namespace MelangeDB;

/// <summary>
/// What a producer is given: who is asking, and a read of committed state that holds still for the
/// whole call. Unlike a <see cref="ReducerContext"/> there is no random source and no event
/// publishing — a producer commits nothing and is never replayed, so it needs neither.
/// </summary>
public sealed class ProducerContext
{
    public ProducerContext(
        Identity caller,
        ConnectionId connectionId,
        Timestamp timestamp,
        ulong lsn,
        IDbView db,
        CancellationToken cancellationToken,
        CallerClaims? claims = null)
    {
        ArgumentNullException.ThrowIfNull(db);
        Caller = caller;
        ConnectionId = connectionId;
        Timestamp = timestamp;
        Lsn = lsn;
        Db = db;
        CancellationToken = cancellationToken;
        Claims = claims ?? CallerClaims.Empty;
    }

    /// <summary>Who is asking. Stable across reconnects and restarts.</summary>
    public Identity Caller { get; }

    /// <summary>Which socket the call arrived on; <see cref="ConnectionId.None"/> over HTTP or in-process.</summary>
    public ConnectionId ConnectionId { get; }

    /// <summary>When the call started.</summary>
    public Timestamp Timestamp { get; }

    /// <summary>
    /// The commit <see cref="Db"/> answers for. Pass it as an archive query's <c>AsOfLsn</c> so the
    /// archive answers for the same commit, and a row moved from hot to archive mid-call is seen once.
    /// </summary>
    public ulong Lsn { get; }

    /// <summary>
    /// The hot store pinned at <see cref="Lsn"/> for the whole call: every read sees the same
    /// committed state, across awaits and threads, however many commits land meanwhile. Read-only;
    /// archive tables are unreadable through it, as in a reducer — inject <c>IArchiveReader</c>.
    /// The pin is released once the result is encoded, so a lazy query returned inside the result
    /// is still readable; one kept anywhere else is not.
    /// </summary>
    public IDbView Db { get; }

    /// <summary>Fires when the caller disconnects or the call reaches <c>Producers:TimeoutSeconds</c>.</summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>The claims captured from the caller's token (<c>Auth:CaptureClaims</c>); empty when none were.</summary>
    public CallerClaims Claims { get; }
}
