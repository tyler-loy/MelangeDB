using MelangeDB.Core;

namespace MelangeDB.Sample.Wallet;

/// <summary>One ledger line as a player sees it: no owner column, because it is always theirs.</summary>
public sealed record WalletLine(string Key, long Delta, string Reason, Timestamp At, bool Archived);

/// <summary>
/// A page of a player's history. <see cref="Next"/> is the archive cursor for the following page,
/// or null on the last one. <see cref="Stale"/> means the archive part reflects Postgres alone,
/// without the log tail it has not applied yet — passed on rather than hidden.
/// </summary>
public sealed record WalletHistory(List<WalletLine> Lines, string? Next, bool Stale);

/// <summary>Players may read their own history; a guest has none to read.</summary>
public sealed class PlayersOnly : IReducerPolicy
{
    public bool MayCall(string reducerName, PolicyContext context) => !context.IsGuest;
}

/// <summary>
/// How archived history reaches a client: not a table it may read, but a producer that decides what
/// the caller sees — their own entries, and nothing else.
/// </summary>
public sealed class WalletProducers(IArchiveReader archive)
{
    private const int PageSize = 50;

    /// <summary>
    /// The caller's history, newest first. The first page carries the whole hot ledger (bounded by
    /// the hot window) and the first archive page; each later page, keyed by the previous
    /// <see cref="WalletHistory.Next"/>, continues the archive. The archive is read as of
    /// <c>ctx.Lsn</c>, the commit the hot read answers for, so an entry the sweep moves mid-call is
    /// listed exactly once.
    /// </summary>
    [Producer(Policy = typeof(PlayersOnly))]
    public async Task<WalletHistory> History(ProducerContext ctx, string? after = null)
    {
        var lines = new List<WalletLine>();
        if (after is null)
        {
            lines.AddRange(ctx.Db.WalletEntry.Owner.Filter(ctx.Caller)
                .OrderByDescending(static entry => entry.At.UnixTimeMicroseconds)
                .Select(static entry => new WalletLine(entry.Key, entry.Delta, entry.Reason, entry.At, Archived: false)));
        }

        var page = await archive.ReadAsync<WalletEntryArchive>(
            new ArchiveQuery
            {
                Column = nameof(WalletEntryArchive.Owner),
                Equal = ctx.Caller,
                Descending = true,
                After = after,
                Limit = PageSize,
                AsOfLsn = ctx.Lsn,
            },
            ctx.CancellationToken);
        lines.AddRange(page.Rows.Select(static entry => new WalletLine(entry.Key, entry.Delta, entry.Reason, entry.At, Archived: true)));
        return new WalletHistory(lines, (string?)page.Next, page.Stale);
    }
}
