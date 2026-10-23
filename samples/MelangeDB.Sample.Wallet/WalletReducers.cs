using Microsoft.Extensions.Options;

namespace MelangeDB.Sample.Wallet;

/// <summary>How long an entry stays in the hot ledger before the sweep archives it.</summary>
public sealed class WalletOptions
{
    public int HotWindowDays { get; set; } = 30;
}

public sealed class WalletReducers(IOptionsMonitor<WalletOptions> options)
{
    /// <summary>Entries moved per sweep: one bounded write set, so a backlog drains over several ticks.</summary>
    private const int SweepBatch = 1_000;

    [Reducer(ReducerKind.Init)]
    public void Seed(ReducerContext ctx) =>
        ctx.Db.WalletArchiveTick.Insert(new WalletArchiveTick { ScheduledAt = ScheduleAt.Interval(TimeSpan.FromMinutes(1)) });

    /// <summary>
    /// Credits (positive) or debits (negative) the caller's wallet. Idempotent on <paramref name="key"/>:
    /// a key already in the hot ledger or already archived has been applied, so a replay pays nothing.
    /// </summary>
    [Reducer]
    public void Apply(ReducerContext ctx, string key, long delta, string reason)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new RejectedException("An entry needs an idempotency key.");
        if (ctx.Db.WalletEntry.Key.Find(key) is not null || ctx.Db.WalletEntryArchive.Key.Contains(key))
            return;

        var wallet = ctx.Db.Wallet.Owner.Find(ctx.Caller) ?? new Wallet { Owner = ctx.Caller };
        if (wallet.Balance + delta < 0)
            throw new RejectedException($"Insufficient funds: balance {wallet.Balance}, debit {-delta}.");

        if (ctx.Db.Wallet.Owner.Find(ctx.Caller) is null)
            ctx.Db.Wallet.Insert(wallet with { Balance = delta });
        else
            ctx.Db.Wallet.Update(wallet with { Balance = wallet.Balance + delta });
        ctx.Db.WalletEntry.Insert(new WalletEntry { Key = key, Owner = ctx.Caller, At = ctx.Timestamp, Delta = delta, Reason = reason });
    }

    /// <summary>
    /// The sweep: entries older than the hot window move to the archive. Delete and insert are one
    /// write set, so each move is atomic; the range is an index scan on <c>At</c>, never a table scan.
    /// </summary>
    [Reducer]
    public void ArchiveOldEntries(ReducerContext ctx, WalletArchiveTick timer)
    {
        var window = TimeSpan.FromDays(options.CurrentValue.HotWindowDays);
        var cutoff = new Timestamp(ctx.Timestamp.UnixTimeMicroseconds - (long)window.TotalMicroseconds);
        foreach (var entry in ctx.Db.WalletEntry.At.Filter(new Timestamp(0), cutoff).Take(SweepBatch).ToList())
        {
            ctx.Db.WalletEntry.Key.Delete(entry.Key);
            ctx.Db.WalletEntryArchive.Insert(new WalletEntryArchive
            {
                Key = entry.Key,
                Owner = entry.Owner,
                At = entry.At,
                Delta = entry.Delta,
                Reason = entry.Reason,
            });
        }
    }
}
