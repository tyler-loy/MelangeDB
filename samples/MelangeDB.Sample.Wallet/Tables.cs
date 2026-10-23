namespace MelangeDB.Sample.Wallet;

/// <summary>The balance reducers read and update. One row per owner, hot — a reducer never sums history.</summary>
[Table(Placement = Placement.Global)]
public partial struct Wallet
{
    [PrimaryKey] public Identity Owner;

    public long Balance;
}

/// <summary>
/// The recent ledger: hot, so reducers can read it. The primary key is the caller's idempotency
/// key, so a redelivered credit or refund finds its own earlier entry instead of paying twice.
/// </summary>
[Table(Placement = Placement.Global)]
public partial struct WalletEntry
{
    [PrimaryKey] public string Key;

    [Index] public Identity Owner;

    [Index] public Timestamp At;

    public long Delta;

    public string Reason;
}

/// <summary>
/// The archive: entries older than the hot window, in Postgres only. Same columns, carried across
/// the move unchanged. <c>KeepKeys</c> keeps just the keys in the hot store, which is what lets the
/// idempotency check see an entry after it has left the hot window.
/// </summary>
[Table(Tier = StorageTier.Archive, Placement = Placement.Global, KeepKeys = true)]
public partial struct WalletEntryArchive
{
    [PrimaryKey] public string Key;

    [Index] public Identity Owner;

    [Index] public Timestamp At;

    public long Delta;

    public string Reason;
}

/// <summary>The archive sweep's timer: one repeating row, seeded at init.</summary>
[Table(Scheduled = nameof(WalletReducers.ArchiveOldEntries))]
public partial struct WalletArchiveTick
{
    [PrimaryKey, AutoInc] public ulong Id;

    public ScheduleAt ScheduledAt;
}
