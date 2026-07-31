namespace MelangeDB.Storage.Postgres.Tests;

public enum AccountKind : short
{
    Free = 1,
    Paid = 2,
}

/// <summary>The WorldStat shape: written by gameplay, read by admin aggregates.</summary>
[Table(Tier = StorageTier.Relational)]
public partial struct Stat
{
    [PrimaryKey]
    [AutoInc]
    public long Id;

    [Index]
    public string Metric;

    public long Value;

    public Timestamp At;
}

/// <summary>One column of every mappable kind, so the type map is exercised end to end.</summary>
[Table(Tier = StorageTier.Relational, Placement = Placement.Global)]
public partial struct Account
{
    [PrimaryKey]
    [AutoInc]
    public long Id;

    [Unique]
    public string Email;

    public Identity Owner;

    public Timestamp CreatedAt;

    public AccountKind Kind;

    public byte[] Avatar;

    public double Balance;

    public float Ratio;

    public bool Active;

    public ulong Flags;
}

/// <summary>Hot-tier: proves mixed transactions and that the applier skips hot ops.</summary>
[Table]
public partial struct HotCounter
{
    [PrimaryKey]
    public long Id;

    public long Count;
}
