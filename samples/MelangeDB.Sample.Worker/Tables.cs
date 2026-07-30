namespace MelangeDB.Sample;

/// <summary>One greeted visitor. Declared, never registered — the generator does the rest.</summary>
[Table(Public = true)]
public partial struct Visitor
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public string Name;

    public Timestamp VisitedAt;

    /// <summary>Whether the feature flag was on when this visitor was greeted.</summary>
    public bool GreetedExcitedly;
}

/// <summary>A private running total, proving two tables commit atomically from one reducer.</summary>
[Table]
public partial struct GreetingTotal
{
    [PrimaryKey]
    public byte Key;

    public long Count;
}
