namespace MelangeDB.Core.Tests;

[Table(Public = true, Residency = Residency.Resident)]
public partial struct Player
{
    [PrimaryKey]
    public Identity Id;

    [Index]
    public int RoomId;

    public float X;

    public float Y;

    public string Name;
}

[Table]
public partial struct InventoryItem
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public Identity Owner;

    public string ItemName;

    public int Quantity;
}

[Table(Tier = StorageTier.Relational, Placement = Placement.Global)]
public partial struct Registration
{
    [PrimaryKey]
    [AutoInc]
    public long Id;

    [Unique]
    public string Email;

    public Timestamp CreatedAt;
}

public enum ChunkKind
{
    Empty,
    Rock,
    Ore,
}

[Table]
public partial struct TerrainChunk
{
    [PrimaryKey]
    public long ChunkId;

    public byte[] Data;

    public ChunkKind Kind;
}

/// <summary>Timer rows: implicitly private, implicitly Local, exactly one ScheduleAt column.</summary>
[Table(Scheduled = nameof(DecayReducers.Decay))]
public partial struct DecayTimer
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;

    public string Target;
}

public sealed class DecayReducers
{
    [Reducer]
    public void Decay(ReducerContext ctx, DecayTimer timer)
    {
    }
}
