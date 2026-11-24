namespace MelangeDB.Storage.Faster.Tests;

[Table(Public = true)]
public partial struct Creature
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public int ChunkId;

    public string Name;

    public float X;
}

[Table(Residency = Residency.Resident)]
public partial struct ItemDefinition
{
    [PrimaryKey]
    public int Id;

    public string Name;

    public int Value;
}

[Table(Public = true)]
public partial struct TerrainBlob
{
    [PrimaryKey]
    public long ChunkId;

    [Index]
    public int Region;

    public byte[] Data;
}

[Table(Residency = Residency.Auto)]
public partial struct AutoSized
{
    [PrimaryKey]
    public long Id;

    public byte[] Payload;
}

[Table(Placement = Placement.Global)]
public partial struct NamedThing
{
    [PrimaryKey]
    [AutoInc]
    public long Id;

    [Unique]
    public string Name;
}

/// <summary>Long string keys and a long indexed string: entries too big to sit inline in a native directory page.</summary>
[Table]
public partial struct LongKeyed
{
    [PrimaryKey]
    public string Key;

    [Index]
    public string Tag;

    public int Version;
}
