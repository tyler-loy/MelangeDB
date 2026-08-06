namespace MelangeDB.Benchmarks;

// The row types every suite measures against, declared here rather than nested inside the suites
// for one reason: the source generator only emits a RowCodec for a public or internal type
// (ModelExtractor's accessibility check), and a suite whose rows fall back to the reflection path
// measures code the server does not run. Gap 4 in particular is a question *about* the generated
// codec, so the fallback would answer the wrong question entirely.
//
// Fields are assigned through the schema's accessors and the generated codec rather than by these
// files, which is what CS0649 objects to.
#pragma warning disable CS0649

/// <summary>The read-view suite's table: a primary key and a fixed-size payload.</summary>
[Table]
internal struct BenchRow
{
    [PrimaryKey]
    public ulong Id;

    public byte[] Payload;
}

/// <summary>A row with a payload and no secondary index — the commit path's subject.</summary>
[Table]
internal struct CommitRow
{
    [PrimaryKey]
    public ulong Id;

    public byte[] Payload;
}

/// <summary>A position update: the shape a game tick sends at 15 Hz, over and over.</summary>
[Table]
internal struct NarrowRow
{
    [PrimaryKey]
    public ulong Id;

    public float X;
    public float Y;
    public float Z;
}

/// <summary>A full entity record — the shape an initial subscription set carries.</summary>
[Table]
internal struct WideRow
{
    [PrimaryKey]
    public ulong Id;

    public string Name;
    public float X;
    public float Y;
    public float Z;
    public float Yaw;
    public int Health;
    public int Mana;
    public int Level;
    public long Experience;
    public ulong GuildId;
    public Timestamp LastSeen;
}

/// <summary>
/// The table the fan-out suite's subscribers watch. Public because tables are private by default
/// and no subscription may name a private one — which is the correct default and the reason this is
/// the only row type here that opts in.
/// </summary>
[Table(Public = true)]
internal struct FanoutRow
{
    [PrimaryKey]
    public ulong Id;

    public string Name;
    public float X;
    public float Y;
    public float Z;
    public int Health;
    public int Level;
}

/// <summary>An unindexed table: the case where batching an apply should barely register.</summary>
[Table]
internal struct PlainRow
{
    [PrimaryKey]
    public ulong Id;

    public byte[] Payload;
}

/// <summary>Two secondary indexes — the multiplier on every version publish.</summary>
[Table]
internal struct IndexedRow
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public ulong RoomId;

    [Index]
    public ulong OwnerId;
}

/// <summary>One secondary index — the index-maintenance suite's floor.</summary>
[Table]
internal struct Index1Row
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public ulong A;

    public ulong B;
    public ulong C;
    public ulong D;
    public ulong E;
    public ulong F;
    public ulong G;
    public ulong H;
}

/// <summary>Four secondary indexes on the same column set.</summary>
[Table]
internal struct Index4Row
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public ulong A;

    [Index]
    public ulong B;

    [Index]
    public ulong C;

    [Index]
    public ulong D;

    public ulong E;
    public ulong F;
    public ulong G;
    public ulong H;
}

/// <summary>
/// Eight secondary indexes. The per-column encode cost is multiplied by this, which is the whole
/// reason finding #5 asked for a single-pass extract rather than a column-at-a-time one.
/// </summary>
[Table]
internal struct Index8Row
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public ulong A;

    [Index]
    public ulong B;

    [Index]
    public ulong C;

    [Index]
    public ulong D;

    [Index]
    public ulong E;

    [Index]
    public ulong F;

    [Index]
    public ulong G;

    [Index]
    public ulong H;
}

/// <summary>A single indexed column over a large key space — the range-seek suite's subject.</summary>
[Table]
internal struct RangeRow
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public ulong Bucket;

    public byte[] Payload;
}

#pragma warning restore CS0649
