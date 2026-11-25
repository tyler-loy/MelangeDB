using System.Reflection;
using MelangeDB.Core;

namespace MelangeDB.Benchmarks;

/// <summary>
/// Builds a registry holding the <b>generated</b> schema for a table rather than a reflection-built
/// one.
/// <para>
/// <c>SchemaRegistry.FromTypes</c> walks the type with reflection and produces a schema whose
/// <c>Codec</c> is null, so every path that asks "is there a generated codec" takes its fallback.
/// That is fine for a suite measuring containers and wrong for one measuring the codec — the
/// generator emits <c>MelangeModel</c> for this assembly, and its schemas are the ones the server
/// actually runs. Anything measuring encode, decode, or index maintenance goes through here.
/// </para>
/// </summary>
internal static class BenchSchema
{
    private static readonly IReadOnlyList<TableSchema> Generated = LoadGenerated();

    public static SchemaRegistry For(string tableName) => For([tableName]);

    /// <summary>A registry holding several generated tables, for a suite whose policies read across them.</summary>
    public static SchemaRegistry For(params string[] tableNames) =>
        new([.. tableNames.Select(tableName => Generated.FirstOrDefault(t => t.Name == tableName)
            ?? throw new InvalidOperationException(
                $"No generated schema for '{tableName}'. Tables: {string.Join(", ", Generated.Select(t => t.Name))}."))]);

    private static IReadOnlyList<TableSchema> LoadGenerated()
    {
        var assembly = typeof(BenchSchema).Assembly;
        var attribute = assembly.GetCustomAttribute<MelangeGeneratedModelAttribute>()
            ?? throw new InvalidOperationException(
                "The benchmark assembly has no generated model; the CodeGen analyzer reference is missing.");
        return ((IMelangeModel)Activator.CreateInstance(attribute.ModelType)!).Tables();
    }
}

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

    /// <summary>Indexed so a fan-out subscriber can predicate on it — the shape that decoded the row per subscriber.</summary>
    [Index]
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

/// <summary>
/// Eight indexes on a row that also carries a string and a byte array. The scalar rows above make
/// a full deserialize cheap, which is the case where extracting a column at a time costs almost
/// nothing; these columns are the ones that have to be allocated and copied every time the row is
/// deserialized, so this is where "deserialize once instead of per column" is supposed to pay.
/// </summary>
[Table]
internal struct MixedIndex8Row
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

    public string Name;
    public string Description;
    public byte[] Blob;
}

/// <summary>One indexed column on a row carrying a string and a byte array.</summary>
[Table]
internal struct MixedIndex1Row
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public ulong A;

    public string Name;
    public string Description;
    public byte[] Blob;
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

/// <summary>
/// A hundred-column entity with an owner: the column-policy fan-out's subject (#315), where the
/// cost of a visible-column set grows with the column count.
/// </summary>
[Table(Public = true)]
internal struct PolicyRow
{
    [PrimaryKey]
    public ulong Id;

    public Identity Owner;
    public float X;
    public float Y;
    public float Z;
    public int C00;
    public int C01;
    public int C02;
    public int C03;
    public int C04;
    public int C05;
    public int C06;
    public int C07;
    public int C08;
    public int C09;
    public int C10;
    public int C11;
    public int C12;
    public int C13;
    public int C14;
    public int C15;
    public int C16;
    public int C17;
    public int C18;
    public int C19;
    public int C20;
    public int C21;
    public int C22;
    public int C23;
    public int C24;
    public int C25;
    public int C26;
    public int C27;
    public int C28;
    public int C29;
    public int C30;
    public int C31;
    public int C32;
    public int C33;
    public int C34;
    public int C35;
    public int C36;
    public int C37;
    public int C38;
    public int C39;
    public int C40;
    public int C41;
    public int C42;
    public int C43;
    public int C44;
    public int C45;
    public int C46;
    public int C47;
    public int C48;
    public int C49;
    public int C50;
    public int C51;
    public int C52;
    public int C53;
    public int C54;
    public int C55;
    public int C56;
    public int C57;
    public int C58;
    public int C59;
    public int C60;
    public int C61;
    public int C62;
    public int C63;
    public int C64;
    public int C65;
    public int C66;
    public int C67;
    public int C68;
    public int C69;
    public int C70;
    public int C71;
    public int C72;
    public int C73;
    public int C74;
    public int C75;
    public int C76;
    public int C77;
    public int C78;
    public int C79;
    public int C80;
    public int C81;
    public int C82;
    public int C83;
    public int C84;
    public int C85;
    public int C86;
    public int C87;
    public int C88;
    public int C89;
    public int C90;
    public int C91;
    public int C92;
    public int C93;
    public int C94;
    public int C95;
}

/// <summary>
/// A caller's own record, which a row policy on <see cref="PolicyRow"/> reads to judge visibility:
/// wide, like the player row a game's policy reads, so a lookup is a full decode.
/// </summary>
[Table(Public = true)]
internal struct ViewerRow
{
    [PrimaryKey]
    public Identity Id;

    public int Room;
    public string Name;
    public int V00;
    public int V01;
    public int V02;
    public int V03;
    public int V04;
    public int V05;
    public int V06;
    public int V07;
    public int V08;
    public int V09;
    public int V10;
    public int V11;
    public int V12;
    public int V13;
    public int V14;
    public int V15;
    public int V16;
    public int V17;
    public int V18;
    public int V19;
    public int V20;
    public int V21;
    public int V22;
    public int V23;
    public int V24;
    public int V25;
    public int V26;
    public int V27;
    public int V28;
    public int V29;
    public int V30;
    public int V31;
    public int V32;
    public int V33;
    public int V34;
    public int V35;
    public int V36;
    public int V37;
    public int V38;
    public int V39;
    public int V40;
    public int V41;
    public int V42;
    public int V43;
    public int V44;
    public int V45;
    public int V46;
    public int V47;
    public int V48;
    public int V49;
    public int V50;
    public int V51;
    public int V52;
    public int V53;
    public int V54;
    public int V55;
    public int V56;
    public int V57;
    public int V58;
    public int V59;
}

#pragma warning restore CS0649
