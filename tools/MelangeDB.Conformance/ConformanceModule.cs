namespace MelangeDB.Conformance;

/// <summary>An enum column with a byte underlying kind: the wire carries the number, the manifest the names.</summary>
public enum Mood : byte
{
    Calm = 0,
    Alert = 1,
    Furious = 255,
}

/// <summary>
/// Every column kind the row format has, in one table, so one initial set exercises every decoder.
/// <see cref="Group"/> is the indexed column the corpus subscribes by (equality and range).
/// </summary>
[Table(Public = true)]
public partial struct Sample
{
    [PrimaryKey]
    public long Id;

    [Index]
    public int Group;

    public bool Flag;
    public sbyte I8;
    public byte U8;
    public short I16;
    public ushort U16;
    public int I32;
    public uint U32;
    public long I64;
    public ulong U64;
    public float F32;
    public double F64;
    public string? Text;
    public byte[]? Blob;
    public Identity Who;
    public Timestamp At;
    public Mood Mood;
}


/// <summary>
/// A row whose <see cref="Hidden"/> column only its owner sees: the corpus's source of non-empty
/// column masks. String-keyed, so the corpus also carries a string-encoded primary key.
/// </summary>
[Table(Public = true)]
public partial struct Secret
{
    [PrimaryKey]
    public string Name;

    public Identity Owner;
    public int Visible;
    public string Hidden;
}

/// <summary>Hides <see cref="Secret.Hidden"/> from everyone but the row's owner.</summary>
public sealed class OwnerSeesHidden : IColumnPolicy<Secret>
{
    public ColumnMask VisibleTo(in Secret row, PolicyContext ctx) =>
        row.Owner == ctx.Caller ? ColumnMask.All : ColumnMask.All.Except(nameof(Secret.Hidden));
}

/// <summary>A producer's result record: positional fields, a nullable string, and a list.</summary>
public sealed record GroupSummary(int Count, long MaxId, string? Label, List<long> Ids);

public sealed class ConformanceReducers
{
    [Reducer]
    public void PutSample(ReducerContext ctx, long id, int variant, int group) =>
        ctx.Db.Sample.Insert(SampleRows.Build(id, variant, group));

    [Reducer]
    public void ChangeSample(ReducerContext ctx, long id, int variant)
    {
        var existing = ctx.Db.Sample.Id.Find(id) ?? throw new RejectedException($"No sample {id}.");
        ctx.Db.Sample.Update(SampleRows.Build(id, variant, existing.Group));
    }

    [Reducer]
    public void DeleteSample(ReducerContext ctx, long id) => ctx.Db.Sample.Id.Delete(id);

    [Reducer]
    public void PutSecret(ReducerContext ctx, string name, Identity owner, int visible, string hidden) =>
        ctx.Db.Secret.Insert(new Secret { Name = name, Owner = owner, Visible = visible, Hidden = hidden });

    /// <summary>
    /// A row too large for a small <c>Subscriptions:MaxBufferedBytes</c>: its one delta is enough
    /// to trip backpressure, which is how the suite makes <c>overflow_resync</c> happen on purpose.
    /// </summary>
    [Reducer]
    public void PutBlob(ReducerContext ctx, long id, int group, int size)
    {
        var row = SampleRows.Build(id, 0, group);
        row.Blob = [.. Enumerable.Range(0, size).Select(static i => (byte)(i * 31))];
        ctx.Db.Sample.Insert(row);
    }

    /// <summary>Always refuses: the corpus's <c>rejected</c> result.</summary>
    [Reducer]
    public void Refuse(ReducerContext ctx, string reason) => throw new RejectedException(reason);
}

public sealed class ConformanceProducers
{
    [Producer]
    public GroupSummary Summarize(ProducerContext ctx, int group, string? label = null)
    {
        var ids = ctx.Db.Sample.Group.Filter(group, group).Select(static sample => sample.Id).OrderBy(static id => id).ToList();
        return new GroupSummary(ids.Count, ids.Count == 0 ? 0 : ids[^1], label, ids);
    }
}

/// <summary>
/// The fixed rows the corpus records, built server-side from a variant number so the recorder can
/// check every decoded value against the value that was written. Variant 1 is the edge-value row:
/// each kind's extremes, NaN, negative zero, infinities, the empty and non-ASCII string, and a
/// null blob.
/// </summary>
public static class SampleRows
{
    public static readonly Identity OtherIdentity = new(Enumerable.Range(1, Identity.Size).Select(static i => (byte)i).ToArray());

    public static Sample Build(long id, int variant, int group) => variant switch
    {
        0 => new Sample
        {
            Id = id,
            Group = group,
            Flag = true,
            I8 = -5,
            U8 = 200,
            I16 = -1234,
            U16 = 54321,
            I32 = -123456789,
            U32 = 3_000_000_000,
            I64 = -1_234_567_890_123,
            U64 = 12_345_678_901_234_567_890,
            F32 = 1.5f,
            F64 = -2.25,
            Text = "plain",
            Blob = [0xde, 0xad, 0xbe, 0xef],
            Who = OtherIdentity,
            At = new Timestamp(1_767_225_600_000_000),
            Mood = Mood.Alert,
        },
        1 => new Sample
        {
            Id = id,
            Group = group,
            Flag = false,
            I8 = sbyte.MinValue,
            U8 = byte.MaxValue,
            I16 = short.MinValue,
            U16 = ushort.MaxValue,
            I32 = int.MinValue,
            U32 = uint.MaxValue,
            I64 = long.MinValue,
            U64 = ulong.MaxValue,
            F32 = float.NaN,
            F64 = -0.0,
            Text = "été ☃ \U0001F600",
            Blob = null,
            Who = Identity.None,
            At = new Timestamp(-1),
            Mood = Mood.Furious,
        },
        2 => new Sample
        {
            Id = id,
            Group = group,
            Flag = true,
            I8 = sbyte.MaxValue,
            U8 = 0,
            I16 = short.MaxValue,
            U16 = 0,
            I32 = int.MaxValue,
            U32 = 0,
            I64 = long.MaxValue,
            U64 = 0,
            F32 = float.PositiveInfinity,
            F64 = double.NegativeInfinity,
            Text = string.Empty,
            Blob = [],
            Who = OtherIdentity,
            At = new Timestamp(0),
            Mood = Mood.Calm,
        },
        3 => new Sample
        {
            Id = id,
            Group = group,
            Text = null,
            Blob = [.. Enumerable.Range(0, 300).Select(static i => (byte)i)],
            F32 = float.Epsilon,
            F64 = double.MaxValue,
            Who = OtherIdentity,
        },
        _ => throw new RejectedException($"Unknown sample variant {variant}."),
    };
}
