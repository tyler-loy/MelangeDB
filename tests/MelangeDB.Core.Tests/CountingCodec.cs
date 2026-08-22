namespace MelangeDB.Core.Tests;

/// <summary>How many times a table's rows were decoded — the observable a decode-cost test asserts on.</summary>
internal interface IDecodeCounter
{
    long Deserializations { get; }

    void Reset();
}

/// <summary>
/// A generated codec with a counter on <see cref="Deserialize"/>. Every path that turns a row's
/// bytes back into a struct — the typed one, the boxed one, the per-column encoders — ends here,
/// so a test that wants to say "this operation decodes each row once, not once per reader" can
/// count, where timing would only suggest. Built over a real table's generated codec so that
/// everything else about the table is exactly what ships.
/// </summary>
internal sealed class CountingCodec<TRow>(RowCodec<TRow> inner) : RowCodec<TRow>, IDecodeCounter
    where TRow : struct
{
    private long _deserializations;

    public long Deserializations => Interlocked.Read(ref _deserializations);

    public void Reset() => Interlocked.Exchange(ref _deserializations, 0);

    public override byte[] Serialize(in TRow row) => inner.Serialize(in row);

    public override TRow Deserialize(ReadOnlySpan<byte> data)
    {
        Interlocked.Increment(ref _deserializations);
        return inner.Deserialize(data);
    }

    public override RowKey EncodePrimaryKey(in TRow row) => inner.EncodePrimaryKey(in row);

    public override RowKey? EncodeColumn(string column, in TRow row) => inner.EncodeColumn(column, in row);

    public override void AssignAutoInc(ref TRow row, AutoIncStage stage, TableId table) => inner.AssignAutoInc(ref row, stage, table);

    /// <summary>
    /// A registry in which every listed table's codec counts. The tables are the generated model's,
    /// with nothing changed but the codec.
    /// </summary>
    public static SchemaRegistry Registry(params Type[] tables)
    {
        var wrapped = new List<TableSchema>();
        foreach (var table in new MelangeDB.Generated.MelangeModel().Tables())
        {
            if (!tables.Contains(table.RowType))
                continue;
            var codec = (RowCodec)Activator.CreateInstance(
                typeof(CountingCodec<>).MakeGenericType(table.RowType), table.Codec)!;
            wrapped.Add(new TableSchema(
                table.RowType,
                table.Name,
                table.Columns,
                table.IsPublic,
                table.Tier,
                table.Residency,
                table.Placement,
                table.ShardBy,
                table.Scheduled,
                codec));
        }

        return new SchemaRegistry(wrapped);
    }

    public static IDecodeCounter CounterFor(SchemaRegistry registry, Type table) =>
        (IDecodeCounter)registry.Get(table).Codec!;
}
