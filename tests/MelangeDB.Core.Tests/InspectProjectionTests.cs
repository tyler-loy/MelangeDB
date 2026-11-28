using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The inspector's projection (road-to-0.3 phase 21, step 4): the world as of an LSN, rebuilt
/// forward from the snapshot or from empty, agreeing byte for byte with what the engine held; and
/// the honest refusal where the log has no way to run backwards.
/// </summary>
public class InspectProjectionTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly List<IDisposable> _disposables = [];

    public void Dispose()
    {
        foreach (var disposable in Enumerable.Reverse(_disposables))
            disposable.Dispose();
        foreach (var root in _roots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private EngineHarness Harness()
    {
        var harness = new EngineHarness(tables: [typeof(Player), typeof(InventoryItem)]);
        _disposables.Add(harness);
        return harness;
    }

    private InspectSource Open(string path)
    {
        var source = InspectSource.Open(path);
        _disposables.Add(source);
        return source;
    }

    private static List<string> EngineDump(MelangeEngine engine) =>
        [.. engine.Schema.Tables
            .SelectMany(table => engine.HotStore.Scan(table.Id).Select(pair => $"{table.Id}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}"))
            .Order(StringComparer.Ordinal)];

    private static List<string> ProjectionDump(InspectProjection projection) =>
        [.. projection.Counts.Keys
            .SelectMany(table => projection.Rows(table).Select(row => $"{table}|{row.Key}|{Convert.ToHexStringLower(row.Row.Span)}"))
            .Order(StringComparer.Ordinal)];

    private static void Play(EngineHarness harness)
    {
        harness.Invoke("Spawn", ctx =>
        {
            ctx.Db.Insert(new Player { Id = Identity.Hash("alice"), RoomId = 1, Name = "alice" });
            ctx.Db.Insert(new Player { Id = Identity.Hash("bob"), RoomId = 1, Name = "bob" });
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("alice"), ItemName = "sword", Quantity = 1 });
        });
        harness.Invoke("Move", ctx => ctx.Db.Update(new Player { Id = Identity.Hash("bob"), RoomId = 2, Name = "bob" }));
        harness.Invoke("Drop", ctx => ctx.Db.Delete<InventoryItem>(1UL));
    }

    [Fact]
    public void A_replay_from_empty_agrees_with_the_engine_and_stands_at_every_lsn_on_the_way()
    {
        var harness = Harness();
        Play(harness);
        var expected = EngineDump(harness.Engine);
        var source = Open(harness.Options.CommitLog.Path);

        var head = InspectProjection.Build(source, source.Facts.HeadLsn);
        Assert.False(head.FromSnapshot);
        Assert.Equal((0UL, 3L, 3UL), (head.StartLsn, head.Replayed, head.AtLsn));
        Assert.Equal(expected, ProjectionDump(head));

        var afterSpawn = InspectProjection.Build(source, 1);
        var items = TableId.FromName("InventoryItem");
        Assert.Single(afterSpawn.Rows(items));
        var bob = afterSpawn.Find(TableId.FromName("Player"), KeyCodec.EncodeIdentity(Identity.Hash("bob")))!.Value;
        Assert.Equal(1UL, bob.WrittenAt);
        Assert.Equal(1, source.DecodeRow(bob.WrittenAt, bob.Table, bob.Row.Span).Columns[1].Value);
        Assert.Empty(InspectProjection.Build(source, 0).Counts);

        // Carried forward one record at a time, it lands where a fresh build does.
        foreach (var record in source.Records(2))
            afterSpawn.Apply(record);
        Assert.Equal(ProjectionDump(head), ProjectionDump(afterSpawn));
        Assert.Empty(afterSpawn.Rows(items));
        Assert.Throws<InvalidOperationException>(() => afterSpawn.Apply(source.Record(2)!));
    }

    [Fact]
    public void A_compacted_directory_builds_from_its_snapshot_and_refuses_what_lies_below_it()
    {
        var harness = Harness();
        Play(harness);
        harness.Options.Resume.RetentionWindowSeconds = 0;
        var snapshotLsn = harness.Engine.TakeSnapshot()!.Value;
        harness.Invoke("Late", ctx => ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("bob"), ItemName = "torch", Quantity = 2 }));
        var expected = EngineDump(harness.Engine);
        var source = Open(harness.Options.CommitLog.Path);

        var head = InspectProjection.Build(source, source.Facts.HeadLsn);
        Assert.True(head.FromSnapshot);
        Assert.Equal((snapshotLsn, 1L), (head.StartLsn, head.Replayed));
        Assert.Equal(expected, ProjectionDump(head));
        Assert.Null(InspectProjection.ExplainUnbuildable(source, snapshotLsn));

        var refusal = InspectProjection.ExplainUnbuildable(source, snapshotLsn - 1);
        Assert.NotNull(refusal);
        Assert.Contains($"snapshot is at LSN {snapshotLsn}", refusal);
        Assert.Contains($"truncation base", refusal);
        Assert.Contains("never what it replaced", refusal);
        Assert.Equal(refusal, Assert.Throws<InvalidOperationException>(() => InspectProjection.Build(source, snapshotLsn - 1)).Message);
        Assert.Contains("past the head", InspectProjection.ExplainUnbuildable(source, source.Facts.HeadLsn + 1));
    }

    public struct Entry
    {
        public long Id;
        public long Delta;
    }

    public struct Audit
    {
        public long Id;
        public long Delta;
    }

    [Fact]
    public void An_archive_tier_table_is_left_out_and_named()
    {
        var root = Directory.CreateTempSubdirectory("melange-inspect-tier-").FullName;
        _roots.Add(root);
        var options = new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
        };
        var audit = SchemaShapeTests.Declare<Audit>("Audit", "Id", autoInc: "Id");
        using (var engine = new MelangeEngine(options, new SchemaRegistry(
            [SchemaShapeTests.Declare<Entry>("Entry", "Id", autoInc: "Id"), new TableSchema(audit.RowType, audit.Name, audit.Columns, tier: StorageTier.Archive)])))
        {
            engine.Invoke("Earn", EngineHarness.Caller, ctx =>
            {
                ctx.Db.Insert(new Entry { Delta = 5 });
                ctx.Db.Insert(new Audit { Delta = 5 });
            });
        }

        var source = Open(options.CommitLog.Path);
        var projection = InspectProjection.Build(source, source.Facts.HeadLsn);
        Assert.Equal(["Audit"], projection.LeftOut.Select(static t => t.Name));
        Assert.True(projection.IsLeftOut(TableId.FromName("Audit")));
        Assert.Equal([TableId.FromName("Entry")], projection.Counts.Keys);
        Assert.Empty(projection.Rows(TableId.FromName("Audit")));
    }
}
