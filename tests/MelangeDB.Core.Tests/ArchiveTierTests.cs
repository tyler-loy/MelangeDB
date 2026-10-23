using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The archive tier in the engine (road-to-0.3 phase 25): archive ops ride the log and never
/// reach the hot store, through commit, recovery, and snapshots alike; reducers append and delete
/// by key and cannot read; AutoInc ids stay unique across restarts though no row is ever held
/// here; and every tier change across the hot store's boundary refuses boot.
/// </summary>
public class ArchiveTierTests : IDisposable
{
    private readonly List<string> _roots = [];

    public void Dispose()
    {
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

    public struct Entry
    {
        public long Id;
        public long Delta;
    }

    public struct Archived
    {
        public long Id;
        public long Delta;
    }

    private static TableSchema WithTier(TableSchema table, StorageTier tier) =>
        new(table.RowType, table.Name, table.Columns, tier: tier);

    private static TableSchema EntryTable() => SchemaShapeTests.Declare<Entry>("Entry", "Id", autoInc: "Id");

    private static TableSchema ArchiveTable(StorageTier tier = StorageTier.Archive) =>
        WithTier(SchemaShapeTests.Declare<Archived>("Archived", "Id", autoInc: "Id"), tier);

    private MelangeDbOptions NewOptions()
    {
        var root = Directory.CreateTempSubdirectory("melange-archive-").FullName;
        _roots.Add(root);
        return new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
            Snapshots = { Enabled = true, IntervalTransactions = long.MaxValue },
            Resume = { RetentionWindowSeconds = 0 },
        };
    }

    private static MelangeEngine Boot(MelangeDbOptions options, StorageTier archiveTier = StorageTier.Archive) =>
        new(options, new SchemaRegistry([EntryTable(), ArchiveTable(archiveTier)]));

    private static long HotRows<T>(MelangeEngine engine) => engine.HotStore.Count(engine.Schema.Get(typeof(T)).Id);

    private static List<RowOp> ArchiveOpsInLog(MelangeEngine engine)
    {
        var archive = engine.Schema.Get(typeof(Archived)).Id;
        return engine.Log.ReadFrom(engine.Log.BaseLsn + 1)
            .SelectMany(record => record.WriteSet)
            .Where(op => op.Table == archive)
            .ToList();
    }

    [Fact]
    public void An_archive_insert_rides_the_log_and_never_reaches_the_hot_store()
    {
        var options = NewOptions();
        using var engine = Boot(options);
        engine.Invoke("Record", EngineHarness.Caller, ctx =>
        {
            ctx.Db.Insert(new Entry { Delta = 5 });
            ctx.Db.Insert(new Archived { Delta = 7 });
        });

        Assert.Equal(1, HotRows<Entry>(engine));
        Assert.Equal(0, HotRows<Archived>(engine));
        var op = Assert.Single(ArchiveOpsInLog(engine));
        Assert.Equal(RowOpKind.Insert, op.Kind);
    }

    [Fact]
    public void Moving_a_row_into_the_archive_is_one_atomic_write_set()
    {
        var options = NewOptions();
        using var engine = Boot(options);
        var entry = default(Entry);
        engine.Invoke("Earn", EngineHarness.Caller, ctx => entry = ctx.Db.Insert(new Entry { Delta = 10 }));

        var lsn = engine.Invoke("ArchiveOld", EngineHarness.Caller, ctx =>
        {
            ctx.Db.Delete<Entry>(entry.Id);
            ctx.Db.Insert(new Archived { Id = entry.Id, Delta = entry.Delta });
        });

        Assert.Equal(0, HotRows<Entry>(engine));
        var record = Assert.Single(engine.Log.ReadFrom(lsn));
        Assert.Equal(2, record.WriteSet.Count);
    }

    [Fact]
    public void Recovery_and_snapshots_hold_nothing_of_an_archive_table()
    {
        var options = NewOptions();
        using (var first = Boot(options))
        {
            for (var i = 0; i < 3; i++)
                first.Invoke("Record", EngineHarness.Caller, ctx => ctx.Db.Insert(new Archived { Delta = i }));
        }

        // Recovery replays the log; the archive ops in it are skipped like everywhere else.
        using (var replayed = Boot(options))
        {
            Assert.Equal(0, HotRows<Archived>(replayed));
            Assert.NotNull(replayed.TakeSnapshot());
        }

        // The snapshot truncated the log behind it, and it carries no archive rows either.
        using var fromSnapshot = Boot(options);
        Assert.True(fromSnapshot.Log.BaseLsn > 0);
        Assert.Equal(0, HotRows<Archived>(fromSnapshot));
    }

    [Fact]
    public void AutoInc_ids_stay_unique_across_a_snapshot_though_no_archive_row_is_held()
    {
        var options = NewOptions();
        var ids = new List<long>();
        using (var first = Boot(options))
        {
            for (var i = 0; i < 3; i++)
                first.Invoke("Record", EngineHarness.Caller, ctx => ids.Add(ctx.Db.Insert(new Archived { Delta = i }).Id));
            first.TakeSnapshot();
        }

        using var second = Boot(options);
        second.Invoke("Record", EngineHarness.Caller, ctx => ids.Add(ctx.Db.Insert(new Archived { Delta = 99 }).Id));
        Assert.Equal(ids.Count, ids.Distinct().Count());
        Assert.True(ids[^1] > ids.Take(3).Max());
    }

    [Fact]
    public void Every_read_of_an_archive_table_is_refused_with_the_reason()
    {
        var options = NewOptions();
        using var engine = Boot(options);
        Action<ReducerContext>[] reads =
        [
            ctx => ctx.Db.Find<Archived>(1L),
            ctx => _ = ctx.Db.Scan<Archived>().ToList(),
            ctx => _ = ctx.Db.Filter<Archived>("Id", 1L).ToList(),
            ctx => _ = ctx.Db.Count<Archived>(),
            ctx => _ = ctx.Db.Any<Archived>(),
        ];

        foreach (var read in reads)
        {
            var refused = Assert.ThrowsAny<InvalidOperationException>(() => engine.Invoke("Peek", EngineHarness.Caller, read));
            Assert.Contains("archive table", refused.Message, StringComparison.Ordinal);
            Assert.Contains("cannot read", refused.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Update_is_refused_and_delete_is_blind()
    {
        var options = NewOptions();
        using var engine = Boot(options);
        var refused = Assert.ThrowsAny<InvalidOperationException>(() =>
            engine.Invoke("Edit", EngineHarness.Caller, ctx => ctx.Db.Update(new Archived { Id = 1, Delta = 2 })));
        Assert.Contains("never update", refused.Message, StringComparison.Ordinal);

        // No row with key 42 was ever written; the delete still stages, because whether Postgres
        // holds it is unknowable here. A second delete of the same key in one transaction is a no-op.
        var lsn = engine.Invoke("Erase", EngineHarness.Caller, ctx =>
        {
            Assert.True(ctx.Db.Delete<Archived>(42L));
            ctx.Db.Delete<Archived>(42L);
        });
        var op = Assert.Single(Assert.Single(engine.Log.ReadFrom(lsn)).WriteSet);
        Assert.Equal(RowOpKind.Delete, op.Kind);
    }

    [Theory]
    [InlineData(StorageTier.Hot, StorageTier.Archive, "would drop its rows from the hot store")]
    [InlineData(StorageTier.HotWithRelationalBackplane, StorageTier.Archive, "would drop its rows from the hot store")]
    [InlineData(StorageTier.Archive, StorageTier.Hot, "would present an empty hot table")]
    [InlineData(StorageTier.Archive, StorageTier.HotWithRelationalBackplane, "would present an empty hot table")]
    public void A_tier_change_across_the_hot_store_boundary_refuses_boot(StorageTier from, StorageTier to, string reason)
    {
        var options = NewOptions();
        using (var before = Boot(options, from))
            before.Invoke("Record", EngineHarness.Caller, ctx => ctx.Db.Insert(new Archived { Delta = 1 }));

        var refused = Assert.Throws<SchemaShapeException>(() => Boot(options, to));
        Assert.Contains(reason, Assert.Single(refused.Reasons), StringComparison.Ordinal);
    }

    [Fact]
    public void The_reflection_path_refuses_what_the_analyzer_refuses()
    {
        var columns = SchemaShapeTests.Declare<Archived>("Archived", "Id").Columns;
        Assert.Throws<NotSupportedException>(() => new TableSchema(typeof(Archived), "Archived", columns, isPublic: true, tier: StorageTier.Archive));
        Assert.Throws<NotSupportedException>(() => new TableSchema(typeof(Archived), "Archived", columns, tier: StorageTier.Archive, placement: Placement.Replicated));
    }

    [Fact]
    public void An_archive_table_without_postgres_refuses_to_start()
    {
        var schema = new SchemaRegistry([EntryTable(), ArchiveTable()]);
        var refused = Assert.Throws<InvalidOperationException>(() =>
            MelangeDbHostedService.RequirePostgresForArchive(schema, new PostgresOptions()));
        Assert.Contains("Archived", refused.Message, StringComparison.Ordinal);
        Assert.Contains("lost the first time the log truncated", refused.Message, StringComparison.Ordinal);

        MelangeDbHostedService.RequirePostgresForArchive(schema, new PostgresOptions { ConnectionString = "Host=x" });
        MelangeDbHostedService.RequirePostgresForArchive(new SchemaRegistry([EntryTable()]), new PostgresOptions());
    }
}
