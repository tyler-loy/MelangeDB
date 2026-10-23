using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The archive key set (road-to-0.3 phase 25, <c>KeepKeys</c>): an archive table's keys kept in
/// the hot store, maintained by the engine from the archive's ops. A reducer can ask whether a key
/// was archived — the idempotency check — and a duplicate insert is refused before commit. The set
/// is an ordinary hot table, so it survives restarts and snapshots; turning it on for an existing
/// archive table is refused, and turning it off is not.
/// </summary>
public class ArchiveKeySetTests : IDisposable
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

    private static TableSchema EntryTable(bool keepKeys)
    {
        var declared = SchemaShapeTests.Declare<Entry>("Entry", "Id");
        return new TableSchema(declared.RowType, declared.Name, declared.Columns, tier: StorageTier.Archive, keepKeys: keepKeys);
    }

    private MelangeDbOptions NewOptions()
    {
        var root = Directory.CreateTempSubdirectory("melange-keyset-").FullName;
        _roots.Add(root);
        return new MelangeDbOptions
        {
            CommitLog = { Path = Path.Combine(root, "log") },
            HotStore = { Path = Path.Combine(root, "hot") },
            Snapshots = { Enabled = true, IntervalTransactions = long.MaxValue },
            Resume = { RetentionWindowSeconds = 0 },
        };
    }

    private static MelangeEngine Boot(MelangeDbOptions options, bool keepKeys = true) =>
        new(options, new SchemaRegistry([EntryTable(keepKeys)]));

    private static bool Contains(MelangeEngine engine, long key)
    {
        var found = false;
        engine.Invoke("Check", EngineHarness.Caller, ctx => found = ctx.Db.ContainsKey<Entry>(key));
        return found;
    }

    private static void Archive(MelangeEngine engine, long key) =>
        engine.Invoke("Archive", EngineHarness.Caller, ctx => ctx.Db.Insert(new Entry { Id = key, Delta = key }));

    [Fact]
    public void The_key_set_holds_the_keys_and_nothing_else()
    {
        using var engine = Boot(NewOptions());
        Archive(engine, 7);

        var keySet = engine.Schema.KeySetFor(engine.Schema.Get(typeof(Entry)).Id)!;
        Assert.Equal(ArchiveKeySet.NameFor("Entry"), keySet.Name);
        Assert.Equal(1, engine.HotStore.Count(keySet.Id));
        Assert.Equal(0, engine.HotStore.Count(engine.Schema.Get(typeof(Entry)).Id));
        Assert.True(Contains(engine, 7));
        Assert.False(Contains(engine, 8));
    }

    [Fact]
    public void A_duplicate_archive_key_is_refused_before_commit()
    {
        using var engine = Boot(NewOptions());
        Archive(engine, 1);
        var head = engine.Log.HeadLsn;

        var refused = Assert.ThrowsAny<InvalidOperationException>(() => Archive(engine, 1));
        Assert.Contains("already been archived", refused.Message, StringComparison.Ordinal);
        Assert.Equal(head, engine.Log.HeadLsn);
    }

    [Fact]
    public void Contains_sees_the_transactions_own_writes_and_a_delete_removes_the_key()
    {
        using var engine = Boot(NewOptions());
        engine.Invoke("Both", EngineHarness.Caller, ctx =>
        {
            Assert.False(ctx.Db.ContainsKey<Entry>(3L));
            ctx.Db.Insert(new Entry { Id = 3, Delta = 3 });
            Assert.True(ctx.Db.ContainsKey<Entry>(3L));
        });

        engine.Invoke("Erase", EngineHarness.Caller, ctx => ctx.Db.Delete<Entry>(3L));
        Assert.False(Contains(engine, 3));
    }

    [Fact]
    public void The_key_set_survives_a_restart_and_a_snapshot()
    {
        var options = NewOptions();
        using (var first = Boot(options))
        {
            Archive(first, 1);
            Archive(first, 2);
            Assert.NotNull(first.TakeSnapshot());
            Archive(first, 3);
        }

        using var second = Boot(options);
        Assert.True(second.Log.BaseLsn > 0);
        Assert.True(Contains(second, 1));
        Assert.True(Contains(second, 3));
        Assert.Throws<InvalidOperationException>(() => Archive(second, 2));
    }

    [Fact]
    public void Without_keep_keys_contains_is_refused_with_the_fix()
    {
        using var engine = Boot(NewOptions(), keepKeys: false);
        var refused = Assert.ThrowsAny<InvalidOperationException>(() => Contains(engine, 1));
        Assert.Contains("KeepKeys = true", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Turning_keep_keys_on_for_an_existing_archive_table_refuses_boot_and_turning_it_off_does_not()
    {
        var withoutKeys = NewOptions();
        using (var first = Boot(withoutKeys, keepKeys: false))
            Archive(first, 1);
        var refused = Assert.Throws<SchemaShapeException>(() => Boot(withoutKeys, keepKeys: true));
        Assert.Contains("now declares KeepKeys", Assert.Single(refused.Reasons), StringComparison.Ordinal);

        var withKeys = NewOptions();
        using (var first = Boot(withKeys, keepKeys: true))
            Archive(first, 1);
        using var dropped = Boot(withKeys, keepKeys: false);
        Assert.Null(dropped.Schema.KeySetFor(dropped.Schema.Get(typeof(Entry)).Id));
    }

    [Fact]
    public void Keep_keys_belongs_to_archive_tables_and_the_key_set_prefix_to_the_engine()
    {
        var columns = SchemaShapeTests.Declare<Entry>("Entry", "Id").Columns;
        Assert.Throws<NotSupportedException>(() => new TableSchema(typeof(Entry), "Entry", columns, keepKeys: true));
        Assert.Throws<ArgumentException>(() => new SchemaRegistry([new TableSchema(typeof(Entry), ArchiveKeySet.NameFor("X"), columns)]));
    }
}
