using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Issue #307: MelangeLog holds keys of at most 1 KiB. A longer primary key used to reach the commit
/// log and then fail in the store, closing the database to writes, and fail again on every replay,
/// so it could not start. The engine now refuses such a write before anything is appended.
/// </summary>
public sealed class LongKeyTests
{
    private static readonly TableId Table = TableId.FromName("LongKeyed");

    [Fact]
    public void A_key_too_long_for_melangelog_is_refused_before_the_log_and_the_database_carries_on()
    {
        using var harness = Harness(PagedEngine.MelangeLog);
        var key = new string('k', 1_100);

        var refused = Assert.ThrowsAny<Exception>(() =>
            harness.Invoke("insert", ctx => ctx.Db.Insert(new LongKeyed { Key = key, Tag = "t", Version = 1 })));
        Assert.IsNotType<CommitOutcomeUnknownException>(refused);
        Assert.Contains("longer than the hot store holds", Flatten(refused));

        // Nothing reached the log, the database still takes writes, and a restart replays cleanly.
        harness.Invoke("after", ctx => ctx.Db.Insert(new LongKeyed { Key = "short", Tag = "t", Version = 1 }));
        harness.Restart();
        Assert.Equal(1, harness.Engine.HotStore.Count(Table));
        harness.Invoke("again", ctx => ctx.Db.Insert(new LongKeyed { Key = "short2", Tag = "t", Version = 1 }));
    }

    [Fact]
    public void A_key_at_the_stated_limit_is_accepted()
    {
        using var harness = Harness(PagedEngine.MelangeLog);
        var limit = harness.Engine.HotStore.MaxKeyBytes;
        Assert.InRange(limit, 900, 1_024);
        var column = harness.Engine.Schema.Get(typeof(LongKeyed)).PrimaryKey;

        // The longest string whose encoded key fits, then one character more.
        var length = 1;
        while (SchemaKeyCodec.Encode(column, new string('a', length + 1)).Length <= limit)
            length++;
        var fits = new string('a', length);
        harness.Invoke("fits", ctx => ctx.Db.Insert(new LongKeyed { Key = fits, Tag = "t", Version = 1 }));
        Assert.ThrowsAny<Exception>(() =>
            harness.Invoke("over", ctx => ctx.Db.Insert(new LongKeyed { Key = fits + "a", Tag = "t", Version = 1 })));
        harness.Restart();
        Assert.Equal(1, harness.Engine.HotStore.Count(Table));
    }

    [Fact]
    public void An_engine_without_a_key_limit_takes_the_long_key()
    {
        using var harness = Harness(PagedEngine.Faster);
        Assert.Equal(int.MaxValue, harness.Engine.HotStore.MaxKeyBytes);
        var key = new string('k', 4_000);
        harness.Invoke("insert", ctx => ctx.Db.Insert(new LongKeyed { Key = key, Tag = "t", Version = 1 }));
        harness.Restart();
        Assert.Equal(1, harness.Engine.HotStore.Count(Table));
    }

    private static string Flatten(Exception exception) =>
        exception.InnerException is { } inner ? exception.Message + " | " + Flatten(inner) : exception.Message;

    private static StoreHarness Harness(PagedEngine engine) =>
        new(
            StoreKind.Faster,
            options => options.Residency.Default = Residency.Paged,
            tables: [typeof(LongKeyed)],
            provider: new FasterHotStoreProvider(engine));
}
