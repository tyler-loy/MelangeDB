using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// <c>HotStore:PagedEngine</c> picks the engine under the paged store, and switching it is a restart:
/// the store is rebuilt from the commit log, so nothing engine-specific has to survive.
/// </summary>
public sealed class PagedEngineSelectionTests
{
    [Fact]
    public void The_default_engine_is_faster()
    {
        Assert.Equal(PagedEngine.Faster, new MelangeDbOptions().HotStore.PagedEngine);
    }

    [Fact]
    public void Melangelog_is_selected_by_the_option()
    {
        if (Environment.GetEnvironmentVariable("MELANGE_TEST_PAGED_ENGINE") is { Length: > 0 })
            return; // The suite-wide override replaces the option; this test is about the option.

        using var harness = new StoreHarness(StoreKind.Faster, options => options.HotStore.PagedEngine = PagedEngine.MelangeLog);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 3, Name = "heron", X = 2.5f }));

        Assert.Equal("melangelog", ((FasterHotStore)harness.Engine.HotStore).EngineName);
        Creature? found = null;
        harness.Invoke("read", ctx => found = ctx.Db.Find<Creature>(1UL));
        Assert.Equal("heron", found?.Name);
    }

    [Fact]
    public void Switching_engines_between_restarts_keeps_every_row()
    {
        if (Environment.GetEnvironmentVariable("MELANGE_TEST_PAGED_ENGINE") is { Length: > 0 })
            return;

        using var harness = new StoreHarness(StoreKind.Faster);
        for (var i = 0; i < 50; i++)
        {
            var chunk = i;
            harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = chunk, Name = $"c{chunk}", X = chunk }));
        }

        var before = harness.Dump();
        harness.Options.HotStore.PagedEngine = PagedEngine.MelangeLog;
        harness.Restart();
        Assert.Equal(before, harness.Dump());

        harness.Options.HotStore.PagedEngine = PagedEngine.Faster;
        harness.Restart();
        Assert.Equal(before, harness.Dump());
    }
}
