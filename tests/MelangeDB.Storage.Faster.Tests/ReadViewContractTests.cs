using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// The pinned-read contract: the same suite runs against both stores and both must answer
/// identically, even though they pin by completely different means. The in-memory store versions
/// every container; the FASTER store versions only what lives in managed memory and covers a paged
/// row's payload with an undo overlay, because a hybrid-log upsert overwrites in place and leaves no
/// old version to read. If those two ever diverge, a reducer's behaviour would depend on which
/// storage engine was configured — which is the one thing the seam exists to prevent.
/// </summary>
public class ReadViewContractTests
{
    public static TheoryData<StoreKind> Stores => new(StoreKind.InMemory, StoreKind.Faster);

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_row_inserted_after_the_view_opened_is_invisible_to_it(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "wolf" }));

        using var view = OpenView(harness);
        harness.Invoke("more", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "bear" }));

        Assert.Equal(1, view.Count(Creatures));
        Assert.Equal(2, harness.Engine.HotStore.Count(Creatures));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_row_updated_after_the_view_opened_reads_at_its_pinned_value(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "wolf", X = 1.5f }));

        using var view = OpenView(harness);
        harness.Invoke("move", ctx =>
        {
            var creature = ctx.Db.Find<Creature>(1UL)!.Value;
            ctx.Db.Update(creature with { Name = "dire wolf", X = 99f });
        });

        var pinned = ReadCreature(harness, view, 1UL);
        Assert.Equal("wolf", pinned.Name);
        Assert.Equal(1.5f, pinned.X);

        Creature? live = null;
        harness.Invoke("read", ctx => live = ctx.Db.Find<Creature>(1UL));
        Assert.Equal("dire wolf", live!.Value.Name);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_row_deleted_after_the_view_opened_is_still_there(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "wolf" }));

        using var view = OpenView(harness);
        harness.Invoke("cull", ctx => Assert.True(ctx.Db.Delete<Creature>(1UL)));

        Assert.Equal("wolf", ReadCreature(harness, view, 1UL).Name);
        Assert.Equal(1, view.Count(Creatures));
        Assert.Equal(0, harness.Engine.HotStore.Count(Creatures));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_resident_table_is_pinned_the_same_way_a_paged_one_is(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new ItemDefinition { Id = 1, Name = "pick", Value = 10 }));

        using var view = OpenView(harness);
        harness.Invoke("reprice", ctx =>
        {
            var item = ctx.Db.Find<ItemDefinition>(1)!.Value;
            ctx.Db.Update(item with { Value = 999 });
            ctx.Db.Insert(new ItemDefinition { Id = 2, Name = "torch", Value = 5 });
        });

        var items = Table<ItemDefinition>(harness);
        Assert.True(view.TryGetRow(items, Key<ItemDefinition>(harness, 1), out var pinned));
        Assert.Equal(10, ((ItemDefinition)RowSerializer.Deserialize(SchemaOf<ItemDefinition>(harness), pinned)).Value);
        Assert.Equal(1, view.Count(items));
        Assert.Equal(2, harness.Engine.HotStore.Count(items));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void An_out_of_line_blob_rewritten_after_the_view_opened_still_reads_at_its_pinned_bytes(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        var original = new byte[4096];
        Array.Fill(original, (byte)0xAB);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new TerrainBlob { ChunkId = 1, Region = 3, Data = original }));

        using var view = OpenView(harness);
        var replacement = new byte[4096];
        Array.Fill(replacement, (byte)0xCD);
        harness.Invoke("rewrite", ctx =>
        {
            var blob = ctx.Db.Find<TerrainBlob>(1L)!.Value;
            ctx.Db.Update(blob with { Data = replacement });
        });

        var blobs = Table<TerrainBlob>(harness);
        Assert.True(view.TryGetRow(blobs, Key<TerrainBlob>(harness, 1L), out var pinnedRow));
        var pinned = (TerrainBlob)RowSerializer.Deserialize(SchemaOf<TerrainBlob>(harness), pinnedRow);
        Assert.Equal(original, pinned.Data);

        TerrainBlob? live = null;
        harness.Invoke("read", ctx => live = ctx.Db.Find<TerrainBlob>(1L));
        Assert.Equal(replacement, live!.Value.Data);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void An_index_scan_resolves_against_the_pinned_version(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "wolf" }));

        using var view = OpenView(harness);
        harness.Invoke("move", ctx =>
        {
            var creature = ctx.Db.Find<Creature>(1UL)!.Value;
            ctx.Db.Update(creature with { ChunkId = 8 });
        });

        Assert.Single(view.ScanIndex(Creatures, "ChunkId", Index<Creature>(harness, "ChunkId", 7)));
        Assert.Empty(view.ScanIndex(Creatures, "ChunkId", Index<Creature>(harness, "ChunkId", 8)));
        Assert.Empty(harness.Engine.HotStore.ScanIndex(Creatures, "ChunkId", Index<Creature>(harness, "ChunkId", 7)));
        Assert.Single(harness.Engine.HotStore.ScanIndex(Creatures, "ChunkId", Index<Creature>(harness, "ChunkId", 8)));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void An_index_range_scan_is_pinned_too(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            ctx.Db.Insert(new TerrainBlob { ChunkId = 1, Region = 2, Data = [1] });
            ctx.Db.Insert(new TerrainBlob { ChunkId = 2, Region = 4, Data = [2] });
        });

        using var view = OpenView(harness);
        harness.Invoke("more", ctx => ctx.Db.Insert(new TerrainBlob { ChunkId = 3, Region = 3, Data = [3] }));

        var blobs = Table<TerrainBlob>(harness);
        var low = Index<TerrainBlob>(harness, "Region", 0);
        var high = Index<TerrainBlob>(harness, "Region", 9);
        Assert.Equal(2, view.ScanIndexRange(blobs, "Region", low, high).Count());
        Assert.Equal(3, harness.Engine.HotStore.ScanIndexRange(blobs, "Region", low, high).Count());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_lazy_scan_held_across_writes_completes_on_the_row_set_it_started_on(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 20; i++)
                ctx.Db.Insert(new Creature { ChunkId = 7, Name = $"c{i:D2}" });
        });

        using var view = OpenView(harness);
        using var scan = view.Scan(Creatures).GetEnumerator();
        Assert.True(scan.MoveNext());

        harness.Invoke("churn", ctx =>
        {
            for (var i = 0; i < 20; i++)
                ctx.Db.Insert(new Creature { ChunkId = 9, Name = $"n{i:D2}" });
            var third = ctx.Db.Find<Creature>(3UL)!.Value;
            ctx.Db.Update(third with { Name = "changed" });
            Assert.True(ctx.Db.Delete<Creature>(5UL));
        });

        var seen = 1;
        var names = new List<string> { NameOf(harness, scan.Current.Value) };
        while (scan.MoveNext())
        {
            seen++;
            names.Add(NameOf(harness, scan.Current.Value));
        }

        Assert.Equal(20, seen);
        Assert.Contains("c02", names);  // the row that was updated, at its pinned name
        Assert.Contains("c04", names);  // the row that was deleted, still present
        Assert.DoesNotContain("changed", names);
        Assert.Equal(39, harness.Engine.HotStore.Count(Creatures));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Two_views_opened_at_different_lsns_disagree_and_both_are_right(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("a", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "a" }));
        using var early = OpenView(harness);
        harness.Invoke("b", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "b" }));
        using var late = OpenView(harness);
        harness.Invoke("c", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "c" }));

        Assert.Equal(1, early.Count(Creatures));
        Assert.Equal(2, late.Count(Creatures));
        Assert.Equal(3, harness.Engine.HotStore.Count(Creatures));
        Assert.True(early.Lsn < late.Lsn);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void The_view_reports_the_lsn_it_was_pinned_at(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "a" }));

        using var view = OpenView(harness);
        var pinned = harness.Engine.HotStore.AppliedLsn;
        harness.Invoke("more", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "b" }));

        Assert.Equal(pinned, view.Lsn);
        Assert.NotEqual(pinned, harness.Engine.HotStore.AppliedLsn);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_disposed_view_refuses_reads(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "a" }));

        var view = OpenView(harness);
        view.Dispose();

        Assert.Throws<ObjectDisposedException>(() => view.Count(Creatures));
        Assert.Throws<ObjectDisposedException>(() => view.Scan(Creatures));
        Assert.Throws<ObjectDisposedException>(() => view.TryGetRow(Creatures, Key<Creature>(harness, 1UL), out _));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Disposing_a_view_twice_is_harmless(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        var view = OpenView(harness);
        view.Dispose();
        view.Dispose();
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void A_view_over_an_empty_store_reads_empty_rather_than_throwing(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        using var view = OpenView(harness);

        Assert.Equal(0, view.Count(Creatures));
        Assert.Empty(view.Scan(Creatures));
        Assert.Empty(view.ScanKeys(Creatures));
        Assert.False(view.TryGetRow(Creatures, Key<Creature>(harness, 1UL), out _));
    }

    [Fact]
    public void A_residency_change_while_a_view_is_open_fails_it_loudly_rather_than_answering_wrongly()
    {
        using var harness = new StoreHarness(StoreKind.Faster);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "wolf" }));

        using var view = OpenView(harness);
        Assert.Equal(1, view.Count(Creatures));

        ((IResidencyControl)harness.Engine.HotStore).ApplyResidency("Creature", Residency.Resident);

        // Promotion rewrote where the rows live, so the version this view pinned no longer describes
        // them. Answering from it would be plausible and wrong.
        var failure = Assert.Throws<InvalidOperationException>(() => view.Count(Creatures));
        Assert.Contains("residency tier", failure.Message, StringComparison.Ordinal);
    }

    private static IHotStoreReadView OpenView(StoreHarness harness) =>
        ((IReadViewSource)harness.Engine.HotStore).OpenReadView();

    private static TableId Creatures { get; } = TableId.FromName("Creature");

    private static TableId Table<TRow>(StoreHarness harness)
        where TRow : struct => SchemaOf<TRow>(harness).Id;

    private static TableSchema SchemaOf<TRow>(StoreHarness harness)
        where TRow : struct => harness.Engine.Schema.Tables.Single(t => t.RowType == typeof(TRow));

    private static RowKey Key<TRow>(StoreHarness harness, object primaryKey)
        where TRow : struct => SchemaKeyCodec.Encode(SchemaOf<TRow>(harness).PrimaryKey, primaryKey);

    private static RowKey Index<TRow>(StoreHarness harness, string column, object value)
        where TRow : struct => SchemaKeyCodec.Encode(SchemaOf<TRow>(harness).Column(column), value);

    private static Creature ReadCreature(StoreHarness harness, IHotStoreReadView view, ulong id)
    {
        Assert.True(view.TryGetRow(Creatures, Key<Creature>(harness, id), out var row));
        return (Creature)RowSerializer.Deserialize(SchemaOf<Creature>(harness), row);
    }

    private static string NameOf(StoreHarness harness, ReadOnlyMemory<byte> row) =>
        ((Creature)RowSerializer.Deserialize(SchemaOf<Creature>(harness), row)).Name;
}
