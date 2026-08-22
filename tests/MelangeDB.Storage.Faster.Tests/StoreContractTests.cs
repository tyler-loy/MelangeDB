using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// The store contract: the same suite runs against the in-memory store and the FASTER store, and
/// every operation must return identical results — point lookups, full scans, index scans, range
/// scans, and byte-faithful recovery. The in-memory store is the reference implementation; the
/// FASTER store earns its place by being indistinguishable through the seam.
/// </summary>
public class StoreContractTests
{
    public static TheoryData<StoreKind> Stores => new(StoreKind.InMemory, StoreKind.Faster);

    [Theory]
    [MemberData(nameof(Stores))]
    public void Point_lookup_returns_inserted_row(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 7, Name = "wolf", X = 1.5f }));

        Creature? found = null;
        harness.Invoke("read", ctx => found = ctx.Db.Find<Creature>(AutoIncFirstId));
        Assert.NotNull(found);
        Assert.Equal("wolf", found.Value.Name);
        Assert.Equal(7, found.Value.ChunkId);
        Assert.Equal(1.5f, found.Value.X);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Missing_key_returns_null(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        Creature? found = new Creature();
        harness.Invoke("read", ctx => found = ctx.Db.Find<Creature>(123456UL));
        Assert.Null(found);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Full_scan_returns_rows_in_primary_key_order(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 20; i >= 1; i--)
                ctx.Db.Insert(new ItemDefinition { Id = i, Name = $"item-{i}", Value = i * 10 });
        });

        var ids = new List<int>();
        harness.Invoke("read", ctx =>
        {
            foreach (var item in ctx.Db.Scan<ItemDefinition>())
                ids.Add(item.Id);
        });
        Assert.Equal(Enumerable.Range(1, 20), ids);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Index_equality_scan_matches(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 30; i++)
                ctx.Db.Insert(new Creature { ChunkId = i % 3, Name = $"c{i}", X = i });
        });

        var hits = new List<string>();
        harness.Invoke("read", ctx =>
        {
            foreach (var creature in ctx.Db.Filter<Creature>("ChunkId", 1))
                hits.Add(creature.Name);
        });
        Assert.Equal(10, hits.Count);
        Assert.All(hits, name => Assert.Equal(1, int.Parse(name[1..]) % 3));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Index_range_scan_matches(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 50; i++)
                ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = i, Data = [1, 2, 3] });
        });

        var regions = new List<int>();
        harness.Invoke("read", ctx =>
        {
            foreach (var chunk in ctx.Db.FilterRange<TerrainBlob>("Region", 10, 19))
                regions.Add(chunk.Region);
        });
        Assert.Equal(Enumerable.Range(10, 10), regions);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Update_maintains_indexes(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 1, Name = "wolf", X = 0 }));
        harness.Invoke("move", ctx =>
        {
            var creature = ctx.Db.Find<Creature>(AutoIncFirstId)!.Value;
            ctx.Db.Update(creature with { ChunkId = 2 });
        });

        var oldChunk = new List<Creature>();
        var newChunk = new List<Creature>();
        harness.Invoke("read", ctx =>
        {
            oldChunk.AddRange(ctx.Db.Filter<Creature>("ChunkId", 1));
            newChunk.AddRange(ctx.Db.Filter<Creature>("ChunkId", 2));
        });
        Assert.Empty(oldChunk);
        Assert.Single(newChunk);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Delete_removes_row_and_index_entries(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = 4, Name = "boar", X = 0 }));
        harness.Invoke("delete", ctx => Assert.True(ctx.Db.Delete<Creature>(AutoIncFirstId)));

        harness.Invoke("read", ctx =>
        {
            Assert.Null(ctx.Db.Find<Creature>(AutoIncFirstId));
            Assert.Empty(ctx.Db.Filter<Creature>("ChunkId", 4));
            Assert.Empty(ctx.Db.Scan<Creature>());
        });
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Unique_constraint_enforced_against_committed_state(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx => ctx.Db.Insert(new NamedThing { Name = "alpha" }));
        Assert.Throws<InvalidOperationException>(() =>
            harness.Invoke("dup", ctx => ctx.Db.Insert(new NamedThing { Name = "alpha" })));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Restart_recovers_byte_identical_state(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 25; i++)
            {
                ctx.Db.Insert(new Creature { ChunkId = i % 5, Name = $"c{i}", X = i * 0.5f });
                ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = i / 5, Data = MakeBlob(i, 1024) });
            }

            ctx.Db.Insert(new ItemDefinition { Id = 1, Name = "sword", Value = 100 });
        });
        harness.Invoke("mutate", ctx =>
        {
            ctx.Db.Delete<Creature>(AutoIncFirstId);
            var blob = ctx.Db.Find<TerrainBlob>(3L)!.Value;
            ctx.Db.Update(blob with { Data = MakeBlob(99, 2048) });
        });

        var before = harness.Dump();
        harness.Restart();
        Assert.Equal(before, harness.Dump());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Both_stores_produce_identical_dumps_for_the_same_history(StoreKind kind)
    {
        // The cross-check that anchors the contract: one op script, both engines, equal bytes.
        using var reference = new StoreHarness(StoreKind.InMemory);
        using var subject = new StoreHarness(kind);
        foreach (var harness in new[] { reference, subject })
        {
            harness.Invoke("seed", ctx =>
            {
                for (var i = 0; i < 40; i++)
                    ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = i % 4, Data = MakeBlob(i, 300 + i * 17) });
                for (var i = 1; i <= 10; i++)
                    ctx.Db.Insert(new ItemDefinition { Id = i, Name = $"i{i}", Value = i });
            });
            harness.Invoke("mutate", ctx =>
            {
                ctx.Db.Delete<TerrainBlob>(7L);
                var chunk = ctx.Db.Find<TerrainBlob>(8L)!.Value;
                ctx.Db.Update(chunk with { Region = 9, Data = MakeBlob(8, 5000) });
            });
        }

        Assert.Equal(reference.Dump(), subject.Dump());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Blob_rows_round_trip_byte_identically_across_the_out_of_line_threshold(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);

        // 255 stays inline, 256 goes out of line, 100k exercises a multi-page payload; null and
        // empty are the degenerate framings.
        int[] sizes = [0, 1, 255, 256, 257, 4096, 100_000];
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < sizes.Length; i++)
                ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = 0, Data = MakeBlob(i, sizes[i]) });
            ctx.Db.Insert(new TerrainBlob { ChunkId = 100, Region = 0, Data = null! });
        });

        harness.Invoke("read", ctx =>
        {
            for (var i = 0; i < sizes.Length; i++)
            {
                var row = ctx.Db.Find<TerrainBlob>((long)i);
                Assert.NotNull(row);
                Assert.Equal(MakeBlob(i, sizes[i]), row.Value.Data);
            }

            Assert.Null(ctx.Db.Find<TerrainBlob>(100L)!.Value.Data);
        });

        var before = harness.Dump();
        harness.Restart();
        Assert.Equal(before, harness.Dump());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void AutoInc_never_reassigns_across_restart(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        var first = 0UL;
        harness.Invoke("seed", ctx => first = ctx.Db.Insert(new Creature { ChunkId = 0, Name = "a", X = 0 }).Id);
        harness.Restart();
        var second = 0UL;
        harness.Invoke("more", ctx => second = ctx.Db.Insert(new Creature { ChunkId = 0, Name = "b", X = 0 }).Id);
        Assert.True(second > first, $"id {second} must be allocated past recovered id {first}");
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Existence_apis_answer_without_materializing(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("empty-checks", ctx =>
        {
            Assert.False(ctx.Db.Any<Creature>());
            Assert.Equal(0, ctx.Db.Count<Creature>());
            Assert.Null(ctx.Db.First<Creature>());
        });

        harness.Invoke("seed", ctx =>
        {
            for (var i = 1; i <= 5; i++)
                ctx.Db.Insert(new ItemDefinition { Id = i, Name = $"i{i}", Value = i });
        });

        Assert.Throws<RejectedException>(() => harness.Invoke("checks", ctx =>
        {
            Assert.True(ctx.Db.Any<ItemDefinition>());
            Assert.Equal(5, ctx.Db.Count<ItemDefinition>());
            Assert.Equal(1, ctx.Db.First<ItemDefinition>()!.Value.Id);

            // Overlay-aware: pending ops adjust the answer inside the transaction.
            ctx.Db.Insert(new ItemDefinition { Id = 0, Name = "pending", Value = 0 });
            ctx.Db.Delete<ItemDefinition>(5);
            Assert.Equal(5, ctx.Db.Count<ItemDefinition>());
            Assert.Equal(0, ctx.Db.First<ItemDefinition>()!.Value.Id);
            throw new RejectedException("abort: checks only");
        }));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Store_count_and_scankeys_agree_with_scan(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            for (var i = 0; i < 12; i++)
                ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = 0, Data = MakeBlob(i, 500) });
        });

        var table = harness.Engine.Schema.Get(typeof(TerrainBlob)).Id;
        var store = harness.Engine.HotStore;
        Assert.Equal(12, store.Count(table));
        Assert.Equal(store.Scan(table).Select(p => p.Key).ToList(), store.ScanKeys(table).ToList());
    }

    /// <summary>
    /// <c>ScanKeyRange</c> against both engines, and against both of the FASTER store's key
    /// directories — a paged table's is its directory of hybrid-log locations, a resident table's
    /// is its row map, and the two are different objects behind the same call.
    /// <para>
    /// The window is the assertion; the <em>cost</em> of reaching it cannot be seen from out here,
    /// which is why <c>RowDirectoryTests</c> counts comparisons instead. What this pins is that
    /// seeking did not change which rows a window holds — the failure a seek off by one would
    /// produce is a silently short answer, and a subscription would report it as "no terrain here".
    /// </para>
    /// </summary>
    [Theory]
    [MemberData(nameof(Stores))]
    public void Scan_key_range_answers_the_window_on_paged_and_resident_tables(StoreKind kind)
    {
        using var harness = new StoreHarness(kind);
        harness.Invoke("seed", ctx =>
        {
            // Sparse on purpose: bounds that name no row must still land on the right neighbours.
            for (var i = 0; i < 40; i += 2)
            {
                ctx.Db.Insert(new TerrainBlob { ChunkId = i, Region = 0, Data = MakeBlob(i, 500) });
                ctx.Db.Insert(new ItemDefinition { Id = i, Name = $"item-{i}", Value = i });
            }
        });

        var store = harness.Engine.HotStore;
        foreach (var table in new[]
        {
            harness.Engine.Schema.Get(typeof(TerrainBlob)).Id,
            harness.Engine.Schema.Get(typeof(ItemDefinition)).Id,
        })
        {
            var all = store.ScanKeys(table).ToList();
            Assert.Equal(20, all.Count);

            // A window in the middle, by exact bounds and then by bounds that fall between rows.
            var exact = store.ScanKeyRange(table, all[5], all[8]).ToList();
            Assert.Equal(all.GetRange(5, 4), exact);
            Assert.Equal(all.GetRange(5, 4), store.ScanKeyRange(table, all[5], all[8]).ToList());

            // Both ends, a single-key window, and one past the last key.
            Assert.Equal([all[0]], store.ScanKeyRange(table, all[0], all[0]).ToList());
            Assert.Equal(all, store.ScanKeyRange(table, all[0], all[^1]).ToList());
            Assert.Equal([all[^1]], store.ScanKeyRange(table, all[^1], all[^1]).ToList());
            Assert.Empty(store.ScanKeyRange(table, all[^1], all[0]).ToList());

            // And it agrees with the filtered walk it replaced, which is the regression itself.
            Assert.Equal(
                all.Where(k => k.CompareTo(all[3]) >= 0 && k.CompareTo(all[15]) <= 0).ToList(),
                store.ScanKeyRange(table, all[3], all[15]).ToList());
        }
    }

    private const ulong AutoIncFirstId = 1UL;

    internal static byte[] MakeBlob(int seed, int length)
    {
        var data = new byte[length];
        var value = unchecked((uint)(seed * 2654435761));
        for (var i = 0; i < length; i++)
        {
            value = value * 1664525 + 1013904223;
            data[i] = (byte)(value >> 24);
        }

        return data;
    }
}
