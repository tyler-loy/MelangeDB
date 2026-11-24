using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// <c>HotStore:NativeDirectory</c>: the paged store's key directory and indexes in native
/// copy-on-write trees. Whatever the representation, the store must answer every lookup, key walk
/// and index scan the same; a read view must keep reading its version through any number of later
/// commits and collections; and the bookkeeping must survive tier migrations and a restart. CI also
/// runs the whole suite a second time with MELANGE_TEST_NATIVE_DIRECTORY=0, the managed trees.
/// </summary>
public sealed class NativeDirectoryTests
{
    private static readonly TableId Creatures = TableId.FromName("Creature");

    [Fact]
    public void Native_and_managed_directories_answer_identically_through_random_writes_and_a_restart()
    {
        using var native = Harness(native: true);
        using var managed = Harness(native: false);
        var random = new Random(20261124);
        var live = new List<ulong>();
        for (var round = 0; round < 60; round++)
        {
            // One transaction of up to 20 writes: inserts, moves between chunks, deletes.
            var ops = Enumerable.Range(0, random.Next(1, 21)).Select(_ => (Kind: random.Next(10), Chunk: random.Next(12), Pick: random.Next())).ToArray();
            void Apply(StoreHarness harness, List<ulong> ids, bool record)
            {
                harness.Invoke("write", ctx =>
                {
                    foreach (var (kind, chunk, pick) in ops)
                    {
                        if (kind < 5 || ids.Count == 0)
                        {
                            var inserted = ctx.Db.Insert(new Creature { ChunkId = chunk, Name = $"c{pick}", X = pick % 100 });
                            if (record)
                                ids.Add(inserted.Id);
                        }
                        else if (kind < 8)
                        {
                            var id = ids[pick % ids.Count];
                            ctx.Db.Update(ctx.Db.Find<Creature>(id)!.Value with { ChunkId = chunk });
                        }
                        else
                        {
                            var id = ids[pick % ids.Count];
                            ctx.Db.Delete<Creature>(id);
                            if (record)
                                ids.Remove(id);
                        }
                    }
                });
            }

            var managedIds = new List<ulong>(live);
            Apply(native, live, record: true);
            Apply(managed, managedIds, record: true);
            Assert.Equal(live, managedIds);
            AssertSame(native, managed);
        }

        Assert.True(native.Engine.HotStore.Statistics().NativeDirectoryBytes > 0);
        Assert.Equal(0, managed.Engine.HotStore.Statistics().NativeDirectoryBytes);

        native.Restart();
        AssertSame(native, managed);
    }

    /// <summary>
    /// Every commit supersedes a version, and a superseded version's copied pages must go back as
    /// soon as no reader holds it, not when a collection happens to run: the native directory
    /// allocates almost nothing managed, so collections are rare, and waiting for them let a
    /// million single-row updates pile up gigabytes of dead pages.
    /// </summary>
    [Fact]
    public void Superseded_versions_free_their_pages_without_waiting_for_a_collection()
    {
        using var harness = Harness(native: true);
        for (var i = 0; i < 2_000; i++)
        {
            var chunk = i % 16;
            harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = chunk, Name = $"c{chunk}" }));
        }

        var store = harness.Engine.HotStore;
        var settled = store.Statistics().NativeDirectoryBytes;
        var peak = settled;
        var random = new Random(7);

        // No collection may run during the updates, which is the production case the GC path got
        // wrong: the native directory allocates almost nothing managed. Each commit copies a page
        // or two per tree, so a thousand that waited for the GC would leave megabytes behind.
        Assert.True(GC.TryStartNoGCRegion(200L << 20), "could not start a no-GC region");
        try
        {
            for (var i = 0; i < 1_000; i++)
            {
                var id = (ulong)random.Next(1, 2_001);
                var chunk = random.Next(16);
                harness.Invoke("move", ctx => ctx.Db.Update(ctx.Db.Find<Creature>(id)!.Value with { ChunkId = chunk }));
                peak = Math.Max(peak, store.Statistics().NativeDirectoryBytes);
            }

            Assert.Equal(System.Runtime.GCLatencyMode.NoGCRegion, System.Runtime.GCSettings.LatencyMode);
        }
        finally
        {
            if (System.Runtime.GCSettings.LatencyMode == System.Runtime.GCLatencyMode.NoGCRegion)
                GC.EndNoGCRegion();
        }

        // Read after every commit, with no collection to rescue it: only deterministic release
        // keeps the peak near the live trees.
        Assert.True(peak <= settled * 2 + (256 << 10), $"native directory peaked at {peak >> 10} KiB; {settled >> 10} KiB live before the updates");
    }

    [Fact]
    public void A_read_view_keeps_its_version_through_later_commits_and_collections()
    {
        using var harness = Harness(native: true);
        for (var i = 0; i < 200; i++)
        {
            var chunk = i % 4;
            harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = chunk, Name = $"c{chunk}" }));
        }

        var store = harness.Engine.HotStore;
        using var view = ((IReadViewSource)store).OpenReadView();
        var before = view.ScanIndex(Creatures, "ChunkId", ChunkKey(harness, 0)).Select(static r => r.Key).ToArray();
        var keysBefore = view.Scan(Creatures).Select(static r => r.Key).ToArray();

        for (var round = 0; round < 50; round++)
        {
            harness.Invoke("churn", ctx =>
            {
                foreach (var creature in ctx.Db.Scan<Creature>().Take(20).ToArray())
                    ctx.Db.Update(creature with { ChunkId = (creature.ChunkId + 1) % 4 });
                ctx.Db.Insert(new Creature { ChunkId = 0, Name = "late" });
            });
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.Equal(before, view.ScanIndex(Creatures, "ChunkId", ChunkKey(harness, 0)).Select(static r => r.Key).ToArray());
        Assert.Equal(keysBefore, view.Scan(Creatures).Select(static r => r.Key).ToArray());
        Assert.Equal(200 + 50, store.Count(Creatures));
    }

    [Fact]
    public void The_directory_survives_promotion_and_demotion()
    {
        using var harness = Harness(native: true);
        for (var i = 0; i < 300; i++)
        {
            var chunk = i % 5;
            harness.Invoke("seed", ctx => ctx.Db.Insert(new Creature { ChunkId = chunk, Name = "x" }));
        }

        var store = (IResidencyControl)harness.Engine.HotStore;
        store.ApplyResidency("Creature", Residency.Resident);
        Assert.Equal(60, harness.Engine.HotStore.ScanIndex(Creatures, "ChunkId", ChunkKey(harness, 3)).Count());
        store.ApplyResidency("Creature", Residency.Paged);
        Assert.Equal(300, harness.Engine.HotStore.Count(Creatures));
        Assert.Equal(60, harness.Engine.HotStore.ScanIndex(Creatures, "ChunkId", ChunkKey(harness, 3)).Count());
        harness.Invoke("after", ctx => ctx.Db.Insert(new Creature { ChunkId = 3, Name = "y" }));
        Assert.Equal(61, harness.Engine.HotStore.ScanIndex(Creatures, "ChunkId", ChunkKey(harness, 3)).Count());
    }

    /// <summary>
    /// Keys and index values far longer than a directory page holds inline (1 KiB pages keep
    /// entries up to about 240 bytes in the page): they move out of line, and every lookup, index
    /// scan, update, delete and the restart must still agree with the managed directory.
    /// </summary>
    [Fact]
    public void Long_keys_and_index_values_live_out_of_line_and_answer_the_same()
    {
        using var native = Harness(native: true, typeof(LongKeyed));
        using var managed = Harness(native: false, typeof(LongKeyed));
        var table = TableId.FromName("LongKeyed");
        var random = new Random(11);
        string Text(int length, int seed) => new string((char)('a' + seed % 26), length) + seed;
        // Row keys stay under MelangeLog's 1 KiB key limit; index values are directory data only, so may run longer.
        var keys = Enumerable.Range(0, 150).Select(i => Text(300 + random.Next(500), i)).ToArray();
        var tags = Enumerable.Range(0, 6).Select(i => Text(250 + random.Next(1_000), 1_000 + i)).ToArray();

        foreach (var harness in new[] { native, managed })
        {
            var r = new Random(3);
            for (var i = 0; i < keys.Length; i++)
            {
                var (key, tag) = (keys[i], tags[r.Next(tags.Length)]);
                harness.Invoke("insert", ctx => ctx.Db.Insert(new LongKeyed { Key = key, Tag = tag, Version = 0 }));
            }

            for (var i = 0; i < keys.Length; i += 3)
            {
                var (key, tag) = (keys[i], tags[r.Next(tags.Length)]);
                harness.Invoke("retag", ctx => ctx.Db.Update(new LongKeyed { Key = key, Tag = tag, Version = 1 }));
            }

            for (var i = 1; i < keys.Length; i += 7)
            {
                var key = keys[i];
                harness.Invoke("delete", ctx => ctx.Db.Delete<LongKeyed>(key));
            }
        }

        void Agree()
        {
            var schema = native.Engine.Schema.Get(typeof(LongKeyed));
            Assert.Equal(managed.Engine.HotStore.Count(table), native.Engine.HotStore.Count(table));
            Assert.Equal(managed.Engine.HotStore.ScanKeys(table), native.Engine.HotStore.ScanKeys(table));
            foreach (var tag in tags)
            {
                var value = SchemaKeyCodec.Encode(schema.Column("Tag"), tag);
                Assert.Equal(
                    managed.Engine.HotStore.ScanIndex(table, "Tag", value).Select(static r => r.Key),
                    native.Engine.HotStore.ScanIndex(table, "Tag", value).Select(static r => r.Key));
            }

            foreach (var key in keys)
                Assert.Equal(managed.Engine.HotStore.ContainsKey(table, SchemaKeyCodec.Encode(schema.PrimaryKey, key)), native.Engine.HotStore.ContainsKey(table, SchemaKeyCodec.Encode(schema.PrimaryKey, key)));
        }

        Agree();
        native.Restart();
        Agree();
    }

    private static void AssertSame(StoreHarness native, StoreHarness managed)
    {
        var a = native.Engine.HotStore;
        var b = managed.Engine.HotStore;
        Assert.Equal(b.Count(Creatures), a.Count(Creatures));
        Assert.Equal(
            b.Scan(Creatures).Select(static r => (r.Key, r.Value.ToArray())),
            a.Scan(Creatures).Select(static r => (r.Key, r.Value.ToArray())),
            static (x, y) => x.Key == y.Key && x.Item2.AsSpan().SequenceEqual(y.Item2));
        for (var chunk = 0; chunk < 12; chunk++)
        {
            Assert.Equal(
                b.ScanIndex(Creatures, "ChunkId", ChunkKey(managed, chunk)).Select(static r => r.Key),
                a.ScanIndex(Creatures, "ChunkId", ChunkKey(native, chunk)).Select(static r => r.Key));
        }

        Assert.Equal(
            b.ScanIndexRange(Creatures, "ChunkId", ChunkKey(managed, 3), ChunkKey(managed, 8)).Select(static r => r.Key),
            a.ScanIndexRange(Creatures, "ChunkId", ChunkKey(native, 3), ChunkKey(native, 8)).Select(static r => r.Key));
    }

    private static RowKey ChunkKey(StoreHarness harness, int chunk) =>
        SchemaKeyCodec.Encode(harness.Engine.Schema.Get(typeof(Creature)).Column("ChunkId"), chunk);

    private static StoreHarness Harness(bool native, Type? table = null) =>
        new(
            StoreKind.Faster,
            options =>
            {
                options.Residency.Default = Residency.Paged;
                options.HotStore.NativeDirectory = native;
            },
            tables: [table ?? typeof(Creature)],
            directoryOverride: false);
}
