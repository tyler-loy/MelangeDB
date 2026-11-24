using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Phase 30 step 2c on the paged store: a follower bootstraps through FASTER's bulk recovery (the
/// snapshot loaded into builders, published once) and then applies the tail record by record,
/// ending with the same rows and the same secondary index as the owner.
/// </summary>
public class FasterLogFollowerTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-faster-follower-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A handle Windows has not released yet; the temp reaper gets it.
        }
    }

    public struct Mob
    {
        public ulong Id;
        public int ChunkId;
        public string Name;
    }

    private static TableSchema Declare()
    {
        var columns = typeof(Mob).GetFields()
            .OrderBy(f => f.MetadataToken)
            .Select(f => new ColumnSchema
            {
                Name = f.Name,
                ClrType = f.FieldType,
                Kind = f.FieldType == typeof(ulong) ? ColumnKind.UInt64 : f.FieldType == typeof(int) ? ColumnKind.Int32 : ColumnKind.String,
                IsPrimaryKey = f.Name == "Id",
                IsIndexed = f.Name == "ChunkId",
                GetValue = row => f.GetValue(row),
                SetValue = (row, value) => f.SetValue(row, value),
            })
            .ToList();
        return new TableSchema(typeof(Mob), "Mob", columns);
    }

    private MelangeDbOptions Options(string hot) => new()
    {
        HotStore = { Path = Path.Combine(_root, hot), Engine = HotStoreEngine.Faster, MemoryBudgetBytes = 8 * 1024 * 1024 },
        CommitLog = { Path = Path.Combine(_root, "log") },
        Resume = { RetentionWindowSeconds = 0 },
    };

    [Fact]
    public void A_follower_on_the_paged_store_matches_the_owner_through_bootstrap_and_tail()
    {
        var registry = new SchemaRegistry([Declare()]);
        var table = registry.Get(typeof(Mob));
        using var owner = new MelangeEngine(Options("owner-hot"), registry, null, null, TestEngine.Provider());
        for (var i = 0; i < 40; i++)
        {
            var id = i;
            owner.Invoke("seed", Identity.Hash("test"), ctx => ctx.Db.Insert(new Mob { Id = (ulong)(id + 1), ChunkId = id % 4, Name = $"mob{id}" }));
        }

        Assert.NotNull(owner.TakeSnapshot());
        for (var i = 40; i < 60; i++)
        {
            var id = i;
            owner.Invoke("tail", Identity.Hash("test"), ctx => ctx.Db.Insert(new Mob { Id = (ulong)(id + 1), ChunkId = id % 4, Name = $"mob{id}" }));
        }

        using var follower = new LogFollower(Options("follower-hot"), registry, originator: 0, hotStoreProvider: TestEngine.Provider());
        follower.Ceiling = owner.Log.DurableLsn;
        while (follower.Poll() > 0)
        {
        }

        Assert.Equal(owner.Log.HeadLsn, follower.AppliedLsn);
        var expected = owner.HotStore.Scan(table.Id).Select(static pair => Convert.ToHexStringLower(pair.Value.Span)).Order().ToList();
        var actual = follower.Store!.Scan(table.Id).Select(static pair => Convert.ToHexStringLower(pair.Value.Span)).Order().ToList();
        Assert.Equal(60, actual.Count);
        Assert.Equal(expected, actual);
        Assert.Equal(15, follower.Store.ScanIndex(table.Id, "ChunkId", SchemaKeyCodec.Encode(table.Column("ChunkId"), 2)).Count());
    }
}
