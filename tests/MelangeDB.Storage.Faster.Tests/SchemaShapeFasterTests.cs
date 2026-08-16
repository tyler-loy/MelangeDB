using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// Hot-tier schema migration under the FASTER store (road-to-0.2 phase 16). The store's files are
/// a projection recovery rebuilds, so a migration boot is proof the whole paged pipeline — index
/// re-extraction, directory entries, blob splitting — works from re-encoded bytes rather than the
/// bytes the files were built from.
/// </summary>
public class SchemaShapeFasterTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("melange-shape-faster-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    public struct MobV1
    {
        public ulong Id;
        public int ChunkId;
        public string Name;
    }

    public struct MobV2
    {
        public ulong Id;
        public int ChunkId;
        public long Health;
        public string Name;
    }

    private MelangeDbOptions OptionsFor() => new()
    {
        HotStore =
        {
            Path = Path.Combine(_root, "hot"),
            Engine = HotStoreEngine.Faster,
            MemoryBudgetBytes = 8 * 1024 * 1024,
        },
        CommitLog = { Path = Path.Combine(_root, "log") },
        Resume = { RetentionWindowSeconds = 0 },
    };

    private static MelangeEngine Boot(MelangeDbOptions options, TableSchema table) =>
        new(options, new SchemaRegistry([table]), null, null, new FasterHotStoreProvider());

    private static TableSchema Declare<TRow>() where TRow : struct
    {
        var columns = typeof(TRow).GetFields()
            .OrderBy(f => f.MetadataToken)
            .Select(f => new ColumnSchema
            {
                Name = f.Name,
                ClrType = f.FieldType,
                Kind = f.FieldType == typeof(ulong) ? ColumnKind.UInt64
                    : f.FieldType == typeof(int) ? ColumnKind.Int32
                    : f.FieldType == typeof(long) ? ColumnKind.Int64
                    : ColumnKind.String,
                IsPrimaryKey = f.Name == "Id",
                IsIndexed = f.Name == "ChunkId",
                GetValue = row => f.GetValue(row),
                SetValue = (row, value) => f.SetValue(row, value),
            })
            .ToList();
        return new TableSchema(typeof(TRow), "Mob", columns);
    }

    [Fact]
    public void A_migration_boot_rebuilds_the_paged_store_and_its_indexes_from_re_encoded_bytes()
    {
        var options = OptionsFor();
        using (var v1 = Boot(options, Declare<MobV1>()))
        {
            for (var i = 0; i < 25; i++)
            {
                var id = i;
                v1.Invoke("seed", Identity.Hash("test"), ctx =>
                    ctx.Db.Insert(new MobV1 { Id = (ulong)(id + 1), ChunkId = id % 5, Name = $"mob{id}" }));
            }

            Assert.NotNull(v1.TakeSnapshot());
        }

        using (var v2 = Boot(options, Declare<MobV2>()))
        {
            var table = v2.Schema.Get(typeof(MobV2));
            var rows = v2.HotStore.Scan(table.Id)
                .Select(pair => (MobV2)RowSerializer.Deserialize(table, pair.Value))
                .OrderBy(m => m.Id)
                .ToList();
            Assert.Equal(25, rows.Count);
            Assert.All(rows, m => Assert.Equal(0L, m.Health));
            Assert.Equal("mob0", rows[0].Name);
            Assert.Equal("mob24", rows[24].Name);

            // The index on ChunkId rebuilt from bytes whose Name slice moved when Health landed.
            Assert.Equal(5, v2.HotStore.ScanIndex(table.Id, "ChunkId", SchemaKeyCodec.Encode(table.Column("ChunkId"), 2)).Count());

            v2.Invoke("later", Identity.Hash("test"), ctx =>
                ctx.Db.Insert(new MobV2 { Id = 100, ChunkId = 2, Health = 50, Name = "fresh" }));
        }

        // The boot after the sealed migration reads the new-shape snapshot; nothing left to map.
        using var again = Boot(options, Declare<MobV2>());
        var reread = again.Schema.Get(typeof(MobV2));
        Assert.Equal(26, again.HotStore.Scan(reread.Id).Count());
        Assert.Equal(6, again.HotStore.ScanIndex(reread.Id, "ChunkId", SchemaKeyCodec.Encode(reread.Column("ChunkId"), 2)).Count());
    }
}
