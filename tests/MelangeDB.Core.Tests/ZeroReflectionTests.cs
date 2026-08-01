using System.Runtime.CompilerServices;
using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Zero reflection on the invocation path. Proven two ways: behaviourally, by running a full
/// transaction against schemas whose reflection accessors throw on touch; and textually, by
/// scanning the emitted generated sources for reflection APIs.
/// </summary>
public class ZeroReflectionTests
{
    [Fact]
    public void Invocation_path_never_touches_reflection_accessors()
    {
        var poisoned = PoisonedRegistry(typeof(Player), typeof(InventoryItem), typeof(Registration), typeof(TerrainChunk));
        var root = Directory.CreateTempSubdirectory("melange-poison-").FullName;
        var options = new MelangeDbOptions
        {
            HotStore = { Path = Path.Combine(root, "hot") },
            CommitLog = { Path = Path.Combine(root, "log") },
        };
        try
        {
            using var engine = new MelangeEngine(options, poisoned);
            var alice = Identity.Hash("alice");
            engine.Invoke("Everything", alice, ctx =>
            {
                // Insert with AutoInc, indexed and unique columns, read-your-writes, equality and
                // range filters, update, delete — every op the invocation path offers, including
                // the store's index maintenance after commit.
                ctx.Db.Player.Insert(new Player { Id = alice, RoomId = 1, Name = "A" });
                ctx.Db.InventoryItem.Insert(new InventoryItem { Owner = alice, ItemName = "pick", Quantity = 1 });
                ctx.Db.Registration.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp });
                ctx.Db.TerrainChunk.Insert(new TerrainChunk { ChunkId = 3, Kind = ChunkKind.Ore, Data = [7] });

                var player = ctx.Db.Player.Id.Find(alice);
                Assert.NotNull(player);
                ctx.Db.Player.Update(player.Value with { RoomId = 2 });
                Assert.Single(ctx.Db.Player.RoomId.Filter(2));
                Assert.Single(ctx.Db.Player.RoomId.Filter(0, 5));
                Assert.Single(ctx.Db.Player.Iter());
            });

            engine.Invoke("Committed", alice, ctx =>
            {
                Assert.Single(ctx.Db.InventoryItem.Owner.Filter(alice));
                Assert.NotNull(ctx.Db.Registration.Email.Find("a@example.com"));
                Assert.True(ctx.Db.TerrainChunk.ChunkId.Delete(3L));
            });
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Generated_sources_contain_no_reflection_apis()
    {
        var objRoot = Path.Combine(RepoRoot(), "tests", "MelangeDB.Core.Tests", "obj");
        var generated = Directory
            .GetFiles(objRoot, "*.g.cs", SearchOption.AllDirectories)
            .Where(f => f.Contains("generated", StringComparison.OrdinalIgnoreCase))
            .ToList();
        Assert.NotEmpty(generated);

        string[] forbidden = ["System.Reflection", "Activator", ".GetType(", "GetMethod", "MethodInfo", ".Invoke("];
        foreach (var file in generated)
        {
            var source = File.ReadAllText(file);
            foreach (var api in forbidden)
                Assert.False(source.Contains(api, StringComparison.Ordinal), $"{file} contains forbidden '{api}'.");
        }
    }

    [Fact]
    public void Core_hot_path_sources_contain_no_reflection_apis()
    {
        // The reflection serializer and the startup-only model discovery are the two sanctioned
        // uses of reflection in Core; the invocation path itself must never grow one. This fails
        // the moment Type.GetType, MethodInfo.Invoke, or Activator appear in a hot-path file.
        string[] hotPathFiles =
        [
            "MelangeEngine.cs",
            "TransactionDb.cs",
            "WriteSet.cs",
            Path.Combine("Store", "InMemoryHotStore.cs"),
            Path.Combine("Serialization", "RowCodec.cs"),
            Path.Combine("Serialization", "RowWriter.cs"),
            Path.Combine("Serialization", "SchemaKeyCodec.cs"),
            Path.Combine("Serialization", "ReducerArgsReader.cs"),
            Path.Combine("Dispatch", "ReducerDescriptor.cs"),
            Path.Combine("Hosting", "MelangeReducerHost.cs"),
        ];
        string[] forbidden = ["System.Reflection", "MethodInfo", "GetMethod", "Type.GetType", "Activator.CreateInstance"];

        foreach (var file in hotPathFiles)
        {
            var path = Path.Combine(RepoRoot(), "src", "MelangeDB.Core", file);
            Assert.True(File.Exists(path), $"Hot-path file {file} moved; update this test's list.");
            var source = File.ReadAllText(path);
            foreach (var api in forbidden)
                Assert.False(source.Contains(api, StringComparison.Ordinal), $"{file} contains forbidden '{api}'.");
        }
    }

    [Fact]
    public void Generated_accessors_are_readonly_structs_that_wrap_the_view_without_copying()
    {
        // Settle-check for the struct-mutation decision: the handle and column accessors are
        // readonly structs holding only the IDbView reference, so obtaining ctx.Db.Player.Id
        // copies a reference, never a row.
        foreach (var type in new[]
        {
            typeof(MelangeDB.Generated.PlayerHandle),
            typeof(MelangeDB.Generated.PlayerIdAccessor),
            typeof(MelangeDB.Generated.PlayerRoomIdAccessor),
        })
        {
            Assert.True(type.IsValueType, $"{type.Name} is not a struct.");
            Assert.NotNull(type.GetCustomAttributes(typeof(IsReadOnlyAttribute), inherit: false).SingleOrDefault());
            var field = Assert.Single(type.GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic));
            Assert.Equal(typeof(IDbView), field.FieldType);
        }
    }

    private static SchemaRegistry PoisonedRegistry(params Type[] tables)
    {
        var generated = EngineHarness.GeneratedRegistry(tables);
        return new SchemaRegistry(generated.Tables.Select(schema => new TableSchema(
            schema.RowType,
            schema.Name,
            schema.Columns.Select(column => new ColumnSchema
            {
                Name = column.Name,
                ClrType = column.ClrType,
                Kind = column.Kind,
                IsEnum = column.IsEnum,
                IsPrimaryKey = column.IsPrimaryKey,
                IsAutoInc = column.IsAutoInc,
                IsUnique = column.IsUnique,
                IsIndexed = column.IsIndexed,
                GetValue = _ => throw new InvalidOperationException($"Reflection accessor for {schema.Name}.{column.Name} used on the invocation path."),
                SetValue = (_, _) => throw new InvalidOperationException($"Reflection accessor for {schema.Name}.{column.Name} used on the invocation path."),
            }).ToArray(),
            schema.IsPublic,
            schema.Tier,
            schema.Residency,
            schema.Placement,
            schema.ShardBy,
            schema.Scheduled,
            schema.Codec)));
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "MelangeDB.slnx")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        return directory.FullName;
    }
}
