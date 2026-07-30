using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// The generated codecs implement the same versioned v1 format as the reflection serializer —
/// proven both by byte equality and by replaying a reflection-written log through generated
/// registration.
/// </summary>
public class SerializerCompatTests
{
    [Fact]
    public void Log_written_via_reflection_path_replays_through_generated_serializers()
    {
        using var harness = new EngineHarness(useReflectionSchema: true);
        var alice = Identity.Hash("alice");
        harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new Player { Id = alice, RoomId = 7, X = 1.5f, Y = -2.5f, Name = "Alice" });
            ctx.Db.Insert(new Player { Id = Identity.Hash("nameless"), RoomId = 7, Name = null! });
            ctx.Db.Insert(new InventoryItem { Owner = alice, ItemName = "pick", Quantity = 3 });
            ctx.Db.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp });
            ctx.Db.Insert(new TerrainChunk { ChunkId = -42, Data = [1, 2, 255], Kind = ChunkKind.Ore });
            ctx.Db.Insert(new TerrainChunk { ChunkId = 9, Data = null!, Kind = ChunkKind.Empty });
        });
        var reflectionDump = harness.Dump();
        harness.Engine.Dispose();

        // Reopen the same log under generated registration: same projection bytes, and the
        // generated codecs read every row the reflection serializer wrote.
        using var engine = new MelangeEngine(harness.Options, EngineHarness.GeneratedRegistry(
            typeof(Player), typeof(InventoryItem), typeof(Registration), typeof(TerrainChunk)));
        Assert.Equal(reflectionDump, harness.Dump(engine.HotStore));

        engine.Invoke("Verify", EngineHarness.Caller, ctx =>
        {
            var player = ctx.Db.Player.Id.Find(alice);
            Assert.NotNull(player);
            Assert.Equal("Alice", player.Value.Name);
            Assert.Equal(1.5f, player.Value.X);
            Assert.Null(ctx.Db.Player.Id.Find(Identity.Hash("nameless"))!.Value.Name);

            var item = Assert.Single(ctx.Db.InventoryItem.Owner.Filter(alice));
            Assert.Equal("pick", item.ItemName);
            Assert.Equal(1UL, item.Id);

            var registration = ctx.Db.Registration.Email.Find("a@example.com");
            Assert.NotNull(registration);

            var chunk = ctx.Db.TerrainChunk.ChunkId.Find(-42L);
            Assert.Equal(new byte[] { 1, 2, 255 }, chunk!.Value.Data);
            Assert.Equal(ChunkKind.Ore, chunk.Value.Kind);
            Assert.Null(ctx.Db.TerrainChunk.ChunkId.Find(9L)!.Value.Data);
        });
    }

    [Fact]
    public void Generated_and_reflection_serializers_produce_identical_bytes()
    {
        var reflection = SchemaRegistry.FromTypes(typeof(Player), typeof(TerrainChunk), typeof(Registration));
        var generated = EngineHarness.GeneratedRegistry(typeof(Player), typeof(TerrainChunk), typeof(Registration));

        var player = new Player { Id = Identity.Hash("p"), RoomId = -3, X = float.MaxValue, Y = float.Epsilon, Name = "Zoë" };
        var chunk = new TerrainChunk { ChunkId = long.MinValue, Data = [0, 1, 2], Kind = ChunkKind.Rock };
        var registration = new Registration { Id = 12, Email = null!, CreatedAt = new Timestamp(1234567) };

        AssertSameBytes(reflection, generated, player);
        AssertSameBytes(reflection, generated, chunk);
        AssertSameBytes(reflection, generated, registration);
    }

    [Fact]
    public void Generated_codec_round_trips_reflection_bytes_and_vice_versa()
    {
        var reflection = SchemaRegistry.FromTypes(typeof(Player));
        var generated = EngineHarness.GeneratedRegistry(typeof(Player));
        var codec = Assert.IsType<RowCodec<Player>>(generated.Get(typeof(Player)).Codec, exactMatch: false);

        var original = new Player { Id = Identity.Hash("p"), RoomId = 42, X = -1f, Y = 2f, Name = "P" };
        var reflectionBytes = RowSerializer.Serialize(reflection.Get(typeof(Player)), original);
        var viaCodec = codec.Deserialize(reflectionBytes);
        Assert.Equal(original, viaCodec);

        var codecBytes = codec.Serialize(in original);
        var viaReflection = (Player)RowSerializer.Deserialize(reflection.Get(typeof(Player)), codecBytes);
        Assert.Equal(original, viaReflection);
    }

    private static void AssertSameBytes<TRow>(SchemaRegistry reflection, SchemaRegistry generated, TRow row)
        where TRow : struct
    {
        var reflectionBytes = RowSerializer.Serialize(reflection.Get(typeof(TRow)), row);
        var codec = Assert.IsType<RowCodec<TRow>>(generated.Get(typeof(TRow)).Codec, exactMatch: false);
        Assert.Equal(reflectionBytes, codec.Serialize(in row));
    }
}
