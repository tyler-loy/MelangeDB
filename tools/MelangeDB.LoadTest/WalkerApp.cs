using MelangeDB.Cluster;
using Microsoft.Extensions.DependencyInjection;

namespace MelangeDB.LoadTest;

// ---- The load-test workload schema: the spatial test app's shape, sized for crowds. ----
// A contiguous world of chunks, blocks of WxH chunks per shard, players walking between them.

/// <summary>The tool's chunk-id encoding: <c>(cx &lt;&lt; 16) | cy</c> — the same packing the cluster tests use.</summary>
public static class Chunks
{
    public static uint Id(int cx, int cy) => (uint)((cx << 16) | (ushort)cy);

    public static ChunkPosition At(ulong id) => new((int)(id >> 16), (int)(id & 0xFFFF));
}

/// <summary>Static world state: one row per chunk. Never migrates — terrain has no anchor.</summary>
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(ChunkId))]
public partial struct Terrain
{
    [PrimaryKey]
    public ulong Id;

    [Index]
    public uint ChunkId;

    public string Biome;
}

/// <summary>
/// The player's position row — the anchor their migration follows. <see cref="Seq"/> is the
/// driver's call-site sequence number: embedded by the Move call and read back off the client's
/// own subscription delta, it is what makes call-to-delta latency measurable per tick.
/// </summary>
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(ChunkId))]
public partial struct PlayerPos
{
    [PrimaryKey]
    public Identity PlayerId;

    [Index]
    public uint ChunkId;

    public long Seq;
}

/// <summary>
/// The failover measurement's shard heat: rows whose only job is to be recovered. A crowded
/// shard's rebuild cost is its row count and bytes, whatever the rows mean, so the sweep varies
/// these and holds everything else still. Paged by default, like the reference workload's world.
/// </summary>
[Table(Placement = Placement.Partitioned, ShardBy = nameof(ChunkId))]
public partial struct Ballast
{
    [PrimaryKey]
    public long Id;

    [Index]
    public uint ChunkId;

    public byte[] Payload;
}

/// <summary>Which shard owns each player's rows — the session-to-shard mapping, on the hub.</summary>
[Table(Public = true, Placement = Placement.Global)]
public partial struct PlayerShardMap
{
    [PrimaryKey]
    public Identity PlayerId;

    public ulong Shard;
}

public sealed class WalkerReducers
{
    [Reducer]
    public void PlaceTerrain(ReducerContext ctx, uint chunkId)
    {
        if (ctx.Db.Terrain.Id.Find((ulong)chunkId) is null)
            ctx.Db.Terrain.Insert(new Terrain { Id = chunkId, ChunkId = chunkId, Biome = "dune" });
    }

    /// <summary>One tick: the position row moves (or stays put) in one local transaction.</summary>
    [Reducer]
    public void Move(ReducerContext ctx, uint chunkId, long seq)
    {
        if (ctx.Db.PlayerPos.PlayerId.Find(ctx.Caller) is { } pos)
            ctx.Db.PlayerPos.Update(pos with { ChunkId = chunkId, Seq = seq });
        else
            ctx.Db.PlayerPos.Insert(new PlayerPos { PlayerId = ctx.Caller, ChunkId = chunkId, Seq = seq });
    }

    /// <summary>Failover seeding: one transaction inserting <paramref name="count"/> ballast rows in one chunk.</summary>
    [Reducer]
    public void SeedBallast(ReducerContext ctx, uint chunkId, long firstId, int count, int payloadBytes)
    {
        for (var i = 0; i < count; i++)
        {
            var payload = new byte[payloadBytes];
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(payload, firstId + i);
            ctx.Db.Ballast.Insert(new Ballast { Id = firstId + i, ChunkId = chunkId, Payload = payload });
        }
    }

    /// <summary>
    /// Hub-side: records which shard owns the caller's rows. Called once by each client to place
    /// its spawn (so the world starts populated across shards, not queued behind one), then kept
    /// current by the transfer listener as handoffs move the player.
    /// </summary>
    [Reducer]
    public void SetPlayerShard(ReducerContext ctx, ulong shard)
    {
        if (ctx.Db.PlayerShardMap.PlayerId.Find(ctx.Caller) is { } existing)
            ctx.Db.PlayerShardMap.Update(existing with { Shard = shard });
        else
            ctx.Db.PlayerShardMap.Insert(new PlayerShardMap { PlayerId = ctx.Caller, Shard = shard });
    }
}

/// <summary>The rows that follow a migrating player: just the position row in this workload.</summary>
public sealed class WalkerHandoffSet : IHandoffSet
{
    public void Collect(Identity anchor, ShardKey shard, IDbView shardDb, IHandoffCollector rows)
    {
        if (shardDb.Find<PlayerPos>(anchor) is { } pos)
            rows.Add(pos);
    }
}

/// <summary>The position row anchors migration, with hysteresis — players pace, creatures don't exist here.</summary>
public sealed class WalkerAnchors : IMigrationAnchors
{
    public MigrationAnchor? AnchorOf(MelangeDB.Core.TableSchema table, in RowRef row) => table.Name switch
    {
        nameof(PlayerPos) => new MigrationAnchor((Identity)row.Column(nameof(PlayerPos.PlayerId))!, Immediate: false),
        _ => null,
    };
}

/// <summary>Keeps the hub's session-to-shard map current: the locator reads what this writes. Idempotent.</summary>
public sealed class WalkerTransferListener(IServiceProvider services) : IShardTransferListener
{
    public void OnTransferred(Identity entity, ShardKey from, ShardKey to) =>
        services.GetRequiredService<MelangeDB.Core.MelangeReducerHost>()
            .Call("SetPlayerShard", entity, to.Value);
}
