using MelangeDB.Cluster;

namespace MelangeDB.Cluster.Tests;

// ---- The reference spatial-game schema: a contiguous world of chunks, blocks of 4x4 chunks. ----

/// <summary>The tests' chunk-id encoding: <c>(cx &lt;&lt; 16) | cy</c> — the developer's knowledge, not MelangeDB's.</summary>
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

/// <summary>The player's position row — the anchor their migration follows.</summary>
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(ChunkId))]
public partial struct PlayerPos
{
    [PrimaryKey]
    public Identity PlayerId;

    [Index]
    public uint ChunkId;

    public int Steps;
}

/// <summary>Player-owned companion state (the inventory shape): shares the player's chunk, follows on handoff.</summary>
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(ChunkId))]
public partial struct Pack
{
    [PrimaryKey]
    public Identity PlayerId;

    [Index]
    public uint ChunkId;

    public int Gold;
}

/// <summary>A creature: its own anchor, transferring on crossing (the settled creature-AI decision).</summary>
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(ChunkId), Residency = Residency.Resident)]
public partial struct Critter
{
    [PrimaryKey]
    public Identity Id;

    [Index]
    public uint ChunkId;

    public Identity TargetPlayer;

    public bool HasTarget;

    public long Ticks;
}

/// <summary>Which shard owns each player's rows — the session-to-shard mapping, on the hub.</summary>
[Table(Public = true, Placement = Placement.Global)]
public partial struct PlayerShardMap
{
    [PrimaryKey]
    public Identity PlayerId;

    public ulong Shard;
}

/// <summary>Per-shard creature-AI timer: each shard ticks its own block's critters, nowhere else.</summary>
[Table(Scheduled = nameof(SpatialReducers.CritterTick))]
public partial struct CritterTicker
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;

    public int BlockX;

    public int BlockY;
}

public sealed class SpatialReducers
{
    /// <summary>The tests' world geometry; the fixture's strategy uses the same numbers.</summary>
    public const int BlockW = 4;

    public const int BlockH = 4;

    public static Identity CritterId(ulong seed) => Identity.Hash($"critter-{seed}");

    public static (int Bx, int By) BlockOfChunk(uint chunkId)
    {
        var chunk = Chunks.At(chunkId);
        return (Floor(chunk.Cx, BlockW), Floor(chunk.Cy, BlockH));

        static int Floor(int v, int d) => v < 0 && v % d != 0 ? v / d - 1 : v / d;
    }

    [Reducer]
    public void PlaceTerrain(ReducerContext ctx, uint chunkId)
    {
        if (ctx.Db.Terrain.Id.Find((ulong)chunkId) is null)
            ctx.Db.Terrain.Insert(new Terrain { Id = chunkId, ChunkId = chunkId, Biome = "dune" });
    }

    /// <summary>One step: the position row and its companion move in one local transaction.</summary>
    [Reducer]
    public void Move(ReducerContext ctx, uint chunkId)
    {
        if (ctx.Db.PlayerPos.PlayerId.Find(ctx.Caller) is { } pos)
            ctx.Db.PlayerPos.Update(pos with { ChunkId = chunkId, Steps = pos.Steps + 1 });
        else
            ctx.Db.PlayerPos.Insert(new PlayerPos { PlayerId = ctx.Caller, ChunkId = chunkId, Steps = 1 });

        if (ctx.Db.Pack.PlayerId.Find(ctx.Caller) is { } pack)
            ctx.Db.Pack.Update(pack with { ChunkId = chunkId });
        else
            ctx.Db.Pack.Insert(new Pack { PlayerId = ctx.Caller, ChunkId = chunkId, Gold = 0 });
    }

    [Reducer]
    public void EarnGold(ReducerContext ctx, int amount)
    {
        var pack = ctx.Db.Pack.PlayerId.Find(ctx.Caller) ?? throw new RejectedException("no pack; Move first");
        ctx.Db.Pack.Update(pack with { Gold = pack.Gold + amount });
    }

    [Reducer]
    public void SpawnCritter(ReducerContext ctx, ulong seed, uint chunkId) =>
        ctx.Db.Critter.Insert(new Critter { Id = CritterId(seed), ChunkId = chunkId });

    /// <summary>The critter aggros onto the calling player and will chase them, boundaries included.</summary>
    [Reducer]
    public void AggroCritter(ReducerContext ctx, ulong seed)
    {
        var critter = ctx.Db.Critter.Id.Find(CritterId(seed)) ?? throw new RejectedException("no such critter");
        ctx.Db.Critter.Update(critter with { TargetPlayer = ctx.Caller, HasTarget = true });
    }

    /// <summary>Writes a critter row directly — the read-only border-band invariant test's probe.</summary>
    [Reducer]
    public void ShoveCritter(ReducerContext ctx, ulong seed, uint chunkId)
    {
        var critter = ctx.Db.Critter.Id.Find(CritterId(seed)) ?? throw new RejectedException("no such critter");
        ctx.Db.Critter.Update(critter with { ChunkId = chunkId });
    }

    [Reducer]
    public void DespawnCritter(ReducerContext ctx, ulong seed) => ctx.Db.Critter.Id.Delete(CritterId(seed));

    [Reducer]
    public void ScheduleCritterTick(ReducerContext ctx, long everyMs, int bx, int by) =>
        ctx.Db.CritterTicker.Insert(new CritterTicker
        {
            ScheduledAt = ScheduleAt.Interval(TimeSpan.FromMilliseconds(everyMs)),
            BlockX = bx,
            BlockY = by,
        });

    /// <summary>
    /// The per-shard AI tick. It filters to critters whose chunk resolves to this shard's block —
    /// the convention scheduled reducers over Partitioned tables must follow, because border-band
    /// copies of the neighbours' critters are present in this engine and read-only (the guard
    /// makes a violation loud, and one throwing row would abort the whole tick).
    /// </summary>
    [Reducer]
    public void CritterTick(ReducerContext ctx, CritterTicker timer)
    {
        foreach (var critter in ctx.Db.Critter.Iter().ToList())
        {
            if (BlockOfChunk(critter.ChunkId) != (timer.BlockX, timer.BlockY))
                continue;

            var next = critter;
            if (critter.HasTarget && ctx.Db.PlayerPos.PlayerId.Find(critter.TargetPlayer) is { } target
                && target.ChunkId != critter.ChunkId)
            {
                var here = Chunks.At(critter.ChunkId);
                var there = Chunks.At(target.ChunkId);
                next = next with
                {
                    ChunkId = Chunks.Id(here.Cx + Math.Sign(there.Cx - here.Cx), here.Cy + Math.Sign(there.Cy - here.Cy)),
                };
            }

            ctx.Db.Critter.Update(next with { Ticks = next.Ticks + 1 });
        }
    }

    /// <summary>Hub-side: records which shard owns the caller's rows (written by the transfer listener).</summary>
    [Reducer]
    public void SetPlayerShard(ReducerContext ctx, ulong shard)
    {
        if (ctx.Db.PlayerShardMap.PlayerId.Find(ctx.Caller) is { } existing)
            ctx.Db.PlayerShardMap.Update(existing with { Shard = shard });
        else
            ctx.Db.PlayerShardMap.Insert(new PlayerShardMap { PlayerId = ctx.Caller, Shard = shard });
    }
}

/// <summary>The rows that follow a migrating entity: a player's position and pack, or a critter.</summary>
public sealed class SpatialHandoffSet : IHandoffSet
{
    public void Collect(Identity anchor, ShardKey shard, IDbView shardDb, IHandoffCollector rows)
    {
        if (shardDb.Find<PlayerPos>(anchor) is { } pos)
            rows.Add(pos);
        if (shardDb.Find<Pack>(anchor) is { } pack)
            rows.Add(pack);
        if (shardDb.Find<Critter>(anchor) is { } critter)
            rows.Add(critter);
    }
}
