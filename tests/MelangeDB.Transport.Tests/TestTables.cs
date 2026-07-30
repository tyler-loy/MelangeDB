namespace MelangeDB.Transport.Tests;

/// <summary>Terrain-shaped: blob rows addressed by id, spatially indexed — the range-streaming table.</summary>
[Table(Public = true)]
public partial struct Chunk
{
    [PrimaryKey]
    public long Id;

    [Index]
    public long X;

    public byte[] Data;
}

/// <summary>Player-shaped: identity-keyed with an equality-indexed room.</summary>
[Table(Public = true)]
public partial struct PlayerState
{
    [PrimaryKey]
    public Identity Id;

    [Index]
    public int RoomId;

    public string Name;

    public float X;
}

/// <summary>Skill-shaped: the projection table — deltas must stay silent for non-projected columns.</summary>
[Table(Public = true)]
public partial struct Skill
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public long PlayerNum;

    public string Name;

    public long TotalXp;

    public int Level;
}

/// <summary>Private: no subscription may name it, and the error must not confirm it exists.</summary>
[Table]
public partial struct SecretTable
{
    [PrimaryKey]
    public ulong Id;

    public string Data;
}

public sealed class TransportReducers
{
    [Reducer]
    public void SetChunk(ReducerContext ctx, long id, long x, byte[] data)
    {
        var chunk = new Chunk { Id = id, X = x, Data = data };
        if (ctx.Db.Chunk.Id.Find(id) is not null)
            ctx.Db.Chunk.Update(chunk);
        else
            ctx.Db.Chunk.Insert(chunk);
    }

    [Reducer]
    public void DeleteChunk(ReducerContext ctx, long id) => ctx.Db.Chunk.Id.Delete(id);

    [Reducer]
    public void Spawn(ReducerContext ctx, string name, int roomId)
    {
        var player = new PlayerState { Id = ctx.Caller, RoomId = roomId, Name = name, X = 0 };
        if (ctx.Db.PlayerState.Id.Find(ctx.Caller) is not null)
            ctx.Db.PlayerState.Update(player);
        else
            ctx.Db.PlayerState.Insert(player);
    }

    [Reducer]
    public void Move(ReducerContext ctx, float x)
    {
        var player = ctx.Db.PlayerState.Id.Find(ctx.Caller)
            ?? throw new RejectedException("not spawned");
        ctx.Db.PlayerState.Update(player with { X = x });
    }

    [Reducer]
    public void AddSkill(ReducerContext ctx, long playerNum, string name, long xp, int level) =>
        ctx.Db.Skill.Insert(new Skill { PlayerNum = playerNum, Name = name, TotalXp = xp, Level = level });

    [Reducer]
    public void SetSkillXp(ReducerContext ctx, ulong id, long xp)
    {
        var skill = ctx.Db.Skill.Id.Find(id) ?? throw new RejectedException("no such skill");
        ctx.Db.Skill.Update(skill with { TotalXp = xp });
    }

    [Reducer]
    public void SetSkillLevel(ReducerContext ctx, ulong id, int level)
    {
        var skill = ctx.Db.Skill.Id.Find(id) ?? throw new RejectedException("no such skill");
        ctx.Db.Skill.Update(skill with { Level = level });
    }

    [Reducer]
    public void AddSecret(ReducerContext ctx, ulong id, string data) =>
        ctx.Db.SecretTable.Insert(new SecretTable { Id = id, Data = data });

    [Reducer]
    public void Noop(ReducerContext ctx)
    {
    }
}
