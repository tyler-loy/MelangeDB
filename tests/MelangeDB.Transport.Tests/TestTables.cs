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

/// <summary>Where an item sits — the union case: own pack is private, a world container is shared.</summary>
public enum ContainerKind
{
    PlayerPack = 0,
    WorldContainer = 1,
}

/// <summary>Inventory-shaped: the row-policy table. In a full-loot game these rows are wallhack-grade intel.</summary>
[Table(Public = true)]
public partial struct InventoryItem
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public Identity OwnerId;

    public ContainerKind Container;

    public string ItemName;
}

/// <summary>Private: the table the admin policy reads — the SpacetimeDB-footgun scenario.</summary>
[Table]
public partial struct AdminIdentity
{
    [PrimaryKey]
    public Identity Id;
}

/// <summary>AI-shaped: [ServerOnly] columns are a complete AI oracle if they ever reach a frame.</summary>
[Table(Public = true)]
public partial struct Creature
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public float X;

    [ServerOnly]
    public ulong NextThinkAt;

    [ServerOnly]
    public ulong SpookedUntil;
}

/// <summary>Own items, plus anything in a world container — visibility as a union of reasons.</summary>
public sealed class InventoryVisibility : IRowPolicy<InventoryItem>
{
    public bool IsVisibleTo(in InventoryItem row, PolicyContext ctx) =>
        row.OwnerId == ctx.Caller || row.Container == ContainerKind.WorldContainer;
}

/// <summary>
/// The flagship policy: reads the PRIVATE AdminIdentity table. In SpacetimeDB the equivalent RLS
/// rule fails to evaluate for ordinary clients and kills their whole subscription; here a
/// non-admin simply gets false and their subscription is untouched.
/// </summary>
public sealed class AdminSeesAllInventory : IRowPolicy<InventoryItem>
{
    public bool IsVisibleTo(in InventoryItem row, PolicyContext ctx) =>
        ctx.Db.AdminIdentity.Id.Find(ctx.Caller) is not null;
}

/// <summary>
/// A dynamic mask that depends on the row: position is hidden from other players while its owner
/// is in the hideout room — so moving rooms changes the mask mid-subscription.
/// </summary>
public sealed class HideoutHidesPosition : IColumnPolicy<PlayerState>
{
    public const int HideoutRoom = 13;

    public ColumnMask VisibleTo(in PlayerState row, PolicyContext ctx) =>
        row.Id == ctx.Caller || row.RoomId != HideoutRoom
            ? ColumnMask.All
            : ColumnMask.All.Except(nameof(PlayerState.X));
}

/// <summary>Reducer authorization backed by the same private table the row policy reads.</summary>
public sealed class AdminOnly : IReducerPolicy
{
    public bool MayCall(string reducer, PolicyContext ctx) =>
        ctx.Db.AdminIdentity.Id.Find(ctx.Caller) is not null;
}

public sealed class PolicyReducers
{
    [Reducer]
    public void GiveItem(ReducerContext ctx, Identity owner, int container, string itemName) =>
        ctx.Db.InventoryItem.Insert(new InventoryItem { OwnerId = owner, Container = (ContainerKind)container, ItemName = itemName });

    [Reducer]
    public void MoveItem(ReducerContext ctx, ulong id, Identity newOwner, int container)
    {
        var item = ctx.Db.InventoryItem.Id.Find(id) ?? throw new RejectedException("no such item");
        ctx.Db.InventoryItem.Update(item with { OwnerId = newOwner, Container = (ContainerKind)container });
    }

    [Reducer]
    public void AddAdmin(ReducerContext ctx, Identity id) =>
        ctx.Db.AdminIdentity.Insert(new AdminIdentity { Id = id });

    [Reducer]
    public void SpawnCreature(ReducerContext ctx, float x, ulong nextThinkAt) =>
        ctx.Db.Creature.Insert(new Creature { X = x, NextThinkAt = nextThinkAt, SpookedUntil = 0 });

    [Reducer]
    public void MoveCreature(ReducerContext ctx, ulong id, float x)
    {
        var creature = ctx.Db.Creature.Id.Find(id) ?? throw new RejectedException("no such creature");
        ctx.Db.Creature.Update(creature with { X = x });
    }

    [Reducer]
    public void NudgeCreatureThink(ReducerContext ctx, ulong id, ulong nextThinkAt)
    {
        var creature = ctx.Db.Creature.Id.Find(id) ?? throw new RejectedException("no such creature");
        ctx.Db.Creature.Update(creature with { NextThinkAt = nextThinkAt });
    }

    [Reducer(Policy = typeof(AdminOnly))]
    public void ClearCreatures(ReducerContext ctx)
    {
        foreach (var creature in ctx.Db.Creature.Iter().ToList())
            ctx.Db.Creature.Id.Delete(creature.Id);
    }
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
