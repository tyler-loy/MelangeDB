using System.Collections.Concurrent;

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

/// <summary>
/// The issue #122 shape: a sparse counter, and beside it the boolean a caller denormalises when the
/// counter has no predicate that fits it. Both columns are indexed and both select the identical
/// row set — that identity is the whole argument for <c>&lt;&gt;</c> existing, so the tests assert
/// it rather than restating it.
/// </summary>
[Table(Public = true)]
public partial struct EditedChunk
{
    [PrimaryKey]
    public long Id;

    [Index]
    public uint EditCount;

    /// <summary>The workaround column, kept here as the counterexample to its own necessity.</summary>
    [Index]
    public bool IsEdited;

    /// <summary>Signed, so the refusal for kinds whose default is not their minimum has a subject.</summary>
    [Index]
    public int Elevation;
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

/// <summary>
/// Every client-visible column kind in one row — the wire-fidelity table. Protocol v2 puts the
/// server's own row bytes on the wire, so "the client reads what the server wrote" stopped being a
/// property of a coercion table and became a property of the format itself. This is what proves it.
/// </summary>
[Table(Public = true)]
public partial struct AllKinds
{
    [PrimaryKey]
    public long Id;

    public bool Flag;

    public sbyte Int8;

    public byte UInt8;

    public short Int16;

    public ushort UInt16;

    public int Int32;

    public uint UInt32;

    public long Int64;

    public ulong UInt64;

    public float Float32;

    public double Float64;

    public string Text;

    public byte[] Blob;

    public Identity Who;

    public Timestamp At;
}

/// <summary>
/// Node-local: the one placement a shard node's own engine legitimately holds, so it is what a
/// clustered-role bulk test can write without tripping the Partitioned refusal (#115).
/// </summary>
[Table(Placement = Placement.Local)]
public partial struct NodeCounter
{
    [PrimaryKey]
    public long Id;

    public long Count;
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

/// <summary>
/// Relational-tier and private — the WorldStat shape: written by gameplay, read by admin tooling.
/// What owner-mode ad-hoc SQL (rows and aggregates) exists for.
/// </summary>
[Table(Tier = StorageTier.Relational)]
public partial struct WorldStat
{
    [PrimaryKey]
    [AutoInc]
    public long Id;

    public string Metric;

    public long Value;

    public Timestamp At;
}

/// <summary>AI-shaped: [ServerOnly] columns are a complete AI oracle if they ever reach a frame.</summary>
[Table(Public = true, Residency = Residency.Resident)]
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

    /// <summary>Stands in for a commit guard's transient refusal (a handoff freeze, a border copy).</summary>
    [Reducer]
    public void RefuseTransiently(ReducerContext ctx) =>
        throw new TransientRejectionException("a row is frozen mid-handoff (test stand-in)");

    /// <summary>
    /// Stands in for ordinary library code failing inside a reducer body (issue #98): the real
    /// case was an ArgumentOutOfRangeException from a row decode two layers down. Nobody writes
    /// the throw on purpose; everybody reaches it eventually.
    /// </summary>
    [Reducer]
    public void ThrowArgumentFromBody(ReducerContext ctx, uint anything) =>
        throw new ArgumentOutOfRangeException(nameof(anything), "from the body, not from dispatch");

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

    /// <summary>Writes the counter and the flag together, the way a caller carrying both must.</summary>
    [Reducer]
    public void EditChunk(ReducerContext ctx, long id, uint editCount, int elevation)
    {
        var chunk = new EditedChunk { Id = id, EditCount = editCount, IsEdited = editCount != 0, Elevation = elevation };
        if (ctx.Db.EditedChunk.Id.Find(id) is not null)
            ctx.Db.EditedChunk.Update(chunk);
        else
            ctx.Db.EditedChunk.Insert(chunk);
    }

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

    /// <summary>Writes two tables in one transaction — the frame-tick pump's atomicity subject.</summary>
    [Reducer]
    public void SpawnWithSkill(ReducerContext ctx, string name, int roomId, long playerNum, string skillName)
    {
        ctx.Db.PlayerState.Insert(new PlayerState { Id = ctx.Caller, RoomId = roomId, Name = name, X = 0 });
        ctx.Db.Skill.Insert(new Skill { PlayerNum = playerNum, Name = skillName, TotalXp = 0, Level = 1 });
    }

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
    public void RecordStat(ReducerContext ctx, string metric, long value) =>
        ctx.Db.WorldStat.Insert(new WorldStat { Metric = metric, Value = value, At = ctx.Timestamp });

    [Reducer]
    public void Noop(ReducerContext ctx)
    {
    }

    /// <summary>Scheduled by <see cref="RespawnTick"/>; a client naming it is told "unknown".</summary>
    [Reducer]
    public void Respawn(ReducerContext ctx, RespawnTick timer)
    {
    }
}

/// <summary>Timer rows for the wire-facing scheduled-reducer tests. Implicitly private and Local.</summary>
[Table(Scheduled = nameof(TransportReducers.Respawn))]
public partial struct RespawnTick
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;
}

/// <summary>Private: what the lifecycle reducers write when asked to, one row per transition.</summary>
[Table]
public partial struct SessionLog
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public string Kind;

    public Identity Who;
}

/// <summary>
/// In-memory record of lifecycle fires. Recording costs no log records, so unrelated tests keep
/// their LSN arithmetic; <see cref="WriteRows"/> opts a test into row-writing lifecycle reducers.
/// </summary>
public sealed class SessionEvents
{
    public ConcurrentQueue<(string Kind, Identity Caller, ConnectionId Connection)> Events { get; } = [];

    public bool WriteRows { get; set; }

    public int Count(string kind, Identity caller) =>
        Events.Count(e => e.Kind == kind && e.Caller == caller);
}

public sealed class LifecycleReducers(SessionEvents events)
{
    /// <summary>Not client-callable; a client naming it is told "unknown", never "forbidden".</summary>
    [Reducer(ReducerKind.ClientConnected)]
    public void OnConnect(ReducerContext ctx)
    {
        events.Events.Enqueue(("connect", ctx.Caller, ctx.ConnectionId));
        if (events.WriteRows)
            ctx.Db.SessionLog.Insert(new SessionLog { Kind = "connect", Who = ctx.Caller });
    }

    [Reducer(ReducerKind.ClientDisconnected)]
    public void OnDisconnect(ReducerContext ctx)
    {
        events.Events.Enqueue(("disconnect", ctx.Caller, ctx.ConnectionId));
        if (events.WriteRows)
            ctx.Db.SessionLog.Insert(new SessionLog { Kind = "disconnect", Who = ctx.Caller });
    }
}
