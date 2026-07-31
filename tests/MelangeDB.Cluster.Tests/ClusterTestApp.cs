using System.Collections.Concurrent;
using MelangeDB.Cluster;

namespace MelangeDB.Cluster.Tests;

// ---- The reference instanced-game schema: one table per placement, plus a per-shard timer. ----

/// <summary>Which instance each player is in — the session-to-shard mapping, on the hub.</summary>
[Table(Public = true, Placement = Placement.Global)]
public partial struct PlayerLocation
{
    [PrimaryKey]
    public Identity PlayerId;

    public uint InstanceId;
}

[Table(Public = true, Placement = Placement.Global)]
public partial struct GlobalCounter
{
    [PrimaryKey]
    public long Id;

    public long Value;
}

[Table(Public = true, Placement = Placement.Replicated, Residency = Residency.Resident)]
public partial struct ItemDef
{
    [PrimaryKey]
    public long Id;

    public string Name;
}

[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(InstanceId))]
public partial struct Mob
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    [Index]
    public uint InstanceId;

    public int Hp;
}

/// <summary>Player-owned partitioned state: follows the player between instances on handoff.</summary>
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(InstanceId))]
public partial struct PlayerState
{
    [PrimaryKey]
    public Identity PlayerId;

    [Index]
    public uint InstanceId;

    public int Gold;
}

[Table(Placement = Placement.Local)]
public partial struct TickCount
{
    [PrimaryKey]
    public long Id;

    public long Count;
}

/// <summary>Timer rows are Local, so each shard engine's timers fire on that shard's owner.</summary>
[Table(Scheduled = nameof(ClusterReducers.Tick))]
public partial struct ShardTick
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;
}

public sealed record MobDied(ulong MobId, uint InstanceId);

/// <summary>Collects handled events; the fixture asserts where handlers actually ran.</summary>
public sealed class EventReceipts
{
    public ConcurrentQueue<string> Items { get; } = new();
}

public sealed class MobDiedHandler(EventReceipts receipts) : IEventHandler<MobDied>
{
    public Task HandleAsync(MobDied @event, CancellationToken cancellationToken)
    {
        receipts.Items.Enqueue($"MobDied:{@event.MobId}@{@event.InstanceId}");
        return Task.CompletedTask;
    }
}

public sealed class ClusterReducers
{
    [Reducer]
    public void SetLocation(ReducerContext ctx, uint instanceId)
    {
        if (ctx.Db.PlayerLocation.PlayerId.Find(ctx.Caller) is { } existing)
            ctx.Db.PlayerLocation.Update(existing with { InstanceId = instanceId });
        else
            ctx.Db.PlayerLocation.Insert(new PlayerLocation { PlayerId = ctx.Caller, InstanceId = instanceId });
    }

    [Reducer]
    public void BumpGlobal(ReducerContext ctx, long delta)
    {
        if (ctx.Db.GlobalCounter.Id.Find(1L) is { } existing)
            ctx.Db.GlobalCounter.Update(existing with { Value = existing.Value + delta });
        else
            ctx.Db.GlobalCounter.Insert(new GlobalCounter { Id = 1, Value = delta });
    }

    [Reducer]
    public void UpsertItemDef(ReducerContext ctx, long id, string name)
    {
        if (ctx.Db.ItemDef.Id.Find(id) is { } existing)
            ctx.Db.ItemDef.Update(existing with { Name = name });
        else
            ctx.Db.ItemDef.Insert(new ItemDef { Id = id, Name = name });
    }

    [Reducer]
    public void DeleteItemDef(ReducerContext ctx, long id) => ctx.Db.ItemDef.Id.Delete(id);

    [Reducer]
    public void SpawnMob(ReducerContext ctx, uint instanceId, int hp) =>
        ctx.Db.Mob.Insert(new Mob { InstanceId = instanceId, Hp = hp });

    [Reducer]
    public void HitMob(ReducerContext ctx, ulong mobId, int damage)
    {
        var mob = ctx.Db.Mob.Id.Find(mobId) ?? throw new RejectedException("no such mob");
        ctx.Db.Mob.Update(mob with { Hp = mob.Hp - damage });
    }

    [Reducer]
    public void KillMob(ReducerContext ctx, ulong mobId)
    {
        var mob = ctx.Db.Mob.Id.Find(mobId) ?? throw new RejectedException("no such mob");
        ctx.Db.Mob.Id.Delete(mobId);
        ctx.Publish(new MobDied(mobId, mob.InstanceId));
    }

    [Reducer]
    public void GrantGold(ReducerContext ctx, uint instanceId, int amount)
    {
        if (ctx.Db.PlayerState.PlayerId.Find(ctx.Caller) is { } existing)
            ctx.Db.PlayerState.Update(existing with { Gold = existing.Gold + amount });
        else
            ctx.Db.PlayerState.Insert(new PlayerState { PlayerId = ctx.Caller, InstanceId = instanceId, Gold = amount });
    }

    /// <summary>Deliberately writes rows in two shards — the transaction the span check exists to catch.</summary>
    [Reducer]
    public void SpanBoth(ReducerContext ctx, uint instanceA, uint instanceB)
    {
        ctx.Db.Mob.Insert(new Mob { InstanceId = instanceA, Hp = 1 });
        ctx.Db.Mob.Insert(new Mob { InstanceId = instanceB, Hp = 1 });
    }

    [Reducer]
    public void ScheduleTick(ReducerContext ctx, long everyMs) =>
        ctx.Db.ShardTick.Insert(new ShardTick { ScheduledAt = ScheduleAt.Interval(TimeSpan.FromMilliseconds(everyMs)) });

    [Reducer]
    public void Tick(ReducerContext ctx, ShardTick timer)
    {
        if (ctx.Db.TickCount.Id.Find(1L) is { } existing)
            ctx.Db.TickCount.Update(existing with { Count = existing.Count + 1 });
        else
            ctx.Db.TickCount.Insert(new TickCount { Id = 1, Count = 1 });
    }
}

/// <summary>The rows that follow a player between instances: their PlayerState.</summary>
public sealed class PlayerStateHandoffSet : IHandoffSet
{
    public void Collect(Identity player, ShardKey shard, IDbView shardDb, IHandoffCollector rows)
    {
        if (shardDb.Find<PlayerState>(player) is { } state)
            rows.Add(state);
    }
}
