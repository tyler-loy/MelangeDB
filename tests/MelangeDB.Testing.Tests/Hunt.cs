namespace MelangeDB.Testing.Tests;

// A small game, as a consumer would write it: creatures with health, a hunter's attack that
// depends on an injected loot table, a regeneration tick, and a reducer with a bug in it.

[Table(Public = true, Residency = Residency.Resident)]
public partial struct Creature
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public string Kind;

    public int Health;
}

[Table(Residency = Residency.Resident)]
public partial struct Loot
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public Identity Owner;

    public string Item;
}

/// <summary>The world tick: every live creature regenerates a point of health.</summary>
[Table(Scheduled = nameof(HuntReducers.Regenerate), Residency = Residency.Resident)]
public partial struct RegenTimer
{
    [PrimaryKey]
    [AutoInc]
    public ulong Id;

    public ScheduleAt ScheduledAt;
}

/// <summary>What a kill drops; substituted in tests.</summary>
public interface ILootTable
{
    string Roll(string kind);
}

public sealed class RandomLoot : ILootTable
{
    public string Roll(string kind) => $"{kind}-hide-{Random.Shared.Next(1000)}";
}

public sealed class HuntReducers(ILootTable loot)
{
    [Reducer(ReducerKind.Init)]
    public void Init(ReducerContext ctx) =>
        ctx.Db.RegenTimer.Insert(new RegenTimer { ScheduledAt = ScheduleAt.Interval(TimeSpan.FromSeconds(10)) });

    [Reducer]
    public void Spawn(ReducerContext ctx, string kind, int health) =>
        ctx.Db.Creature.Insert(new Creature { Kind = kind, Health = health });

    [Reducer]
    public void Attack(ReducerContext ctx, ulong creatureId, int damage)
    {
        var creature = ctx.Db.Creature.Id.Find(creatureId) ?? throw new RejectedException("No such creature.");
        if (creature.Health <= 0)
            throw new RejectedException("It is already dead.");
        creature.Health = Math.Max(0, creature.Health - damage);
        ctx.Db.Creature.Update(creature);
        if (creature.Health == 0)
            ctx.Db.Loot.Insert(new Loot { Owner = ctx.Caller, Item = loot.Roll(creature.Kind) });
    }

    /// <summary>Reads and writes nothing.</summary>
    [Reducer]
    public void Look(ReducerContext ctx, ulong creatureId) => _ = ctx.Db.Creature.Id.Find(creatureId);

    /// <summary>A bug: divides by the creature's health, which is zero for a corpse.</summary>
    [Reducer]
    public void Inspect(ReducerContext ctx, ulong creatureId)
    {
        var creature = ctx.Db.Creature.Id.Find(creatureId)!.Value;
        ctx.Db.Creature.Update(creature with { Health = 100 / creature.Health });
    }

    [Reducer]
    public void Regenerate(ReducerContext ctx, RegenTimer timer)
    {
        foreach (var creature in ctx.Db.Creature.Iter().ToList())
        {
            if (creature.Health is > 0 and < 10)
                ctx.Db.Creature.Update(creature with { Health = creature.Health + 1 });
        }
    }
}
