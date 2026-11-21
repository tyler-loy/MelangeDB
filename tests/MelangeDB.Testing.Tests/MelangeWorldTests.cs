using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MelangeDB.Testing.Tests;

/// <summary>
/// The four tests a game author writes first, written with the kit against a small game: a reducer
/// commits what it should, a rejection rejects, a tick fires, the world survives a restart. Plus
/// what the kit itself promises: substituted services, faults kept apart from rejections, captured
/// commits, and a clock that fires exactly what is due.
/// </summary>
public class MelangeWorldTests
{
    private sealed class FixedLoot : ILootTable
    {
        public string Roll(string kind) => $"{kind}-pelt";
    }

    private static MelangeWorld Hunt() => MelangeWorld.Create(typeof(Creature).Assembly)
        .WithServices(services => services.AddSingleton<ILootTable, FixedLoot>())
        .Build();

    private static Creature Spawned(MelangeWorld world, string kind, int health) =>
        Assert.Single(world.Call("Spawn", world.Identity("keeper"), kind, health)!.Inserted<Creature>());

    [Fact]
    public void A_reducer_commits_what_it_should()
    {
        using var world = Hunt();
        var deer = Spawned(world, "deer", 10);

        var hit = world.Call("Attack", world.Identity("alice"), deer.Id, 4)!;
        var change = Assert.Single(hit.Of<Creature>());
        Assert.Equal(RowOpKind.Update, change.Kind);
        Assert.Equal(6, change.Row!.Value.Health);
        Assert.Empty(hit.Inserted<Loot>());
        Assert.Equal(world.Identity("alice"), hit.Caller);
        Assert.Equal(6, world.Read(db => db.Creature.Id.Find(deer.Id)!.Value.Health));

        // The kill drops loot from the substituted table, owned by the caller.
        var kill = world.Call("Attack", world.Identity("alice"), deer.Id, 6)!;
        var loot = Assert.Single(kill.Inserted<Loot>());
        Assert.Equal("deer-pelt", loot.Item);
        Assert.Equal(world.Identity("alice"), loot.Owner);
    }

    [Fact]
    public void A_rejection_rejects_and_a_fault_is_not_a_rejection()
    {
        using var world = Hunt();
        var deer = Spawned(world, "deer", 3);
        world.Call("Attack", world.Identity("alice"), deer.Id, 3);
        var head = world.Engine.Log.HeadLsn;

        var refused = world.Rejects("Attack", world.Identity("bob"), deer.Id, 1);
        Assert.Equal("It is already dead.", refused.Message);
        Assert.Equal(head, world.Engine.Log.HeadLsn);

        var fault = world.Faults("Inspect", world.Identity("bob"), deer.Id);
        Assert.IsType<DivideByZeroException>(fault);

        // Each helper refuses the other's outcome, and both refuse a commit.
        Assert.Throws<MelangeTestFailure>(() => world.Rejects("Inspect", world.Identity("bob"), deer.Id));
        Assert.Throws<MelangeTestFailure>(() => world.Faults("Attack", world.Identity("bob"), deer.Id, 1));
        var boar = Spawned(world, "boar", 5);
        Assert.Throws<MelangeTestFailure>(() => world.Rejects("Attack", world.Identity("bob"), boar.Id, 1));
    }

    [Fact]
    public void A_tick_fires_exactly_when_it_is_due()
    {
        using var world = Hunt();
        var deer = Spawned(world, "deer", 5);

        // Init scheduled regeneration every ten seconds. Nothing is due yet.
        world.AdvanceBy(TimeSpan.FromSeconds(9));
        Assert.Equal(5, world.Read(db => db.Creature.Id.Find(deer.Id)!.Value.Health));

        world.AdvanceBy(TimeSpan.FromSeconds(1));
        Assert.Equal(6, world.Read(db => db.Creature.Id.Find(deer.Id)!.Value.Health));
        Assert.Equal("Regenerate", world.LastCommit!.Reducer);

        // Thirty more seconds is three more ticks, each its own commit, all before AdvanceBy returns.
        var before = world.Commits.Count;
        world.AdvanceBy(TimeSpan.FromSeconds(30));
        Assert.Equal(9, world.Read(db => db.Creature.Id.Find(deer.Id)!.Value.Health));
        Assert.Equal(3, world.Commits.Skip(before).Count(static c => c.Reducer == "Regenerate"));
        Assert.Equal(ManualClock.DefaultStart + TimeSpan.FromSeconds(40), world.Now);
    }

    [Fact]
    public void The_world_survives_a_restart_and_keeps_ticking()
    {
        using var world = Hunt();
        var deer = Spawned(world, "deer", 5);
        world.Call("Attack", world.Identity("alice"), deer.Id, 2);
        world.AdvanceBy(TimeSpan.FromSeconds(10));

        world.RestartAndVerify();
        Assert.Equal(4, world.Read(db => db.Creature.Id.Find(deer.Id)!.Value.Health));

        // The timer row survived with the rest, so the tick resumes on the same clock.
        world.AdvanceBy(TimeSpan.FromSeconds(10));
        Assert.Equal(5, world.Read(db => db.Creature.Id.Find(deer.Id)!.Value.Health));

        // AutoInc continues past everything recovered.
        var boar = Spawned(world, "boar", 1);
        Assert.True(boar.Id > deer.Id);
    }

    [Fact]
    public void Commits_are_captured_from_the_first_including_init()
    {
        using var world = Hunt();
        var init = Assert.Single(world.Commits);
        Assert.Equal("Init", init.Reducer);
        Assert.Single(init.Inserted<RegenTimer>());

        // A call that writes nothing returns no commit, and captures none.
        Assert.Null(world.Call("Look", world.Identity("x"), 999UL));
        Assert.Single(world.Commits);
    }

    [Fact]
    public void Identities_are_stable_per_name_and_distinct_across_names()
    {
        using var world = Hunt();
        Assert.Equal(world.Identity("alice"), world.Identity("alice"));
        Assert.NotEqual(world.Identity("alice"), world.Identity("bob"));
    }

    [Fact]
    public void The_clock_fires_nothing_on_a_jump_until_time_next_advances()
    {
        var clock = new ManualClock();
        var fired = new List<DateTimeOffset>();
        using var timer = clock.CreateTimer(_ => fired.Add(clock.GetUtcNow()), null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

        clock.Jump(TimeSpan.FromSeconds(12));
        Assert.Empty(fired);

        // Overdue timers fire at the jumped-to time, once, then keep their period from there.
        clock.Advance(TimeSpan.Zero);
        Assert.Equal([ManualClock.DefaultStart + TimeSpan.FromSeconds(12)], fired);
        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(2, fired.Count);
        Assert.Equal(ManualClock.DefaultStart + TimeSpan.FromSeconds(17), fired[^1]);
        Assert.Equal([TimeSpan.FromSeconds(5)], clock.ArmedDelays);
    }
}
