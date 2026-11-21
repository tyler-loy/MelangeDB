# Testing a game's reducers

`MelangeDB.Testing` runs your tables and reducers in a real MelangeDB engine, over a throwaway
directory, on a clock that moves only when the test moves it. Nothing is mocked: the commit log,
the scheduler, the in-memory hot store and the reducer dispatch path are the ones production runs.
It has no test framework dependency; assert with whatever you already use.

```
dotnet add package MelangeDB.Testing
```

The test project references the game assembly (the one the source generator ran in) and names it:

```csharp
using MelangeDB.Testing;

await using var world = MelangeWorld.Create(typeof(Creature).Assembly)
    .WithServices(services => services.AddSingleton<ILootTable, FixedLoot>())
    .Build();
```

`Create` takes one or more assemblies and registers their generated tables, reducers and producers.
`WithServices` runs after MelangeDB's own registrations, so a substitution there wins. The world
starts as a host does: `Init` runs, and anything it schedules is armed on the world's clock.

These are the four tests to write first.

## 1. A reducer commits what it should

```csharp
var alice = world.Identity("alice");
var hit = world.Call("Attack", alice, deerId, 4)!;

var change = Assert.Single(hit.Of<Creature>());
Assert.Equal(RowOpKind.Update, change.Kind);
Assert.Equal(6, change.Row!.Value.Health);
Assert.Equal(6, world.Read(db => db.Creature.Id.Find(deerId)!.Value.Health));
```

- **`Call`** goes through the production dispatch path. The arguments are encoded and decoded as a
  client's would be, validated against the declared parameter types, and the reducer runs in its
  own DI scope. It returns the commit, or null if the reducer wrote nothing.
- **`Of<T>()`** decodes the commit's write set for one table through the game's generated codec.
  `Inserted<T>()`, `Updated<T>()` and `DeletedCount<T>()` are shortcuts. The write set is what the
  log recorded and what subscribers were sent, so this asserts on the observable contract.
- **`Read`** reads committed state through the same generated accessors a reducer uses.
- **`Identity("alice")`** is a stable identity per name: the same name is the same caller
  everywhere, and two names never collide.
- **`Commits`** and **`LastCommit`** hold every commit since the world started, including `Init`
  and every scheduled fire.

## 2. A rejection rejects

```csharp
var refused = world.Rejects("Attack", alice, corpseId, 1);
Assert.Equal("It is already dead.", refused.Message);

var bug = world.Faults("Inspect", alice, corpseId);
Assert.IsType<DivideByZeroException>(bug);
```

A rejection (`RejectedException`) is the reducer deliberately saying no. A fault is anything else:
an exception from a bug, an argument that fails validation, an unknown reducer name. The two
helpers keep them apart. **`Rejects`** fails if the call faulted or committed. **`Faults`** fails if
the call was rejected or committed. A test that accepted either outcome would let a crash pass as
a refusal.

## 3. A tick fires

```csharp
world.AdvanceBy(TimeSpan.FromSeconds(9));   // nothing due yet
world.AdvanceBy(TimeSpan.FromSeconds(1));   // the ten-second tick fires and commits
Assert.Equal("Regenerate", world.LastCommit!.Reducer);

world.AdvanceBy(TimeSpan.FromSeconds(30));  // three more ticks, three commits, in order
```

**Every timer that comes due fires before `AdvanceBy` returns**, synchronously on the test's
thread, earliest first, each committing before the next fires. That is a guarantee of the kit's
clock (`ManualClock`), not an accident of timing, and it is what makes tick tests deterministic.
There are no sleeps and no polling.

- **`AdvanceTo(instant)`** moves to an absolute time; **`Now`** reads it. Worlds start at a fixed
  instant (`ManualClock.DefaultStart`) unless the builder says `.StartingAt(...)`.
- **The kit knows time, not ticks.** A game that counts ticks writes a one-line helper:
  `void Ticks(int n) => world.AdvanceBy(TickInterval * n);`.
- **`world.Clock.Jump(by)`** moves time without firing anything, as a stalled process would; the
  next advance fires what became overdue. Use it to test catch-up behaviour.

## 4. The world survives a restart

```csharp
world.RestartAndVerify();
world.AdvanceBy(TimeSpan.FromSeconds(10));  // the tick resumes from its recovered timer row
```

**`Restart`** stops the world as a deploy does (drain, flush, close) and starts it again on the
same directory, clock and services; the engine rebuilds from what is on disk. **`RestartAndVerify`**
also fails unless every table holds exactly the rows it held before, byte for byte. After a
restart, `Engine` and `Services` are new objects; the captured commits carry on.

**Durability:** a world fsyncs on an interval by default (`CommitLog:FsyncPolicy = Interval`),
which is much faster across a large suite. A restart closes the log cleanly, so it loses nothing a
restart can see. What it does not model is a power cut, where unsynced writes are gone. A test
about what a crash leaves behind should build its world `.WithDurableCommits()`, which fsyncs every
commit as a deployment that cannot lose an acknowledged write does.

## Beyond the four

- **`WithSetting(key, value)`** sets any configuration key, as appsettings would.
- **`ConfigureMelange(builder => ...)`** adds to the MelangeDB registration: event handlers, the
  FASTER hot store (`UseFasterHotStore()`), anything the host's builder takes.
- **`world.Engine`** is the engine itself, **an escape hatch rather than the intended path.** It
  reaches what the kit's surface does not, such as `Engine.Log` and `Engine.TakeSnapshot()`. Avoid
  `Engine.Invoke(...)` with an ad-hoc body: it skips argument decoding, validation and the
  reducer's DI scope, which is the path `Call` exists to cover.
- **`ManualClock`** is public, for a test that composes its own host: register it as the
  `TimeProvider` and advance it.

## What the kit does not do

- **It does not mock the engine, the store or the log.** A second implementation of the thing
  under test would diverge from it invisibly.
- **It does not run a cluster.** Cross-shard logic needs membership, handoff and fencing, which a
  single engine does not have.
- **It does not test transport or clients.** Subscriptions over a socket, wire fidelity and client
  caches need a server; stand up a host for those.
- **It does not run calls as a client would be policed.** `Call` is an in-process call, so
  reducer policies and rate limits, which apply to client-originated calls, are not applied.
