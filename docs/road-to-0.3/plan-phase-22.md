# Phase 22 — `MelangeDB.Testing`, the reducer test kit

**Status: Planned.**

**Goal:** a package a game author reaches for on day one, in which ticks, time, identity, and write
sets are first-class assertions rather than things each consumer re-derives. Writing a test for a
scheduled reducer should take one `using` and three lines, and it should exercise the real engine.

```csharp
await using var world = MelangeWorld.Create()
    .WithServices(services => services.AddSingleton<ILootTable, TestLoot>())
    .Build();

world.AdvanceTo(TimeSpan.FromSeconds(60));          // fourteen simulation timers fire, deterministically
world.Call("Attack", world.Identity("alice"), deerId);

Assert.Single(world.LastCommit.WriteSet.Of<Creature>());
Assert.Equal(0, world.Read(db => db.Creature.Id.Find(deerId)!.Health));
```

**Depends on:** nothing hard. It pairs naturally with [phase 21](plan-phase-21.md) — that phase's
read-only reader API is the obvious way to assert against a decoded write set, and building the two
without knowing about each other would mean writing that decoding twice. Either can ship first.

## Why here

**The simulation is the scheduled reducers, and they are the least testable thing in the product.**
[REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md) calls scheduled reducers the design's largest
omission — fourteen of them run the entire world — and a consumer who cannot easily test a tick will
test what is easy instead: a live host, over a socket, covering the paths that do not need a clock.
The interesting cases are precisely the ones that need one.

**Almost all of the machinery is built and proven, and none of it is published.** This is the part
that makes the phase small:

- `InMemoryHotStore` already exists and is already the default when `UseFasterHotStore()` is not
  registered. Ordering principle 1 pre-answers the question a test kit would otherwise have to argue
  — the in-memory projection is a legitimate projection, not a stub, because the commit log is the
  source of truth.
- `MelangeEngine` already accepts an injected `TimeProvider`.
- A hand-cranked `TimeProvider` whose due timers **fire synchronously on the advancing thread**
  already exists and already makes the scheduler tests deterministic with no wall-clock sleeps
  anywhere. `MelangeScheduler` arms a `TimeProvider` timer, so this works by construction.
- `MelangeReducerHost` is already the public surface for calling a *declared* reducer by name with
  real arguments, real validation, and real rejection.
- `ICommitObserver.OnCommit(CommitRecord)` is already public and already fires once per committed
  record, in LSN order, under the write lock — which is a write-set assertion hook with no new API.

The signal that this should be a package is that the deterministic clock has already been written
**twice** inside this repo: `tests/MelangeDB.Host.Tests/ManualTimeProvider.cs`, and a second ad-hoc
one inside `RateLimiterEvictionTests`. When the same 60 lines appear twice in one repository, every
consumer is about to write a third.

**And a test kit is where recovery testing becomes routine.** The phase 11 port found a recovery
regression that a green suite had missed. `EngineHarness.Restart()` — abandon the engine, rebuild
from the log alone — is the single highest-value thing this kit can hand a consumer, and today it is
an internal class in this repo's own test project.

## Deliverables

**The `MelangeDB.Testing` package**, versioned and released with everything else
([RELEASING.md](../RELEASING.md): all packages ship at one version).

**`MelangeWorld` — the builder and the world.** Wraps `AddMelangeDb` and a temp data directory,
takes a `WithServices` hook so a reducer's injected dependencies can be substituted (DI in tests is
the whole reason phase 02 exists), and disposes its directory on teardown.

**A supported deterministic clock.** The existing `ManualTimeProvider` promoted to public API as
`world.AdvanceBy` / `world.AdvanceTo`, with the synchronous-fire property documented as a guarantee
rather than left as an implementation detail — it is the property every scheduled-reducer test
depends on. **The two internal copies are deleted in the same change**; the kit becomes the only
implementation, and this repo's own suite is its first consumer.

**Declared-reducer calls, not ad-hoc bodies.** `world.Call(name, caller, args...)` goes through
`MelangeReducerHost`, so a test exercises argument decoding, validation, rejection, and isolation as
production does. `EngineHarness.Invoke` runs an arbitrary `Action<ReducerContext>` under a name —
right for testing the engine, wrong as the thing a game author writes tests with, because it skips
the entire dispatch path they are trying to cover.

**Write-set assertions.** A commit observer capturing records into `world.LastCommit` and
`world.Commits`, with typed access — `WriteSet.Of<Creature>()` — decoded through the consumer's own
generated codecs. The write set is what the log records and what subscribers receive, so asserting
on it is asserting on the observable contract rather than on incidental store state.

**Named test identities.** `world.Identity("alice")` over `Identity.Hash`, so tests read as roles
and two tests never accidentally share a caller.

**Restart and recovery in one call.** `world.Restart()`, rebuilding from the log alone, plus the
assertion helper for the shape that matters: the world after restart equals the world before.

**Reducer-failure assertions.** Helpers for "this call was rejected" and "this reducer threw," kept
distinct — the port found `unknown_reducer` masking genuine reducer faults, and a kit that makes the
two look alike would let that class of bug back in.

**Documentation.** A new `docs/TESTING.md` written as the four tests a game author actually writes
first (a reducer commits what it should, a rejection rejects, a tick fires, the world survives a
restart); new nouns into [GLOSSARY.md](../GLOSSARY.md).

## Out of scope

**Any fake or mock of the engine, the store, or the log.** The kit runs the real engine over the
real in-memory projection and the real commit log. A mock hot store would be a second implementation
of the thing under test, and its divergence from the real one would be invisible and permanent.

**A cluster harness.** `tests/MelangeDB.Cluster.Tests/ClusterFixture.cs` exists and is a genuinely
different beast — membership, multiple nodes, handoff, fencing. Publishing a single-engine kit does
not commit to publishing that one, and conflating them would make the simple case pay for the hard
one. Recorded as the natural follow-up if consumers start writing cross-shard logic.

**Transport-level and client-level testing.** Subscriptions over a socket, wire fidelity, and client
cache behaviour are what `MelangeDB.Transport.Tests` covers, and testing them needs a server, not a
world. A consumer wanting an end-to-end test should stand up a host; this kit is for the half below
that.

**A test framework, or an opinion about one.** No xunit, NUnit, or MSTest dependency — the kit
exposes values and the consumer asserts with whatever they already use.

**Property-based or generative tick testing.** Tempting, given a deterministic clock and a
transactional world, and a much larger idea than this phase. If it happens it belongs on top of this
package rather than inside it.

## Decisions to settle

### A real commit log on a temp directory, or an in-memory `ICommitLog`

`FileCommitLog` is currently the only implementation. An in-memory log would make tests faster and
leave no directories behind; it would also be a second implementation of the durability discipline
— the ordering, the epoch, the torn-tail handling, the group-commit watermark — and the most likely
thing in this package to silently diverge from the real one.

**Leaning:** the real `FileCommitLog` on a temp directory. It keeps `Restart()` honest, which is the
kit's most valuable feature, and it means a passing test used the same code path production does.
**Open:** the default fsync policy. `EngineHarness` defaults to `OnCommit`, which is correct and
slow across a large suite; the kit probably wants `Interval` by default with recovery-shaped tests
opting up explicitly, and if so, that trade needs to be stated in `TESTING.md` rather than buried —
a test suite that never fsyncs is not testing what a crash sees.

### How the consumer's tables and reducers reach the kit

This repo's harness constructs a registry from `MelangeDB.Generated.MelangeModel()`, which is
generated into *this* assembly. A consumer's model is generated into theirs, so the kit has to find
it: reflect over the consuming assembly for the generated type, or have the source generator emit a
testing entry point the kit calls directly.

**Leaning:** emit the entry point. The generator already runs in every consuming project, the
project's instinct is generator-over-reflection, and `ZeroReflectionTests` exists to keep the
invocation path clean — setup is not the invocation path, so reflection here would not violate that
rule, but it would be the first place this codebase reached for reflection when a generator was
already available. **Open:** whether that entry point is emitted always or behind a property, since
it is dead code in a shipping game.

### Whether `AdvanceTo` takes wall-clock or the world's own time

Grok's sketch used `AdvanceTo(Timestamp.FromSeconds(60))`; the underlying provider advances a
`DateTimeOffset`. Scheduled reducers are driven by `ScheduleAt` on timer rows, which is world time.

**Leaning:** offer both and make the difference explicit in the names — `AdvanceBy(TimeSpan)` as the
common case, `AdvanceToTick(n)` where a consumer's tick cadence is known. **Open:** whether the kit
should know about tick cadence at all, or whether that is the consumer's helper to write on top of
`AdvanceBy`. Probably the latter; a database that ships an opinion about tick rate has crossed into
being a game framework.

### Whether the kit exposes the engine directly

An escape hatch (`world.Engine`) makes every advanced case possible and makes the curated surface
optional.

**Leaning:** expose it, documented as an escape hatch and not the intended path. The alternative is
a consumer blocked on a missing helper with no way forward, which is how a test kit gets abandoned
after its first gap. What the kit should *not* do is expose it so casually that
`world.Engine.Invoke(...)` becomes the idiom — which is the ad-hoc-body path this phase exists to
replace.
