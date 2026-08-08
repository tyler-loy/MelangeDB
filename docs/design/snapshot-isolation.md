# Snapshot isolation for read-heavy reducers

**Goal:** `[Reducer(Isolation = Isolation.Snapshot)]` — the reducer body runs against a stable read
view *outside* the engine's write lock. Only reconcile, the commit guards, and the log append
serialize. A sweep that spends 200 ms reading and 0.2 ms writing stops charging the other 199.8 ms
to every writer on the engine.

**Status:** **built.** The store half landed first (pinned reads in both hot stores); the reducer half —
the `Isolation` axis, the engine's unlocked-body path, write-set reconcile, and the telemetry split — landed
after it. The open questions in [Decisions to settle](#decisions-to-settle) are refinements and guardrails,
not gaps in the feature: none of them block declaring `Isolation.Snapshot` on a reducer today.

**Depends on:** [plan-phase-01](../road-to-0.1/plan-phase-01.md) (the write lock, the write set),
[plan-phase-07](../road-to-0.1/plan-phase-07.md) (the store seam this changes).

## Why

Today the write lock covers the *whole* transaction — body, guards, append, fsync, observers, and
any automatic snapshot the commit triggers ([DESIGN.md](../DESIGN.md) §4). That is the correct
default and it is what makes a reducer a transaction. It is also, for a read-heavy sweep, almost
entirely wasted: the sweep holds every other writer out while doing arithmetic.

The [reference workload](../REFERENCE-WORKLOAD.md) has two of these and they are its most expensive
reducers. `GrowFlora` (`server/module/Reducers/Flora.cs:193`) decodes flora blobs, counts per species,
computes favorability, and evolves region power across a window of chunks — then writes a handful of
saplings. The creature sweep (`Creatures.cs:470`) is the same shape. Both already window their work
(`FloraChunkWindowPerTick`, `CreatureChunkWindowPerTick`), which is the mitigation
[DESIGN.md](../DESIGN.md) §4 recommends, so the cost is already understood and already being paid —
in latency, by everyone else, every tick.

**Neither sweep is read-only**, which is what rules out the obvious cheaper feature. Both end with a
cursor advance — `ctx.Db.FloraTick.Update(tick with { NextChunk = ... })` — that fires whether or not
anything grew, and the creature sweep additionally writes region rows most ticks. A reducer kind that
forbade writing would have no customer here. What these reducers want is not *no lock*; it is **not
holding the lock while computing**.

## The rule that decides eligibility

This is the first thing the feature's documentation must say, before the syntax:

> **Snapshot isolation is safe for recompute-from-scratch and unsafe for read-modify-write.**

A body that reads state, computes a value from it, and writes that value is safe: if the state moved
under it, two concurrent runs each write a defensible answer and the last one wins. A body that reads
a value, adds a delta, and writes the sum is **not** safe: two concurrent runs read the same number
and one increment is lost, silently and permanently.

Both shapes live in the same reducer in the reference workload. The creature sweep's births and culls
recompute from the chunk's residents — safe. Its
`CensusApply(ctx, sp.SpeciesId, now, alive: -1, culled: 1)` applies deltas to a census row — a
read-modify-write, and under snapshot isolation it would lose counts. That is why the flag is
**opt-in per reducer and never inferred**: the compiler cannot tell these apart, and the module
author can.

`ReconcileOps` ([MelangeEngine.cs:381](../../src/MelangeDB.Core/MelangeEngine.cs)) does not rescue
this. It fixes op *shape* — an update of a row someone deleted becomes an insert, a delete of a
missing row drops — not op *value*. It is necessary (see Deliverables) and nowhere near sufficient.

## Why an axis on `[Reducer]`, not a `ReducerKind` and not a new noun

**Not a `ReducerKind`.** That enum's doc comment says what it is: *"What triggers a reducer."*
`Standard`, `ClientConnected`, `ClientDisconnected`, `Init` are all triggers. Isolation is not a
trigger — it is a property of how the body is isolated, orthogonal to what fired it. The enum is
single-valued and positional (`ReducerAttribute(ReducerKind kind)`), so putting isolation there makes
it un-combinable: no snapshot-isolated `ClientConnected`, no snapshot-isolated anything but
`Standard`. Note also that *scheduled* is not a `ReducerKind` at all — it lives on the table as
`[Table(Scheduled = "GrowFlora")]` — so the axis must be declarable on a reducer that a table points
at, independently of both.

**Not a new verb-noun** (`[Sweep]`, `[Survey]`). Nothing downstream changes. Same
`ReducerDescriptor`, same generated validate/invoke delegates, same log record, same policy
resolution, same `melange.reducer` span, same scheduler dispatch. Only `MelangeEngine.Invoke`
branches on it. Forking the concept would duplicate the whole dispatch and codegen surface to express
one bit.

So it is a third axis on `[Reducer]`, alongside `Site` and `Policy`:

```csharp
[Reducer(Isolation = Isolation.Snapshot)]
public void GrowFlora(ReducerContext ctx, FloraTick tick)
```

## Deliverables

**`Isolation` on `[Reducer]`**
- `Isolation.Serialized` (default) — today's behaviour, and the honest name for it: one global lock
  around the whole body *is* serializable.
- `Isolation.Snapshot` — the body runs lock-free against a stable read view; the write set it
  produces is reconciled, guarded, and appended under the lock.
- Threaded through `ReducerDescriptor` and the generated registration exactly as `Site` and `Policy`
  are.

**Pinned reads in `IHotStore` — the actual work, and it is done; see
[Decisions settled](#decisions-settled).** The read surface is now `IHotStoreReader`, shared by the
live store and by `IHotStoreReadView` — a view pinned at one LSN, handed out by the optional
`IReadViewSource` capability in the manner of `IResidencyControl`. Both stores implement it, by
different means, and `ReadViewContractTests` runs one suite against both so they cannot drift.

**Write-set reconcile before the guards.** A body that decided against a snapshot can emit an update
for a row since deleted or an insert for a key since taken. `ReconcileOps` already solves exactly
this for the cluster's apply path and should be reused, run under the lock, before
`RunCommitGuards`. This is precedent, not new machinery — and it is the reason this feature is
buildable rather than a rewrite.

**Telemetry that tells the truth about the lock.** `1003 SlowReducer` thresholded on total
duration, which operators are told to read as global write latency
([OBSERVABILITY.md](../OBSERVABILITY.md)). For a snapshot reducer that reading is false — the body
blocks nobody. So:
- `1003` fires on the **locked portion**, not the total, for every isolation level. Worth noting what
  this did *not* change: a serialized transaction's clock already started inside the lock, so its locked
  portion and its total are the same interval and that path's warnings are byte-for-byte what they were.
  The change reads like a behaviour change to a stable EventId and is, in practice, a no-op everywhere
  except the reducers this feature added.
- The warning and the `melange.slow_reducer` span event carry the isolation, so a dashboard can tell
  a 500 ms serialized transaction from a 500 ms snapshot one that stalled nothing.
- The `melange.reducer` span carries the isolation as a tag, and total duration stays reported —
  a snapshot reducer that takes 500 ms is still worth knowing about, just not as write latency.

**Documentation, per the standing conventions in [ROADMAP.md](../ROADMAP.md).**
`Isolation` and *snapshot reducer* into [GLOSSARY.md](../GLOSSARY.md); the `1003` change and the new
tag into [OBSERVABILITY.md](../OBSERVABILITY.md); the `Telemetry:SlowReducerMs` entry in
[CONFIGURATION.md](../CONFIGURATION.md) reworded, since "how long may one transaction freeze every
other writer" stops being the whole story; [DESIGN.md](../DESIGN.md) §4 extended.

## Out of scope

**`Isolation.ReadOnly`.** A level that forbids writing entirely would let the engine skip the commit
path altogether. It is a strict subset of `Snapshot` and trivial to add later as a third enum value —
but it has no customer: both reference sweeps write every tick, and so does the hot→relational
aggregation case. Add it when something asks for it.

**Full optimistic concurrency.** Tracking a read set and retrying on conflict is the textbook answer
and it fights this codebase on two fronts. `GrowFlora` seeds its RNG from `ctx.Timestamp`, and the
analyzer bans ambient time precisely *because* the body runs once — a retried body makes different
decisions. And `ctx.Publish` stages events that would have to be unwound. Snapshot isolation here
deliberately does **no read-set validation**: the module author declares the reads advisory, and the
declaration is the contract.

**Cross-tier isolation.** There is nothing to extend it across. A reducer never touches Postgres:
writing a `Tier = StorageTier.Relational` table stages ops into the same write set as any other
table, and `PostgresRelationalTier` is an `ILogApplier` with its own checkpoint that consumes the log
on its own thread, outside the write lock, explicitly permitted to lag (`1601
PostgresApplierStalled`: *"Writes and subscriptions are unaffected"*). Relational tables also live in
the hot store like everything else — `Tier` means *additionally* Postgres, not instead
([plan-phase-08](../road-to-0.1/plan-phase-08.md)) — so the hot store is the only store in the
transaction path, and isolation covers exactly it.

This makes the **hot→relational aggregation reducer the best customer for the feature**, better than
either sweep: a wide read, a narrow write, reads that are advisory by construction (an aggregate is a
summary; a row that moved mid-scan shifts it by an epsilon), and an output landing in a tier whose
contract already says eventually consistent by design. The staleness snapshot isolation introduces is
strictly smaller than the staleness the destination already has. It cannot be observed through the
path that consumes it.

**Inferring the flag, or defaulting to it.** See the eligibility rule.

## Decisions settled

- **It is `Isolation` on `[Reducer]`**, a third axis next to `Site` and `Policy` — not a
  `ReducerKind`, not a new attribute.
- **`Isolation.Snapshot` / `Isolation.Serialized`** as the value names, accepting that *snapshot*
  is overloaded in this codebase against durability snapshots (`SnapshotFile`, `TakeSnapshot()`,
  `Snapshots:IntervalTransactions`, `1502 SnapshotWritten`). It is the correct database term and a
  reader who knows it knows immediately what was bought and lost. The runner-up was
  `Reads = Reads.Advisory`, which collides with nothing but states a claim rather than a mechanism.
  **The collision is a documentation burden, and log lines about this feature must not say
  "snapshot" unqualified.**
- **No read-set validation.** The declaration is the contract.
- **Recompute-safe, read-modify-write-unsafe** is the eligibility rule, and it leads the docs.
- **`1003` thresholds on the locked portion**, at every isolation level.
- **The write set is reconciled** under the lock before the guards run.
- **Read-your-writes inside the body is unaffected** — the write-set overlay is transaction-local and
  has nothing to do with which store view the reads resolve against.
- **Isolation covers the hot store only**, because it is the only store in the transaction path.
- **The in-memory store pins by holding its containers persistently**, not by copying and not by
  keeping a second projection. Each table's rows and indexes are `ImmutableSortedDictionary` /
  `ImmutableSortedSet`; a write publishes a new version, and `OpenReadView` captures the current one
  per table. Row payloads are shared across versions — they are already never mutated in place, only
  replaced — so a pin costs container nodes, not a copy of the data.

  Measured at one million 96-byte rows, persistent against the mutable containers it replaced:

  | | mutable | persistent | |
  | --- | --- | --- | --- |
  | container memory | 61.0 MB | 61.0 MB | **1.00×** |
  | bulk build (replay) | 286.3 ms | 161.9 ms | 0.57× |
  | point read | 1124.1 ms | 1114.4 ms | 0.99× |
  | full scan | 9.8 ms | 12.1 ms | 1.24× |
  | one put | 0.22 µs | 0.39 µs | 1.78× |
  | **pin a read view** | **28.6 ms** (clone) | **~0 ms** (capture) | — |

  Memory being identical is what settles it: the objection to a persistent structure was that
  MelangeDB exists to fix a RAM ceiling, and it does not cost RAM. The put regression is ~0.17 µs on
  an operation that runs under the write lock, against a copying pin that would have cost 28.6 ms
  *inside that same lock* on the first write after any view opened. Bulk load gets faster, because
  recovery builds through the containers' builders and publishes one version at the end.

  The other two candidates are rejected on those numbers: **copy-on-write by cloning** pays the
  28.6 ms per table per snapshot window, in the lock, which is worse than the stall the feature
  removes; **a second projection off the applier pipeline** doubles resident memory permanently and
  buys arbitrary staleness instead of a known LSN.

  Reproducible: `dotnet run -c Release --project bench/MelangeDB.Benchmarks -- --filter '*Container*'`.

- **The FASTER store pins the same two ways it stores.** Everything it holds in managed memory — the
  key directory, the secondary indexes, and a resident table's rows — becomes persistent containers
  and is captured by reference, exactly as above. A **paged** row's payload cannot be: it lives in
  the hybrid log, where an upsert overwrites in place and leaves no old version to read. Those are
  covered by an **undo overlay** — while any view is open, a write to a paged row first stashes that
  row's pre-image on every view that has not already recorded one.

  The cost is therefore proportional to **writes during the window**, not to table size: a sweep
  reading a million rows while fifty change pays for fifty pre-image reads. While no view is open it
  costs nothing at all, which is the overwhelmingly common case. Not chosen: FASTER's own
  checkpoint/versioning machinery, which phase 07 deliberately kept out of the picture
  ([plan-phase-07](../road-to-0.1/plan-phase-07.md)) — recovery is the engine's, and pinning a read
  view is not a reason to reopen that.

  Measured at the store seam, 100,000 rows, one paged table:

  | | in-memory | FASTER |
  | --- | --- | --- |
  | open a read view | 37.9 ns, 280 B | 58.0 ns, 400 B |
  | apply 100 rows, no view open | 56.6 µs | 32.1 µs |
  | apply 100 rows, **a view open** | 55.9 µs (**0.99×**) | 50.9 µs (**1.58×**) |
  | scan 100,000 rows, live | 2.08 ms | 29.5 ms |
  | scan 100,000 rows, **through a view** | 1.81 ms (0.87×) | 36.6 ms (**1.24×**) |

  Opening is tens of nanoseconds and independent of row count in both stores, which is the property
  that makes this a pin rather than a copy wearing a different name. Holding one open is **free** in
  the in-memory store — the containers were already persistent, so there is nothing extra to do —
  and costs the FASTER store **~188 ns per paged row written**, which is the pre-image read. A
  hundred-row transaction landing in the middle of a sweep pays 19 µs for it.

  Reproducible: `dotnet run -c Release --project bench/MelangeDB.Benchmarks -- --filter '*ReadView*'`.

  **The known limitation.** A paged row read through a view takes the store's own lock for the
  duration of that row's read, because the hybrid log sits behind a single FASTER session. That still
  frees the engine's *write* lock for the whole reducer body, which is the point of the feature, but
  it does not make paged reads concurrent with writers. A **resident** table has no such limit — it
  reads entirely from the pinned containers, lock-free — and a table a sweep scans hot is exactly the
  one to declare `Residency.Resident`. Giving each view its own FASTER session would lift the
  limitation; it is not done, and it is not measured.

- **AutoInc ids are reserved as allocated, not staged.** *Settled during implementation; this was not in
  the original design and is the one thing building it turned up that argument had missed.*

  `AutoIncStage.Allocate` runs **in the body**. It read the sequencer's next value and consumed it only at
  `Commit`, which is safe when a serialized transaction is the only one running and is two separate bugs when
  it is not: concurrent read and write on a plain `Dictionary` is undefined, and — worse, because it is
  silent — two concurrent bodies peek the same base and **allocate the same id**. That surfaces as a
  duplicate-key insert, or, once reconcile has done its job, as one transaction's row quietly becoming an
  update over the other's.

  A snapshot transaction therefore reserves from the durable sequence as it allocates, under the sequencer's
  own lock — not the engine's write lock, so it does not undo the feature. The price is that an aborted
  snapshot transaction leaves a **gap**, where an aborted serialized one still consumes nothing. That is
  within the sequencer's stated contract — ids are **unique, not dense**
  ([plan-phase-01](../road-to-0.1/plan-phase-01.md)) — and it costs nothing durable, since the sequence is
  rebuilt at recovery by re-observing what actually committed.

  The general lesson, which applies to anything else later moved off the lock: **the write lock was doing
  more work than the design gave it credit for.** Every piece of engine state a body touches was implicitly
  single-threaded. The sequencer was the only one here — commit guards, the log, and the observers all run
  under the lock still, telemetry is `Counter`/`Histogram`, and the table access guard is a stateless
  delegate — but "only one" was a finding, not an assumption.

- **A store without `IReadViewSource` degrades to serialized and says so once** (`1004
  SnapshotIsolationUnavailable`). *Settled during implementation.* Isolation is a **latency** property, not
  a semantic one: a body written for snapshot isolation is still correct when run serialized, just slower.
  Refusing to start would turn a performance feature into a hard dependency on a store capability that
  `IReadViewSource` deliberately makes optional. Degrading *silently* is the option that is actually wrong,
  which is why this warns rather than merely not-crashing. Both shipped stores implement the capability, so
  the path exists for third-party and future stores — and is tested through a deliberately capability-less
  wrapper, because an untested fallback is how a fallback becomes a crash.

- **The log record's timestamp is taken at append, not at body start.** *Settled during implementation.* The
  body still gets a stable start-of-transaction clock in `ctx.Timestamp` — a scheduled sweep derives its next
  fire from it, and deriving that from commit time would drift the cadence by however long the body happened
  to take. But stamping the *record* with it would let a body that ran 200 ms append a record older than one
  a serialized transaction appended meanwhile, putting the log's timestamps out of order against its own
  LSNs.

- **A residency change invalidates open views, loudly.** Promoting or demoting a table rewrites where
  its rows live, so a view pinned across one would be answering from bookkeeping that no longer
  describes the data. The view throws on next use, naming the table and pointing at
  `Residency:AutoThresholdBytes` in case an `Auto` table is flipping under load. A plausible wrong
  answer is the worse outcome.

- **The analyzer warns on the detectable read-modify-write shape** (`MELANGE0023`,
  `SnapshotReadModifyWriteAnalyzer`). *Settled after the original design; this was "what the
  analyzer can enforce" above.* Inside a body declared `Isolation.Snapshot`, a row obtained from a
  generated single-row `Find` — through the wrappers the shape is written with: `?? throw`,
  `.Value`, `GetValueOrDefault()`, `with`, and local copies to a fixpoint — and passed back to the
  table handle's `Update` is flagged. That is `CensusApply`, and it is the move-player shape: the
  write-back carries *every* column of a row read from a pinned view, so a concurrent commit to any
  other column of the same row is silently reverted, which makes a reducer that looks like a blind
  position write a read-modify-write at row granularity.

  Warning, not error, exactly as argued above: a recompute of a row the body also read is
  legitimate, so silence proves nothing and a firing may be suppressed knowingly. Deliberately
  narrow: rows from `Iter`/`Filter`/`First` are not tracked, because updating rows mid-sweep is
  what the feature's legitimate customers — the reference sweeps — do every tick, and drowning them
  in warnings would teach authors to suppress the diagnostic wholesale.

## Decisions to settle

- **Whether a paged read through a view should get its own FASTER session.** It would make paged
  reads genuinely concurrent with writers instead of serializing each row on the store lock. The
  correctness argument is harder than it looks — the overlay probe and the store read stop being
  atomic with each other — and a resident table, which is what a hot sweep should declare, does not
  need it. Measure the gap before building it.
- **Whether a long-held view should be bounded.** A pinned view keeps its versions alive, so a reducer
  that holds one for minutes retains every row those tables replaced meanwhile. Nothing enforces a
  ceiling today, and nothing reports one being held.
- **Whether concurrent snapshot reducers writing the same row should be visible.** Last-writer-wins
  is correct for the recompute shape and silent for the read-modify-write shape. A commit guard that
  counted overlapping write sets between concurrent snapshot transactions would surface the mistake
  the analyzer cannot prove.
- **Whether a snapshot reducer may be client-callable.** Nothing prevents it, and the policy pipeline
  is unchanged. But the failure mode of a wrong `Isolation` on a client-called reducer is lost writes
  under contention, which is exactly the load a client-called reducer sees.
- **How stale the snapshot may be**, and whether that is bounded or merely observable. A pinned view
  held by a long sweep also pins whatever the store needs to keep it — which interacts with log
  truncation and the automatic snapshot path.
