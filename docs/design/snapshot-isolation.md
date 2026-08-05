# Snapshot isolation for read-heavy reducers

**Goal:** `[Reducer(Isolation = Isolation.Snapshot)]` — the reducer body runs against a stable read
view *outside* the engine's write lock. Only reconcile, the commit guards, and the log append
serialize. A sweep that spends 200 ms reading and 0.2 ms writing stops charging the other 199.8 ms
to every writer on the engine.

**Status:** designed, not built. Nothing below ships until the `IHotStore` contract change does.

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

**Snapshot reads in `IHotStore` — this is the actual work.** The current contract
([IHotStore.cs:9](../../src/MelangeDB.Abstractions/IHotStore.cs)) is explicit: *reads are safe only
while no `Apply` runs*, and scans are lazy, so a lock-free sweep racing an apply "throws 'collection
was modified' at best and yields a half-applied batch at worst." `InMemoryHotStore` is plain
`Dictionary`/`SortedDictionary` with no synchronization. Every implementation needs a way to hand out
a read view pinned at an LSN that an `Apply` cannot disturb. FASTER has the machinery natively;
the in-memory store does not. **The reducer-side plumbing is the easy half of this feature.**

**Write-set reconcile before the guards.** A body that decided against a snapshot can emit an update
for a row since deleted or an insert for a key since taken. `ReconcileOps` already solves exactly
this for the cluster's apply path and should be reused, run under the lock, before
`RunCommitGuards`. This is precedent, not new machinery — and it is the reason this feature is
buildable rather than a rewrite.

**Telemetry that tells the truth about the lock.** `1003 SlowReducer` currently thresholds on total
duration, which operators are told to read as global write latency
([OBSERVABILITY.md](../OBSERVABILITY.md)). For a snapshot reducer that reading is false — the body
blocks nobody. So:
- `1003` fires on the **locked portion**, not the total, for every isolation level.
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

## Decisions to settle

- **How the in-memory store takes a snapshot.** Copy-on-write per table, epoch-based versioning, or a
  second projection maintained by its own applier off the existing pipeline (which would reuse
  `ILogApplier` and its checkpoint wholesale). The third is the least new machinery and the most
  memory — and RAM is the pain point MelangeDB exists to fix, so any per-table copy has to be opt-in
  and has to show up in the startup residency report and `1501 ResidencyReport`. **This is the
  decision the feature actually hangs on.**
- **What FASTER's implementation costs.** It has concurrent sessions; whether a pinned read view is
  free, cheap, or a checkpoint is unmeasured.
- **What the analyzer can enforce.** Read-modify-write is undecidable in general, but the common
  shape — `Find` by primary key, then `Update` the same row — is detectable, and a *warning*
  (`MELANGE0023`, the next free id) on that shape inside a snapshot reducer would catch `CensusApply`
  and cost little. Warning, not error: the false-positive rate is unknown and a body that recomputes
  a row it also read is legitimate.
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
