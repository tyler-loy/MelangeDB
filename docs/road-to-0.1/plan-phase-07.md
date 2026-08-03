# Phase 07 — Durable hot store: paging, residency, large values

**Goal:** the working set bounds memory instead of the total dataset. This is the phase that answers the
original RAM complaint.

**Depends on:** [01](plan-phase-01.md), [02](plan-phase-02.md).

## Why here

Deliberately late. Because the commit log is the source of truth, the in-memory projection from phase 01 is a
legitimate implementation rather than a stub — so everything above could be built and proven first, and this
phase is a swap behind `IHotStore` rather than a foundation everything waits on.

It also comes **before clustering**, and that ordering is load-bearing: cold world data grows with area (the
N² term), live simulation grows with player density. Sharding alone just re-bills the N² problem as more
machines holding cold terrain. Paging attacks the bigger term and needs no coordination layer.

## Deliverables

**`MelangeDB.Storage.Faster` — `FasterHotStore`**
- `IHotStore` over `Microsoft.FASTER.Core` 2.6.5, giving a hybrid log with spill-to-disk.
- Recorded constraint: **Tsavorite is not published on NuGet.** It lives in `microsoft/garnet` at
  `libs/storage/Tsavorite/cs` as an internal, significantly-diverged fork of FASTER; `Microsoft.Garnet` is
  the Redis-protocol *server* and the wrong abstraction. Vendoring Tsavorite source stays an option for
  later **if and only if** benchmarks justify it.
- Secondary index maintenance, including the range indexes the terrain-streaming subscriptions need.

**Residency** — the resolution of the sharpest tension the audit found. The reference workload does
`foreach (var x in ctx.Db.Table.Iter())` in **52 places**, which is nearly free when everything is resident
and becomes I/O the moment a store pages. "Just cache it" reintroduces the RAM ceiling by the back door, so
instead the memory budget becomes **declarative**:
```csharp
[Table(Public = true, Residency = Residency.Resident)]   // small, bounded, scan-heavy
public partial struct ItemDefinition { /* ... */ }
```

### The 52 scans are four problems, not one

Classifying every site by how often it actually runs changes the scope considerably. By call frequency:
~24 are init/admin/migration (`InitFlora`, `ResetResourcePopulation` — 7 sites alone — `ClearWaterData`,
`BackfillAttributes`), ~10 are scheduled reducers, ~18 are client-facing. **Roughly half don't matter**: a
scan in a reducer an operator runs once may page from disk all day.

By *fix*, which is what determines the work:

| Group | ~Sites | Fix |
| --- | --- | --- |
| **Missing index** | 6–8 | `[Index]` / `[Unique]`, not residency |
| **Existence check** | 5 | `.Any()`; costs one page either way |
| **Genuinely wants `Resident`** | 8 | Residency's real job |
| **Wants *sharding*, not residency** | 6 | Fixed for free by phases 09–10 |

The missing-index group is worth calling out because it reframes the porting cost as a *benefit*.
`SetPlayerName` scans `PlayerState` for name uniqueness and documents why: *"No unique index on Name (renames
are rare and player_state is small), so this scan is the only thing keeping two players from sharing a
nameplate."* That is a latent index bug an all-in-RAM database permitted. `FindBookByItemDef`,
`FindPlayerByName`, and `HitchVehicle` are the same shape.

One in this group needs care rather than an attribute: **`[Unique]` is a single-writer guarantee and is
restricted to non-partitioned tables**, and `PlayerState` becomes `Partitioned` in phase 11 — a unique index
cannot span shards. A globally-unique name is a *claim*, and claims live in a small `Global` table on the hub:
claim the name there, then write the shard-local row. Renames are rare, so the two-step is fine — unlike
gathering, which is exactly why the placement rule exists.

The sharding group — `DecayBuildings` materialising all of `PlacedBuilding` into a `List` every tick,
`RecountCensus` scanning `Creature` and `CreatureCorpse` — is unbounded and residency cannot help it. Per-shard,
those tables are small. **Do not over-engineer residency for scans that partitioning fixes.**

### Decision: opt-in `Resident`, default `Paged`

Settled in favour of opt-in over resident-until-a-size-threshold, for two reasons.

**A size threshold makes memory a function of data size, which is the SpacetimeDB failure mode with a delay.**
A fresh deployment behaves like SpacetimeDB until a table crosses the threshold — and that crossing is a
performance cliff arriving under production load, on the largest world first. It is the same objection that
rules out "just add a cache": the ceiling returns through the back door, now nondeterministically.

**Opt-in makes the resident footprint a declared, computable artifact.** You can read the source and answer
"does a 20km world fit in 8GB." With a threshold you can only find out by running it — and that is precisely
the question this project exists to answer.

The porting-cost objection is weaker than it appears: ~12 tables need annotating, not 82.

This only holds if a missing annotation is cheap to discover, so these ship *with* the decision:

1. **A compile-time analyzer** flagging unindexed full scans over non-resident tables. This makes the analyzer
   the porting tool — port with the paged default, compile, and it produces the exact list instead of requiring
   guesswork.
2. **A startup residency report** — per table row count and measured bytes, plus the buffer-pool cap, summing
   to the real budget. Observable, not theoretical.
3. **`.Any()` / `.Count` / `.First()`** so existence checks cannot accidentally scan.
4. **`Residency.Auto`** available for anyone explicitly wanting threshold behaviour, never as the default.
5. **Per-table configuration override** (`MelangeDb:Residency:<TableName>`), with the attribute as the default.
   The right set depends on deployment size — a 2km test world and a 20km production world differ — and an
   operator hitting a slow scan should be able to fix it without a code change and a redeploy.

### Configuration items go in the register

This phase introduces the first settings a user will realistically tune, so it also establishes the standing
convention: **every configuration item is added to [CONFIGURATION.md](../CONFIGURATION.md) in the same change that
introduces it.** Not at the end of the phase, and not "when the docs get written." An undocumented knob is how
a library turns into folklore, and this is the phase where the knobs start mattering.

That register is the single source of truth for key names, defaults, and reload semantics, and the rule applies
to every phase from here on — not only this one.

**Large values out of line** — blob columns are the dominant memory consumer: the reference workload stores
one RLE-compressed terrain blob per chunk across ~24.6k chunks, plus flora, water, and three LOD tables.
Large values are stored out of line so scanning a table by key does not fault in every blob.

**Bulk ingestion** — world generation writes tens of thousands of rows in one pass. One large write set, not
one transaction per row.

**Snapshots and log compaction** — required before the log outgrows disk. Snapshot at an LSN, truncate behind
it, respecting the slowest applier's *and* the slowest event subscriber's checkpoint — *live* subscribers only:
checkpoints idle past `Events:SubscriberExpirySeconds` are evicted (phase 06) precisely so an abandoned one
cannot pin retention forever.

## Out of scope

Clustering (09–10). Replacing the in-memory store — `InMemoryHotStore` stays, permanently, as the fast path
for tests.

## Decisions to settle

- ~~**Residency default.**~~ **Settled: opt-in `Resident`, default `Paged`** — see above for the reasoning and
  the four supports it ships with.
- ~~**Eviction policy.**~~ **Settled: FASTER's own log eviction, nothing cleverer.** FASTER's hybrid log
  evicts by address order — the in-memory tail of the log is the buffer pool, and records fall out of it
  oldest-written-first as the tail advances past `HotStore:MemoryBudgetBytes`. That is LRU-by-page-of-write,
  not LRU-by-access, and it ships as-is: what MelangeDB adds on top is only the *split* of the budget (main
  records and out-of-line blobs get separate hybrid logs, so blob churn cannot evict hot main records) and the
  residency tiers around it (resident tables never enter the pool at all; the key directory and indexes are
  pinned bookkeeping, so a miss costs one read, never an index walk on disk). Read-hot-record copy-to-tail
  stays off. The spatial eviction-hint idea stays unbuilt, as the plan ordered — measure first.
- ~~**Does the store own indexes, or the applier?**~~ **Settled: the store, reaffirmed for FASTER.** The
  phase-01 arrangement survives contact with a paging engine unchanged: `FasterHotStore` maintains its key
  directory and every secondary index (equality and range) behind the same `IHotStore.Apply` seam, and the
  applier pipeline stayed untouched by the engine swap — which was the point of the seam. The paging engine
  adds one refinement that settles the question for good: each row's directory entry records its encoded
  index values, so index maintenance on update and delete never reads the old row back from disk. An
  applier-driven index would have to do exactly that fault, on every update, from outside the store.
- ~~**Snapshot format**~~ — **Settled: full.** One CRC-guarded file beside the log (epoch, LSN, AutoInc
  sequence table, every row), written to a temp file and atomically swapped, streamed in both directions so a
  snapshot larger than memory neither buffers on write nor materializes on load. Incremental was rejected
  because a chain of increments is a second replay mechanism living beside the log — which already is one —
  and restart cost would grow with chain length. At the reference scale a full dump is seconds of sequential
  I/O; revisit only if snapshot pause time ever shows up in a measurement.

### Implementation decisions recorded

- **FASTER is a projection; recovery is ours.** The engineering timebox came due exactly where the risk
  section predicted — FASTER's checkpoint machinery versus our log — and the simpler composition won, taken
  further than the fallback the plan allowed: `FasterHotStore` rebuilds from snapshot + log replay on *every*
  start, clean shutdown included, and FASTER's checkpoint/recovery is not used at all. One recovery story
  covers both store engines, crash consistency is inherited from the log (there is no FASTER-side state to
  tear), and the seam stays clean. The recorded cost: startup time proportional to snapshot size — sequential
  I/O, seconds at the reference scale (~24.6k blob rows bulk-load in under half a second in the recorded
  benchmark, and snapshot load is the same write path).
- **Out-of-line blobs split the serialized row byte-exactly.** A `byte[]` payload of 256 bytes or more moves
  to the blob log; the main record keeps the column's null flag and length prefix, and the splice on read
  reproduces the original bytes exactly — serialized bytes are a row's identity, and the same test suite
  proves both stores byte-identical. The threshold is a constant, not configuration: below it the indirection
  costs more than it saves, and a knob would be folklore.
- **`Residency.Auto` starts resident and demotes loudly** (EventId 1505) when the table crosses
  `Residency:AutoThresholdBytes` — threshold behaviour by explicit request only, never the default, exactly
  as the residency decision ordered.
- **`CommitLog:GroupCommit` shipped accepted-and-reserved** (the `Scheduler:MaxConcurrentTicks` precedent):
  the engine's single-writer lock serializes commits, so there are never two appends in flight for one fsync
  to cover — the bulk path is the batching that exists. Recorded in the register with the reasoning.
- **The zero-filled torn tail.** Extending the kill tests to the paging store exposed a phase-01 latent bug:
  a zero-filled torn tail parses as a zero-length record whose declared CRC (zero) equals the CRC of zero
  bytes, crashing recovery in the codec. The log now treats a zero-length frame as torn everywhere it walks
  records. The kill-test pattern earning its keep.
- **The recorded numbers** (Debug build, local NVMe; the tests assert loose floors — 10x and 5x — so they
  hold on slower machines, and print the measured values): bulk-loading the reference 24.6k×1KB blob workload
  ran at **19.6µs/row against 860µs/row** for per-row transactions under the default durable fsync — **44x**;
  a FASTER resident full scan of 50k rows measured **1.05x** the in-memory store (3.83ms vs 3.64ms); the
  125MiB-dataset-under-8MiB-budget test held working-set and heap growth under half the dataset with zero
  incorrect reads, and a key walk over 100MiB of blobs faulted zero pages.

## Done when

- A dataset **larger than the process's memory limit** is queried correctly, with resident memory staying
  bounded. This is the phase's whole point and needs a real test with a hard cap, not an estimate.
- Point lookups and range scans over a paged table return identical results to `InMemoryHotStore` — the same
  test suite runs green against both stores.
- A `Resident` table's full scan performs within a small factor of the in-memory store.
- Loading ~24.6k blob rows via the bulk path is dramatically faster than per-row transactions, with a number
  recorded.
- Scanning a blob table by key does not resident-fault the blobs, asserted by measuring memory.
- Snapshot, truncate, restart, and recover produces identical state; truncation never passes the slowest
  applier or event subscriber checkpoint.
- Crash-consistency: kill during heavy writes, restart, verify no torn rows and no lost committed
  transactions.
- The analyzer flags a deliberately-added unindexed scan over a paged table, and stays silent for the same scan
  over a `Resident` one.
- The startup residency report's total matches measured process memory within a stated tolerance — a budget
  that doesn't predict reality is worse than none.
- Every setting this phase introduced appears in [CONFIGURATION.md](../CONFIGURATION.md) with its real default,
  verified against the code rather than the plan.

## Risks

- **This is the phase most likely to consume months.** FASTER's API is powerful and unforgiving, and its
  session/epoch model does not map obviously onto a transactional store. Timebox the integration; the
  in-memory store means there's always a working system to fall back to.
- **FASTER is effectively frozen** — active development moved to Tsavorite inside Garnet. Acceptable for a
  stable dependency, but it means bugs won't be fixed upstream. Keep the `IHotStore` seam clean.
- **Residency will be litigated repeatedly.** Whatever default is chosen, someone will hit the other case.
  Make it per-table overridable and easy to observe (a diagnostic listing resident tables and their footprint).
