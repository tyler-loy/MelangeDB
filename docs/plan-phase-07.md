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
convention: **every configuration item is added to [CONFIGURATION.md](CONFIGURATION.md) in the same change that
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
it, respecting the slowest applier's *and* the slowest event subscriber's checkpoint.

## Out of scope

Clustering (09–10). Replacing the in-memory store — `InMemoryHotStore` stays, permanently, as the fast path
for tests.

## Decisions to settle

- ~~**Residency default.**~~ **Settled: opt-in `Resident`, default `Paged`** — see above for the reasoning and
  the four supports it ships with.
- **Eviction policy.** LRU by page is the obvious start; a spatial workload might do better with
  eviction hints from the shard strategy. Don't build the clever version first.
- **Does the store own indexes, or the applier?** Deferred from phase 01; must be answered here.
- **Snapshot format** — full table dump versus incremental. Full is simpler and probably fine at this scale.

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
- Every setting this phase introduced appears in [CONFIGURATION.md](CONFIGURATION.md) with its real default,
  verified against the code rather than the plan.

## Risks

- **This is the phase most likely to consume months.** FASTER's API is powerful and unforgiving, and its
  session/epoch model does not map obviously onto a transactional store. Timebox the integration; the
  in-memory store means there's always a working system to fall back to.
- **FASTER is effectively frozen** — active development moved to Tsavorite inside Garnet. Acceptable for a
  stable dependency, but it means bugs won't be fixed upstream. Keep the `IHotStore` seam clean.
- **Residency will be litigated repeatedly.** Whatever default is chosen, someone will hit the other case.
  Make it per-table overridable and easy to observe (a diagnostic listing resident tables and their footprint).
