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
- `Resident` tables are pinned wholly in memory; scans over them stay fast and honest.
- Everything else pages. Memory is bounded by what was *declared* resident rather than by the whole dataset.
- An analyzer warns on an unindexed full scan over a non-resident table.
- **Open decision carried from DESIGN.md §10:** opt-in `Resident` (predictable budget, but annotation work
  when porting) versus resident-until-a-size-threshold (existing all-in-RAM code ports untouched, fuzzier
  budget). Settle this in this phase — it changes the porting story in phase 11.

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

- **Residency default** (above) — the biggest call in this phase.
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

## Risks

- **This is the phase most likely to consume months.** FASTER's API is powerful and unforgiving, and its
  session/epoch model does not map obviously onto a transactional store. Timebox the integration; the
  in-memory store means there's always a working system to fall back to.
- **FASTER is effectively frozen** — active development moved to Tsavorite inside Garnet. Acceptable for a
  stable dependency, but it means bugs won't be fixed upstream. Keep the `IHotStore` seam clean.
- **Residency will be litigated repeatedly.** Whatever default is chosen, someone will hit the other case.
  Make it per-table overridable and easy to observe (a diagnostic listing resident tables and their footprint).
