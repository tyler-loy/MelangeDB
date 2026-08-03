# Coverage check: the reference workload against the MelangeDB design

The reference workload is a live SpacetimeDB game — 82 tables, three client trees, in production —
and the only real workload MelangeDB has to satisfy. This audits [DESIGN.md](DESIGN.md) against what
that module actually uses.

Every number below was measured against its source rather than estimated. The game itself is a
private codebase, so it is referred to throughout the docs as "the reference workload"; what matters
here is the shape of what it does, all of which is reproduced in the tables that follow.

## The workload, measured

| | |
| --- | --- |
| Tables | **82** (71 `Public = true`, 11 private) |
| Reducers | **119** across 38 files, ~13.5k LOC |
| Scheduled reducers | **14** |
| Lifecycle reducers | 2 (`ClientConnected`, `ClientDisconnected`) |
| BTree indexes | 34 |
| `[AutoInc]` columns | 44 |
| `[Unique]` columns | 2 |
| RLS filters (`ClientVisibilityFilter`) | 9 |
| Point lookups (`.Find`) | 302 |
| Index scans (`.Filter`) | 89 |
| **Full table scans (`.Iter`)** | **52** |
| Tables with `byte[]` blob columns | 7 |
| Distinct client binding trees | 3 (Godot game, admin web, terrain-gen CLI) |

## What the design already covers

**Single-table subscriptions — fully validated.** Every one of the ~30 subscription queries in the
client is single-table. The shapes in use are exactly three: whole-table (`SELECT * FROM recipe`),
equality (`WHERE owner_id = {id}`), and **range** (`WHERE chunk_id >= {lo} AND chunk_id <= {hi}`,
which is how terrain streams as the player moves). **No subscription anywhere uses a join.** §5's
decision to ship single-table filters and defer incremental join maintenance is not a compromise
here — it covers the real workload as-is. This was the design's riskiest bet and it paid off.

**The hybrid Tsavorite+Postgres split — already built by hand.** `tools/admin-web/Services/PostgresStore.cs`
plus a `ScrapeWorker` samples `WorldStat`/`CreatureCensus` rows out of SpacetimeDB and into Postgres
so the admin console can run time-series aggregates (`date_trunc('hour', at)`, `COUNT(*)`) that
SpacetimeDB can't. You independently arrived at "hot world state + relational servicey tier" and
paid for it with a bespoke scrape worker. In MelangeDB that worker is deleted and `WorldStat` becomes
`[Table(Tier = StorageTier.Relational)]`.

**The library model retires an entire toolchain.** The module currently compiles to WASM via
NativeAOT-LLVM (latest commit: "Move server module to net10.0 via NativeAOT-LLVM"), and every schema
change requires `spacetime publish` then `spacetime generate` across *three* binding trees, then a
restart of the admin console because it binds to the old schema. The in-process library model deletes
the WASM toolchain and the publish step outright.

**Determinism is a non-issue.** Zero uses of `new Random()` or `Random.Shared` in the module;
`ctx.Timestamp` is used 78 times and is already an injected value. Reducers here are *already*
written in the deterministic style §3 asks for.

## Gaps the design must close

### 1. Scheduled reducers — the largest gap

DESIGN.md does not mention scheduling at all. The reference workload has **14** scheduled reducers, and they are
not peripheral — they *are* the simulation: `TickCreatures`, `SimulateCreaturePopulation`, `GrowFlora`,
`RespawnResource`, `TickBreath`, `TickStationHeat`, `WorkTick`, `ExpireTrade`, `ExpireProjects`,
`DecayBuildings`, `DecayCorpse`, `DecayFelledTree`, `CompactChunk`, `CompactFlora`.

The SpacetimeDB pattern is a private table whose rows are timer entries:

```csharp
[SpacetimeDB.Table(Accessor = "CreatureAiTick", Scheduled = "TickCreatures", ScheduledAt = nameof(ScheduledAt))]
public partial struct CreatureAiTick { [PrimaryKey, AutoInc] public ulong Id; public ScheduleAt ScheduledAt; }
```

Storing timers *as rows* is the right idea and MelangeDB should keep it — it makes schedules
transactional and recoverable, since they survive in the log like any other data.

**This has a direct clustering consequence.** A scheduled reducer must fire on exactly one node. That
makes timers the first thing that genuinely forces the deferred clustering question, and it argues
for single-writer designs (Raft leader owns the timer wheel) over sharded multi-writer. Worth noting
in §9 as evidence, not just as an open question.

Also note `CompactChunk` / `CompactFlora`: the game already uses scheduled compaction to fold player
edits back into terrain blobs. That is log compaction by hand, at the application layer.

### 2. Lifecycle reducers

`ClientConnected` sets `IsOnline`, spawns first-time players at the world spawn, and creates combat
state; `ClientDisconnected` is the counterpart. MelangeDB needs these as first-class hooks.

Worth avoiding a wart documented in `Lib.cs`: in SpacetimeDB, **owner SQL queries over HTTP also fire
`ClientConnected` with a fresh ConnectionId**, so the module has to detect tooling identities to avoid
creating ghost player rows and inflating login counts. MelangeDB should separate "a client session
began" from "someone ran a query."

### 3. Table visibility is missing from the design

71 of 82 tables are explicitly `Public = true`; the default is private. DESIGN.md's `[Table]` has a
`Tier` but **no visibility** — and RLS is meaningless without it. Visibility (is this table syncable
to clients at all?) is orthogonal to tier (where is it stored?) and both are needed.

### 4. RLS: union composition — and an opportunity

Nine filters, and the composition semantics are load-bearing: `inventory_item` has *three* filters
(owner, world containers, vehicles) that **union**, so a player sees their own items plus the contents
of any open chest or cart. Any MelangeDB policy model must union rather than intersect.

There's a real win available here. `Rls.cs` documents this footgun:

> an earlier revision added admin-bypass rules joining the private `admin_identity` table; evaluating
> them for a normal client failed with "no such table: admin_identity" and killed the client's
> **entire** subscription (gray screen, no spawn).

That failure exists because RLS rules are SQL strings evaluated in the client's restricted namespace.
DI-resolved policy objects running in-process have no such restriction — they can freely read private
tables like `AdminIdentity`, which makes admin-bypass trivial instead of impossible. This is a
concrete, demonstrable advantage to lead with.

### 5. Column projection in subscriptions

Not every subscription is `SELECT *`:
`SELECT skill_id, total_xp, level FROM player_skill WHERE player_identity = {id}`. The subscription
engine needs column projection, which also means partial row deltas on the wire.

### 6. Large blob columns — revisit the "no blobs" scope call

Seven tables carry `byte[]` payloads: `TerrainChunkData`, `TerrainColumn`, `TerrainLOD3`,
`TerrainLOD4`, `TerrainLODWater`, `WaterChunk`, `FloraBlob`. `TerrainChunkData` is one RLE-compressed
blob per chunk over a 157×157 chunk grid (~24.6k rows, per the `cx * 157 + cy` key convention).

We scoped blob storage *out*, but these aren't a side feature — they're the materialized world and
almost certainly the dominant memory consumer, i.e. the direct cause of the RAM complaint that
motivated the project. The design needs large-value handling (out-of-line storage, so a row's blob
isn't faulted in for a scan that only reads its key) even though a separate S3-style blob API stays
out of scope.

### 7. The sharp tension: 52 full table scans vs. a paging store

This is the finding most likely to bite.

`.Iter()` appears **52 times** — `foreach (var c in ctx.Db.Creature.Iter())`, over `PlacedBuilding`,
`InventoryItem`, `ItemDefinition`, `HomesteadClaim`, and more. In SpacetimeDB these are nearly free
*because* the table is already in RAM. That's the property we set out to remove.

Swap in a store that pages to disk and every one of those 52 sites becomes potential I/O. **The design
goal and the existing code are in direct conflict**, and "just add a cache" isn't an answer — it
reintroduces the RAM ceiling by the back door.

Two honest options, and this is a decision for you:

- **Per-table residency.** `[Table(Residency = Residency.Resident)]` pins small, hot, scan-heavy
  tables (`ItemDefinition`, `CreatureSpecies`, `Recipe` — config-ish and bounded) fully in memory,
  while large tables (`TerrainChunkData`, `InventoryItem`) page. Memory is then bounded by *declared*
  resident tables rather than by everything. Keeps `.Iter()` fast where it's used and honest where it
  isn't.
- **Make scans explicit and index the rest.** Treat unindexed full scans as a smell, add covering
  indexes, and force scan sites to opt in via a distinct API. More correct long-term, but it means
  rewriting a good number of those 52 sites when porting.

Per-table residency is the pragmatic call: many of the scanned tables are genuinely small reference
data, and it makes the memory budget declarative instead of accidental. Several of the 52 are also
just `foreach (...) { exists = true; break; }` existence checks that want a `.Any()` and no scan at all.

### 8. Smaller gaps

- **AutoInc (44 columns).** With the log as the commit point, sequence values must be assigned into
  the write set *before* the append, from a durable, recoverable per-table sequence. Straightforward,
  but it needs specifying — this is exactly the kind of thing that breaks on replay.
- **Ad-hoc SQL endpoint.** Admin tooling runs one-shot queries with aggregates (`COUNT(*)`) that
  subscriptions can't express. Needed for parity with the existing admin console.
- **Struct tables.** The reference workload's tables are `public partial struct` mutated with `with` expressions;
  DESIGN.md §2 shows classes. Value types matter for allocation on the reducer hot path — match it.
- **Bulk ingestion.** terrain-gen writes ~24.6k chunk blobs through reducers. That needs a bulk path,
  not 24.6k transactions.
- **Multi-target codegen.** Three consumers (Godot C#, admin web, CLI) generate from one schema.
  `MelangeDB.CodeGen` should target multiple output trees from the start.

## Verdict

The core bets hold. Single-table subscriptions cover the real workload with no joins needed; the
hybrid storage split is something you'd already built by hand; the library model deletes a WASM
toolchain and a three-way codegen dance.

The design's real omission is **scheduled reducers** — 14 of them run the entire simulation, and they
also force the clustering question sooner than §9 assumes. The real *risk* is **item 7**: 52 full
table scans are load-bearing in a codebase written against an all-in-RAM database, and escaping RAM
is the whole point of MelangeDB. That needs a decision before the storage layer gets built, not after.
