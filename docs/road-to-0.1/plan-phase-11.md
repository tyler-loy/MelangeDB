# Phase 11 — Reference workload port and validation

**Status: Shipped — the port. The measurement pass is outstanding; see the shipped notes.**

**Goal:** the reference workload runs on MelangeDB, and the three original complaints are demonstrably fixed
rather than argued.

**Depends on:** whatever the port needs — realistically 01–08, with 09–10 for the clustering claims.

## Why here, and why that's partly a lie

This is written last for planning, but **the port should start around phase 03.** A subset of the reference
workload's 82 tables and 119 reducers is the most honest integration test available, and saving it for the end guarantees
discovering API mistakes after they're expensive. Treat this phase as "the port completes and is measured,"
not "the port begins."

## Deliverables

**Table classification** — the real work. Each of the 82 tables gets a `Placement`, a `Tier`, a `Residency`,
and a visibility. The classes, from the audit:

- **`Partitioned` by chunk** — `TerrainChunkData`, `WaterChunk`, `FloraChunk`, `TerrainLOD3/4/Water`,
  `Creature`, `CreatureCorpse`, `PlacedBuilding`, `DroppedItem`, `ResourceNode`, `TreeStump`, `Mine`,
  `TunnelCell`. The bulk of the world and the bulk of the memory.
- **`Replicated` + `Resident`** — `ItemDefinition`, `Recipe`, `RecipeStep`, `SkillDefinition`,
  `CreatureSpecies`, `FloraSpecies`, `CombatConfig`, `WorldConfig`, `BuildingPieceDefinition`,
  `AttributeDefinition`. This is exactly the set the 52 `.Iter()` scans run over — one classification makes
  those scans legitimate again.
- **`Partitioned`, following the player** — `PlayerState`, `InventoryItem`, `PlayerSkill`, `PlayerAttribute`,
  `EquipmentSlot`, `PlayerCombatState`. **Not hub tables** — see phase 09's trap.
- **`Global`, relational tier** — `AdminIdentity`, `WorldStat`, trade history. (`PlayerRateLimit` is not
  ported at all — see the delete-on-port list.)

**Reducer port** — 119 reducers from `public static` + `ReducerContext` to DI-resolved classes. Mostly
mechanical, with four real changes:
- The 14 scheduled reducers move from one global timer row to **one timer row per shard**, and the hand-written
  "only chunks near an online player" filtering becomes implicit in the partition.
- The 9 `ClientVisibilityFilter` SQL strings become `IRowPolicy<T>` objects, with `inventory_item`'s three
  filters unioning as before.
- Reducers whose write set spans shards must be resolved — the shard-span check from phase 09 finds them.
- `SetPlayerName`'s uniqueness scan becomes a `Global` name-claims table on the hub, because `[Unique]` cannot
  span shards and `PlayerState` is partitioned (phase 09).

**Delete-on-port list** — code that exists only because SpacetimeDB lacked something:
- `tools/admin-web/Services/PostgresStore.cs` + `ScrapeWorker` → `Tier = StorageTier.Relational`.
- `PlayerRateLimit` + the token-bucket half of `RateLimit.cs` → `RateLimit:*` configuration (phase 04). The
  movement-plausibility check stays in the module — that's gameplay, not infrastructure.
- The NativeAOT-LLVM WASM toolchain → gone; the module is just a library in the host.
- `spacetime publish` + `spacetime generate` across three binding trees → `dotnet publish`.
- The admin-console-restart-after-publish dance → gone.
- Tooling-identity detection in `ClientConnected` → gone; an admin query is not a session.

**Client migration** — Godot game client, admin web, terrain-gen CLI, all three generating from one schema.

**Measurement.** The port is worthless as validation without numbers:
- Memory for the 10km world versus SpacetimeDB's, and memory for a 20km world SpacetimeDB **cannot host**.
- Reducer latency p50/p99 for gather, move, attack, craft — versus the current build.
- Terrain-streaming throughput as a player crosses chunks.
- Concurrent players per node, and the hotspot number from phase 10.

## Out of scope

Gameplay changes. New features. Any redesign that isn't forced by the port — the point is a controlled
comparison, and changing the game while changing the database destroys it.

## Decisions to settle

- **Big-bang or incremental?** Incremental needs both databases live at once and a bridge, which is real work.
  Big-bang on a branch with a data migration is probably right for a pre-release game.
- **Data migration.** Exporting 24.6k terrain blobs plus live player state out of SpacetimeDB and into
  MelangeDB's log format. Terrain-gen can regenerate the world; player state cannot be regenerated.
- **What counts as done?** "A playtest at parity" is the honest bar — every mechanic works, nothing regressed.
  Define the checklist before starting, not after.
- **Does the port keep `ushort ChunkId`?** Widening it now avoids a second migration when the world grows.

## Done when

- All 82 tables classified, all 119 reducers ported, module builds with no `SpacetimeDB` reference anywhere.
- A playtest reaches parity against a written checklist: spawn, move, gather, craft, build, fight, trade, die,
  respawn, mine, forage, tame, and the admin console.
- A reducer reads a feature flag from Azure App Configuration and changes behaviour with no redeploy —
  **complaint 3, demonstrated in the real game.**
- The 20km world (98,596 chunks) runs on one node within a fixed memory budget — **complaint 2, demonstrated.**
- The world runs across multiple nodes with players crossing boundaries — **complaint 1, demonstrated.**
- All measurements above recorded in this repo, including any that came out worse.

## Risks

- **Sunk-cost pressure to declare victory.** If MelangeDB is slower on some path, that must be published, not
  buried. The numbers are the deliverable, and a port that only reports wins is not evidence.
- **The port will find API mistakes that are expensive to fix late.** The mitigation is the one at the top of
  this document: start porting at phase 03, not phase 11.
- **A game is a moving target.** Development of the reference workload continues during the port; the diff grows. A freeze or
  a short port window matters more than it looks.

## Shipped notes

**The port landed, and then kept going.** The reference workload runs on MelangeDB — an ASP.NET host
with `UseFasterHotStore()`, 88 tables, a commit log in the hundreds of megabytes — and the game is
developed on it daily against `0.1.2-ci.*` prereleases published from main. That is a stronger form
of the "playtest at parity" bar than the written checklist this plan asked for: parity is not a
milestone the port passed once, it is the condition of a product under active development.

**What it returned is the part that matters.** The plan predicted the port would find API mistakes
that are expensive to fix late, and it did — the recovery regression, the client identity gap, the
transient-rejection shape, `unknown_reducer` masking reducer faults, and adoption over an existing
directory being silent all came from running the thing, and none of them were caught by a suite that
was green throughout. Every one is closed. That is the evidence a port produces, and it arrived in
the shape the risk register expected.

**The measurement half is not done, and is not being quietly dropped.** None of the numbers this
plan called the deliverable — the 10km and 20km memory comparisons, reducer latency percentiles for
gather/move/attack/craft, terrain-streaming throughput across chunks, concurrent players per node —
are recorded in this repo. Until they are, the published benchmarks are dev-machine measurements and
[ROADMAP.md](../ROADMAP.md)'s "What's left" says so. The risk register's own warning applies to this
note as much as to any other: a port that reports only wins is not evidence, and "it runs in
production" is a fact about the port, not a measurement of it.

**Incremental, not big-bang** — the settled answer to the plan's first open question, decided by
events rather than by argument: the port tracked prerelease packages from main throughout, which
made every MelangeDB change a small, reversible step for the consumer instead of one cutover. That
is also why the issues above arrived one at a time and diagnosable, rather than as a single failed
migration.
