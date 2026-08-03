# Phase 08 — The Postgres tier and ad-hoc SQL

**Goal:** `[Table(Tier = StorageTier.Relational)]` tables live in Postgres, applied from the log, and admin
tooling can run one-shot SQL including aggregates.

**Depends on:** [01](plan-phase-01.md), [07](plan-phase-07.md) for compaction interaction.

## Why here

This closes M1 and it deletes real code the reference project already wrote by hand: `admin-web` runs a
`PostgresStore` plus a `ScrapeWorker` that samples `WorldStat`/`CreatureCensus` rows out of SpacetimeDB into
Postgres, purely so the console can run `date_trunc('hour', at)` and `COUNT(*)` queries SpacetimeDB can't.
That worker exists because the database had no relational tier. Here it becomes a table attribute.

## Deliverables

**`MelangeDB.Storage.Postgres`**
- `AddPostgres(connectionString)` — **opt-in**. A deployment with no relational tables needs no Postgres at
  all, and the zero-infra single-file story must stay intact.
- Schema generation / migration for relational-tier tables from the same `TableSchema` the hot store uses, so
  one schema definition serves both tiers.
- `ILogApplier` writing relational rows, **with its own LSN checkpoint** — so Postgres may lag the hot store
  and resume from its own position after downtime. This is the property that makes the whole two-backend
  design work without 2PC.
- Reads for relational tables inside a reducer go through the write-set overlay, so read-your-writes holds
  even though the tier is eventually consistent with the log.

**Ad-hoc SQL**
- A query endpoint for one-shot SQL with aggregates — `COUNT(*)`, `SUM`, `GROUP BY`, time bucketing — which
  live subscriptions deliberately cannot express. Needed for parity with the existing admin console.
- Two explicit modes: **policy-enforced** (row policies from phase 04 apply) and **owner** (they don't).
  Ambiguity here is a security hole; there is no third mode and no default-to-owner.

## Out of scope

Postgres as the hot tier. Cross-tier joins in subscriptions. Sharding the relational tier (09 notes it as
"probably Postgres's problem").

## Decisions to settle

- **Does ad-hoc SQL read the hot tier too, or only Postgres?** Only-Postgres is far simpler but then admin
  tooling can't query world state, which is most of what it wants. A read-only SQL view over the hot store is
  a real feature, not a footnote — scope it deliberately.
- **Migration strategy.** Adding a column to a relational table must not require dropping data. This is where
  the schema-migration question deferred in DESIGN.md §10 becomes concrete and can no longer be dodged.
- **Applier lag visibility.** If Postgres is minutes behind, someone must be able to see that. Expose the
  gap between the log head and each applier's checkpoint as a health metric.
- **Transactional grouping.** Applying one Postgres transaction per log record is correct and slow. Batching
  N records per transaction is fast and still correct given the checkpoint, provided the checkpoint advances
  only with the batch.

## Decisions settled (phase 08 shipped)

- **Ad-hoc SQL reads both tiers, split by shape.** The four **row shapes** run against the **hot
  store at head** for every queryable table — which includes relational tables, per the decision
  below — exactly as phases 03/04 built them. **Aggregate shapes** (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`,
  `GROUP BY`, `DATE_TRUNC` bucketing) run against **Postgres**, are valid **only for
  relational-tier tables**, and reflect the applier's checkpoint (the documented lag). What admin
  tooling can do: row queries over anything visible in its mode (current state, no lag), and
  aggregates over relational tables. What it cannot: aggregates over hot-tier tables, and joins.
  The "read-only SQL view over the hot store" beyond the four shapes stays future work — the four
  shapes already are that view for the workload audited.
- **Aggregates are owner-mode only.** Row policies are in-process DI code; they cannot be pushed
  into Postgres, and computing an "enforced" aggregate without them would be the silent security
  hole the two-mode contract exists to prevent. A policy-enforced aggregate is refused loudly
  (`owner_required`), not computed unenforced and not answered empty.
- **Owner-mode authorization is a role claim** — `Sql:OwnerRole`, default `melange-owner`, the
  `Auth:GuestRole` precedent: the IdP is the gate, and an explicit owner-identity list in MelangeDB
  config would be a second identity system. In `Owner` mode a caller without the claim gets
  `403 owner_required`; there is no silent downgrade. Owner mode may additionally name **private
  relational-tier** tables (rows and aggregates) — WorldStat-shaped data is private by default and
  exists precisely for admin tooling — while private hot tables stay server-internal in every mode.
- **Migration strategy: additive automatic, destructive manual.** Under `Postgres:AutoMigrate`,
  missing tables are created and missing columns added (`ADD COLUMN`, NOT NULL kinds backfilled
  with the kind's zero value) — data is never dropped or nulled. A changed column type — or any
  other destructive disagreement — is refused loudly (EventId 1604) in **both** settings. With
  AutoMigrate off (the default), the applier validates and stalls with the exact pending DDL in the
  log; running it manually recovers without a restart. Postgres columns not in the schema are left
  alone. This closes the relational half of DESIGN.md §10's migration deferral.
- **Transactional grouping: batch per `Postgres:ApplyBatchSize` (default 100), checkpoint inside
  the batch.** One Postgres transaction per batch of log records, the checkpoint row updated in
  that same transaction — so "applied" and "checkpointed" cannot diverge and resume is gap-free and
  duplicate-free by construction. Records touching no relational table still advance the
  checkpoint. The batch size is live-reloadable and floor-clamped to 1.
- **Relational rows also live in the hot store (option a).** Tier means *additionally Postgres*,
  not *instead of the hot store* — the three axes (tier, placement, residency) stay orthogonal, as
  the glossary always claimed. This keeps reducer reads, read-your-writes, uniqueness enforcement,
  and public-table subscriptions working through the one existing path, with the bound made
  explicit: relational tables page like any table under phase 07's buffer pool, and may be pinned
  or paged via the ordinary residency knobs. The rejected option (b) — hot store holds nothing,
  reducer reads error — would have made `[Unique]` on a relational column unenforceable at commit
  time, turning every duplicate into a poisoned applier instead of a rejected transaction.
- **Checkpoint provenance is checked.** The checkpoint row records the log epoch; a mismatch stalls
  loudly (EventId 1605) rather than applying LSNs against the wrong log. A tier attached after log
  truncation cannot replay from the start, so it bootstraps from the hot store at one consistent
  LSN (EventId 1606) and continues from there.
- **`WaitForApplied(lsn)` shipped minimal and honest** as
  `PostgresRelationalTier.WaitForAppliedAsync` — it completes when the checkpoint reaches the LSN,
  and it can wait as long as Postgres is down; callers bring their own timeout.

## Done when

- A relational table's rows appear in Postgres after commit, with the applier checkpoint advancing.
- Stopping Postgres does not stop the server: writes continue, hot-tier reads and subscriptions are unaffected,
  and the applier catches up on reconnect with no gaps or duplicates.
- A reducer reads a relational row it wrote in the same transaction — the overlay path.
- A mixed reducer writing both a hot and a relational table is atomic: kill the process immediately after
  commit and both tiers converge to the committed state.
- Ad-hoc SQL runs the aggregates the reference admin console needs (hourly bucketing, counts).
- Policy-enforced SQL mode cannot see rows a subscription would hide; owner mode can. Both tested.
- Log compaction respects the Postgres applier's checkpoint and cannot truncate ahead of it.
- A deployment with no relational tables starts and runs with no Postgres configured at all.

## Risks

- **Silent applier stall** is the dangerous failure: writes keep succeeding while Postgres falls hours behind
  and nobody notices until an admin query returns stale data. Health metrics and a loud log are not optional.
- **The lag is user-visible in ways that surprise people.** "I registered but the account isn't in Postgres
  yet" is a legitimate consequence of the design. Document the guarantee precisely, and consider a
  `WaitForApplied(lsn)` primitive for the narrow flows that genuinely need read-after-write across tiers.
