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
