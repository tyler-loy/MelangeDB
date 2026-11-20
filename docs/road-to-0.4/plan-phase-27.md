# Phase 27 — Constraint changes are migrations

**Status: Planned.** The refusal half should ship as a 0.3.x fix ahead of the milestone. See
[Splitting the phase](#splitting-the-phase).

**Goal:** adding `[Unique]` to a column that already holds rows checks those rows. Duplicates refuse
the boot loudly and name the duplicated values and the keys that hold them, the way a destructive
change already refuses. A constraint change becomes a recorded migration in the shape history,
rather than something a rebuilt index silently assumes.

**Depends on:** nothing. It extends [phase 16](../road-to-0.2/plan-phase-16.md)'s migration rule,
"additive automatic, destructive refused and loud", to a kind of change phase 16 classified as a
projection.

## Why here

**It is a correctness bug, found while planning this milestone.** Phase 16 classed "added or removed
indexes" as additive: indexes are projections, and recovery rebuilds them from the declaration. That
is right for `[Index]`. It is wrong for `[Unique]`, which is not only an index but a *claim about the
data*, and nothing checks the claim against data that already exists. Probed on 2026-11-20 against
`main`:

1. Boot `Hero` with `X` indexed and insert two rows with `X = 10`.
2. Reboot with `X` declared `[Unique]`.
3. **The boot succeeds.** `ScanIndex(X = 10)` returns two rows under a unique index.
4. A third insert with `X = 10` is refused: `unique constraint on 'X' violated`.

So the engine enforces a constraint on new writes that its existing data already breaks. Every
reader written against `[Unique]`, whether a reducer's `Find` on the unique column or a client's
unique-index lookup, has been promised one row and has two to choose from. Which one it gets is
whatever the index walk returns first, which is not a contract anyone wrote down.

**SpacetimeDB checks this** (2.7, #4465): adding a unique or primary-key constraint is a
non-breaking migration when the existing data satisfies it, and fails safely naming the duplicates
when it does not. That is exactly the behaviour this project's own migration rule would have
predicted, had uniqueness been in the shape.

## Deliverables

- **The refusal.** At boot, a unique index built over existing rows that finds a duplicate refuses
  the boot. The message names the table, the column, each duplicated value (truncated past a
  bound), and the primary keys holding it, and says what to do: dedupe with the old declaration,
  then redeploy. Same tone and EventId family as phase 16's destructive refusals.
- **Constraints in the shape.** The shape sidecar records which columns are unique, so "this boot
  *adds* a constraint" is knowable from the history rather than inferred, and the check runs once,
  at the migration, rather than on every rebuild. Removing `[Unique]` is additive and records the
  same way. See the decision below on the old sidecars that do not carry this.
- **The relational backplane, established.** `PostgresSchemaManager` creates a unique index as
  `ux_<table>_<column>` and a plain one as `ix_…`, so `[Index]` → `[Unique]` issues a new
  `CREATE UNIQUE INDEX` and leaves the old `ix_` behind. Over duplicates, that DDL fails in Postgres.
  The phase must establish what the applier does with that today (poison, retry, skip), and make
  the hot-tier refusal fire first so the relational tier never sees a constraint the hot tier did
  not accept.
- **[MIGRATION.md](../MIGRATION.md)**: constraint changes get their own row in the
  additive/destructive table, with the dedupe recipe.
- Tests: the probe above as a failing test first; a satisfied constraint migrates; removal is
  additive; a cluster `Global`/`Replicated`/`Local` table refuses on the node that owns it.

## Out of scope

- **Moving the primary key.** Phase 16 refuses it as destructive and this phase keeps that.
  SpacetimeDB allows *adding* a primary key, but every MelangeDB table already has one, and moving
  it re-identifies every row: client caches, resume anchors, event payloads, and the Postgres
  copy all key on it. That is a rekey, not a constraint change, and it stays a manual migration.
- **Uniqueness across shards.** [CLUSTERING.md](../CLUSTERING.md) restricts `[Unique]` to
  non-partitioned tables at compile time, as a single-writer guarantee. Unchanged.
- **Automatic dedupe.** Which duplicate survives is a game decision, not a database one.

## Splitting the phase

**Leaning: split.** The refusal is small, has a failing test today, and closes a correctness hole
in shipped code. It should not wait for a milestone; ship it as a 0.3.x fix. What remains for the
phase is the shape-history half and the relational-tier half, which change a file format and
need the decisions below.

## Decisions to settle

### Check on every rebuild, or only at a recorded migration

Without uniqueness in the shape, the only safe place to check is every unique-index build. That is
always correct and costs a duplicate check per unique index on every boot that rebuilds, which is
cheap next to the rebuild itself. With uniqueness in the shape, the check runs once.

**Leaning:** both, in order. The 0.3.x fix checks on every build, because it needs no format change.
The phase then adds the shape field and narrows the check to the migration, keeping the rebuild
check behind a debug assertion.

### Old sidecars that do not record uniqueness

A sidecar written before this phase has no constraint field. Treating "absent" as "no column was
unique" would make the first post-upgrade boot look like it adds every `[Unique]` in the schema
and check them all, which is correct and costs one pass. Treating it as "unknown, adopt silently"
would skip the check for exactly the deployments that might have been bitten by the bug.

**Leaning:** absent means "check everything once". It is the same adoption cost phase 16 paid, and
it is the only reading that finds damage the bug already did.

### What a refusal tells an operator to do

The engine cannot dedupe for them, but a refusal that only says "duplicates exist" sends the
operator to write a reducer under the old declaration. **Open:** whether `melange` grows a read-only
verb that lists the duplicates from a data directory offline, or whether the refusal message,
bounded, is enough.
