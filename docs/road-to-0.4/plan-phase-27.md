# Phase 27 — Constraint changes are migrations

**Status: Built.** Fixes [#192](https://github.com/tyler-loy/MelangeDB/issues/192). Ships whole
with 0.4, not split into an earlier fix. See [Splitting the phase](#splitting-the-phase) and
[As built](#as-built).

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

**Settled (2026-11-20): not split; the whole phase ships with 0.4.** The case for splitting was
that the refusal is small, has a failing test today, and closes a correctness hole in shipped code,
so it could ship as a 0.3.x fix ahead of the shape-history and relational-tier halves. The decision
was to fix it once, with the milestone, rather than ship an interim check that the shape-history
half would then replace. Until 0.4, the hole is recorded in
[#192](https://github.com/tyler-loy/MelangeDB/issues/192), and the workaround is to dedupe before
adding `[Unique]`.

## Decisions to settle

**All three settled by the owner on 2026-11-22, as built.**

### Check on every rebuild, or only at a recorded migration

Without uniqueness in the shape, the only safe place to check is every unique-index build. That is
always correct and costs a duplicate check per unique index on every boot that rebuilds, which is
cheap next to the rebuild itself. With uniqueness in the shape, the check runs once.

**Built as leaned: at the recorded migration.** With the phase shipping whole there is no interim
release that needs an every-build check. The check runs at the boot that adds a `[Unique]`, once
on the first boot over a sidecar that predates uniqueness recording, and once when adopting over
existing records. The steady-state boot checks nothing. The debug-only rebuild assertion the
leaning mentioned was not built: write-time enforcement already covers every path except bulk
ingestion's within-batch gap (see [As built](#as-built)), and an assertion that only fires in
Debug would not have caught #192 in production either.

### Old sidecars that do not record uniqueness

A sidecar written before this phase has no constraint field. Treating "absent" as "no column was
unique" would make the first post-upgrade boot look like it adds every `[Unique]` in the schema
and check them all, which is correct and costs one pass. Treating it as "unknown, adopt silently"
would skip the check for exactly the deployments that might have been bitten by the bug.

**Built as leaned: absent means "check everything once".** An absent field is not reported as a
change, so it makes no marker or reign. Every unique column is checked, and on success the field
is filled in place (EventId 1010), the way `RecordMissingTiers` fills tiers. The fill runs only
after the check passes. A sidecar that recorded uniqueness the rows break would let every later
boot skip the check that should have caught it.

### What a refusal tells an operator to do

The engine cannot dedupe for them, but a refusal that only says "duplicates exist" sends the
operator to write a reducer under the old declaration. Open was whether `melange` grows a read-only
verb that lists the duplicates from a data directory offline, or whether the refusal message,
bounded, is enough.

**Built: the bounded message, no verb.** The refusal names up to ten duplicated values per column
and up to five keys per value, with counts for the rest. It also adds a specific hint when the
column is new in the same deploy, since then every row holds its default. That is enough to write
the dedupe reducer. A verb can follow if a refusal ever names more than an operator can act on.

## As built

- **Where it lives.** `TableShape.Unique` records each table's non-key unique columns, null in old
  sidecars. `ShapeCompatibility` classifies an added or removed `[Unique]` as additive (a recorded
  reign, like a hot-to-hot tier change) and lists the columns to check in
  `ShapeDiff.UniqueToCheck`. `UniqueConstraintCheck` runs after recovery and before
  `CompleteShapeMigration`, so a refusal leaves no marker and no new reign.
  `ShapeHistory.RecordUnique` fills old sidecars after the check passes.
- **Cost.** One sequential `Scan` per table, holding the values seen so far for each checked
  column: at most one extra copy of what that column's in-memory secondary index already holds, for
  a check that runs once per change. An ordered walk of the index would need less memory, but it
  would need a new member on the public `IHotStore`, which every third-party store would then have
  to implement.
- **The relational backplane, established.** The tier is constructed from the engine, so the hot
  tier's refusal always fires before any Postgres DDL. What was broken was the other direction:
  `ux_`/`ix_` naming plus create-if-not-exists meant **removing** `[Unique]` left `ux_<table>_<column>`
  enforcing in Postgres, and the applier's first duplicate failed against it.
  `PostgresSchemaManager` now drops it, applied under `AutoMigrate` or printed in the refusal
  otherwise, and drops a plain `ix_` that a new `ux_` supersedes.
- **Not addressed, recorded.** `/melange/bulk` checks uniqueness against committed rows but not
  within its own batch (documented on `MelangeEngine.BulkInsert`), so one bulk load can still commit
  duplicates. The next boot that checks the column names them, but nothing checks between boots.
  Separately, a `CREATE UNIQUE INDEX` over a Postgres copy that lags the hot store could meet
  duplicates the hot store has already resolved. That would stall the tier until it catches up, and
  it is unchanged by this phase.
- **Tests.** `UniqueConstraintMigrationTests` (Core): the #192 probe as a refusal; distinct rows
  migrate and are recorded; removal; a column added as unique; an empty table; old sidecars, both
  clean and with duplicates; adoption; nulls; the dry-run diff; the refusal's bounds. Three
  `SchemaMigrationTests` (Postgres): a stale `ux_` dropped so duplicates apply, a superseded `ix_`
  dropped, and the `AutoMigrate`-off refusal printing the drop.
