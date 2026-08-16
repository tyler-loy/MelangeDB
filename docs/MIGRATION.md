# Schema migration

How a deployment with a changed schema boots against an existing world. The one rule, both tiers,
one sentence: **additive changes are automatic and loud; destructive changes are refused and
manual.** The relational tier has worked this way since phase 08 (`Postgres:AutoMigrate`,
[CONFIGURATION.md](CONFIGURATION.md)); the hot tier works this way since phase 16, and this page
walks the whole deploy.

## Why the hot tier needs machinery at all

Row format v1 is positional: a row is its columns' bytes in declaration order — no count, no
names, no per-row version. That is what makes a row cost nothing to fan out (the store already
holds the bytes the wire wants), and it is also why the bytes alone cannot say what they mean.
The `melange.shape` sidecar, beside the log, is what says it: per table, the ordered column list
(name and kind) — kept as a **history**, each entry fenced by the LSN it governs from, because
log records outlive deployments. Every reader that decodes a stored row picks the shape governing
that row's LSN; the booting engine compares the newest entry against the code's schema.

## The add-a-column deploy, end to end

You add `public int Level;` to `Hero` — anywhere in the struct; position does not matter, because
migration matches columns **by name** — and redeploy. On boot, per engine:

1. The engine detects the additive difference and takes the migration path: recovery rebuilds the
   projections as it always does, but every old-shape row is re-encoded to the new shape on the
   way through — existing columns keep their values wherever they moved to, `Level` fills with
   zero (each kind's zero: numeric zero, null string/bytes, the zero identity — exactly what
   serializing a `new Hero()` would produce).
2. An empty **marker record** is appended to the log, and the new shape's reign begins at the
   marker's LSN. The marker is why a snapshot's shape is never ambiguous: the migration's own
   snapshot lands *at* the marker, above every LSN an old-shape row was written under.
3. The sidecar gains the new entry, EventId 1006 logs the changes loudly — automatic must never
   mean silent — and an immediate snapshot seals the migration so the rebuild cost is paid once.
   (If snapshots are disabled or the snapshot fails, nothing is wrong: every boot decodes by LSN
   through the history, and the next boot simply transforms again.)
4. The Postgres tier, if configured, does its phase 08 half of the same deploy: the additive DDL
   under `Postgres:AutoMigrate`, or a stall with the exact DDL printed when that is off.

Client bindings are the other half of the story and already ship: the schema hash changes, and a
stale client fails `ClientRowShape.Verify` at subscription time — structural refusal, not decode
garbage. Regenerate bindings with the deploy.

What counts as additive: added columns (any position), reordered columns, added tables, added or
removed indexes (indexes are projections; recovery rebuilds them from the declaration). What is
refused as destructive: a removed table, a removed column, a changed column kind, a moved
`[PrimaryKey]`. A **rename is a removal plus an addition** — the column's name is its identity —
so rename refuses too; rename it back, or treat it as a deliberate manual migration.

## The refusal

A destructive boot throws `SchemaShapeException` naming every reason, the sidecar path, and the
remediation. Nothing is touched: the previous schema still boots the directory unchanged. The
supported paths out:

- **Restore the declaration** (the usual answer — most destructive diffs are accidents).
- **Deliberate destructive migration, manually**: with the old schema still deployed, write a
  reducer that moves the data (copy the column, convert the values), deploy it, run it, then
  deploy the schema change as the additive-plus-ignored-leftovers it has become. Dropping a
  column whose data you have truly abandoned is the one case with no mechanical path today; it
  is deliberately a decision, not a flag.

## Operational notes

- **The upgrade rule.** The first boot that creates `melange.shape` (upgrading MelangeDB itself,
  or restoring a pre-phase-16 archive) *adopts* the booting code's schema as the shape of all
  existing records — the only possible reading. So: never combine the MelangeDB upgrade with a
  schema change in one deploy; boot the old schema once, then change it.
- **Clusters deploy one binary.** Shard engines each keep their own sidecar and migrate
  independently at open, but border and replica streams exchange current-shape rows — mixed-schema
  fleets are not supported; a schema change is a stop-the-fleet deploy.
- **Do not delete the sidecar.** A deleted sidecar re-adopts the current code's schema over
  records that may predate it, which silently mis-reads them — the exact failure this file
  exists to prevent. It rides into every `.mbak` archive; restore it from backup.
- **Custom log readers.** Anything that reads `CommitRecord`s it did not just watch commit — a
  lagging applier's catch-up, a replay tool — must route records through
  `MelangeEngine.TransformToCurrentShape` before decoding rows. Pipeline-driven appliers get
  this automatically.
