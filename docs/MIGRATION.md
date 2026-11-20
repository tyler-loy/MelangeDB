# Schema migration

How a deployment with a changed schema boots against an existing world. The one rule, both tiers,
one sentence: **additive changes are automatic and loud; destructive changes are refused and
manual.** The relational backplane has worked this way since phase 08 (`Postgres:AutoMigrate`,
[CONFIGURATION.md](CONFIGURATION.md)); the hot tier works this way since phase 16, and this page
walks the whole deploy.

## Upgrading to 0.3: the relational backplane rename

`StorageTier.Relational` is now `StorageTier.HotWithRelationalBackplane`. It is a rename, not a
migration:

```csharp
[Table(Tier = StorageTier.Relational)]                 // 0.2 — now error CS0619
[Table(Tier = StorageTier.HotWithRelationalBackplane)] // 0.3 — same table, same behaviour
```

Replace the name everywhere and redeploy. **Nothing on disk changes:** the enum value is still `1`,
so the shape sidecar, the hot store, the Postgres schema, and the applier's checkpoint are all
exactly as they were, and the first 0.3 boot is an ordinary restart. If you passed
`allowPrivateRelational:` by name to `ServerSubscription.Compile`, it is now `allowPrivateBackplane:`.
Log queries and alerts need no change: EventIds and their names are unchanged (1607 is still
`RelationalTablesWithoutPostgres`; only its message text names the new tier), as are the wire
error codes.

The old name was retired rather than reused because a Postgres-only tier is coming
([phase 25](road-to-0.3/plan-phase-25.md)), and a table that silently changed meaning on upgrade
would lose its hot rows. That is why `Relational` stays as a compile error rather than coming back
with a different meaning.

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

One engine skips steps 2 and 3's marker and snapshot: one whose log has never been committed to,
such as a cluster node that holds no partitions yet. No stored byte was written under its current
shape, so there is nothing to rebuild. The sidecar's entry is rewritten in place, EventId 1006
still logs the change, and the log stays empty, so the engine still counts as fresh and its init
reducers still run on its first open.

Client bindings are the other half of the story and already ship: the schema hash changes, and a
stale client fails `ClientRowShape.Verify` at subscription time — structural refusal, not decode
garbage. Regenerate bindings with the deploy.

What counts as additive: added columns (any position), reordered columns, added tables, added or
removed `[Index]` (indexes are projections; recovery rebuilds them from the declaration), adding or
removing `[Unique]` — checked against the existing rows when added; see [Adding or removing
`[Unique]`](#adding-or-removing-unique) — and a tier change between `Hot` and
`HotWithRelationalBackplane` (see [Changing a table's tier](#changing-a-tables-tier)). What is
refused as destructive: a removed table, a removed column, a changed column kind, a moved
`[PrimaryKey]`. A **rename is a removal plus an addition** — the column's name is its identity —
so rename refuses too; rename it back, or treat it as a deliberate manual migration.

## Adding or removing `[Unique]`

`[Unique]` is an index *and* a claim about the data, and the claim is checked at the boot that
makes it. Declaring a column `[Unique]` over a table that already has rows makes that boot scan
the table once. If any value is held by more than one row, the boot refuses and records nothing.
The refusal names each duplicated value, up to ten per column, and the primary keys holding it, up
to five per value. The directory still boots under the previous declaration. If the values are all
distinct, the change is recorded like any other additive one: a marker record, a new entry in the
shape sidecar, and EventId 1006 naming it.

**The engine cannot dedupe for you**, because which row survives is a decision about your game.
Boot the previous declaration, run a reducer that rewrites or deletes all but one row per
duplicated value, then redeploy with `[Unique]`.

**A column added as `[Unique]` in the same deploy** starts at its default in every existing row,
which is a duplicate as soon as two rows exist. Declare it nullable (null is never a duplicate),
or add it without `[Unique]`, fill it with distinct values, and declare it unique in a later deploy.
The refusal says so when this is the cause.

**Removing `[Unique]`** relaxes the claim, so nothing is checked and the change is recorded the
same way. On a table with a relational backplane, the Postgres projection's `ux_<table>_<column>`
index is dropped on the tier's next start, because a unique index left in Postgres would refuse the
applier's first duplicate. With `Postgres:AutoMigrate` off, that drop is part of the refusal's
printed DDL, like an added column. A plain `ix_` index that a new unique index supersedes is
dropped too, under `AutoMigrate`.

**Upgrading from a build before 0.4**: those builds never recorded uniqueness in the sidecar, and
never checked a `[Unique]` added over existing rows (issue
[#192](https://github.com/tyler-loy/MelangeDB/issues/192)). So the first boot after upgrading
checks every `[Unique]` column once. If it finds duplicates an earlier build let in, it refuses
exactly as above. If it finds none, it records the columns in place, with no marker, and logs
EventId 1010.

`/melange/bulk` checks uniqueness against committed rows but not within its own batch, so a single
bulk load can still commit duplicates (see `MelangeEngine.BulkInsert`). The next boot that checks
the column will name them.

## Changing a table's tier

The shape sidecar records each table's tier, because where a table's rows live is part of what the
data directory holds. Between `Hot` and `HotWithRelationalBackplane` the rows stay in the hot store
either way, so the change is additive: the boot is a migration boot (EventId 1006 names the tier
change), and the new tier governs from the marker.

- **`Hot` → `HotWithRelationalBackplane`.** The Postgres applier skipped the table's ops while it was
  hot, so its checkpoint is already past the table's whole history and replay cannot produce it.
  On its next start the applier finds the table missing from its projected-tables registry
  (`__melange_projected`) and fills its Postgres table from the hot store at one consistent LSN,
  replacing anything it held, before resuming (EventId 1609, with the row count). Before 0.3 this
  copy did not happen: the Postgres table was created empty and only rows written after the change
  ever reached it.
- **`HotWithRelationalBackplane` → `Hot`.** The rows were in the hot store all along, so nothing moves.
  The Postgres table is left as is and stops being maintained (EventId 1610); drop it when you no
  longer want it. Moving the table back onto the backplane later copies it afresh rather than
  resuming the stale copy.

A change into or out of `Archive` is refused at boot, because it moves rows across the hot store's
boundary. `Hot` or backplane → `Archive` would drop the table's rows from the hot store with no
guarantee Postgres holds all of them; `Archive` → anything would boot an empty hot table whose rows
live only in Postgres. The supported path is the one the tier is designed around: declare a new
`Archive` table and move rows into it with a reducer — `Insert` into the archive and `Delete` from the
hot table in the same transaction, in batches, which makes each move atomic.

`KeepKeys` on an archive table follows the same logic. Turning it on for an archive table that already
exists is refused, because the keys archived so far live only in Postgres and the new key set would
start without them — `Contains` would answer false for exactly the keys an idempotency check exists to
catch. Declare it on a new archive table and move to it. Turning it off is allowed: the key set is derived
from the archive's ops, so dropping it loses nothing but the ability to ask.

A recorded tier this build does not recognize — one written by a newer MelangeDB — is refused like any
destructive change: booting it under a tier whose rows live elsewhere would present an empty table as
the whole truth.

A sidecar written before tiers were recorded (any 0.2 data directory) has each table's tier filled in
from the running code on the first 0.3 boot, in place and without a migration (EventId 1009). That is
the adoption reading again, for one field, so the upgrade rule below covers it: don't change a table's
tier in the same deploy as the upgrade. The Postgres half needs no such care. A 0.2 projection has no
registry, so the first 0.3 start seeds it from the backplane tables Postgres already holds, and copies
any it does not.

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

  **The adoption announces itself.** When it happens over a directory that already holds records,
  the boot logs **EventId 1008 `ShapeAdopted`** at warning level, naming the LSN it adopted over
  and quoting the rule. A new world naming its first shape is silent — nothing is being
  reinterpreted there.

  **`Schema:AllowAdoption` defaults to `false`, and that first boot refuses.** The engine cannot
  verify that the rule was followed, and the two errors are not symmetric: refusing costs one boot
  and one setting, while assuming wrongly costs rows silently mis-read and found arbitrarily later.
  So it stops and names both branches — set the key to `true` for that one boot if the schema did
  *not* change in this deploy, or boot the previous schema first if it did. Turn it back off
  afterwards; the adoption that then proceeds logs EventId 1008. The refusal writes nothing, so the
  next boot decides afresh.

  This is the `Postgres:AutoMigrate` posture applied to the hot tier's one irreversible assumption,
  and it is a **once-per-directory** gate by construction: every directory written by a build that
  has the sidecar carries one, so only an upgrade from a build that predates it — or a sidecar that
  was deleted or lost — reaches the question. A fresh install has no records to reinterpret and
  never meets the setting at all.

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

### "I already booted the wrong way" — recovering an adopted mis-decode

The failure is per-table and arrives late: everything untouched by the schema change keeps working
perfectly, and the tables that changed read as garbage or throw
(`Table 'X': the stored row (N byte(s)) does not decode…`) the first time something reads them —
possibly long after a boot that looked clean.

**Rewrite the affected rows through the bulk endpoint.** `POST /melange/bulk` bypasses reducers, so
it never reads the old bytes — exactly the property needed when *reading* is what fails. Supply
every column; the rewritten rows land under the current shape and the table is consistent again.
Only tables whose shape changed need it, and it is otherwise an ordinary write, so nothing else in
the world is disturbed.

The endpoint is gated, deliberately, and both gates are off by default — so this recovery needs
two things set before it will answer anything but `403`:

- **`Bulk:Enabled`** — `true` for the duration of the repair.
- **`Bulk:OwnerRole`** — the caller's token must carry that role claim. Bulk writes bypass every
  reducer and its policies, so a valid token alone is not enough.

Turn both back off afterwards.

The alternative — revert the schema, boot, export, re-apply — is strictly worse and needs the old
binary. If the data is unreadable *and* unrecoverable by rewriting, restore from the archive taken
before the deploy and redo the upgrade in two steps.
