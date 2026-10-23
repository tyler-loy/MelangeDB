# Phase 25 — The archive tier, and the relational tier renamed for what it is

**Status: Planned. Contains a breaking change** — `StorageTier.Relational` is renamed; see
[the rename](#the-rename-breaking).

**Goal:** a table can live in Postgres *only*. Rows written to it ride the commit log like any other
write, land in Postgres through the existing applier, and never occupy the hot store, a snapshot, or
a backup archive. Reducers can append to it and cannot read it; everything outside a reducer can
query it. The motivating shape is a high-churn history — a wallet ledger, a trade log, a combat
record — where the recent rows stay hot and queryable from reducers, and the old ones move to an
archive that stays reachable, just slower and on the database engine.

Adding a tier that really is relational exposes that the existing one is misnamed: `Relational`
rows live in the hot store and Postgres holds a copy. So the phase also renames it to what it is —
**hot with a relational backplane** — before a second Postgres-backed tier makes the old name
actively misleading.

**Depends on:** nothing new. The applier, its checkpoint, the truncation floors, the shape sidecar,
the aggregate endpoint, and the restore epoch machinery all exist (phases 07, 08, 15, 16, 18). This
phase changes what several of them are allowed to assume.

## Why here

Every game with an economy accumulates history faster than it accumulates state. A player's balance
is one row; the entries that produced it grow without bound for as long as they play. Today a team
has three options, and each of them is worse than it looks:

- **Keep the ledger hot.** `Residency.Paged` already keeps old rows out of RAM, so memory is not the
  problem. But every row still lives in the hot store's files, in every snapshot, and in every
  `.mbak`, so disk, snapshot time, backup size, and restore time all grow with the game's age rather
  than with its population.
- **Keep it hot and mirror it.** `Tier = StorageTier.Relational` puts the same rows in Postgres for
  SQL access. It does not remove them from the hot store — the relational tier today is a mirror,
  not a destination (see below) — so it solves querying and nothing else.
- **Archive by hand.** An event handler writes old rows to a Postgres table the team owns. That works,
  and it means the team now writes and owns the DDL, the idempotency, the migration story, and what
  happens to that table on a restore — which is exactly the part this project has already solved
  once, in phase 08, and should not make every consumer solve again.

That is the 0.3 theme: a surface every team will otherwise build badly, once each.

**The reference workload has no ledger.** Vibe Shaft has currency-adjacent tables but nothing
append-only and unbounded in this shape, so unlike most phases this one is not pulled by a measured
pain in [REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md). It is pulled by a shape every economy has,
and the sample this phase adds is the worked example.

## What the relational tier is today

This is the fact the whole phase turns on, so it is stated precisely.

A `Relational` table is **a hot table with a Postgres projection.** Its rows are in the hot store;
reducers read and write them there; read-your-writes is the hot store's overlay, as for any table.
`PostgresRelationalTier` projects the same rows into Postgres off the commit path. Two pieces of
shipped machinery depend on the hot copy existing:

- **Bootstrap.** When the applier has no valid checkpoint and the log has been truncated,
  `BootstrapFromStoreAsync` rebuilds the projection by `DELETE FROM` every relational table and
  re-inserting what `HotStore.Scan` returns at a consistent LSN.
- **Restore.** [BACKUP.md](../BACKUP.md) says the archive carries "the truth, not the projections" —
  no Postgres dump — and that after a restore the clean remediation for the epoch-mismatch refusal
  (EventId 1605) is an empty schema, which bootstrap refills.

Both are correct because Postgres is disposable: everything in it can be regenerated from the log
and the hot store. **An archive table is the first thing in Postgres that cannot be.** Once the log
truncates past an archived row, Postgres holds the only copy. Every deliverable below that is not
plumbing is a consequence of that one sentence.

It is also why the name has to change. Today "relational tier" names the one place Postgres appears,
and the docs say "the relational tier is eventually consistent" and "the relational tier
re-bootstraps from the log" as if those were facts about Postgres. They are facts about the mirror.
With an archive beside it, every one of those sentences becomes true of one Postgres-backed tier and
false of the other.

## The shape

```csharp
[Table(Public = true)]
public partial struct WalletEntry                       // hot: recent, reducer-readable
{
    [PrimaryKey, AutoInc] public long Id;
    [Index] public Identity Player;
    [Index] public Timestamp At;
    public long Delta;
    public string Reason;
}

[Table(Tier = StorageTier.Archive)]                     // Postgres only; append from reducers
public partial struct WalletEntryArchive
{
    [PrimaryKey] public long Id;                        // carried over, not re-allocated
    [Index] public Identity Player;
    [Index] public Timestamp At;
    public long Delta;
    public string Reason;
}

[Table(Scheduled = nameof(WalletReducers.ArchiveWallet))]
public partial struct WalletArchiveTick
{
    [PrimaryKey, AutoInc] public ulong Id;
    public ScheduleAt ScheduledAt;                      // a repeating interval
}

[Reducer]
public void ArchiveWallet(ReducerContext ctx, WalletArchiveTick timer)
{
    var cutoff = new Timestamp(ctx.Timestamp.UnixTimeMicroseconds - ThirtyDaysMicros);
    foreach (var entry in ctx.Db.WalletEntry.At.Filter(new Timestamp(0), cutoff).Take(5_000).ToList())
    {
        ctx.Db.WalletEntry.Delete(entry.Id);
        ctx.Db.WalletEntryArchive.Insert(new WalletEntryArchive { Id = entry.Id, /* ... */ });
    }
}
```

The move is one write set, so it is one log append: the row leaves the hot store and enters the
archive atomically, with no 2PC, for the same reason everything else here is atomic. The sweep is an
index range, not a scan, so MELANGE0017 has nothing to say about it. The balance a reducer actually
needs stays a hot row it maintains — a reducer never sums history.

## Deliverables

**The tier.** `StorageTier.Archive` in `src/MelangeDB.Abstractions/TableAttribute.cs`.
`ModelExtractor.cs:34` currently maps the tier as `t == 1 ? "Relational" : "Hot"`, which would
silently turn `Archive` into `Hot` in the generated registration; it becomes a switch like its
`Residency` and `Placement` neighbours. The manifest does not carry the tier today and gains it, so
client generators can omit archive tables from table handles entirely.

<a id="the-rename-breaking"></a>
**The rename (breaking).** `StorageTier.Relational` becomes `StorageTier.HotWithRelationalBackplane`
(name per the decision below), and "relational tier" becomes "relational backplane" in every doc
that means the mirror. The enum becomes:

```csharp
public enum StorageTier
{
    Hot = 0,
    HotWithRelationalBackplane = 1,         // was Relational; same value, same behaviour

    [Obsolete("Renamed to HotWithRelationalBackplane. Rows live in the hot store; Postgres holds a " +
              "copy. For Postgres-only storage, see StorageTier.Archive.", error: true)]
    Relational = 1,

    Archive = 2,
}
```

The numeric value stays `1`, so an assembly compiled against 0.2 keeps meaning the same thing and no
data moves: no hot-store rebuild, no Postgres DDL, no shape-sidecar change, and the applier's
checkpoint row is untouched. The break is source-only — `Tier = StorageTier.Relational` stops
compiling, with an error that names the replacement and points at the new tier for anyone who
assumed the old name meant Postgres-only. What is in and out of the rename:

- **Renamed:** the enum member; the prose term in DESIGN.md, CLUSTERING.md, BACKUP.md,
  MIGRATION.md, CONFIGURATION.md, OBSERVABILITY.md, and GLOSSARY.md (whose *Tier* row becomes
  `Hot`, `HotWithRelationalBackplane`, or `Archive`); the boot warning
  `LogRelationalWithoutPostgres` and its message; the `allowPrivateRelational` parameter on
  `ServerSubscription.Compile`, which now admits both Postgres-backed tiers.
- **Not renamed:** types named for Postgres rather than for the tier — `PostgresRelationalTier`,
  `IRelationalQueryExecutor`, `RelationalAggregateQuery` — because they serve both Postgres-backed
  tiers and "relational" is still true of what they touch; the wire error codes `not_relational`,
  `no_relational_tier`, and `relational_unavailable` in `MelangeErrorCodes`, because clients match on
  those strings and the condition they report ("this needs Postgres") is unchanged; the `Postgres:*`
  configuration keys; the `"postgres"` applier name and its checkpoint row.
- **Recorded:** a CHANGELOG *Breaking* entry and a MIGRATION.md upgrade note — the one-line
  find-and-replace, and the sentence that nothing on disk changes.

**An insert-only accessor.** The emitter generates `Insert` and a blind `Delete(key)` for an archive
table, and no `Find`, `Iter`, `Filter`, `FilterRange`, or `Update` — the absent methods are the
documentation. `TransactionDb` backs that up for the reflection path: `Find`/`Scan`/`Update` on an
archive table throw, and `Insert` skips `Exists`, `CheckUniqueConstraints`, and `IndexPending`,
because there is nothing in the hot store to check against. `Delete` stages without the existence
check (see the decision on deletes).

**Analyzer rules**, continuing from MELANGE0023: an archive table may not be `Public`; may not
declare `[Unique]`; may not set `Residency`; and its primary key must be `[AutoInc]`, a `Guid`, or
explicitly marked as carried from a hot table's AutoInc key. The `[Unique]` and key rules are not
style — see the decision on constraints for why a violation here is the worst failure this tier can
have.

**The hot store forgets archive tables.** Both stores already ignore write-set ops for tables they
do not hold — `InMemoryHotStore.cs:80` and `:94`, `FasterHotStore.cs:170` — so building their table
maps from the registry *minus* archive tables is most of the work. The snapshot capture
(`MelangeEngine.cs:949`), the startup residency report, and the phase 16 shape-migration rebuild
filter the same way. The shape sidecar still records archive tables, so a destructive change to one
is refused at boot exactly like a hot table's.

**The applier learns the difference.** `PostgresRelationalTier` projects `HotWithRelationalBackplane` and `Archive`
tables alike, with three changes:

- Archive tables get hidden `_melange_lsn bigint` and `_melange_epoch uuid` columns, written from the
  record on every insert, and an index on them. This is what lets restore trim (below).
- Archive inserts never overwrite: a primary-key collision dead-letters the incoming row into
  `melange_archive_dead_letter`, in the same Postgres transaction as the batch and its checkpoint,
  instead of the mirror's upsert. See the decision on constraints.
- `BootstrapFromStoreAsync` neither scans nor `DELETE`s archive tables. Bootstrapping a mirror is
  still safe; bootstrapping an archive from a store that never held it would empty it.
- A fresh anchor against a truncated log, with archive tables present and empty in Postgres, refuses
  loudly under a new EventId rather than anchoring — the archive's missing rows are below `BaseLsn`
  and nothing in this process can produce them.

**Postgres stops being optional for these deployments.** `MelangeDbHostedService.cs:89` warns today
when a backplane table has no Postgres configured, because "the rows still live in the hot store, so
nothing is lost." For an archive table that justification is false — the rows would be lost the
first time the log truncated — so an archive table without `AddPostgres(...)` refuses to start.

**A table's tier becomes part of its recorded shape.** `ShapeHistory.cs` records column names and
kinds and, by design, nothing else: "indexes, residency, policies, and enum-ness are projections or
process policy, rebuilt or re-read from the booting code." Tier was in that category while every
tier kept its rows in the hot store. With `Archive` it is not, and a tier change crossing the hot
boundary loses data silently:

- **`Hot` → `Archive`:** the table's rows are in the hot snapshot, and the next boot drops them
  because the hot store no longer holds that table. Postgres never received them: the applier
  skipped that table's ops while it was hot, and its checkpoint is already past them. Gone, with no
  error.
- **`Archive` → `Hot`:** the hot table boots empty while its rows sit in Postgres. Reducers see a
  table with no history.
- **`Archive` → `HotWithRelationalBackplane`:** the same empty hot table, plus a worse follow-on.
  The next bootstrap runs `DELETE FROM` on a table that is now a mirror, and wipes the only copy.

So the shape sidecar records each table's tier alongside its columns, LSN-fenced like the rest of
the history, and a boot whose tier differs from the recorded one is governed by the decision on
tier changes below. (The shipped `Hot` → backplane change has a milder form of the same blind spot,
as far as the code reads: `EnsureAsync` creates the Postgres table and the applier projects from its
checkpoint onward, so the mirror silently lacks every row written before the change. Nothing is
lost, since the hot store still holds them, but the mirror is quietly incomplete. Recording tier
fixes this case too, by making the change visible at boot.)

**An archive retention floor.** (As built: the floor lives in the engine's truncation decision
beside the resume window and is searched the same way, by commit timestamp; the "sharper refusal"
reuses EventId 1611, whose situation names the lost range and the setting, rather than a new id.)
A new, optional named truncation floor that holds the log back by
`Postgres:ArchiveLogRetentionHours` behind the applier's checkpoint, so a Postgres restored from a
backup taken inside that window can catch up from the log instead of losing the rows in between.
Off by default (unset or `0`), since it is for deployments without Postgres point-in-time recovery;
see the backup decision. The floor resolves hours to an LSN using commit timestamps — the oldest
record within the window — so it is configured in the operator's units and enforced in the log's. It ships
with a sharper refusal for the case it exists to prevent. When the applier's checkpoint is below the
log's base after a Postgres restore, a new EventId names the lost LSN range and the archive tables
it affects, instead of the generic "truncated past checkpoint" message.

**Restore trims instead of wiping.** The epoch-mismatch path splits by tier: mirrored tables
re-bootstrap from the restored log as today; archive tables are trimmed to the restored history,
then the checkpoint is re-anchored under the new epoch. That trim is what keeps a rewind honest:
without it, Postgres holds archived rows from a future that did not happen, and their ids sit above
the restored AutoInc sequence, so the next allocation collides with them. In the simple case the
trim is `DELETE ... WHERE _melange_epoch = @source AND _melange_lsn > @head`; it is not always the
simple case, and today the restored directory cannot name `@source` at all — see the decision on
lineage, which makes a small epoch-lineage sidecar part of this deliverable.

**A reader.** `IArchiveReader`, registered by `AddPostgres`, injectable anywhere outside a reducer:
keyset-paginated queries over an archive table by an indexed column, with the same predicate shapes
subscriptions already compile. It overlays the unapplied log tail for that table — records between
the applier's checkpoint and the durable head — so a row that just left the hot store is never
briefly invisible to a reader that looks in both places (see the decision on the gap).

**A client-facing history query.** A policy-checked, paginated read of an archive table, so "show me
my last 200 transactions" works from a client without the owner-mode ad-hoc surface. The aggregate
endpoint (`AdHocAggregateBuilder.cs:23`) also accepts archive tables, owner-mode only, unchanged.

**Documentation.** DESIGN.md §2 (the fourth tier value and why it is not a flag), §5 (the one place
the log is not the whole truth), and §8; BACKUP.md (what the archive no longer contains, the
Postgres point-in-time recovery requirement and why a plain dump is not enough, the retention floor,
the trim); MIGRATION.md (archive tables migrate on the relational rule only, and the allowed tier
changes);
*archive tier* into [GLOSSARY.md](../GLOSSARY.md); the new EventIds into
[OBSERVABILITY.md](../OBSERVABILITY.md); the history endpoint into
[THREAT-MODEL.md](../THREAT-MODEL.md); any new setting (a page-size cap, at least) into
[CONFIGURATION.md](../CONFIGURATION.md), including the archive retention floor's window. A sample alongside the existing ones: a wallet with a hot
window and an archive sweep.

**Tests**, the ones that pin the invariants rather than the plumbing: a move survives restart with
the row in exactly one place; snapshot and `.mbak` contain no archive rows; bootstrap leaves archive
rows untouched; restore to an earlier LSN trims exactly the rows above it and the next AutoInc does
not collide; the reader sees a just-moved row before the applier does; an archive table without
Postgres refuses boot; a Postgres outage during a sweep grows the log, holds the floor, and
catches up with nothing lost; every refused tier change refuses boot with the table named and no
rows dropped; and a Postgres restored from a backup inside the retention window catches up from the
log, while one restored from outside it refuses under the new EventId with the lost range named;
and a primary-key collision dead-letters exactly that row, in the batch's transaction, while the
applier's checkpoint keeps advancing and every other row in the batch lands.

## Out of scope

**Reads inside a reducer.** A reducer body does no I/O, and a Postgres round trip under the write
lock is the one thing the commit-log design exists to prevent. A reducer that needs history needs a
hot summary of it.

**A declarative age policy.** `[Table(ArchiveAfter = "30d")]` is the obvious next ask and is
deliberately not this phase. The scheduled reducer above is twelve lines, it is the user's code, and
it makes the move visible in the log under a reducer name. A policy can be generated on top of it
later; starting there would hide the one write that matters.

**Partitioned archive tables in a cluster.** The applier's checkpoint is one row keyed `"postgres"`
per Postgres schema, and in a cluster Postgres lives on the hub. A per-player ledger is
naturally `Partitioned`, which means every shard would need its own applier and checkpoint key into
a shared archive. That is real and it is a follow-up; this phase refuses a `Partitioned` archive
table when cluster mode is on, and allows `Global` ones. (As built: refused at write time rather
than at boot. Shard and node-local engines refuse archive writes through the placement guards,
which is where every other placement rule is already enforced.)

**Archive rows in `.mbak`.** See the backup decision; the leaning is that the archive tier is
backed up by Postgres's own tools.

**Moving rows back.** Un-archiving is an `IArchiveReader` read and an ordinary reducer call. No
engine path for it.

## Decisions to settle

### What the renamed tier is called, and whether `Relational` comes back

Candidates were `HotWithRelationalBackplane` (says
exactly what happens), `Backplane` (short, but drops *hot*, which is the whole point), `Mirrored`
(accurate, but says nothing about Postgres).

**Settled:** `HotWithRelationalBackplane`. A tier is written once per table declaration, so length
costs nothing, and the name answers the two questions a reader actually has — can reducers read it
(yes, it's hot) and is it in Postgres (yes, as a backplane) — without a trip to the docs.

The tempting follow-on is to give the freed name `Relational` to the Postgres-only tier, since that
is the tier it accurately describes. **Leaning: not in this phase, and probably never.** A table
still declaring `Relational` after an upgrade would silently change meaning — its rows would leave
the hot store on the next boot and reducers reading it would stop compiling for a reason the error
would not explain. The obsolete-as-error alias exists to make that mistake impossible; reusing the
name would undo it. `Archive` also says something `Relational` does not: what the tier is *for*.

### A new tier value, or a flag on the backplane tier

`[Table(Tier = HotWithRelationalBackplane, HotCopy = false)]` reads as a small variation. It is not one: the table
loses reducer reads, primary-key checks, uniqueness, subscriptions, and its place in backups, and
Postgres becomes required.

**Leaning:** a fourth enum value, `Archive`. DESIGN.md's rule is that the four table knobs must not
be conflated, and a flag that silently changes what three of the other knobs mean would be exactly
that. A distinct value also makes every existing `Tier == Relational` check in the codebase a deliberate
decision at review time rather than a silent miss.

### Two tables, or one table with a hot window

The alternative is one `WalletEntry` whose old rows the engine evicts to Postgres on its own.

**Leaning:** two tables, firmly. A one-table design means `ctx.Db.WalletEntry.Find(id)` returns null
for a row that exists, depending on its age and on how far the applier has got — a reducer-visible
semantic that changes with time and with Postgres's health. Two tables make the boundary a type, and
the compiler enforces it.

### Constraints on an archive table

A reducer cannot check an archive table's keys, so Postgres is the first thing to see a collision,
and it sees it after the record is committed and durable. The two kinds of collision fail in
opposite directions today:

- **Primary key: silent overwrite.** `TableSql.For` writes every op as `INSERT ... ON CONFLICT (pk)
  DO UPDATE`. For a mirror that is right — the hot store already validated the key, so a conflict
  can only be the same row arriving again. For an archive it means a colliding id replaces an
  archived row and nothing notices.
- **Anything else: a wedge.** A `[Unique]` or foreign-key violation throws; the applier retries the
  batch forever, stalls, holds the truncation floor, and grows the log until a human intervenes.

Replays are not a concern either way: the checkpoint commits in the same Postgres transaction as the
batch, so the applier never re-applies a record. **Any conflict on an archive insert is a real
collision.**

**Settled:** make collisions impossible rather than detected, and quarantine them when they happen
anyway. Primary keys come from AutoInc, `Guid`, or a hot table's AutoInc key carried across the
move; `[Unique]` is refused; no foreign keys. A primary-key collision **dead-letters the row** rather
than stalling the applier: one bad row should not hold the truncation floor and grow the log for
every other table while it waits for a human.

- **Nothing is dropped.** Each archive insert is a single statement — the `INSERT ... ON CONFLICT
  (pk) DO NOTHING` in a CTE, followed by an insert of the same row into
  `melange_archive_dead_letter` when the first inserted nothing — so the quarantine commits in the
  same transaction as the batch and its checkpoint. The dead-letter table records the table, the
  key, the LSN, the epoch, the reducer name from the record, the time, and the full row bytes, so
  the row can be inspected and re-applied by hand. (As built: the table is
  `__melange_archive_dead_letter`, matching the applier's other plumbing tables, and the row is
  kept as `row_values` jsonb built from the statement's own parameters rather than as bytes —
  readable, and re-appliable with a plain `INSERT ... SELECT`.)
- **Logged at Error, once per row,** under a new EventId with the table, key, LSN, and reducer
  name. Never the row's values: an archive is where audit and wallet data lives, and log sinks are
  not where it belongs. The dead-letter table holds the payload.
- **Counted.** A `melange.archive.dead_lettered` counter tagged by table, for alerting, and the
  `melange-applier` health check reports *Degraded* while the dead-letter table holds rows nobody
  has cleared. A collision is a bug in the application's keys, so it must be impossible to miss
  rather than merely recorded.
- **Not for everything.** Only primary-key collisions dead-letter. A connection failure, a type
  error, or anything else still stalls and retries as today, because those are not a property of one
  row and skipping them would skip data.

OBSERVABILITY.md gets the EventId, the counter, and the remediation: find the colliding key's
source, fix the key allocation, and re-apply or discard each dead-lettered row deliberately.

### Existence checks: idempotency keys that outlive the hot window

**As built:** the key set is a synthesized hot table, `__keys_<Table>`, registered beside the archive
table and maintained by the engine from the archive's ops in the same place it strips them from the
hot store. Being an ordinary hot table is what lets snapshots, paging, recovery and backups carry it
with no machinery of its own. `IDbView.ContainsKey<TRow>` is the untyped form. One rule the decision
below did not foresee: `KeepKeys` must be declared when the archive table is created, because turning
it on later starts the set without the keys already in Postgres — boot refuses that; turning it off is
fine.

The first real consumer of this shape already needs one reducer-side read of archived rows. Its
`WalletEntry.Id` is the idempotency key for credits, refunds, and redelivered saga legs. The hot
table is capped at each player's newest lines and older lines move to an archive, and the reducer
that applies a wallet change checks `WalletEntry.Id.Find(key) ?? WalletEntryArchive.Id.Find(key)`,
so a replayed old key pays nothing. Today the archive table is hot and `Paged`, which works. As this phase
specifies `Archive`, that second `Find` cannot be written, and a replayed key older than the window
would pay twice.

The reducer does not need the row, only whether the key exists. Three ways to answer that:

- **The app keeps a hot "seen keys" table** and archives only the payload. Works today with no
  engine change, costs one hot row per key forever (or per window, if keys expire), and every team
  writes it again.
- **The app bounds the idempotency window,** so keys older than N days cannot replay. No archive
  read is needed, but this changes the app's semantics and is the app's decision to make, not the
  engine's.
- **The archive tier keeps its primary keys hot.** `Archive` tables hold a key-only set in the hot
  store, with no payload: one key per row, `Paged`, in snapshots and `.mbak`. The generated
  accessor gains `Contains(key)`, still with no `Find`.

**Settled:** the third, opt-in per table and **off by default** (`[Table(Tier = Archive, KeepKeys = true)]`).
It is the seen-keys pattern built once, and it pays for itself twice. It answers idempotency with no
Postgres I/O under the write lock. It also lets `Insert` check the primary key in the reducer again,
so the silent-overwrite case in the constraints decision above becomes an ordinary duplicate-key exception before
commit. The cost is honest and bounded: the key set still grows with history, at the size of a key
rather than a row. It stays in the backup, so the archive's keys survive a lost Postgres even though
its payloads do not.

Off by default because the cost grows with history for every archive table, and most of them —
combat logs, chat history, telemetry — never ask whether an old key exists. The behaviour without it
is documented where a team will read it (the `Archive` XML docs, DESIGN.md §2, and the sample):

- **No `Contains`.** The generated accessor has none, so a reducer cannot ask whether an archived
  key exists. A key that has left the hot table is invisible to reducers.
- **Idempotency stops at the hot window.** A team using archived ids as idempotency keys must turn
  `KeepKeys` on, or keep its own seen-keys table, or bound its replay window — otherwise a key
  replayed after it was archived is processed again. The docs say this in those words, because it is
  the mistake the default makes possible.
- **Collisions are caught after commit.** With no hot key set, `Insert` cannot check the primary
  key, so a collision reaches Postgres and is dead-lettered (see the constraints decision). With
  `KeepKeys`, it is an ordinary duplicate-key exception inside the reducer and the transaction never
  commits.

**Open:** whether an expiring key window (`KeepKeysFor = "90d"`) belongs in the engine or stays the
app's second option above.

### Deletes

Insert-only fits a ledger, but right-to-erasure requests do not care what fits a ledger.

**Leaning:** allow a blind `Delete(key)` from reducers — no existence check, staged as an ordinary
delete op, applied as `DELETE ... WHERE pk = @key`, idempotent under replay. No `Update`: an update
needs the current row to be meaningful, and the current row is not readable.

### Backups

The `.mbak` format's central property is that it carries the truth and not the projections. Archive
rows are now truth, and they are not in it.

**Leaning:** do not put them in. The archive is the part of the dataset most likely to be large,
it is already in a database with mature backup tooling, and streaming it through `.mbak` would make
every backup scale with the game's age, which is the problem this phase removes. Instead:
`backup verify` and `CheckRestore` report archive tables by name as *external — back up with
Postgres*, so the gap is visible at the moment someone checks the backup. **Open:** a
`--include-archive` flag for small deployments, captured at the archive's head LSN after waiting for
the applier to reach it.

**"Back up with Postgres" is not enough as stated, though, because a plain `pg_dump` loses
rows.** A dump taken at LSN X carries the applier's checkpoint at X. The live checkpoint keeps
moving after the dump, to Y, and the log truncates behind it. If Postgres is lost and the dump is
restored, the checkpoint is back at X, but the log no longer holds X to Y. The applier refuses to
start (correctly, rather than skipping the gap), and every archive row committed between the dump
and the failure is gone. The hot tier never has this problem because the hot store is rebuilt from
the log. The archive is the first projection whose backup and the log's truncation have to agree.

**Leaning:** two answers, for two kinds of deployment.

- **Postgres point-in-time recovery is the documented requirement** for archive tables: WAL
  archiving, so a restore can reach the moment before failure rather than the last dump. That closes
  the gap at its source, it is how any team running Postgres for durable data should already back it
  up, and it costs the engine nothing.
- **The archive retention floor** (in the deliverables) is for teams without PITR. Size it to at
  least the dump interval, and a dump restored inside the window catches up from the log. The price
  is log disk proportional to the window. This is also the one floor whose holder is an operator's
  backup schedule rather than a process, so `melange-retention` names it as such.

**Settled: measured in hours,** as `Postgres:ArchiveLogRetentionHours`, off by default. Backup
schedules are written in hours, and an operator sizing the window from a nightly dump should write
`26`, not compute a record count from commit rate. The floor converts hours to an LSN internally
from commit timestamps, and `melange-retention` keeps reporting the pinned distance in records, as
every other floor does. Leave it off when Postgres has point-in-time recovery; set it to at least the
dump interval plus margin when it does not.

**Open:** whether an operator command
that records "a Postgres backup completed at LSN X" should replace the fixed window, so the floor
tracks real backups rather than an estimate of them. That is more precise, and it is one more thing
that has to be wired into somebody's backup job.

### Which tier changes are allowed

With tier in the recorded shape, every change is detectable at boot. The question is which ones
proceed.

**As built, so far: every change into or out of `Archive` is refused**, including the
backplane → `Archive` path the leaning below allows. Implementing that path safely needs one thing
the leaning missed: a mirror made by a 0.2 `Hot` → `Relational` switch never received the rows
written before the switch (the gap the projected-tables registry closes from 0.3 on), and the
registry seeds such a mirror as complete. Dropping hot rows on the strength of that mirror would
lose them. The path needs the applier to re-verify or re-copy the table from the hot store before
the engine drops it, which crosses the engine/applier boundary at boot. Until then the refusal names
the reducer move (Insert into a new archive table, Delete from the old one) as the supported path.

**Leaning:**

- **`HotWithRelationalBackplane` → `Archive`** is allowed, because it is the natural path for a
  team that mirrored a ledger and now wants to stop holding it hot. Postgres already has every row
  up to the applier's checkpoint. Boot waits for the applier to reach the reign LSN, backfills the
  hidden `_melange_lsn` and `_melange_epoch` columns with the reign's values, and then lets the hot
  store drop the table. If Postgres is unreachable, boot refuses rather than dropping hot rows that
  Postgres may not yet hold.
- **`Hot` → `Archive`** is refused, with the two-step remediation printed: declare the table
  `HotWithRelationalBackplane` for one deploy, let the mirror fill, then declare it `Archive`. That
  remediation only works if the first step backfills, which today's `Hot` → backplane change does
  not (see the deliverable). So the backplane change gains a bootstrap of the newly mirrored table
  from the hot store at a consistent LSN, which is the same machinery `BootstrapFromStoreAsync`
  already has, scoped to one table.
- **`Archive` → anything** is refused. Moving archived rows back into the hot store is the
  un-archiving this phase leaves to a reducer and `IArchiveReader`, and doing it at boot would load
  unbounded history into the hot store before the process could serve anything.

**Open:** whether `Hot` → `Archive` should do the two steps itself in one boot rather than refuse.
It could, but a boot that silently moves a table's entire contents to another storage engine is the
kind of thing the `AutoMigrate` posture exists to make deliberate.

### Trimming across more than one restore

**As built:** the lineage sidecar exists (`melange.lineage`, carried in the archive's per-engine
identity as an optional field rather than as a sidecar frame, so the archive format version does not
move), and the trim is a *move*: rows outside the restored history go to `__melange_archive_rewound`
rather than being deleted, so the reconcile can run automatically without being destructive. A
restore whose lineage does not name Postgres's epoch is still refused (1605). The open question
below resolved itself: an archive taken before lineage still names its own source epoch, so a
restore of it gets a one-generation lineage and reconciles normally.

LSNs count within one epoch, and a restore mints a new epoch that continues numbering from the
restored head. Restore once at LSN 100 and run to 500 under epoch B; then restore an older backup of
epoch A at LSN 300. Archive rows from B at LSNs 101–300 are not part of A's history, yet an
LSN-only trim keeps them. That is why the epoch column exists, but the column alone is not enough:
the correct keep-set is "rows whose epoch is in the restored log's lineage, at or below the LSN
where that epoch was left," and the engine does not record lineage today. `melange.epoch` is the
bare new GUID and `melange.base` is eight bytes of LSN; the archive's `SourceEpoch` reaches the
restored directory only for a clone, via `melange.provenance.json`. So after an ordinary restore the
applier cannot name even the *simple* case's `@source` — it sees its own checkpoint's epoch and the
log's new one, and nothing connecting them.

**Leaning:** record lineage — the chain of `(epoch, forked-at LSN)` pairs — in a sidecar beside
`melange.epoch`, written by every restore and clone from the archive identity
(`ArchiveRestore.cs`, where `newEpoch` is minted), and trim against it. It is a small sidecar addition that every future
cross-epoch question will want. **Open:** whether a restore whose lineage cannot be established (an
archive taken before this phase shipped) trims by the simple rule or refuses with the remediation
printed. Refusal is the `AutoMigrate` posture and probably right.

### The visibility gap

**As built (`IArchiveReader`, the first half of the read side):** one filter column — the key or an
indexed column, by value or inclusive range — and primary-key order either way, keyset-paginated. The
checkpoint and the page are read in one repeatable-read transaction so they describe the same moment.
The overlay cap is a constant, 50,000 records, past which the page is marked `Stale` rather than the
read scanning the log; it becomes a setting if anyone needs to tune it. The client-facing endpoint is
the second half.

After a move commits, the row is gone from the hot store immediately and arrives in Postgres when the
applier gets to it. A reader that queries both sees nothing in between.

**Leaning:** `IArchiveReader` overlays the unapplied tail — it reads Postgres, then reads the log from
the applier's checkpoint to the durable head for that table's ops. The log's sparse LSN→offset index
(the third round of [performance-sweep.md](../design/performance-sweep.md)) makes the seek cheap,
and the tail is normally short. The alternative,
`WaitForAppliedAsync` before every read, turns a Postgres outage into an outage of history reads,
which is the opposite of the tier's posture. **Open:** a cap on the overlay when the tail is long
(Postgres down for an hour), after which the reader answers from Postgres and says it is stale.

### Where the client-facing read lives

A paginated history read is neither a subscription nor a reducer.

**Leaning:** HTTP first, under the existing policy ladder, because it is request/response by nature
and needs no protocol change. A websocket verb would be a protocol change landing while
[phase 23](plan-phase-23.md) is pinning that contract for a second client runtime; if this phase
lands first, 23 should cover the endpoint in the TypeScript client, and if 23 lands first, the
endpoint stays HTTP-only rather than reopening the contract.
