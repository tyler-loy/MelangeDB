# Backup and restore

**An unverified backup is a hope, not a backup.** Run `melange backup verify` against every
archive you produce — it is cheap enough for CI — and boot a restored directory in staging before
the day you need to do it in anger. A green verify proves the archive is complete, uncorrupted,
and structurally replayable; only a booted server proves the world.

**Rank the three checks and run all of them, on the cadence each deserves:**

| Check | Proves | Cadence |
| --- | --- | --- |
| `melange backup verify <archive>` | Every frame intact, the chain contiguous, every record parseable, the counts honest | CI, on every archive |
| `melange restore … --check` | The above, plus that recovery itself passes: the log opens, the epoch and snapshot cohere, every sidecar parses | On the schedule, with the restore |
| `host.CheckRestore(dir)` | The above, plus the schema-dependent half — the shape guard, index builds, residency, the projection rebuild | On the schedule, where the schema lives |

Each rung states in its own output what it proves and what it does not, so the ranking survives
being quoted.

MelangeDB's durability story has always been "the directory is the database": stop the process,
copy `CommitLog:Path`, and you have a backup. That is true, was undocumented, and is easy to get
subtly wrong — miss a sidecar, copy mid-compaction, copy while the server is live. The backup
verbs are that folklore made supported: same truth, none of the failure modes.

## The archive

One file, conventionally `world.mbak`, versioned and CRC-guarded from byte zero. Because the
commit log is the source of truth and every store is a projection of it, the archive carries the
**truth, not the projections** — per engine: its identity (source epoch, base LSN, snapshot LSN,
head LSN), its snapshot rows, its log tail above the snapshot LSN, and its sidecars (subscriber
checkpoints ride along; they are part of what has happened). Two properties fall out for free:

- **Small and shaped like the state, not the deployment.** No FASTER files, no Postgres dump. The
  hot store is scratch that recovery rebuilds; the relational tier re-bootstraps from the restored
  log (phase 08 machinery).
- **Store-engine agnostic.** A backup taken from a FASTER deployment restores into an in-memory
  one, and vice versa — the archive predates the projection choice by construction.

Everything streams: an archive larger than memory writes, verifies, and restores without a
materialized copy in between.

## `melange backup <data-dir> [-o world.mbak]`

The offline form: point it at the directory `CommitLog:Path` names, on a **stopped** server. It
refuses a directory whose log is open by a live process — that refusal is the point, because
copying a live directory is exactly how backups go subtly wrong; against a running server, use
the online form below — and it refuses a directory that recovery itself would refuse to boot,
because archiving damaged history as if it were good would turn a bad day into a silent one. The
archive is written to a temp file and swapped in atomically, so an interrupted backup never
leaves a plausible-looking partial archive.

The refusal works through `melange.lock`, an empty sidecar every live server holds exclusively
for its lifetime; the backup takes the same lock for the capture's duration, so a live server
refuses the capture and a capture in flight refuses a starting server — on every platform,
including those whose filesystems do not enforce Windows-style share modes. The same lock makes
two servers pointed at one data directory refuse at boot rather than corrupt each other.

The same operation is available programmatically as `MelangeBackup.Create` in `MelangeDB.Core`.

## `melange backup <url> --token <jwt> [-o world.mbak]`

The online form: streams the archive from a running server's `/melange/backup` endpoint while
commits continue. The capture is consistent at a **fenced LSN** — the durable watermark at the
moment the stream begins (under group commit a record can sit appended while its commit still
waits on the shared fsync, and an archive must never carry an LSN whose caller was not
acknowledged) — and the server holds a **truncation pin** for exactly the stream's duration, so
the snapshot and every record above it stay readable while they stream. Writes that land after
the fence are simply not in this archive; they are in the next one.

The endpoint is gated like the other privileged HTTP surfaces (`Sql:*`, `Bulk:*`): off by default
(`Backup:Enabled` — off answers `403 backup_disabled`), and owner-role-gated when on
(`Backup:OwnerRole`, its own key on purpose — read-everything-as-queries, write-anything, and
read-everything-as-archive are three capabilities). The token can also come from the
`MELANGE_TOKEN` environment variable, which keeps it out of shell history in scripts.

The pin is bounded, like every truncation pin: a client that stops reading is cut off after
`Backup:StreamStallTimeoutMs` with the pin released (EventId 1803), because a wedged backup
client must not become a full disk. The aborted partial download fails `verify`, which is the
point of verify. Watch `melange.backup.duration` — it is also the pin's hold time; archives that
stream for long enough to matter are the cue to back up right after a snapshot, when the tail
riding along is smallest. A pin that outlives its timeout anyway shows up where every other holder
of the log does: as the `backup-pin` truncation floor in `melange.log.truncation_floor`, and in the
`melange-retention` health check's description — see the runbook in
[OBSERVABILITY.md](OBSERVABILITY.md).

In-process schedulers can take the same capture without HTTP: `MelangeBackup.CreateOnline` in
`MelangeDB.Core`.

## `melange restore <archive> -o <data-dir> [--at-lsn <n>] [--check]`

Materializes a data directory a server boots from — through ordinary recovery, the same code
every restart runs. Three semantics are the design, not incidental behavior:

- **A new epoch is minted, always.** A restore is a rewind. A client whose resume cursor sits past
  the restored head must full-resync, not resume into history that no longer happened; the epoch
  mint is the existing mechanism that forces exactly that, so there is no keep-epoch flag whose
  only use would be serving stale cursors.
- **AutoInc sequences restore from the snapshot header** and re-observe the log tail, so a
  restored world allocates ids above everything it has ever handed out. (With `--at-lsn` the tail
  it re-observes is shorter — see below.)
- **The target must be empty, and failure removes everything written.** Restore is replacement,
  not merge and not cloning: a restored archive booted *beside* the deployment that produced it is
  two live worlds sharing an originator id, colliding on every id allocated since the capture.
  Seeding a deliberately separate world is [`melange clone`](#melange-clone-archive--o-data-dir),
  its own verb rather than a flag, because the semantics differ in kind.

### `--at-lsn <n>` — the moment just before the mistake

Restores with the tail cut at LSN `n`; everything above it stays in the archive. The archive
carries the tail record by record, so this collects rather than invents.

- **Refused below the archive's snapshot LSN.** Everything under that floor exists only as
  snapshot state, so the archive cannot rewind to it. The refusal names what to do instead: the
  earlier archive in your series whose snapshot LSN sits at or below the moment you want —
  `melange backup verify` prints each archive's LSN range.
- **Refused above the captured head.** A rewind cannot roll forward.
- **Single-engine archives only.** A cluster archive is per-shard consistent at *different*
  fences, so one LSN names no moment the cluster ever occupied and per-shard LSNs would
  manufacture a consistency the capture never had.
- **Subscriber checkpoints clamp to the cut**, not to the captured head.
- **The rewind is total, ids included.** AutoInc ids allocated between the cut and the captured
  head are free again: the archive carries sequences as of its snapshot, and boot re-observes only
  the records the cut kept. Nothing inside the restored world refers to those ids — that history
  did not happen — and the fresh epoch is what forces every consumer outside it (clients, the
  relational tier) to rebuild rather than carry a stale reference across the boundary. Reconcile
  anything *else* that recorded them; a rewind is a business-level event, not only a technical one.

The window is one archive's: `--at-lsn` reaches back to the archive's snapshot floor, and the
archive series is what reaches further. Streaming records to object storage between archives —
continuous log shipping — would make the timeline continuous instead; it is recorded as the
natural next phase rather than built, because a series of archives is the coarser answer that
already exists.

### `--check` — prove the restore before you need it

Runs the file-level rung of the boot proof against a scratch copy of what it just restored (so a
checked restore is byte-identical to an unchecked one): the real `FileCommitLog` constructor —
epoch, torn tail, CRC, base sidecar — the snapshot opened under the restored epoch, and every
sidecar parsed. Not a re-implementation of recovery's judgements; the same constructor a server
runs at startup, so a refusal here is the refusal a boot would have given, on a day you chose
rather than one the outage did.

It says in its own output what it cannot prove: index builds, residency, and the shape guard's
judgement of your code against these row bytes all need the application's schema registry, which
only the host has. That is the third rung:

```csharp
using var host = builder.Build();          // built, not started
var report = host.CheckRestore(restoredDirectory);
```

`IHost.CheckRestore` boots the restored directory through the ordinary engine constructor with
your own registry, reports per-table row counts, and returns without serving. Build the host
rather than starting it — starting would open this deployment's own data directory beside the one
under test. `MelangeBackup.CheckRestore(directory, schema)` is the same thing without a host. A CI
job's whole contract is that the check throws and the alert is the throw.

The Postgres tier is not in the archive and is not silently overwritten: on first boot after a
restore, the applier's checkpoint belongs to an epoch the restored log has never seen, and the
tier refuses loudly (EventId 1605) with the remediation printed rather than projecting history
that no longer happened — the `AutoMigrate` posture: destructive disagreement is never automatic.
The clean path is an empty schema, which the bootstrap machinery fills from the restored log; the
refusal's same-epoch cousin — a checkpoint ahead of the log's head, the hand-rolled directory
swap — is EventId 1608. Both refusals' remediations are tested to recover when followed
literally.

After restoring, point `CommitLog:Path` at the restored directory (or place it where your
configuration already points). The hot-store directory is scratch and is rebuilt on boot.

## `melange backup verify <archive>`

CRC-walks every frame, then dry-replays the archive — snapshot rows loaded, every log record's
write set applied in order — into an in-memory projection, and prints per-table row counts and
the LSN range. Any corruption fails with the frame named; a flipped bit anywhere in the file is a
failed verify, and `restore` refuses the same archive rather than materializing a partial world.

What verify proves: every frame intact, the record chain contiguous from snapshot to declared
head, every record parseable, the counts honest. What it deliberately does not prove: index
consistency and residency shape, which are projections of schema declarations the archive does
not carry — and that recovery itself passes, which needs a directory rather than an archive. Both
are what the other two rungs are for; see the table at the top.

## `melange clone <archive> -o <data-dir>`

Materializes an **explicitly different world** from a production archive — staging seeded from
production. Everything restore does (fresh epoch, empty target, all-or-nothing), plus the two
deltas that make "different world" true rather than aspirational:

- **Subscriber checkpoints are dropped, not clamped.** A clone has no subscribers yet, and
  production's event-delivery state resuming in staging — handlers deciding they have already
  delivered what this world has never emitted — is exactly the confusion the verb exists to
  prevent. Absent means "from the beginning", which is what a new world means.
- **A provenance sidecar** (`melange.provenance.json`) records the source epoch, the captured
  head, the archive's file name, and both timestamps. The server reads it back and announces it at
  every boot (EventId 1804), so "which world is this and how stale is it" is answered by the log
  of the server you are already looking at. `MelangeBackup.ReadProvenance(dir)` is the
  programmatic form.

A separate verb rather than a flag on restore: the semantics differ in kind, and a flag would
invite using one where the other was meant.

**What the archive cannot carry is yours to separate**, and every one of these is how a clone
stops being one:

- **Its own Postgres schema.** The archive holds no relational tier; point the clone at an empty
  schema and the phase 08 bootstrap fills it from the restored log. Pointing it at production's is
  two worlds writing one projection.
- **Its own data directory and its own fleet.** Never `CommitLog:Path` production's, never a node
  that also serves production.

**Originators are untouched**, which is a deliberate reversal of the sketch in phase 15's record.
Originators exist so allocators that *might meet* never collide; a clone's stores, tier, and
clients never meet production's, so same-valued ids in the two worlds collide nowhere. And a data
directory records no originator to rewrite: it is assigned by the membership store at runtime and
defaults to zero on a single node. The provenance sidecar is what tells the worlds apart. If some
future feature ever lets two worlds exchange rows, that feature owns the question.

Provenance is directory-local and stays out of archives: a backup captures a *world*, and a
restore of a clone's archive is a rewind of the clone. Keeping the archive's sidecar set unchanged
is also what keeps newer archives readable by older builds.

## Scheduling and retention

Deliberately not built in. Cron, object storage, and rotation are the operator's, and better
tools than ours exist; the archive file is the primitive they compose. Take backups after a
snapshot (or on a quiet fleet) and the log tail riding along stays small — `Snapshots:*` in
[CONFIGURATION.md](CONFIGURATION.md) already bounds it.

## The cluster archive

On a hub, `/melange/backup` fans out: the hub's own engine (Global and Replicated tables) plus
every shard engine found under `Cluster:ShardDataPath` on shared storage, one fenced LSN per
engine, under one manifest keyed by shard. Stated honestly: a cluster archive is **per-shard
consistent, not globally consistent** — there is no global total order to capture
([CLUSTERING.md](CLUSTERING.md)), so cross-shard skew is bounded by the capture window, which
games tolerate and ledgers should not be running on this database anyway.

The hub's engine streams under its truncation pin; shard engines stream handle-consistently over
shared storage while their owners keep serving them — no remote pin, no quiesce, no
player-visible pause. Shard border registries (`borrowed.sidecar`) ride along and are rewritten
under each shard's fresh epoch at restore. Node-level engines — the replicated projections shard
nodes hold — are deliberately not in the archive: fresh nodes re-sync them from the restored hub
through the ordinary replica-stream reset machinery, the projections-rebuild rule applied one
level up.

`melange restore cluster.mbak -o <target>` materializes `<target>/hub/` and
`<target>/shards/shard-k/`: point the hub's `CommitLog:Path` at `hub/`, every node's
`Cluster:ShardDataPath` at `shards/`, and boot — shards open through ordinary assignment, each
recovering from its restored log. Every engine gets its own fresh epoch. Restore targets a
stopped deployment; surgically restoring one shard into a live cluster is deliberately deferred
(it interacts with fencing and border streams), as is a per-shard-node backup endpoint — the
hub's whole-cluster form covers every shard engine without either.

`--check` walks a restored cluster directory engine by engine, in the layout the restore wrote.
`--at-lsn` does not apply to it, for the reason above: one LSN names no cross-shard moment.
