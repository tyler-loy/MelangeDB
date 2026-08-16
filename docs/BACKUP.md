# Backup and restore

**An unverified backup is a hope, not a backup.** Run `melange backup verify` against every
archive you produce — it is cheap enough for CI — and boot a restored directory in staging before
the day you need to do it in anger. A green verify proves the archive is complete, uncorrupted,
and structurally replayable; only a booted server proves the world.

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
commits continue. The capture is consistent at a **fenced LSN** — the head at the moment the
stream begins — and the server holds a **truncation pin** for exactly the stream's duration, so
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
riding along is smallest.

In-process schedulers can take the same capture without HTTP: `MelangeBackup.CreateOnline` in
`MelangeDB.Core`.

## `melange restore <archive> -o <data-dir>`

Materializes a data directory a server boots from — through ordinary recovery, the same code
every restart runs. Three semantics are the design, not incidental behavior:

- **A new epoch is minted, always.** A restore is a rewind. A client whose resume cursor sits past
  the restored head must full-resync, not resume into history that no longer happened; the epoch
  mint is the existing mechanism that forces exactly that, so there is no keep-epoch flag whose
  only use would be serving stale cursors.
- **AutoInc sequences restore from the snapshot header** and re-observe the log tail, so a
  restored world allocates ids above everything it has ever handed out.
- **The target must be empty, and failure removes everything written.** Restore is replacement,
  not merge and not cloning: a restored archive booted *beside* the deployment that produced it is
  two live worlds sharing an originator id, colliding on every id allocated since the capture. A
  supported clone verb (new originator, new Postgres schema, explicitly a different world) would
  be its own feature; it is deliberately not a flag on restore.

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
not carry. The full-fidelity check is booting a restored directory in staging — rank your checks
accordingly: verify in CI on every archive, a staged boot on a schedule, before you need either.

## Scheduling and retention

Deliberately not built in. Cron, object storage, and rotation are the operator's, and better
tools than ours exist; the archive file is the primitive they compose. Take backups after a
snapshot (or on a quiet fleet) and the log tail riding along stays small — `Snapshots:*` in
[CONFIGURATION.md](CONFIGURATION.md) already bounds it.

## What's coming in this phase

The cluster archive — hub plus every shard under one manifest, per-shard consistent — lands in
the closing phase 15 slice. This page grows with it.
