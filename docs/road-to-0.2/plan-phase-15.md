# Phase 15 — Backup and restore

**Goal:** an operator can hold a deployment's entire committed history in one archive file, prove
the archive is good, and boot a fresh deployment from it. Three verbs on the existing `melange`
CLI: `melange backup`, `melange restore`, `melange backup verify`.

**Depends on:** nothing in phases 13–14 — this is operational surface over machinery that shipped
in 0.1 (the log, the snapshot, recovery). It lives in 0.2 because it should have existed sooner,
not because anything new must exist first.

## Why here

MelangeDB's durability story so far is "the directory is the database": stop the process, copy
`CommitLog:Path`, and you have a backup — true, undocumented, easy to get subtly wrong (miss a
sidecar, copy mid-compaction), and unusable against a server that must not stop. A database that
asks to hold a persistent world owes its operator a supported answer to "how do I back this up,
and how do I know the backup is good, *before* the day I need it."

The design writes itself off one shipped fact: **the commit log is the source of truth and every
store is a projection of it.** So a backup is the truth, not the projections — snapshot plus log
tail plus sidecars, per engine — and a restore rebuilds the projections the way every restart
already does. Two properties fall out for free:

- **Small and shaped like the state, not the deployment.** No FASTER files, no Postgres dump; the
  archive carries one materialized state capture and a tail of records.
- **Store-engine agnostic.** A backup taken from a FASTER deployment restores into an in-memory
  one, and vice versa — the archive predates the projection choice by construction.

## Deliverables

**The archive format.** One file (`.mbak`), versioned and CRC-guarded in the house style (magic,
format version, checked frames — the log and snapshot formats' conventions). A manifest, then per
engine: identity (epoch, base LSN, head LSN, capture timestamp), the snapshot, the log tail above
the snapshot LSN, and the sidecars (epoch, truncation base, the border-registry sidecar where one
exists). A cluster archive is the hub engine plus every shard engine under one manifest keyed by
shard. Streams throughout — an archive larger than memory writes and reads without a materialized
copy, the `LoadSnapshot` precedent.

**`melange backup <data-dir | url> [-o world.mbak]`** — the schema verb's two-sources-one-writer
pattern:

- *Offline* (path form): reads the data directories directly. Refuses a directory whose log is
  open by a live process — the half-right `cp -r` this verb exists to replace.
- *Online* (URL form): fetches from `/melange/backup` on a running server, which streams the
  archive at a fenced LSN while **pinning log truncation for the duration** — the saga-marker /
  subscriber-checkpoint machinery, and like those pins it is bounded: a stream that stalls past a
  cap is aborted and the pin released, because a wedged backup client must not become a full disk.
  Gated like the other privileged HTTP surfaces (`Sql:*`, `Bulk:*`): off by default
  (`Backup:Enabled`), owner-role-gated when on (`Backup:OwnerRole`, its own key per the
  read-everything ≠ write-anything precedent — and backup is read-*everything* by definition,
  policies included).

On a cluster, the hub's endpoint fans out: it streams its own engine and each shard's (over shared
storage), one fenced LSN per engine. Stated honestly in the docs: a cluster archive is
**per-shard consistent, not globally consistent** — there is no global total order to capture
(CLUSTERING.md), so cross-shard skew is bounded by the capture window, which games tolerate and
ledgers should not be running on this database anyway.

**`melange restore <archive> -o <data-dir>`** — materializes data directories a server boots from:
log tail, snapshot, sidecars, per engine. Refuses a non-empty target. Three semantics that are the
actual design:

- **A new epoch is minted, always.** A restore is a rewind; a client whose resume cursor sits past
  the restored head must full-resync, not resume into history that no longer happened. The epoch
  mint is the existing mechanism that forces exactly that, so restore leans on it rather than
  offering a keep-epoch flag whose only use is serving stale cursors.
- **AutoInc sequences restore from the snapshot header** (they are already in it), so a restored
  world allocates ids above everything it has ever handed out.
- **The Postgres tier is not in the archive and is not silently overwritten.** On first boot after
  a restore, an applier checkpoint *ahead* of the restored head means the relational tier holds a
  future the log no longer contains — refused loudly with the remediation printed (the
  `AutoMigrate` posture: destructive disagreement is never automatic), and the clean path is an
  empty schema, which the phase 08 bootstrap machinery fills from the restored log.

**`melange backup verify <archive>`** — CRC-walks every frame, then dry-replays the archive into
an in-memory projection and prints per-table row counts and the LSN range. Cheap enough to run in
CI against every nightly archive. The sentence the docs lead with: **an unverified backup is a
hope, not a backup.**

**Observability.** Backup duration and bytes, truncation-pin duration, verify results, restore
EventIds (the epoch mint and the Postgres refusal especially — both are the kind of event an
operator greps for at 3 a.m.). Recorded in [OBSERVABILITY.md](../OBSERVABILITY.md) with the
change; `backup`, `archive`, and `restore` enter [GLOSSARY.md](../GLOSSARY.md).

**Configuration** (planned rows in [CONFIGURATION.md](../CONFIGURATION.md)): `Backup:Enabled`,
`Backup:OwnerRole`, `Backup:StreamStallTimeoutMs`.

## Out of scope

**Scheduling and retention** — cron, object storage, and rotation are the operator's, and better
tools than ours exist; the archive file is the primitive they compose. **Incremental backups** —
the snapshot format settled full-not-incremental in phase 07 (a chain is a second replay mechanism
beside the log, which already is one), and the same reasoning holds one level up. **Point-in-time
restore to an arbitrary LSN** — an archive restores its capture point; replay-to-LSN over a kept
archive series is a natural later verb (`restore --at-lsn`) that the format should not preclude
but this phase does not build. **Cross-version restore** — an archive restores into the schema
that wrote it; hot-tier schema migration is DESIGN.md §10's open question, not this phase's.
**Encryption at rest** — the archive inherits whatever the operator's filesystem and object store
provide.

## Decisions to settle

### Settled: restore is for replacement, not cloning

A restored archive booted *beside* the deployment that produced it is two live worlds sharing an
originator id — AutoInc ranges collide with every id the original allocated after the capture, and
both worlds' appliers would fight over one Postgres. Leaning: `restore` documents itself as
replacement, mints the new epoch, and stops there; a supported *clone* verb (new originator id,
new Postgres schema, explicitly a different world) is a real feature with a real customer
(staging environments seeded from production) but it is its own decision, not a flag that falls
out of restore. To settle: whether clone ships in this phase or is recorded as the next verb.

**Settled as the leaning.** Restore is replacement: epoch minted always, empty target required,
no keep-epoch and no clone flag. Clone is recorded here as the next verb when a deployment states
the staging-seeded-from-production need; it will be its own command with its own semantics (new
originator, new Postgres schema), not an option on restore.

### Settled: no quiesced, globally-consistent mode

Per-shard consistency is the default and the honest one. A `--quiesce` mode — brief fence of every
shard to capture one fleet-wide instant — is buildable from phase 13's drain quiesce, but it is a
deliberate world-wide write pause with a player-visible cost, taken for a property (a global
instant) the data model explicitly does not promise elsewhere. Leaning: do not build it until a
consumer states the need; record the refusal reasoning if it stays out.

**Settled as the leaning — not built, refusal recorded.** The data model promises no global total
order anywhere else (CLUSTERING.md); a backup mode that pauses the world to capture a property
nothing in the system otherwise has would make the backup the most invasive operation in the
product. The cluster archive is per-shard consistent, the docs say so plainly, and the round-trip
test asserts shards captured at different LSNs restore to a working world rather than pretending
the skew away.

### Settled: verify's replay depth is structural

The dry replay proves the records parse and apply in order, without the module DLL. What it cannot
prove without the schema is index consistency and residency shape — projections of declarations
the archive does not carry. Leaning: structural replay plus row counts is the verb's contract, and
"boot a real server against a restored directory in staging" is the documented full-fidelity
check; feeding verify a `melange-schema.json` for deeper checks is possible but doubles the verb's
input surface for a check staging does better.

**Settled as the leaning.** Verify takes one input — the archive — and its contract is: every
frame intact (every single-bit flip is caught, exhaustively tested), the record chain contiguous
from snapshot to declared head, every record parseable, counts honest, per-table live-key counts
reported. BACKUP.md ranks the checks explicitly: verify in CI on every archive, a staged boot on
a schedule, so the green checkmark never stands in for the drill.

### Settled: the online endpoint lives where the client socket lives

The hub fans out for the cluster archive, but a single shard node also has engines an operator
might want individually (one shard's archive, for surgical restore). Leaning: the endpoint exists
on any node with `Backup:Enabled`, scoped to the engines that node owns; the hub's additionally
offers the whole-cluster form. Single-shard *restore* into a live cluster (a surgical rollback of
one shard while the rest of the world runs) interacts with fencing and border streams and is
deliberately deferred — restore targets a stopped deployment in this phase.

**Settled, adjusted from the leaning.** The endpoint maps with `MapMelangeSocket` — single-node
servers and the hub — because that is where the HTTP surface and its JWT gate already live; shard
nodes map only internal shard sockets and grew no client-facing HTTP surface for this. The hub's
form covers every shard engine over shared storage with no per-node endpoint needed, and the
capture is handle-consistent (base → snapshot → log handle ordering, gap detection, bounded
retry) rather than remotely pinned — no hub→shard pin channel exists and none was built. A
per-shard-node endpoint for surgical single-shard archives is recorded as the follow-on if a
deployment states the need; single-shard restore into a live cluster stays deferred as planned.

## Done when

- **The round trip is a test, not a promise:** populate an engine (resident, paged, and blob
  tables; AutoInc allocations; scheduled rows), back up, restore, boot — every table scans
  byte-identical, sequences continue without collision, a client holding a pre-restore resume
  cursor is refused resume and full-resyncs through the existing machinery.
- An online backup taken **under sustained live writes** captures one consistent fenced LSN, the
  truncation pin releases (asserted, including on a client that stalls and is aborted), and the
  archive verifies.
- The cluster round trip: hub plus multiple shards out, restored, booted; border registries
  rebuild; the per-shard consistency bound is asserted rather than assumed (shards captured at
  different LSNs restore to a working world).
- A FASTER-written archive restores under the in-memory engine and vice versa — the
  store-agnostic property is a test.
- Every corruption a flipped bit can cause fails `verify` with the frame named, and `restore`
  refuses the same archive rather than materializing a partial world.
- The Postgres checkpoint-ahead refusal fires in a test and its printed remediation, followed
  literally, recovers.
- Configuration rows flip to shipped; OBSERVABILITY.md and GLOSSARY.md carry the additions; the
  backup page in the reference docs leads with the verify sentence.

## Risks

- **The truncation pin under a slow stream.** A multi-GB archive to a slow client pins compaction
  for the duration. The stall cap bounds the pathological case; the honest residual is that backup
  duration scales with state size and the pin with it — the snapshot-interval configuration
  already bounds how much tail rides along, and the docs should say "back up after a snapshot, or
  let the endpoint trigger one first" if measurement shows it matters.
- **Restore-beside-original.** The originator-id collision above is the sharpest data-corruption
  edge in this phase and it happens *outside* the software, in an operator's runbook. The
  refusal-to-restore-into-non-empty-target and the replacement-not-cloning documentation are the
  mitigations; the clone decision is where it gets solved properly.
- **Archive format lock-in.** Like the seam in phase 14, the format is public API the moment one
  nightly job depends on it; versioned from byte zero and read-forever is the contract (the log's
  own FileFormatVersion discipline, applied to a file that leaves the machine).
- **False confidence from verify.** A structural verify that passes is not a booted world; the
  docs must rank the checks (verify in CI, staged boot before you need it) rather than letting the
  green checkmark stand in for the drill.

## Shipped notes

Shipped as three stacked PRs — the archive and the offline verbs, the online form, the cluster
archive — with the decisions above settled in place. Deviations and additions, recorded:

- **The frame CRC covers the frame's own type and length bytes**, not just the payload — a
  strengthening over the log's convention it borrows from, because "every corruption a flipped
  bit can cause fails verify with the frame named" is tested literally, at every byte offset,
  and header bits had to be inside the contract for that sentence to be true.
- **`melange.events.json` rides along**, a sidecar the plan's list did not name. Subscriber
  checkpoints are part of the truth — what has been delivered — so dropping them would turn a
  restore into silent redelivery for every subscriber. Restore clamps any checkpoint above the
  restored head to the head: a checkpoint pointing past a rewind would make its subscriber
  silently skip everything committed after the restore, and clamping turns that into the
  at-least-once redelivery the bus already permits.
- **The archive's contents are logical** — snapshot rows and verbatim record payloads, never file
  bytes — so the archive predates not only the projection choice but the snapshot and log *file*
  format choices too; restore rewrites the snapshot under the fresh epoch through the ordinary
  writer rather than patching bytes.
- **The live-directory refusal grew a lock file.** The first cut probed the log with a write
  request, counting on the live log handle's Read|Delete share to refuse it — which is
  Windows-only enforcement, and CI's Linux runner proved it by not refusing. Unix maps only
  `FileShare.None` onto a real (advisory) lock, so every live `FileCommitLog` now holds an empty
  `melange.lock` sidecar exclusively and the offline capture takes the same lock for the
  capture's duration. The by-product is a genuine double-open guard: two servers pointed at one
  data directory now refuse at boot on every platform, which Unix previously did not detect at
  all.
- **The shared-storage shard capture is handle-consistent, not pinned.** The plan said the hub
  "streams each shard's engine over shared storage" without saying how consistency is had with
  no channel to the owner. The answer shipped: ordered opens (base sidecar → snapshot handle →
  log handle), both walk passes over one handle, a dense-chain check that turns any raced
  truncation into a loud retry (bounded at three) instead of an archived hole. No remote pin
  exists and none was needed.
- **The restored cluster layout** is `hub/` plus `shards/shard-k/` under the target — point the
  hub's `CommitLog:Path` at the former and every node's `Cluster:ShardDataPath` at the latter's
  parent. Node-level engines (replicated projections) are deliberately not in the archive; fresh
  nodes re-sync through the replica stream's reset machinery, which is the projections-rebuild
  rule applied one level up.
- **The Postgres refusal is two refusals.** A real restore always lands in the epoch-mismatch
  stall (1605) because restore always mints — its message now states the restore remediation.
  The plan's literal "checkpoint ahead of the restored head" case can only occur same-epoch (a
  hand-rolled directory swap that kept `melange.epoch`), and that got its own typed refusal and
  EventId 1608. Both remediations are tested to recover when followed literally.
- **The truncation pin is a new scoped engine primitive** (`MelangeEngine.PinTruncation`), not a
  floor registration — `AddTruncationFloor` registrations are deliberately permanent, and a
  per-request registration would have leaked one closure per backup for the life of the engine.
- The kill-of-the-verify-depth question left one residual honest: verify's per-table counts are
  keyed by numeric table id, because the archive carries no schema and inventing names would
  imply knowledge it does not have.
