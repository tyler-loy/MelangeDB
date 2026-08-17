# Phase 19 — Backup, second pass: check, clone, point-in-time

**Status: Shipped.**

**Goal:** the three verbs phase 15 explicitly recorded as next, now with the shipped archive
underneath them: prove a restore boots (`--check`), seed an explicitly different world from a
production archive (`clone`), and restore to a moment just before the mistake (`--at-lsn`).

**Depends on:** phase 15 (shipped). Interacts with phase 16 if it lands first — a restored
directory booted by newer code becomes an ordinary migration boot — but requires nothing from it.

## Why here

Phase 15 drew its own boundaries honestly and each one now has a pull. BACKUP.md says "only a
booted server proves the world" and then leaves the staging boot as the operator's homework —
`--check` is that homework done. The clone decision was settled as "recorded here as the next
verb when a deployment states the staging-seeded-from-production need" — the reference port is
that deployment. And `--at-lsn` was scoped out by name ("a natural later verb that the format
should not preclude but this phase does not build") — the format did not preclude it, because
the archive carries the tail record by record; this phase collects.

## Deliverables

**`restore --check`: the boot-proof, ranked honestly.** Verify proves frames, chains, and
counts; a boot additionally proves recovery's own refusals pass, the epoch and sidecars cohere,
and the stores rebuild — and the *full* boot needs the application's schema (indexes, residency,
reducers), which only the host has. So the check ships in two rungs, each stating what it
proves:

- **CLI rung**: `melange restore <archive> -o <dir> --check` materializes, then runs the real
  file-level recovery machinery against the result — the actual `FileCommitLog` recovery (epoch,
  torn tail, CRC), the actual snapshot open under the restored epoch, sidecar parses — against a
  scratch copy, since construction mutates. What it cannot prove (schema-dependent projection
  builds), it says it cannot prove, in its output — the verify-vs-boot ranking sentence from
  BACKUP.md, now enforced by the tool that embodies it.
- **Host rung**: the full-fidelity check runs where the schema lives. A hosted entry point
  (`MelangeBackup.CheckRestore` / a host flag of the `--melange-schema-export` precedent) boots
  the restored directory through the ordinary engine ctor with the application's registry,
  reports per-table counts and the recovery outcome, and exits without serving. One line in a
  staging runbook, and CI-able: restore nightly archive, host-check it, alert on refusal.

**`melange clone <archive> -o <dir>`: explicitly a different world.** Clone is restore plus the
deltas that make "different world" true rather than aspirational: subscriber checkpoints are
**dropped, not clamped** — a clone has no subscribers yet, and production's event-delivery state
resuming in staging is exactly the confusion the verb exists to prevent; a provenance sidecar
records the source epoch and capture point (the support question "what is this world a clone
of" answered by a file); and the docs own the operational separations the archive cannot carry —
its own Postgres schema (the phase 08 bootstrap fills an empty one, same as restore), its own
data directory, its own fleet. It is a separate verb, not a restore flag, per the phase 15
settlement: the semantics differ in kind, and a flag would invite using one where the other was
meant.

**`restore --at-lsn <n>`: the pre-mistake moment.** Restore, with the tail cut at `n`:

- Refused when `n` is below the archive's snapshot LSN — an archive cannot rewind below its own
  materialized floor; the remediation names the older archive in the operator's series as the
  place that moment still exists. Refused above the captured head — nothing there to restore.
- Subscriber checkpoints clamp to `n` — the phase 15 clamp machinery with the head redefined.
- AutoInc sequences restore from the snapshot header unchanged, which is *safe-high*: ids
  allocated between `n` and the captured head are skipped, never reused. Phase 01's
  unique-not-dense settlement, paying off eight phases later — a dense allocator would make this
  verb a collision generator.
- **Single-engine archives only.** A cluster archive is per-shard consistent at *different*
  fences; one `n` names no cross-shard moment, and per-shard `n`s would manufacture a
  consistency the capture never had. Refused with that sentence, per the phase 15 honesty rule.

**Documentation and observability.** BACKUP.md grows the three verbs and re-ranks its checks
(`verify` in CI → `--check` on the schedule → host-rung boot before you need it); *clone* and
*point-in-time restore* enter [GLOSSARY.md](../GLOSSARY.md); check outcomes and clone provenance
get EventIds in [OBSERVABILITY.md](../OBSERVABILITY.md). No new configuration expected.

## Out of scope

**Surgical single-shard restore into a live cluster** — still deferred from phase 15, still for
the same reason (it interacts with fencing and border streams). **Continuous log shipping** —
streaming records to object storage between archives would make `--at-lsn` fine-grained across
the whole timeline instead of within one archive's window; it is the natural phase after this
one *if* the archive-series cadence proves too coarse in practice, and is recorded rather than
built. **Archive encryption** — unchanged from phase 15. **Cross-world merge** — clone makes two
worlds; nothing ever makes them one again.

## Decisions to settle

### Settled: clone does not change the originator

As the leaning, and the id-collision analysis is the settlement rather than a hand-wave.
Originators exist so *allocators that might meet* never collide. A clone's stores, relational
tier, and clients never meet production's: its Postgres schema is its own (empty, bootstrapped
from the restored log), its data directory is its own, its fleet is its own. Two ids of equal
value in two worlds that never exchange a row collide nowhere — an id is unique within the world
that allocated it, and that is the whole contract.

The half of the phase 15 sketch that turned out incoherent is the operation itself. A data
directory records no originator to rewrite: `MelangeEngine` takes it as a constructor argument,
the membership store assigns it per shard at runtime, and a single-node deployment gets zero. So
"clone rewrites the originator" names nothing that exists at the directory level. What
distinguishes the worlds is the provenance sidecar, which is a support artifact and says so. If a
future feature ever lets two worlds exchange rows, that feature owns the originator question —
and it will own it with a mechanism, not a flag on a restore verb.

### Settled: the CLI rung recovers a scratch copy, and the cost was measured

As the leaning, and the measurement is why it is not a close call. A data-directory-shaped copy
(one large log, one large snapshot, a few small sidecars) runs at disk speed on this box's NVMe:
**21 ms for 64 MB, 66 ms for 256 MB, 274 ms for 1 GB — ~3.7 GB/s.** Against the recovery it
precedes, for a verb that runs at most nightly, that is noise. Threading a read-only mode through
`FileCommitLog`'s constructor — the most load-bearing constructor in the codebase, and the one
whose mutations *are* recovery — to save 274 ms would have been a poor trade at any archive size
this design produces.

The copy also turned out to be what makes the byte-identity property testable at all:
`A_checked_restore_is_byte_identical_to_an_unchecked_one` hashes every file before and after both
rungs, which is a stronger statement than "we were careful".

### Settled: `--check` is a flag on restore

As the leaning. The check *is* a restore that proves itself; a separate verb implies a separate
operation and would have to restore to a temporary directory anyway. The host rung shipped
API-first as planned — `MelangeBackup.CheckRestore(directory, schema)` with an `IHost.CheckRestore`
extension over it — and deliberately has no CLI spelling: the schema lives in the application's
own process, so the host rung's natural home is a line in that application's staging job, not a
flag on a tool that would have to load the module assembly to find a registry.

## Shipped notes

- **The plan's AutoInc claim was wrong, and the correction is the better story.** The plan settled
  `--at-lsn`'s sequence handling as "safe-high: ids allocated between `n` and the captured head are
  skipped, never reused." They are not. The archive carries sequences as of its *snapshot*, and
  boot re-observes only the records the cut kept — so those ids are free again, and the next insert
  takes one.

  Delivering the promise would have required observing the discarded records' AutoInc columns,
  which needs the schema a restore deliberately does not have; the only schema-free approximation
  (reading ids out of row *keys*) silently covers `[PrimaryKey][AutoInc]` and misses everything
  else, and a heuristic that is silently partial is worse than a total statement. So the statement
  is total: **a restore rewinds everything, ids included, and the fresh epoch is what makes that
  safe** — exactly as it already is for LSNs. Nothing inside the restored world refers to the
  discarded ids; every consumer outside it is forced to rebuild by the epoch change. What remains
  is genuinely outside the database's boundary (a ledger, a receipt, an analytics store), and a
  rewind is a business-level event for those regardless. `A_cut_rewinds_the_autoinc_allocator_with_everything_else`
  pins the real behaviour so the docs can never drift back to the promise.

- **A cut stops the writing, never the walk.** The archive's integrity claims — contiguity, the
  promised head, the end frame's counts — are about what was *captured*, so a cut that skipped
  reading the discarded region would let a corrupt archive pass by not looking at the corruption.
  Every frame is read and checked; only the write is filtered.

- **The check's refusals are recovery's refusals, not a second opinion.** The CLI rung runs the
  actual `FileCommitLog` constructor rather than re-deriving its judgements, which is the same
  reasoning that made `LogFileFormat` a read-only *mirror* of recovery in phase 15 rather than a
  second implementation. Where the check adds judgements of its own — snapshot/log epoch coherence,
  a snapshot predating the base — they are the conditions `DataDirectoryCapture` already refuses a
  backup for, stated in the same words.

- **Clone provenance stays out of archives.** `ArchiveRestore` refuses unknown sidecars by design,
  so adding one to the capture list would make newer archives unreadable by older builds — a real
  cost for no gain, since a backup captures a *world* and a restore of a clone's archive is a
  rewind of the clone. The sidecar is directory-local, and a test round-trips a clone through
  backup and restore to hold that.

- **Continuous log shipping stayed out of scope, unchanged**, and is now the natural next phase if
  the archive-series cadence proves too coarse: `--at-lsn` reaches back to one archive's snapshot
  floor, and the series is what reaches further. Surgical single-shard restore, archive
  encryption, and cross-world merge are all unchanged from the plan.
