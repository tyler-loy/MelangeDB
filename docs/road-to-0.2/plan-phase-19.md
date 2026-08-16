# Phase 19 — Backup, second pass: check, clone, point-in-time

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

### Open: does clone change the originator

The phase 15 record sketched clone as "new originator id, new Postgres schema." The schema half
is clearly right. The originator half is less clear than it sounded: originators exist so
*allocators that might meet* never collide, and a clone is a separate world whose stores,
Postgres, and clients never meet production's — same-valued ids in different worlds collide
nowhere. Meanwhile originator assignment is membership's job at runtime, not a value the data
directory owns, so "clone rewrites the originator" may not even be a coherent operation at the
directory level. Leaning: clone does not touch originators; the provenance sidecar is what
distinguishes the worlds for support purposes. To settle by writing the id-collision analysis
into the plan's settlement rather than hand-waving either way — and if a future feature ever
lets two worlds exchange rows, *that* feature owns the originator question.

### Open: where `--check`'s CLI rung runs recovery

Recovery mutates (mints epochs, truncates torn tails), so the CLI rung must not run it against
the directory it just restored — a checked restore should be byte-identical to an unchecked one.
Leaning: copy to a scratch directory and recover the copy — honest and simple, costs one extra
materialization, bounded by archive size which is already "small and shaped like the state." The
alternative (a read-only recovery mode threaded through `FileCommitLog`) touches the most
load-bearing constructor in the codebase for a verb that runs at most nightly. To settle against
the actual copy cost on the port's archive.

### Open: verb spelling

`--check` as a restore flag versus `melange backup check <archive>` as its own verb. Leaning:
flag on restore — the check *is* a restore that proves itself and cleans up nothing extra,
whereas a separate verb implies a separate operation and would restore to a temp dir anyway.
The host rung is API-first regardless; its CLI spelling can follow the host integration that
actually ships.
