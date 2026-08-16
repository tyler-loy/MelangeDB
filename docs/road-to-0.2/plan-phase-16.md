# Phase 16 — Hot-tier schema migration

**Goal:** a redeploy with a changed schema boots against the existing log and snapshot — additive
changes automatically, destructive changes refused loudly with the remediation printed — closing
the half of DESIGN.md §10 that phase 08 left open. Today the engine cannot even *detect* hot-tier
schema drift; this phase makes drift detected always and survivable when it is survivable.

**Depends on:** nothing in phases 13–15, though it completes a story phase 15 started: restore
materializes yesterday's directory, and this phase is what lets today's code boot it.

## Why here

Three shipped facts make this both urgent and tractable:

- **Row format v1 is positional.** A row is its columns' bytes in declaration order — no column
  count, no names, no per-row version (`RowFormat.cs`; `TableSchema` documents "columns in
  declaration order — the order the row serializer writes them"). The format is why a row costs
  nothing to fan out (protocol v2 serves committed bytes verbatim), and also why any schema change
  silently changes what existing bytes *mean*. Adding a property in the middle of a class shifts
  every later column; the engine would replay old rows as garbage, or throw, discovered at index
  extraction or on a client.
- **Nothing persists the shape.** The client side has a schema hash and `ClientRowShape.Verify`
  (phase 12 / protocol v2) — a stale *client* is a handshake refusal. The server has nothing: no
  fingerprint in the snapshot header, none beside the log. The engine boots whatever code it was
  built with against whatever bytes it finds.
- **Phase 08 already settled the posture, one tier up.** Additive changes automatic (an added
  column backfills with its kind's zero value — an additive migration never drops or nulls data);
  destructive changes refused loudly with the pending DDL printed; nothing destructive is ever
  automatic. The hot tier should behave so identically that the operator learns one rule.

The reference port makes this the most predictable future incident: every live game ships schema
changes routinely, and in SpacetimeDB every one means republish plus regenerated bindings — the
pain DESIGN.md §10 records. The client half of the answer shipped in 0.1; this is the server half.

## The design in one sentence

**Persist the shape beside the log, keyed by LSN; decode every replayed row under the shape that
wrote it, mapped to the current shape *by column name*; refuse when the map would drop or
reinterpret data.**

By-name mapping, not trailing-add tolerance, because declaration order is the wire order: a
column added mid-class is byte-wise a reorder, and a trailing-only rule would make "I added a
field to my class" destructive depending on where the cursor was. Under the name map, position is
irrelevant — a change is additive iff every persisted column still exists with the same name and
kind, wherever it moved to.

## Deliverables

**The shape sidecar.** `melange.shape` beside the log: per table, the ordered `(name, kind)`
column list — the manifest/wire-descriptor shape reused — as a *history*, each entry fenced by
the LSN range it governs. A history rather than a single shape because log records outlive
deployments: the tail above the snapshot, and any record an applier or event subscriber still
needs, may span shapes, and each record must decode under the shape current at its LSN. Entries
whose range falls entirely below the truncation base are compacted away. An existing directory
without the sidecar adopts the booting code's shape exactly once — the `melange.epoch` adoption
precedent. The sidecar rides into `.mbak` archives through the existing sidecar frames, so a
restored directory carries its shape history for free.

**Drift detected always.** Boot compares the code's schema against the sidecar's newest shape.
Identical — the overwhelmingly common boot — is today's fast path plus one comparison. Different
takes the migration path or the refusal, per the rule below. There is no configuration for this;
detection is not optional.

**Additive changes migrate automatically.** Additive means: every persisted column still exists
in the code's schema with the same name and the same kind. New columns fill with their kind's
zero value (phase 08's exact backfill rule). The migration is **rebuild-on-boot**: recovery
replays snapshot and tail through the name map — decode under the record's shape, re-encode under
the code's — so the stores only ever hold current-shape bytes and the map lives in exactly one
place, replay decode. The paged store's files hold old-shape rows, and that is fine by
construction: FASTER is a projection and recovery is ours (phase 07); a migration boot is the
one boot that pays a full projection rebuild, and it takes an automatic snapshot at head when it
completes so the cost is paid once and the new shape becomes the floor. EventId + duration + rows
rewritten, logged loudly: automatic must never mean silent.

**Destructive changes refuse loudly.** A dropped column, a changed kind, or a rename (see below)
refuses boot naming the table, the persisted column list, the code's column list, and the
remediation — restore the column, or perform a deliberate manual migration. The `AutoMigrate`
posture verbatim: destructive disagreement is never automatic, and the refusal is the feature.

**The Postgres tier composes, not duplicates.** One deploy with an added column triggers both
halves: the hot tier rebuilds through the name map; the applier's phase 08 machinery emits the
additive DDL (or stalls and prints it, per `Postgres:AutoMigrate`). Nothing new to build —
tested as one scenario, documented as one story.

**Documentation and observability.** *Shape* enters [GLOSSARY.md](../GLOSSARY.md); the migration
and refusal EventIds enter [OBSERVABILITY.md](../OBSERVABILITY.md); DESIGN.md §10's schema
migration bullet gets its strike-through and settlement; a new section in the operations docs
walks the add-a-column deploy end to end, hot tier and Postgres together.

## Out of scope

**A destructive migration verb** (`melange migrate` — drop this column, change that kind, with
data transform hooks). Real feature, own phase, needs a customer with a concrete destructive
change in hand; this phase's refusal message is its placeholder. **Computed backfills** — new
columns fill with zero values, not expressions; a backfill is a reducer the operator writes.
**Log-record rewriting** — the log is immutable history; old records keep their bytes and their
shape entry forever (until truncation), which is the whole point of the shape history.
**Cross-shape restore guarantees beyond the sidecar** — an archive restores the shapes it
carries; booting restored data under newer code is exactly a migration boot, and that falling out
for free is the design working, not a separate deliverable to build.

## Decisions to settle

### Settled: what identifies a column across deployments

By-name mapping makes the column's name its identity, which makes **rename indistinguishable from
drop-plus-add** — refused as destructive. That is honest but will annoy someone eventually.
Leaning: ship with rename-is-destructive and record the demand; if it bites, the answer is a
declared rename (`[RenamedFrom("OldName")]` consumed once by the migration path), not stable
column ids — SpacetimeDB-style ordinal identity is exactly what makes declaration order load-bearing
and reorders destructive, the trap this design exists to avoid.

**Settled as the leaning.** The name is the identity; the refusal message says "if this is a
rename, rename it back" so the accident case self-diagnoses. `[RenamedFrom]` is recorded here as
the answer if real demand arrives; nothing in the shape format precludes it.

### Settled: where the shape history lives

Leaning: the `melange.shape` sidecar as described — it needs no snapshot-header version bump
(MSNP v1 stays v1, phase 15's archives stay readable), it rides the existing sidecar machinery
into backups, and the epoch sidecar already proved the pattern. The alternative — a shape frame
inside the log itself, written on change — is more self-contained but makes the log format carry
schema, which every reader (backup's walker, verify's dry-replay) would then need to understand.
To settle by writing the sidecar's compaction rule down and checking it against the applier's
lowest checkpoint, not just the truncation base.

**Settled as the leaning, with one addition the plan did not foresee: the marker record.** The
sidecar alone left a real ambiguity — a snapshot's rows are written by the *running code*, not
by the shape governing the snapshot's LSN, so a snapshot taken at exactly the pre-migration head
could be either shape and recovery could not tell (a genuine corruption window when the
migration's sealing snapshot overwrote a pre-deploy one at the same LSN). The fix is one empty
write-set record appended at migration: the new reign begins at the marker's LSN, the sealing
snapshot lands *at* the marker, and "a snapshot's shape is the shape governing its own LSN"
becomes unconditionally true. It also puts the migration into the log's own timeline, which is
where a system whose log is the source of truth wants its history recorded. The compaction rule
settled simpler than the plan feared: the truncation base alone suffices, because everything
that reads records — appliers, subscribers, the resume window — already floors truncation, so no
reader can hold a cursor below the base; a reign whose successor began at or below the base has
no readable records left and dies at the next boot.

### Settled: is there any knob at all

Leaning: no. Additive migration is automatic-with-loud-log rather than gated behind an
`AutoMigrate`-style default-off switch, because the tiers differ in kind: Postgres could stall
its applier while the engine served traffic, but the hot tier *is* the engine — a refused
additive migration is a server that will not start, and a knob whose off position means "refuse
to boot on any schema change" is a foot-gun with no story. The asymmetry with phase 08's default
gets a paragraph in the docs rather than a flag. To settle: whether a `--dry-run` style report
(print what a migration boot *would* do, then exit) is worth shipping alongside — leaning yes,
it is nearly free and it is what a cautious operator actually wants before a deploy.

**Settled as the leaning: no knob, and CONFIGURATION.md gains no rows.** The dry-run shipped as
API rather than verb: `ShapeHistory.Load` and `ShapeCompatibility.Compare` are public, so a host
can answer "what would this deploy's boot do" in three lines without booting; a CLI spelling
would need the application's schema and therefore belongs to the host anyway (the same
schema-lives-in-the-host reasoning as phase 19's check verb).

## Shipped notes

Shipped as two stacked PRs — the sidecar and the migration boot; the reader transforms — with the
decisions above settled in place. Deviations and additions, recorded:

- **The marker record** (detailed under the shape-history decision): the plan's sidecar alone
  left a snapshot at exactly the pre-migration head ambiguous between shapes; the empty
  marker record makes "a snapshot's shape is the shape governing its own LSN" unconditionally
  true, puts the migration into the log's own timeline, and makes crash-anywhere re-migration
  the only recovery path — there is deliberately no migrated flag, because decode is by LSN,
  always.
- **The reader inventory was the second slice's real work.** The plan said "every reader picks
  the shape governing that row's LSN"; the sweep of `ReadFrom` call sites found five that decode
  or forward row bytes and could hold a cursor below a migration: pipeline applier catch-up
  (transforms centrally), the Postgres applier's own dispatch loop, resume replay, the hub's
  replica stream, and the border publisher (each one explicit call to
  `MelangeEngine.TransformToCurrentShape`). The sites that read only events, arguments, or
  timestamps — the event bus, cluster event forwarding, handoff-marker recovery, truncation's
  retention scan — were checked and left alone, and backup deliberately streams verbatim bytes.
- **Doctored-sidecar tests.** Proving the resume and Postgres paths transform requires two
  deployments' schemas in test processes whose model is fixed by the source generator; the tests
  stage the migration by rewriting the sidecar to claim two same-kind columns were stored in
  each other's positions — structurally an additive reorder, and unmistakable in assertions,
  because the columns' values trade places. The transport test was verified to fail with the
  transform removed.
- **Compaction settled simpler than planned** (see the shape-history decision): the truncation
  base alone bounds reign lifetime, because everything that reads records already floors
  truncation.
- **Rolling deploys are stop-the-fleet for schema changes**, stated in MIGRATION.md: shard
  engines migrate independently at open, but border and replica streams exchange current-shape
  rows, so mixed-schema fleets are unsupported.
