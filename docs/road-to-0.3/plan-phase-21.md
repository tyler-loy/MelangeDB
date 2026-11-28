# Phase 21 — `melange inspect`

**Status: Planned; plan refreshed for 0.5 and decisions settled (2026-11-27).** Written 2026-08-18,
before the 0.4 and 0.5 work it now has to read; [what changed](#what-changed-since-this-plan-was-written)
says what that work changed, and the [build steps](#build-steps) are the order to land it.

**Goal:** the commit log stops being only a recovery mechanism and becomes a readable record. Open a
data directory or a `.mbak`, jump to an LSN, see what the world looked like, see which reducer
produced that commit and what it wrote, step through a tick window, and answer "who mutated this row
and when" — without a debugger, without the application's assemblies, and without mutating anything.

**Depends on:** nothing new. Every input this phase reads already exists and is already written by
shipped code — the commit log's segments, the `melange.shape` sidecar (phase 16), the snapshot, the
`.mbak` archive (phase 15), the restore lineage (phase 19), and the CLI's existing verb shape
(phases 15 and 19).

## Why here

The audit trail is already on disk, in full, and nothing reads it. `CommitRecord` carries the LSN,
the timestamp, the caller's `Identity`, the reducer name, the serialized arguments, the collapsed
write set, and the published events. Two of those fields are annotated in
`src/MelangeDB.Abstractions/CommitRecord.cs` as, verbatim:

> Metadata for audit, never replayed.

They have been written on every commit since phase 01 and consumed by nothing. This phase is the
consumer.

It is also the thing MelangeDB can do that its alternatives make painful. Postgres gives you a WAL
you are not meant to read and an audit table you had to remember to write; SpacetimeDB gives you
neither. Here the write set *is* the log format, the log is the source of truth by design, and
recovery, backup, and `restore --at-lsn` already paid for every reader this needs. The remaining
work is a surface, not a mechanism — which is exactly the 0.3 theme.

The operational cases it serves are the ones that arrive unscheduled: an exploit investigation
("which identity called this reducer, forty thousand commits ago"), a support question ("why did
this deer teleport"), and the post-incident question that currently has no answer short of attaching
a debugger to a restored copy.

## What decoding actually requires

Scoping this turned up two things that make the phase materially cheaper than it looks, and both
should be stated up front because they remove the fork this plan was expected to open with.

**Rows decode from the shape sidecar, and the sidecar is the *right* source — not a fallback.**
Row format v1 is positional: bytes in declaration order, no names, no per-row version. The
`melange.shape` sidecar beside the log records the ordered column list per table, kept as a
**history fenced by the LSN each entry governs from**, precisely because log records outlive
deployments. That is not merely sufficient for an inspector, it is the only correct source: a
time-travel tool that decoded a year-old row with today's schema would be wrong by construction, and
the manifest describes today's schema. `DataDirectoryCapture` already writes the sidecar into every
`.mbak`, so an archive carries what an inspector needs.

**Arguments decode with no schema at all.** `ReducerArgs` encodes a count followed by
**self-describing tagged values** — `Null`, `Bool`, `Int64`, `UInt64`, `Float64`, `String`, `Bytes`,
`Identity`, `Timestamp`, `Array`. Types and values fall out of the bytes. What is missing is only
*parameter names*, and the manifest is a partial answer even for those: `ManifestEmitter` exports
client-callable reducers only, so the fourteen scheduled reducers that run the reference workload's
entire simulation are not in it.

So the inspector needs no external input beyond the directory or archive it is pointed at. A
manifest, when supplied, adds parameter names for client-callable reducers and nothing else.

## What changed since this plan was written

Five things shipped between this plan and 0.5.2 that the inspector reads or must account for. None of
them changes the phase's shape; three of them change how it is built.

**The log is term-fenced segments (road-to-0.4 phase 30).** A data directory's log is no longer one
file. `melange.segments` names the live segment (`melange.log`, or `melange.log.{term}` once the
shard has changed owner) and every sealed one, each sealed at a **cut**: the last LSN of the intact
prefix the previous owner left. Records past a sealed segment's cut are a previous owner's torn tail
and were never history. The reader walks the sealed segments in order, each up to its cut, then the
live one.

**`FileCommitLog` cannot be the reader.** Opening it takes `melange.lock` exclusively, which a live
engine holds, and its term fence can write: an open under a higher term seals the live segment. So
the reader is new code over what recovery already uses (`LogFileFormat.WalkRecords`, the segment
manifest, `melange.base`), read-only by construction: it opens every file for reading with
`FileShare.ReadWrite | FileShare.Delete`, takes no lock, and never writes. This is also what lets it
read a directory a live engine or another cluster node owns (see the decision below).

**The archive tier (phase 25).** An archive-tier table's rows ride the commit log like any other write
and then live only in Postgres: never in the hot store, a snapshot, or an `.mbak`. So `log`, `record`
and `row` show archive-tier writes for as long as the log holds them, and `at` cannot materialize
those tables at all; its output says which tables it left out and why. The shape sidecar records each
table's tier, so the reader knows which they are without the application. The archive tier does not
extend log history: past `BaseLsn`, an archive-tier row is reachable in Postgres, not through the
inspector.

**Restores have lineage (phase 19).** A restore mints a new epoch that continues numbering from the
restored head, recorded in `melange.lineage`. An LSN means something only with its epoch, so every
subcommand prints the directory's epoch and, when the lineage names ancestors, the fork point below
which the history is inherited. `restore --at-lsn` and `clone` (both shipped) are what a finding
leads to; `melange.provenance.json` marks a clone, and the inspector says when it is reading one.

**Storage engines are pluggable, and MelangeLog is the default (0.5.0).** Irrelevant to the inspector
by design: it reads the log, the snapshot and the sidecars, never a hot store. The snapshot format
(`melange.snapshot`: table, key and row bytes) does not depend on the engine.

Two more facts the original plan got wrong or left open:

- **A directory keeps one snapshot**, `melange.snapshot`, not a series. The plan's "newest snapshot at
  or below the LSN" assumed several. `at` is revised below to what one snapshot allows.
- **Each shard is its own data directory** (`{Cluster:ShardDataPath}/shard-k`), and the hub is
  another. The inspector reads one at a time; cross-shard timelines stay out of scope. Phases 22
  (`MelangeDB.Testing`) and 23 (`melange generate`, the manifest) are built, so the reader API has its
  second consumer waiting and a manifest to take parameter names from.

## Deliverables

**A read-only reader API in Core**, public, over a data directory or an archive: the record stream
by LSN range, the governing shape for an LSN, and row decoding against it. The CLI is its first
consumer but should not be its only one — phase 22's test kit wants to assert against decoded write
sets, and building this as CLI-internal would mean writing it twice.

**`melange inspect log <source>`** — the record stream. One line per commit: LSN, timestamp, caller,
reducer name, write-set size, event count. `--from` / `--to` bound the window; `--reducer`,
`--caller`, and `--table` filter it. This is the "step through a tick window" surface, and a tick
window is just an LSN range with a timestamp column next to it.

**`melange inspect record <source> --at-lsn <n>`** — one commit in full: decoded arguments,
every row operation with its table, key, and decoded row, and every event with its type and depth.

**`melange inspect row <source> --table <t> --key <k>`** — the row's mutation history: every commit
whose write set touched that key, with the reducer and caller that did it. This is "who mutated this
row, and when," and it is a scan of write sets rather than a new index.

**`melange inspect at <source> --at-lsn <n>`** — the world as of a commit, in an in-memory projection:
the snapshot replayed forward to the LSN when the snapshot is at or below it, or the log replayed from
empty when the log still reaches back to LSN 0. Anything else cannot be built forward, because the
log has no before-images to run backwards from, and the command says exactly that, naming the
snapshot's LSN and `BaseLsn`. Dumps a table or a single row. Archive-tier tables are never
materialized. See the decisions below for what else this deliberately cannot do.

**`--json` on every subcommand**, because the second consumer of a forensics tool is always a script.

**Every subcommand opens with the source's facts:** its epoch and lineage, the LSN range it can read
(`BaseLsn` to the head it reached), the snapshot's LSN, and whether it is a clone or a live directory.
Investigations fail at those edges, so they are printed before any answer rather than after a
confusing empty one.

**Documentation.** A new `docs/INSPECT.md` walking the three questions an operator actually arrives
with (what happened at this LSN, what happened to this row, what did the world look like then);
*inspector* and any new nouns into [GLOSSARY.md](../GLOSSARY.md); the read-only and
no-network properties stated in [THREAT-MODEL.md](../THREAT-MODEL.md), because a tool that reads
past every row policy is a threat-model fact whether or not it is documented as one.

## Out of scope

**Writing anything.** The inspector opens sources read-only and has no verb that mutates. This is
not a default; it is the boundary that lets an operator point it at a production data directory. A
"fix this row" verb would be a different tool with a different blast radius, and `restore --at-lsn`
plus a reducer is the supported path to changing history.

**Any network surface.** No `/melange/inspect` endpoint, no server integration. Phase 18 declined a
`/melange/status` endpoint for exactly this reason — a new privileged read surface drags the full
gating ladder (`Enabled` / `OwnerRole` / assertion flags) behind it, and this one would expose the
*entire* log, past every row-level policy, including private tables. File access is the
authorization model, and it is the right one for a forensics tool.

**A viewer, a UI, or anything that draws this.** [ROADMAP.md](../ROADMAP.md) records the stock admin
console as a permanent refusal, and that refusal covers the console-shaped version of this too.
`--json` is the integration point.

**Cross-shard timelines.** A shard is an engine with its own log and its own LSN space; an LSN is
meaningful only within one log, and the epoch exists to make that explicit. Correlating a player's
path across shards is a real question and a different feature, and pretending a global ordering
exists would be the wrong answer to it.

**Decoding event payloads.** An event's type name and depth are structural and get displayed; its
payload is user-defined bytes with no shape record. Hex and a length, honestly labelled.

## Decisions

Settled 2026-11-27 by taking the leanings below, with the open sub-questions answered as stated.

### Standalone CLI, or in-process against the application's registry

The central decision, and the one that sets the phase's ceiling. Standalone means the CLI decodes
from the sidecar with no user assemblies loaded: it works on any directory or archive, on a machine
that has never built the game, from a backup taken by someone else. In-process means hosting the
application's `SchemaRegistry`, which buys indexes, typed access, SQL, and policy evaluation.

**Decided:** standalone, and only standalone in this phase. The forensics case — a backup, an
incident, a support question — is the one nothing else can serve, and it is the case that evaporates
the moment the tool requires a build of the right commit of the game. An in-process inspector is a
strictly easier thing to add later on top of the reader API, and adding it later costs nothing;
starting there would quietly make the tool unavailable exactly when it is wanted.

### What `inspect at` can honestly show without a registry

Materializing the world from the sidecar gives raw rows: correct names, correct kinds, correct
values. It does not give indexes (they are projections rebuilt from declarations), the SQL surface,
residency behaviour, or row-level policies.

**Decided:** ship it with those absences stated in the command's own output rather than only in
docs — a dump header naming what this view is and is not. The failure mode to avoid is an operator
concluding a row is absent when it is merely not reachable by the index they were thinking of. A
`--table` dump is allowed, and stops at 10,000 rows unless `--all` is given, saying how many it left
out; a single row by key is never guarded. The replay is the cost either way; the guard is about
what lands on an operator's terminal.

### The log has no before-image

The write set is post-images plus deletes by key. The pre-image exists at fan-out time because the
store still holds it, but it is not in the record. So `record` can show what a commit *wrote*, not
what it *changed from*, and "why did this deer teleport" wants the difference.

**Decided:** post-image by default, with `--diff` reconstructing the previous value of each touched
key. That is honest and it is slow, and being opt-in is what keeps `inspect log` fast. It is one
replay carried forward across the window, never a replay per record, which makes `--diff` a property
of a windowed scan (`record` is a window of one). It needs what `at` needs: a starting state at or
below the window, so it is refused, with the same explanation, where `at` would be.

### What happens at the truncation floor

An inspector can see back to `BaseLsn` and no further, and "the answer was truncated away" is going
to be a common outcome in exactly the investigations that matter most.

**Decided:** say so precisely rather than returning empty — name `BaseLsn`, and point at
[OBSERVABILITY.md](../OBSERVABILITY.md)'s retention section, since phase 18 gave every floor a name
and the operator's real next question is which holder let the record go. A backup taken before the
truncation is the actual remedy, and the message should say that too.

### Whether `inspect` reads a live directory

Pointing the tool at the data directory of a *running* engine is the case an operator will try
first, under pressure.

**Decided:** allow it, read-only, and treat a torn tail as expected rather than as corruption — the
same posture phase 17's recovery took when buffered appends made multi-record torn tails normal.
Report the head it could read and note that the engine may be ahead of it. Refusing outright would
push operators toward copying a live directory by hand, which is worse.

Shared storage does not change it. The reader takes no lock, so a node holding `melange.lock` is
never in its way, and it reads the segment manifest once, at the start. Two things can move under it
and both are reported, not hidden: the owner appending past the head it reached, and a new owner
sealing the live segment (a failover or a move mid-read), which the reader notices when the manifest
it read no longer matches and says that the shard changed owner while it read. A compaction that
replaces a segment file mid-read ends that read at a record boundary with the same message, since
`FileShare.Delete` keeps the old file readable until it is closed.

## Build steps

Each is one PR, and each leaves `main` with a usable tool.

1. **The reader API in Core.** A read-only source over a data directory or a `.mbak`: its facts
   (epoch, lineage, provenance, `BaseLsn`, snapshot LSN, head), the record stream by LSN range across
   sealed and live segments, the shape governing an LSN, row decoding against it, and argument
   decoding. Tested against directories the engine writes, including one that changed owner, one with
   a torn tail, one compacted, one restored, and a live engine appending while it is read.
2. **`melange inspect log` and `inspect record`**, with `--json`, over step 1. The first useful
   forensics tool: who called what, when, and what it wrote.
3. **`melange inspect row`**: one key's mutation history, a filtered scan of write sets.
4. **`melange inspect at` and `--diff`**: the in-memory projection from the snapshot or from empty,
   the honest refusals, the row guard.
5. **Documentation**: `docs/INSPECT.md`, the glossary, and the threat model's read-only, no-network
   statement. Each earlier step documents its own flags as it lands; this step writes the walkthrough.
