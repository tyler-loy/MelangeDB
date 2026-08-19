# Phase 21 — `melange inspect`

**Status: Planned.**

**Goal:** the commit log stops being only a recovery mechanism and becomes a readable record. Open a
data directory or a `.mbak`, jump to an LSN, see what the world looked like, see which reducer
produced that commit and what it wrote, step through a tick window, and answer "who mutated this row
and when" — without a debugger, without the application's assemblies, and without mutating anything.

**Depends on:** nothing new. Every input this phase reads already exists and is already written by
shipped code — `ICommitLog.ReadFrom`, the `melange.shape` sidecar (phase 16), the `.mbak` archive
(phase 15), and the CLI's existing verb shape (phases 15 and 19).

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

**`melange inspect at <source> --at-lsn <n>`** — the world as of a commit: newest snapshot at or
below the LSN, replayed forward to it, into an in-memory projection. Dumps a table or a single row.
See the decisions below for what this deliberately cannot do.

**`--json` on every subcommand**, because the second consumer of a forensics tool is always a script.

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

## Decisions to settle

### Standalone CLI, or in-process against the application's registry

The central decision, and the one that sets the phase's ceiling. Standalone means the CLI decodes
from the sidecar with no user assemblies loaded: it works on any directory or archive, on a machine
that has never built the game, from a backup taken by someone else. In-process means hosting the
application's `SchemaRegistry`, which buys indexes, typed access, SQL, and policy evaluation.

**Leaning:** standalone, and only standalone in this phase. The forensics case — a backup, an
incident, a support question — is the one nothing else can serve, and it is the case that evaporates
the moment the tool requires a build of the right commit of the game. An in-process inspector is a
strictly easier thing to add later on top of the reader API, and adding it later costs nothing;
starting there would quietly make the tool unavailable exactly when it is wanted.

### What `inspect at` can honestly show without a registry

Materializing the world from the sidecar gives raw rows: correct names, correct kinds, correct
values. It does not give indexes (they are projections rebuilt from declarations), the SQL surface,
residency behaviour, or row-level policies.

**Leaning:** ship it with those absences stated in the command's own output rather than only in
docs — a dump header naming what this view is and is not. The failure mode to avoid is an operator
concluding a row is absent when it is merely not reachable by the index they were thinking of. **Open:**
whether `--table` dumps should be allowed at all without an index, given that a large table at an
LSN is a full scan of a replayed projection; a row count guard with an explicit override is the
likely answer.

### The log has no before-image

The write set is post-images plus deletes by key. The pre-image exists at fan-out time because the
store still holds it, but it is not in the record. So `record` can show what a commit *wrote*, not
what it *changed from*, and "why did this deer teleport" wants the difference.

**Leaning:** post-image by default, with `--diff` reconstructing the previous value by materializing
the touched keys as of `lsn - 1`. That is honest and it is slow, and being opt-in is what keeps
`inspect log` fast. **Open:** whether `--diff` over a window is worth the repeated replay or whether
one replay carried forward across the window is the only sane implementation — probably the latter,
which makes `--diff` a property of a windowed scan rather than of a single record.

### What happens at the truncation floor

An inspector can see back to `BaseLsn` and no further, and "the answer was truncated away" is going
to be a common outcome in exactly the investigations that matter most.

**Leaning:** say so precisely rather than returning empty — name `BaseLsn`, and point at
[OBSERVABILITY.md](../OBSERVABILITY.md)'s retention section, since phase 18 gave every floor a name
and the operator's real next question is which holder let the record go. A backup taken before the
truncation is the actual remedy, and the message should say that too.

### Whether `inspect` reads a live directory

Pointing the tool at the data directory of a *running* engine is the case an operator will try
first, under pressure.

**Leaning:** allow it, read-only, and treat a torn tail as expected rather than as corruption — the
same posture phase 17's recovery took when buffered appends made multi-record torn tails normal.
Report the head it could read and note that the engine may be ahead of it. Refusing outright would
push operators toward copying a live directory by hand, which is worse. **Open:** whether shared
storage in a cluster changes this, since another node may hold the write lock on the log being read.
