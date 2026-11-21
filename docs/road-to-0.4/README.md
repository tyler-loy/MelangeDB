# The road to 0.4

Post-0.3 phase plans, in the same form as [road-to-0.1/](../road-to-0.1/),
[road-to-0.2/](../road-to-0.2/), and [road-to-0.3/](../road-to-0.3/), and numbered continuously
with them: written before the work, "Decisions to settle" answered in place as each one resolves,
and kept afterwards as the decision record.

These are **not** user documentation. [ROADMAP.md](../ROADMAP.md) is the summary, and the reference
docs one level up describe the current system. Where a plan and the code disagree, the code is right.

## The theme

**0.4 closes the gaps SpacetimeDB opened while MelangeDB was being built.** MelangeDB was started on
2026-07-29 against SpacetimeDB 2.7, to fix three things: no clustering, every table in RAM, and a
static-heavy C# runtime with no DI. On 2026-11-20 every SpacetimeDB release from 2.7.0 to 2.16.1
was reviewed against this repo. On the three original complaints, MelangeDB is still ahead:

- **Clustering.** Spatial sharding, handoff, and elastic assignment, against single-database
  leader/follower replication.
- **DI.** The host is yours, and `IConfiguration` is just there. SpacetimeDB added typed
  environment variables, fixed at publish time.
- **RAM.** Residency defaults to `Paged`. SpacetimeDB 2.16.1 added opt-in tiered storage, but its
  indexes still stay in memory.

But in sixteen releases SpacetimeDB also shipped things a team building on MelangeDB would
reasonably expect and not find here. The review counted six, and planning this milestone found a
seventh:

| SpacetimeDB has | Since | MelangeDB today | Answered by |
| --- | --- | --- | --- |
| Unit testing for module code | 2.12 | Planned, not built | [Phase 22](../road-to-0.3/plan-phase-22.md), carried from 0.3 |
| A TypeScript client | 1.x | Planned, not built | [Phase 23](../road-to-0.3/plan-phase-23.md), carried from 0.3 |
| A constraint added by migration is checked against existing rows | 2.7 | Built in phase 27 | [Phase 27](plan-phase-27.md) |
| Old clients survive added columns | 2.16.1 | A stale client is refused at subscribe | [Phase 28](plan-phase-28.md) |
| Automatic reconnect with backoff and token refresh | 2.12 | Manual `ReconnectAsync` | [Phase 28](plan-phase-28.md) |
| An agent (MCP) endpoint over schema, SQL, and reducer calls | 2.7–2.10 | Nothing | [Phase 29](plan-phase-29.md) |
| HA replication of a single database | 2.10 (Maincloud) | Recovery by reassignment only | Phase 30, committed; designed from [phase 20](../road-to-0.3/plan-phase-20.md)'s window |

**Catching up does not mean copying.** Each row is answered in this project's own terms, and two of
them are answered differently on purpose. Phase 28 tolerates added columns *at any position*,
because MelangeDB's subscription descriptor carries every column's name, where SpacetimeDB can only
skip trailing ones. Phase 30 will exist, but not in SpacetimeDB's form: that is consensus
replication, and this project chose "Postgres-backed membership, not Raft" in phase 09 for reasons
that still hold. Its shape is a warm replica under the existing fencing, plus faster detection.

## The phases

| | Phase | Status |
| --- | --- | --- |
| [20](../road-to-0.3/plan-phase-20.md) | The measurement pass, decision-gating half. **Carried from 0.3** | **Measured** 2026-11-20; findings [#198](https://github.com/tyler-loy/MelangeDB/issues/198)–[#204](https://github.com/tyler-loy/MelangeDB/issues/204) |
| [22](../road-to-0.3/plan-phase-22.md) | `MelangeDB.Testing`: the reducer test kit. **Carried from 0.3** | Planned |
| [23](../road-to-0.3/plan-phase-23.md) | The TypeScript client, client conformance, and `melange generate`. **Carried from 0.3** | Planned |
| [27](plan-phase-27.md) | Constraint changes are migrations: a `[Unique]` added over existing rows is checked, not assumed | **Built**; fixes [#192](https://github.com/tyler-loy/MelangeDB/issues/192) |
| [28](plan-phase-28.md) | Clients that outlive a deploy: additive schema changes stay invisible to old bindings, and reconnect is automatic | **Built** |
| [29](plan-phase-29.md) | The agent surface: an opt-in MCP endpoint over the schema, owner-mode SQL, and published signals | Planned |
| 30 | Shard HA | **Committed** 2026-11-20; gets a design record before a plan, as [phase 24](../design/scheduler-lanes.md) did |

The plans for 20, 22, and 23 stay where they were written, in [road-to-0.3/](../road-to-0.3/).
Moving them would break every link into them for no gain; the decision record is the same document
either way. 0.3.0 shipped with phases 25 and 26 and left 20–23 planned. Phase 21
(`melange inspect`) is not a gap SpacetimeDB opened, so it is not scheduled here; it can land in any
release.

## Ordering

**Phase 27 first.** Planning found that a `[Unique]` added to a column that already holds
duplicates boots without complaint, and the index then holds two rows for one "unique" value. Only
the next insert is refused. That is a correctness bug in shipped code, not a missing feature
([#192](https://github.com/tyler-loy/MelangeDB/issues/192)). It was considered for an earlier 0.3.x
fix and deliberately kept with the milestone, so the refusal and the shape-history record that
replaces it land together. Until then, dedupe before adding `[Unique]`.

**Phase 28 lands before phase 23**, for the same reason [phase 26](../road-to-0.3/plan-phase-26.md)
did. Phase 23 writes the client contract down for a second runtime. Tolerating additive changes
changes what a correct client does with a descriptor. Landing it first means the TypeScript client
implements it from day one, and the conformance suite tests it, instead of reopening a contract
that was just pinned. Automatic reconnect is in the same phase because both are about a deployed
client surviving events it did not cause.

**Phases 22 and 29 are independent** of everything else and of each other.

**Phase 30 was planned as gated on phase 20's reassignment-window measurement, and is now
committed instead.** On 2026-11-20 the project owner settled the question the gate left open:
production systems get HA, and the bar for *not* building it is high. A recorded refusal is no
longer an expected outcome. The measurement now shapes the design rather than deciding whether it
happens, and it shaped it a lot ([idea-bin entry](../idea-bin/shard-ha-warm-replica.md)):

- **The window is 12–19 s at defaults, and detection is 10.6–13.4 s of it.** A warm replica
  removes only the 1–6 s of rebuild. Fencing forbids promoting it before the dead owner's lease
  expires, so phase 30's design record has to own detection as well. It still starts with the
  fencing third state, because shortening leases is where that risk sharpens.
- **Three cold-path bugs came first; all are fixed.** A node
  opened an adopted shard inside its heartbeat loop and could be fenced by its own recovery
  ([#198](https://github.com/tyler-loy/MelangeDB/issues/198), fixed). The gateway's upstream connect
  had no timeout, so a dead *machine* stalled clients for the OS connect timeout
  ([#200](https://github.com/tyler-loy/MelangeDB/issues/200), fixed). A FASTER shard appeared to
  lose payloads under reassignment churn ([#199](https://github.com/tyler-loy/MelangeDB/issues/199)):
  that was a read view outliving its store, fixed. Underneath it, a border reset scanned the
  whole paged shard under the engine lock long enough to starve the heartbeat
  ([#208](https://github.com/tyler-loy/MelangeDB/issues/208), fixed). Under HA that would have been
  a false failover. Paged scans are still slow, just no longer blocking
  ([#212](https://github.com/tyler-loy/MelangeDB/issues/212)).

On the phase 28 interaction: the client assumed was phase 28's, with automatic reconnect on. It
makes no difference to the window, because the client never disconnects. The gateway holds or fails
its calls, so reconnect backoff never engages.

## What the review found that is not a gap

Recorded so the next review does not re-derive them:

- **Tiered storage** (2.16.1): phase 07, default `Paged`.
- **Typed environment variables** (2.12): the host's own `IConfiguration`, which is DI complaint 3's
  whole answer.
- **Async inter-database calls via outbox tables** (2.14): the event bus (phase 06) is the same
  shape, a log-backed outbox with at-least-once delivery to handlers that run with full DI.
- **Scheduler drift and head-of-line fixes** (2.7.1, 2.8.3, 2.10.1): phase 05 timers anchor on
  their due time with an overrun policy. Fairness between scheduled and client work is phase 24's
  open question, not a regression.
- **Acknowledging a call only once it is durable** (2.13): `CommitLog:FsyncPolicy = OnCommit` has
  been the default since phase 01.
- **Database forking and time travel** ("coming" after 2.16.1): `melange clone` and
  `restore --at-lsn` (phase 19), with `melange inspect` planned (phase 21).
- **Commit-log segment durability** (2.9, #5785): the same bug class was present here, in the
  commit log's compaction swap and the engine's sidecars, and is fixed separately rather than as a
  phase ([#190](https://github.com/tyler-loy/MelangeDB/pull/190)): those renames now fsync their
  directory.

## What 0.4 deliberately excludes

- **A stock admin console**, still a permanent refusal. Phase 29 is a protocol over signals the
  engine already publishes, which is the side of that line this project committed to. Phase 29's
  plan argues this rather than assuming it.
- **Submodules / module composition** (2.8, 2.13–2.14). Not evaluated by the review; in MelangeDB,
  module code is ordinary assemblies in the developer's own host, so whether this is a gap at all
  needs a consumer asking for it first.
- **Additional client languages beyond TypeScript**, which remain scoped and declined in
  [idea-bin/additional-client-languages.md](../idea-bin/additional-client-languages.md).
- **A coding-agent plugin or skill pack** like SpacetimeDB's Claude and Codex plugins (2.8.1–2.8.2).
  Phase 29 ships the endpoint such a pack would sit on; the pack itself is documentation with
  opinions, and is out of scope for the same reason a console is.

The standing conventions hold here exactly as they did for 0.1 through 0.3: every configuration
item goes in [CONFIGURATION.md](../CONFIGURATION.md) in the change that introduces it, every noun
in [GLOSSARY.md](../GLOSSARY.md), every signal in [OBSERVABILITY.md](../OBSERVABILITY.md).
