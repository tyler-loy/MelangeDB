# Roadmap

Where MelangeDB is, how it got here, and what's left.

0.1 was sequenced as twelve phases and all twelve have shipped; 0.2 added seven more, also shipped;
0.3 plans five. Each phase's full plan — deliverables, what was explicitly out of scope, and the
reasoning behind every decision it settled — lives in [road-to-0.1/](road-to-0.1/),
[road-to-0.2/](road-to-0.2/), and [road-to-0.3/](road-to-0.3/). Those are the authoritative record
of *why* things are the way they are; this page is the summary.

## Standing conventions

These apply to every change, not just the phase that introduced them:

- **Every configuration item goes in [CONFIGURATION.md](CONFIGURATION.md)**, in the same change that
  introduces it — not at the end of a phase. That document is the source of truth for key names,
  defaults, and reload semantics. Undocumented knobs are how a library becomes folklore.
- **Every noun goes in [GLOSSARY.md](GLOSSARY.md)** when the change introducing it lands. Vocabulary
  drift is how a design becomes unexplainable — "region" survived in three documents after the
  concept became "shard," which is exactly the failure the glossary prevents.
- **Every change instruments what it adds**, recorded in [OBSERVABILITY.md](OBSERVABILITY.md). Span
  and metric names are public API — once a dashboard or alert depends on `melange.applier.lag`,
  renaming it is a breaking change.

## Ordering principles

Three constraints drove the sequence, and each one reverses an intuitive ordering:

1. **The in-memory projection came before the real storage engine.** Because the commit log is the
   source of truth, an in-memory hot store is a *legitimate* projection rather than a stub. That let
   the transaction, log, and subscription layers be built and tested end-to-end before any
   storage-engine work — the part most likely to eat months.
2. **Paging came before clustering.** Cold world data grows with area (the N² term); live simulation
   grows with player density. Sharding alone just re-bills N² as more nodes holding cold terrain.
   Paging attacks the bigger term and needs no coordination layer.
3. **Instancing came before spatial sharding.** Instanced shard transitions are explicit and discrete
   — the loading screen *is* the handoff window — so they need no border overlap, interest
   computation, or seamless transfer. Same mechanism, a fraction of the machinery.

All three held up. The one that paid off most was (1): phase 07's storage-engine swap changed nothing
above `IHotStore`, and the applier pipeline was untouched by it.

## M1 — Single-node MelangeDB · shipped

A developer can add the package, define tables and reducers in their own worker service, and have
real clients subscribe to live data. All three original complaints are answered at single-node scale:
DI in phase 02, the RAM ceiling in phase 07, and clustering prepared for by the commit log in 01.

| Phase | Title | What it settled |
| --- | --- | --- |
| [01](road-to-0.1/plan-phase-01.md) | Core engine — schema, write set, transactions, commit log | Log the **write set**, not the invocation, so projections rebuild without re-running user code. A uniform order-preserving byte key. AutoInc ids are **unique, not dense** — originator-prefixed within 63 bits, so a signed `bigint` round-trips and shards never coordinate. Nested reducer calls forbidden. Indexes owned by the store. |
| [02](road-to-0.1/plan-phase-02.md) | Source generator and host integration | Reducers are **synchronous** — the transaction is a synchronous critical section, and `await` invites the I/O the design forbids. Two generators, split immediately. Diagnostics `MELANGE0001`+ as a first-class deliverable, including ambient time/randomness and known I/O types. Argument validation rejects `NaN`/`±Infinity` before a transaction opens. |
| [03](road-to-0.1/plan-phase-03.md) | Transport, subscriptions, and the C# client | Queries are a **SQL subset**, for portability to non-C# clients. **Every frame carries a channel tag** from version one, so multiplexing never becomes a protocol break. `Resume` names a **log epoch**, not a bare LSN. Backpressure defaults to `DropAndResync` over unbounded buffering. Projected subscriptions suppress no-op deltas. |
| [04](road-to-0.1/plan-phase-04.md) | Identity, auth, and row-level policies | **Rows UNION, columns INTERSECT.** Identity hashes issuer *and* subject, so two token sources cannot collide. `Reauthenticate` may refresh a token but **never** change identity. Connect tickets, because browsers cannot set WebSocket headers. Allow-by-default reducer policy paired with an asserted report of every unpoliced reducer. Measured: ~520 ns/row for policy evaluation, so no caching and no invalidation bugs. |
| [05](road-to-0.1/plan-phase-05.md) | Scheduled and lifecycle reducers | Timers are **rows**, so scheduling is transactional and survives restart. Repeating timers derive their next fire and write **nothing** per fire — fourteen idle simulation timers cost zero log records. Fire-once-and-resume after downtime. Scheduled reducers are not client-callable. An admin query is not a session. |
| [06](road-to-0.1/plan-phase-06.md) | The event bus | Events live **in the log record** (format v2, backwards-compatible), not derived from row deltas — `PlayerDied` is not an update. At-least-once and replayable; handlers never block the applier. Cycles bounded by **publish depth**, not detection, because detection is defeatable by rewrapping payloads. Abandoned subscriber checkpoints expire so they cannot pin truncation forever. |
| [07](road-to-0.1/plan-phase-07.md) | Durable hot store — paging, residency, large values | **Opt-in `Resident`, default `Paged`.** A size threshold would make memory a function of data size — the SpacetimeDB failure mode with a delay, arriving under production load. Opt-in makes the resident footprint a declared, computable artifact. FASTER is a projection; **recovery is ours** on every start. Blobs out of line, splitting the row byte-exactly. Measured: bulk load **44×** faster than per-row transactions; a resident scan within **1.05×** of the in-memory store. |
| [08](road-to-0.1/plan-phase-08.md) | The Postgres tier and ad-hoc SQL | Tier means *additionally* Postgres, not instead — the three axes stay orthogonal. Row shapes read the hot store at head; **aggregates are owner-mode only**, because policies are in-process code that cannot be pushed into SQL. Migration is **additive automatic, destructive manual** — refused loudly with the pending DDL printed. Checkpoint written inside the batch it describes, so applied and checkpointed cannot diverge. |

## M2 — Cluster · shipped

| Phase | Title | What it settled |
| --- | --- | --- |
| [09](road-to-0.1/plan-phase-09.md) | Clustering I — placement, hub/shard roles, instancing | **A shard is an engine** — own log, hot store, scheduler, fan-out. Membership is Postgres-backed, not Raft: what must survive a hub restart is small and relational-shaped, and Raft would have bought hub availability at the price of a consensus dependency everywhere. Inter-node identity is a hub-minted signed assertion; the trust boundary that draws is written down rather than implied. The dual attachment is server-internal — **the client protocol does not change**. Reducer execution site resolved at compile time, conservative-plus-loud. |
| [10](road-to-0.1/plan-phase-10.md) | Clustering II — spatial strategy and seamless handoff | Border band **derived, not guessed** (`margin + travel-during-handoff`). The **origin node** decides a handoff — the client's claimed position is never trusted. A mid-handoff reducer call is **queued at the gateway, invisibly**, because reject-and-retry makes every boundary a visible stutter. Border rows count against residency; an "honest footprint" that excluded them wouldn't be. Creature AI transfers ownership on crossing. Measured and published: the **hotspot ceiling** — ~1,100 commits/s per crowded shard at per-commit fsync, ~52,000 at interval fsync. No cluster size changes either number. |

## M3 — Proven

| Phase | Title | Status |
| --- | --- | --- |
| [12](road-to-0.1/plan-phase-12.md) | Typed client bindings | **Shipped.** A language-neutral `melange-schema.json` manifest exported by the `melange` CLI, consumed by the same analyzer to generate typed rows, refcounted merged caches, subscription helpers, and reducer stubs — one tree per consuming project, never by referencing server code. Plus a `Manual` dispatch mode that applies whole frames on the host's own thread, for engines that allow scene mutation only from theirs. See [CLIENT-BINDINGS.md](CLIENT-BINDINGS.md). |
| [11](road-to-0.1/plan-phase-11.md) | Reference workload port and validation | **Shipped.** The 82-table SpacetimeDB game runs on MelangeDB and is developed on it daily — an ASP.NET host with `UseFasterHotStore()`, tracking the `-ci.*` prereleases published from `main` — deliberately not named as a version here, since `VersionPrefix` moves every release and a pinned number dates this page. Parity is a live product rather than a checklist, which is the strongest form the bar could take. The port is also the sharpest source of evidence this repo has: it found the recovery regression, the client identity gap, the transient-rejection shape, the reducer-error mismapping, the silent shape adoption, and the primary-key range walk — none of which the suite caught, the last of them because every range test in the repo runs on a table too small for the distance to a window to be distinguishable from the window. **The plan's measurement half is not recorded here yet;** see [What's left](#whats-left). |

Phase 12 was numbered after 11 but landed first: the port's scoping pass
([#20](https://github.com/tyler-loy/MelangeDB/issues/20)) measured 459 client call sites that are
mechanical against typed bindings and a rewrite without them.

## M4 — 0.2: elastic capacity and operations · shipped

Post-0.1 work, planned in [road-to-0.2/](road-to-0.2/). Phases 13–14 implement
[design/elastic-rebalancing.md](design/elastic-rebalancing.md): shard boundaries stay fixed at
strategy registration, and the elastic layer is the shard → node map — regrouping, never resizing.
Phase 15 is independent operational surface over the 0.1 durability machinery. Phases 16–19 were
planned together after those three shipped and the reference port went live: schema evolution,
the throughput lever, retention observability, and the backup verbs phase 15's decision record
deferred — mutually independent, in any order.

| Phase | Title | Status |
| --- | --- | --- |
| [13](road-to-0.2/plan-phase-13.md) | Clustering III — elastic assignment: per-shard load on heartbeats, the planned drain, the rebalance loop | **Shipped** |
| [14](road-to-0.2/plan-phase-14.md) | Clustering IV — provisioned capacity and scale-in: the `INodeProvisioner` seam, provision-then-reassign, drain-and-decommission | **Shipped** |
| [15](road-to-0.2/plan-phase-15.md) | Backup and restore: the `.mbak` archive (snapshot + log tail, per engine — the truth, not the projections), `melange backup` / `restore` / `backup verify` | **Shipped** |
| [16](road-to-0.2/plan-phase-16.md) | Hot-tier schema migration: the shape sidecar, additive changes replay by name-mapped rebuild, destructive changes refuse loudly — DESIGN.md §10's open half | **Shipped** |
| [17](road-to-0.2/plan-phase-17.md) | Group commit: coalesced fsyncs at unchanged `OnCommit` semantics — the hotspot ceiling re-measured | Shipped |
| [18](road-to-0.2/plan-phase-18.md) | Truncation-floor observability: named floors, the governing-floor gauge and log line, the `melange-retention` health check | Shipped |
| [19](road-to-0.2/plan-phase-19.md) | Backup, second pass: `restore --check` (the boot-proof), `melange clone` (explicitly a different world), `restore --at-lsn` | **Shipped** |

## M5 — 0.3: the day-to-day surface · planned

Post-0.2 work, planned in [road-to-0.3/](road-to-0.3/). 0.1 built the engine and 0.2 was capacity
and operations; both made the database do more. 0.3 is the surface a team building on this every day
still has to invent for itself — inspect what the log already recorded, test a tick without standing
up a host, and reach the world from a browser. Phase 20 is not that, and comes first anyway: it is
the measurement debt below, and three separate decisions are waiting on it.

| Phase | Title | Status |
| --- | --- | --- |
| [20](road-to-0.3/plan-phase-20.md) | The measurement pass, decision-gating half: the reassignment window and the per-tick cost of scheduled reducers. The head-to-head comparison is split out and undecided | Planned |
| [21](road-to-0.3/plan-phase-21.md) | `melange inspect`: time-travel over the commit log — jump to an LSN, see the world, the reducer that produced it, and its write set | Planned |
| [22](road-to-0.3/plan-phase-22.md) | `MelangeDB.Testing`: a published reducer test kit — ticks, time, identity, and write-set assertions as first-class | Planned |
| [23](road-to-0.3/plan-phase-23.md) | The TypeScript client, a written client-conformance definition, and `melange generate --lang` | Planned |
| 24 | Scheduler lane: simulation work yields to client-initiated work when they contend | [Design record](design/scheduler-lanes.md) written; conditional on a phase 20 measurement, and may close as a refusal |

Phases 21–23 are mutually independent and land in any order. Phase 24 is gated on 20 for its sizing
and gets a design record before a plan, the way
[design/elastic-rebalancing.md](design/elastic-rebalancing.md) preceded 13–14 — it is the only one
of the set that changes engine semantics.

## What's left

**The measurements phase 11 promised.** The port itself has landed — the reference workload runs on
MelangeDB and is developed on it — but the controlled comparison the plan called its deliverable has
not been recorded in this repo: memory for the 10km world against SpacetimeDB's and for the 20km
world it cannot host, reducer latency p50/p99 for gather/move/attack/craft, terrain-streaming
throughput across chunk boundaries, and concurrent players per node. Until those land here, **treat
every benchmark in these docs as a dev-machine measurement rather than a production
characteristic** — that distinction is the whole reason the phase asked for numbers, and phase 11's
own risk register is explicit that a port reporting only wins is not evidence.

The measurements that *gate other work* are now [phase 20](road-to-0.3/plan-phase-20.md), including
two phase 11 did not ask for: the reassignment window for a crowded shard, and what the scheduled
reducers cost a player reducer on the same lock.

**The head-to-head comparison against SpacetimeDB is split out of that phase and remains
undecided.** Its baseline froze at the port and the game has kept shipping — including changes its
author judges would not have been feasible on SpacetimeDB at all — so the two sides have diverged in
capability rather than merely in size. Running it would compare two different games; not running it
is a legitimate outcome that has to be *recorded* rather than reached by drift. Until one or the
other happens, the warning above stands unchanged.

Known deferrals, each recorded with its reasoning rather than left as an omission:

- **Joins in subscriptions.** Incrementally maintaining them is differential-dataflow territory; an
  audit of a real 82-table game found **zero** subscriptions using one.
- **Unreliable/UDP transport — permanently.** A reducer is a transaction; a client must know whether
  it committed. Rate limiting plus client-side interpolation is the supported answer.
- **A stock admin console — permanently.** The recurring request is a shipped web UI: live tables,
  ad-hoc SQL, reducer call log, shard map, load, retention floors, backup status, who is connected.
  Every one of those is already reachable, and that is the deliverable: the ad-hoc SQL endpoint
  (phase 08), the `melange-schema.json` manifest (phase 12), and the span and metric register in
  [OBSERVABILITY.md](OBSERVABILITY.md) — `melange.cluster.shard.utilization` and
  `melange.shard.owned` for the shard map and load, `melange.log.truncation_floor` for retention
  floors (phase 18), `melange.backup.*` for backup status (phase 15), `melange.connections.active`
  for sessions, the `melange.reducer` span for the call log. **Publishing the signals is the
  engine's job; prescribing the tool that draws them is not.** A console is a product with opinions
  about who operates it, and it would be built against a surface this project already commits to
  keeping stable. The gap is deliberate rather than an oversight — the standing convention that
  every change instruments what it adds is what keeps the surface complete enough that the gap
  stays cheap to fill.
- **Schema migration against an existing log is no longer deferred** — the relational half settled
  in phase 08, and the hot-tier half shipped as
  [phase 16](road-to-0.2/plan-phase-16.md): additive automatic and loud, destructive refused and
  manual, both tiers one rule. See [MIGRATION.md](MIGRATION.md).
- **Epoch-qualified subscription anchors.** Closed behaviourally in phase 10 by the first-chunk rule;
  the protocol-level hardening is deferred, with the record kept in [plan-phase-10.md](road-to-0.1/plan-phase-10.md).
- **Dynamic *assignment* is no longer deferred** — it is M4 above, planned as phases 13–14.
  What stays deferred: dynamic boundary *splitting* (the quadtree; its narrowed customer and
  reopening trigger are recorded in [design/elastic-rebalancing.md](design/elastic-rebalancing.md)),
  [shard-level HA](idea-bin/shard-ha-warm-replica.md) (a warm-replica shape and its reopening
  trigger are in the idea bin; this deferral stands until that trigger fires), and sharding the hub.
- **Shard-side interest-scoped event delivery.** Nothing in the shipped cross-shard ladder needs it.
- **A complete read-modify-write detector for snapshot reducers.** The detectable common shape —
  `Find`, then `Update` of the same row, inside a body declared `Isolation.Snapshot` — is built as
  the `MELANGE0023` warning. What stays out is completeness: read-modify-write is undecidable in
  general, and a body that recomputes a row it also read is legitimate, so the diagnostic can never
  be an error and its silence is not proof of eligibility. The other open guardrails are recorded in
  [design/snapshot-isolation.md](design/snapshot-isolation.md).

## The 1.0 question, deliberately open

**What 1.0 requires is not decided, and is not being decided yet.** Pre-1.0 means the public API may
break in any release ([RELEASING.md](RELEASING.md), and the caveat at the top of
[CHANGELOG.md](../CHANGELOG.md)), and that has been cheap so far because the only consumer tracks
prereleases from `main` and absorbs breaks as they land.

0.3 is where it stops being free. That milestone adds a second public package
(`MelangeDB.Testing`), a package in a second ecosystem, and — most binding of all — a **written
client-conformance contract**, which is the kind of commitment that gets expensive to walk back once
anyone has implemented against it.

**The trigger for deciding: the reference workload reaching alpha.** An API freeze argued in the
abstract produces a list nobody can check; the same question asked by a product with players has
concrete answers about which breaks actually hurt. Revisit then, with 0.3's numbers and its
conformance document both in hand.

## Ideas that are neither planned nor refused

The deferrals above are **decided**: each one has an argument behind it, and several are permanent.
Things that have been thought about enough to have a shape but not enough to have a verdict live in
[idea-bin/](idea-bin/) instead, one file each, every entry carrying a shape, what it would cost, and
the measurement or named consumer that would turn it into a phase. An idea there is not a commitment
and not a soft refusal — it is an argument left open on purpose.

Where an idea proposes a shape for something the list above already refuses — currently
[shard-level HA](idea-bin/shard-ha-warm-replica.md) — the deferral stands until its trigger fires.
