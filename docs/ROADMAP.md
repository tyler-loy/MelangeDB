# Roadmap

Where MelangeDB is, how it got here, and what's left.

Work was sequenced as twelve phases. Eleven have shipped; one is outstanding. Each phase's full
plan — deliverables, what was explicitly out of scope, and the reasoning behind every decision it
settled — lives in [road-to-0.1/](road-to-0.1/). Those are the authoritative record of *why* things
are the way they are; this page is the summary.

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
| [11](road-to-0.1/plan-phase-11.md) | Reference workload port and validation | **Outstanding.** The port of a live 82-table SpacetimeDB game, and the only thing that will turn "tested" into "proven." |

Phase 12 was numbered after 11 but landed first: the port's scoping pass
([#20](https://github.com/tyler-loy/MelangeDB/issues/20)) measured 459 client call sites that are
mechanical against typed bindings and a rewrite without them.

## What's left

**Phase 11 is the whole remaining bar.** Everything else is implemented and tested; nothing has been
proven against a production workload. Until that port lands, treat recorded benchmarks as
measurements on a dev machine rather than as production characteristics.

Known deferrals, each recorded with its reasoning rather than left as an omission:

- **Joins in subscriptions.** Incrementally maintaining them is differential-dataflow territory; an
  audit of a real 82-table game found **zero** subscriptions using one.
- **Unreliable/UDP transport — permanently.** A reducer is a transaction; a client must know whether
  it committed. Rate limiting plus client-side interpolation is the supported answer.
- **Schema migration against an existing log.** The relational half settled in phase 08; the hot-tier
  half — how column adds replay against the log — is open. See [DESIGN.md](DESIGN.md) §10.
- **Epoch-qualified subscription anchors.** Closed behaviourally in phase 10 by the first-chunk rule;
  the protocol-level hardening is deferred, with the record kept in [plan-phase-10.md](road-to-0.1/plan-phase-10.md).
- **Dynamic rebalancing**, shard-level HA, and sharding the hub. Static assignment will visibly
  hotspot; the ceiling is published so the choice is informed.
- **Shard-side interest-scoped event delivery.** Nothing in the shipped cross-shard ladder needs it.
- **Snapshot isolation for read-heavy reducers.** The write lock covers the whole body, so a sweep
  that reads for 200 ms and writes for 0.2 ms bills the other 199.8 ms to every writer on the engine.
  Designed but not built — it hangs on giving `IHotStore` a read view an `Apply` cannot disturb. See
  [design/snapshot-isolation.md](design/snapshot-isolation.md).
