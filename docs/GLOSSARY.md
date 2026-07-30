# Glossary

The nouns MelangeDB uses, and what each one means here specifically.

> **The rule, as with [CONFIGURATION.md](CONFIGURATION.md) and [OBSERVABILITY.md](OBSERVABILITY.md):** when a
> phase introduces a concept, its noun is defined here in the same change. Vocabulary drift is how a design
> becomes unexplainable — and it already happened once, with "region" surviving in two documents after the
> concept was generalized to "shard."

## Commonly confused

These sets exist because each member sounds like the others and means something different. If you only read one
section, read this one.

### Where data lives — three independent axes

Conflating any two of these is a design error. A table declares all three.

| | Question it answers | Values |
| --- | --- | --- |
| **Tier** | *Which storage engine holds it?* | `Hot` (default) or `Relational` (Postgres) |
| **Placement** | *Which node in a cluster holds it?* | `Partitioned`, `Replicated`, `Global`, `Local` |
| **Residency** | *Must it stay wholly in memory?* | `Resident`, `Paged` (default), `Auto` |

A table can be `Hot` + `Partitioned` + `Paged` (terrain), or `Relational` + `Global` + `Paged` (accounts), or
`Hot` + `Replicated` + `Resident` (item definitions). They don't imply each other.

### Exposure — three levels, not two

| | Meaning |
| --- | --- |
| **Private table** | `Public = false`. Server-internal. No subscription may name it; no policy can reveal it. |
| **Public table** | Syncable to clients, *subject to* row and column policies. Public is permission to be filtered, not permission to be seen. |
| **`[ServerOnly]` column** | A column on a public table that never leaves the process. Compile-time, no per-row cost. |

### Three kinds of policy

| | Decides | Composition |
| --- | --- | --- |
| **Row policy** | Which *rows* a caller sees | **Union** — any policy admitting a row is enough |
| **Column policy** | Which *columns* a caller sees | **Intersection** — every rule must admit the column |
| **Reducer policy** | Whether a caller may *call* a reducer | Single decision |

Rows union, columns intersect. Getting this backwards produces either a leak or an unusable system.

### Delta vs. domain event

Both flow out of the commit log; they are not the same thing.

- A **delta** is a row change sent to a *client* because it subscribed to a query.
- A **domain event** is an application-level fact (`PlayerDied`) published to *server-side handlers*.

### Identity vs. ConnectionId vs. session

- **Identity** — *who*. Stable across reconnects and restarts.
- **ConnectionId** — *which socket*. One identity may hold several at once.
- **Session** — a client's live attachment. Distinct from "someone ran an admin query," which must not fire
  lifecycle reducers.

### Applier vs. projection

- A **projection** is a *derived copy* of state — the hot store, the Postgres tier, a client's row cache.
- An **applier** is the *component* that advances a projection by consuming the log.

Appliers are the verb; projections are the noun.

### Subscription vs. ad-hoc SQL

- A **subscription** is a standing query that streams deltas as things change. Single-table, no joins, no
  aggregates.
- **Ad-hoc SQL** is one-shot, supports aggregates and joins, and does not stream. For tooling.

## Terms deliberately retired

Recorded so they don't creep back.

| Term | Status |
| --- | --- |
| **Module** | SpacetimeDB's deployable unit. **MelangeDB has none** — your host app *is* the module, so the word has no referent and using it implies a boundary that doesn't exist. |
| **Region** | Superseded by **shard**. "Region" implied spatial partitioning was the only model; it is one strategy among several. |
| **Master / daughter node** | The original framing for what are now **hub** and **shard node**. Kept here only as a synonym, since it's a useful mental image. |
| **Cache** | Residency is **not** caching. A cache is best-effort and unbounded in effect; a `Resident` table is a declared, bounded commitment. Calling it a cache invites the RAM ceiling back. |
| **Replica** | Ambiguous between `Replicated` placement and future Raft replication. Say which. |

## Definitions

**Ad-hoc SQL** — A one-shot query, supporting aggregates, run over a tier for tooling purposes. Has two modes:
policy-enforced and owner. Not a subscription.

**Applier** — A component consuming the commit log to advance one projection, holding its own LSN checkpoint so
it may lag independently and resume where it stopped.

**AutoInc** — A column whose value is assigned from a durable per-table sequence, allocated into the write set
*before* the log append so replay never reassigns different ids. The contract is **unique, not dense** — gaps
are normal, which is what lets each shard allocate from an originator-prefixed range with no coordination.
Ids are 64-bit but allocated within 63 (sign bit clear: 16-bit originator, 47-bit sequence), so a value
round-trips through Postgres `bigint` and signed-only client languages unchanged.

**Border band** — In the spatial strategy, the ring of chunks a shard node holds read-only copies of so it can
serve entities just beyond its own boundary. Derived from `InterestOf`.

**Bulk ingestion** — A path appending one large write set instead of one transaction per row, for world
generation and similar mass loads.

**Cold world** — The overwhelming majority of a world's data that no player is near, whose size grows with area
(the N² term). Addressed by paging, not sharding.

**Column mask** — The set of columns visible to a particular caller for a particular row.

**Commit log** — The ordered, append-only, LSN-addressed record of committed transactions. **The system of
record.** Every store is a projection of it. One log per shard.

**Channel** — A logical stream within a connection. Frames carry a channel tag and ordering is guaranteed only
*within* a channel, so bulk transfer can't head-of-line block interactive traffic. Deliberately independent of how
channels are carried — interleaved on one socket, several sockets over HTTP/2, or QUIC streams later.

**Connect ticket** — A single-use, short-lived credential exchanged for a JWT over HTTP and presented when
opening a socket. Exists because the browser WebSocket API cannot set headers, so header-based auth would lock
out web clients entirely.

**Commit point** — The single atomic log append. Before it nothing happened; after it the transaction is
durable. This is what buys atomicity across heterogeneous stores with no 2PC.

**ConnectionId** — Identifies one client socket. Distinct from Identity.

**Delta** — A row insert, update, or delete sent to a subscribed client. May carry a partial row when the
subscription projects columns or a policy masks them.

**Domain event** — An application-level fact published from a reducer via `ctx.Publish`, delivered to
DI-resolved handlers *after* the commit point.

**Event bus** — The delivery mechanism for domain events, implemented as a transactional outbox over the commit
log. A projection, not a second source of truth.

**Fencing token** — A guard ensuring a node wrongly suspected of being dead cannot keep writing rows it no
longer owns.

**Global** — Placement: the table lives on the hub only. In practice the relational tier.

**Guest identity** — An ordinary identity whose token the IdP issued with a guest role claim. MelangeDB mints
nothing — **the IdP is the gate** for guests as for everyone — so "converting" a guest is IdP-side account
linking rather than anything MelangeDB does. Preserve the issuer and subject and the `Identity` never changes.

**Handoff** — Transferring a player's ownership from one shard node to another. Explicit and discrete under
instancing; continuous and implicit under spatial partitioning. The one unavoidable distributed transaction.

**Hot store / hot tier** — The in-process log-structured store holding world state. "Hot" describes *access
pattern*, not volatility: it is durable, and it is not a cache.

**Hub** — The node holding `Global` and `Replicated` tables, identity, the relational tier, and shard
assignment. Every client holds a permanent hub attachment. Originally "master node."

**Identity** — The stable identifier for who is acting: a hash of a token's **issuer and subject**. Issuer
included so subjects from two token sources can never collide into one identity.

**Instance** — Under the instancing strategy, a shard identified by an explicit id column. Instances are
causally disjoint — no interest overlap between them.

**Interest** — The set of foreign shards a node holds read-only slices of, returned by `InterestOf`.

**Local** — Placement: the table lives on one node and never leaves it. Caches, scratch state, telemetry.

**LSN** — Log sequence number. Monotonic within a shard's log. There is **no** cluster-wide ordering.

**Overlay** — The read path inside a transaction: the uncommitted write set layered over the store, which is
what makes read-your-writes work with no I/O in a reducer body.

**Paged** — Residency: the table may spill to disk, so memory is bounded by working set. The default.

**Partitioned** — Placement: rows are split across shard nodes by shard key. **One writer per shard, many
readers** — other nodes may hold read-only slices.

**Placement** — Which node in a cluster holds a table. Ignored entirely by single-node deployments.

**Policy** — A DI-resolved object deciding an access question. See the three kinds above. Because policies run
in-process they may read private tables — the advantage over SQL-string filters.

**Projection** — A derived copy of state, rebuilt by replaying the log. The hot store, the Postgres tier, and a
client's row cache are all projections.

**Reducer** — A method invoked as a single transaction against the database, with dependencies injected from DI.
The term is inherited from SpacetimeDB; **it has nothing to do with a fold or `Array.reduce`.** Reducers are
synchronous and perform no I/O.

**ReducerContext (`ctx`)** — The ambient state a reducer is given so it can stay deterministic and replayable:
`Caller`, `ConnectionId`, `Timestamp`, `Random`, `Db`, and `Publish`. Reaching for `DateTime.Now` or
`new Random()` instead is a bug the analyzer reports.

**Relational tier** — Opt-in Postgres storage for tables declaring `Tier = Relational`. The "servicey" half:
accounts, registration, statistics. Eventually consistent with the log by design.

**Replicated** — Placement: a full copy on every node, written only by the hub. Small bounded reference data.

**Resident** — Residency: the table is pinned wholly in memory. Opt-in, because a resident-by-default store
reproduces the RAM ceiling MelangeDB exists to remove.

**Log epoch** — The identifier naming one commit log incarnation, carried in `Resume` alongside the LSN so a
cursor can never be applied against the wrong log. A client holds one cursor per attachment; a stale or unknown
epoch is an explicit failure answered with full resync.

**Resume** — Reconnecting by naming the log epoch and last LSN a client acknowledged, per attachment, and
receiving only the deltas it missed rather than recomputing a full initial set. Falls back to full resync when
the gap can no longer be served — a decision the *server* makes, since a client assuming it can resume would
silently diverge.

**Residency** — Whether a table must stay in memory. Declared, so the memory budget is computable from source
rather than discovered under load.

**Saga** — A multi-step, eventually-consistent operation with compensating actions, used for handoff and the
rare cross-shard case. Explicitly not ACID.

**Scheduled reducer** — A reducer fired by a timer row rather than by a client. Not client-callable.

**`[ServerOnly]`** — A column attribute: never sent to any client, admin included. Compile-time and free.

**Shard** — An independently writable slice of the world, owned by exactly one node, with its own commit log.
Internally single-writer, so the reducer model is unchanged; "multi-writer" is a property of the *cluster*.

**Shard key** — The value determining which shard owns a row, derived by the shard strategy. **The contract:**
rows mutated in one transaction must resolve to the same shard key.

**Shard node** — A node owning one or more shards and running their `Partitioned` tables and scheduled
reducers. Originally "daughter node."

**Shard strategy (`IShardStrategy`)** — The developer-supplied definition of what a shard *means*. MelangeDB
supplies the mechanism; spatial partitioning and instancing are both first-class, and they compose.

**Shard-span check** — A runtime guard failing loudly when a transaction's write set spans shard keys, since
that contract cannot be verified statically.

**Snapshot** — A materialized state capture at an LSN, allowing the log behind it to be truncated — never past
the slowest applier or event subscriber.

**Subscription** — A standing single-table query producing an initial result set and then a delta stream.
Anchored to one LSN across that boundary so a client observes no gap or duplicate.

**Table** — A `partial struct` with `[Table]`, declaring `Tier`, `Placement`, `Residency`, and visibility. Value
types keep allocation off the reducer hot path.

**Tier** — Which storage engine holds a table: `Hot` or `Relational`.

**Timer row** — A row in a table declaring `Scheduled`, carrying a `ScheduleAt`. Timers are **data, not code**,
which is what makes scheduling transactional, recoverable, and partitionable — an inline `[Cron(...)]` attribute
could be none of those.

**Transaction** — One reducer invocation. Reads through the overlay, accumulates a write set, and commits by a
single log append. Returns to commit, throws to abort with nothing appended.

**Transactional outbox** — The pattern making the event bus safe: events go into the write set and publish only
after the commit point, so an event can never escape for a rolled-back transaction.

**Write set** — The ordered row operations a transaction produced, keyed by table and primary key. **The
authoritative payload of a log record** — logging the write set rather than the reducer invocation is what lets
projections rebuild without re-executing user code.
