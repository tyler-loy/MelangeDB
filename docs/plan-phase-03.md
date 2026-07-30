# Phase 03 — Transport, subscriptions, and the C# client

**Goal:** a real client connects over a websocket, calls reducers, subscribes to a query, and receives live
row deltas as transactions commit.

**Depends on:** [01](plan-phase-01.md), [02](plan-phase-02.md).

## Why here

This is where MelangeDB stops being a document. It is also the last phase of the shortest path to something
demonstrable, so it should be reached before anything in 04–08 is started.

## Deliverables

**Wire format**
- `IMelangeSerializer` with a MessagePack implementation. MessagePack first because implementations exist in
  every client language we'll eventually target; a source-generated binary format can replace it behind the
  interface once there's something to measure.
- Framed binary protocol: `CallReducer`, `Subscribe`, `Unsubscribe`, `SubscriptionApplied` (initial set),
  `TransactionUpdate` (deltas), `ReducerResult`, `Error`. Versioned handshake.

**`MelangeDB.Server`**
- `MapMelangeSocket(path)` on `IEndpointRouteBuilder` — an endpoint in the developer's own ASP.NET Core app,
  not a separate listener.
- **Subscription engine.** Parse and validate a query against the schema; compute the initial result set;
  register the predicate; on each committed transaction, test the write set's row ops and emit per-client
  deltas. Supported shapes, which cover the audited reference workload completely:
  - `SELECT * FROM t` — whole table
  - `SELECT * FROM t WHERE col = :p` — equality on an indexed column
  - `SELECT * FROM t WHERE col BETWEEN :lo AND :hi` — range; this is how spatial data streams
  - `SELECT a, b, c FROM t WHERE ...` — **column projection**, so deltas carry partial rows
- Subscriptions may only name `Public` tables; naming a private table is a clean error, never a silent empty
  result.
- A `SubscriptionApplied` set and the delta stream must be consistent at a single LSN — the client must not
  be able to observe a gap or a duplicate across that boundary.

**`MelangeDB.Client`**
- Connect, authenticate (stubbed until 04), call reducers, await results.
- Locally maintained row cache per subscription with `OnInsert` / `OnUpdate` / `OnDelete` events.
- Reconnect with subscription re-establishment.

## Out of scope

Joins — explicitly deferred, and the audit of a live 82-table SpacetimeDB game found **zero** subscriptions
using one. Auth (04). Generated typed client bindings — hand-written or dynamic access is fine here; codegen
for clients follows once the wire format has settled.

## Decisions to settle

- ~~**Query representation.**~~ **Settled: a SQL subset**, chosen for portability — a typed builder would be
  C#-only and would block the eventual TypeScript client. Cost accepted: we own a parser and must define the
  subset precisely enough that "valid MelangeDB SQL" is unambiguous.
- **Delta granularity for projections.** Does a projected subscription emit an update when a *non-projected*
  column changes? It must not — that's wasted bandwidth on the hottest path — but it requires per-
  subscription column masking in the delta computation.
- **Backpressure.** A client on a slow link during bulk terrain streaming will fall behind. Buffer, drop and
  resync, or disconnect? Needs an answer here, because it shapes the protocol.
- **Fan-out cost.** Naively testing every predicate against every row op is O(subscriptions × ops). Indexing
  subscriptions by table and key range is the fix; know whether phase 03 needs it or can defer it.

## Done when

- The sample worker serves a websocket; a `MelangeDB.Client` console app connects, calls a reducer, and sees
  the resulting row change arrive as a delta.
- All four query shapes above are covered by tests, including projection emitting partial rows.
- A range subscription re-scoped as a simulated player "moves" (changing `:lo`/`:hi`) correctly emits inserts
  for newly-visible rows and deletes for newly-invisible ones — the terrain-streaming pattern.
- Subscribing to a private table returns an explicit error.
- Killing and reconnecting a client restores its subscriptions and converges to correct state.
- A test asserts no gap and no duplicate across the initial-set/delta boundary under concurrent writes.

## Risks

- **The initial-set race is the classic bug here.** Computing a snapshot while transactions commit will
  produce a missed or doubled row unless the snapshot and the delta stream are anchored to the same LSN.
  Write that test first.
- **Terrain-scale initial sets.** A range subscription over chunk blobs can be tens of megabytes. Chunked
  delivery may be needed sooner than expected; note it and measure before designing for it.
