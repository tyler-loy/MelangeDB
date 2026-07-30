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
  `TransactionUpdate` (deltas), `ReducerResult`, `Error`, `Ping`/`Pong`, `Resume`, `Reauthenticate`.
  Versioned handshake. `CallReducer` carries a `traceparent` (see [OBSERVABILITY.md](OBSERVABILITY.md)).
- `Reauthenticate` exists in this phase even though phase 04 owns its *semantics*, because a frame type cannot
  be retrofitted without a protocol version bump. A game session outlives a one-hour JWT; dropping the
  connection at expiry is unacceptable and ignoring expiry means revocation never takes effect, so in-band
  re-auth has to be designed in from the start.
- **TLS and HTTP version are the host's concern** — `wss://`, HTTP/2, and HTTP/3 all come from the developer's
  Kestrel listener configuration (`HttpProtocols.Http1AndHttp2AndHttp3`, on by default since .NET 8), not from
  MelangeDB. Because `MapMelangeSocket` is an endpoint in the host's app rather than a listener we own, protocol
  negotiation is something the library structurally doesn't have to solve. There is deliberately **no**
  MelangeDB setting for protocol version or certificates.
- **The endpoint must accept `CONNECT`, not only `GET`.** WebSockets over HTTP/2 (RFC 8441, supported in Kestrel
  since .NET 7 with automatic negotiation) use a `CONNECT` request, and the ASP.NET Core docs warn this "may
  require updates to existing routes and controllers." A `GET`-only mapping means HTTP/2 WebSockets **silently
  fail to negotiate** and fall back to HTTP/1.1 — losing header compression and multiplexing with no error to
  explain why. This is the one HTTP-version concern that is genuinely ours.
- **Compression** via `permessage-deflate`, configurable. Terrain blobs are already RLE-compressed, but delta
  frames carrying many small rows compress well.
- **Heartbeat.** `Ping`/`Pong` with a timeout, because phase 05's `ClientDisconnected` must fire on ungraceful
  drops and a closed socket is not the only way a client goes away.

**HTTP endpoints.** WebSocket is the wrong shape for two of the three client types in the reference project: the
admin console runs **one-shot SQL over HTTP**, and terrain-gen **bulk-loads ~24.6k chunk rows**. Neither wants a
subscription protocol.
- `POST /melange/call/{reducer}` — one-shot reducer invocation for tooling and CLIs.
- `POST /melange/bulk` — the bulk ingestion path (one large write set, not one transaction per row).
- `POST /melange/sql` — ad-hoc query endpoint; the aggregate-capable implementation lands in phase 08.
- `POST /melange/ticket` — mints a short-lived connect ticket (phase 04).

**Resume, not refetch.** A reconnecting client sends `Resume` with its last-acked LSN and receives the deltas it
missed. Recomputing a full initial set on every network blip means tens of megabytes of terrain for a two-second
outage. Requirements: the client tracks its acked LSN, the server retains enough log to serve the gap, and there
is an explicit fallback to full resync when a client is too far behind (or the log has been truncated past its
position). Getting this wrong is silent state divergence, so the fallback must be detected by the server rather
than assumed by the client.

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

**Subscription cost limits.** Nothing otherwise stops an authenticated client — including a guest — from
subscribing to `SELECT * FROM terrain_chunk_data` with no predicate and pulling ~24.6k compressed terrain blobs,
the entire world, in one request. `MaxPerConnection` caps how *many* subscriptions exist, not what one costs.
This is a denial-of-service surface, so it belongs in the phase that ships subscriptions:
- **Mandatory-predicate tables** — a table may require that a subscription constrain a given column.
- **Bounded range width** — a maximum span on `BETWEEN`, so a client streams a ring around itself, not the map.
- **Row and byte ceilings per subscription**, with a clear error instead of an OOM.
- **Cost estimated and rejected before execution** — by the time you're streaming, the damage is done.
- A `SubscriptionApplied` set and the delta stream must be consistent at a single LSN — the client must not
  be able to observe a gap or a duplicate across that boundary.

**`MelangeDB.Client`**
- Connect, authenticate (stubbed until 04), call reducers, await results.
- Locally maintained row cache per subscription with `OnInsert` / `OnUpdate` / `OnDelete` events.
- Reconnect with `Resume` from the last acked LSN, falling back to full re-establishment when the server says
  the gap can't be served.

## Out of scope

Joins — explicitly deferred, and the audit of a live 82-table SpacetimeDB game found **zero** subscriptions
using one. Auth semantics (04) — this phase ships the frames, not the validation. Generated typed client
bindings — hand-written or dynamic access is fine here; codegen for clients follows once the wire format has
settled.

**Unreliable/UDP transport, permanently.** Games often want fire-and-forget position updates, but a reducer is a
transaction: it either commits or it doesn't, and a client needs to know which. An unreliable path is
incompatible with that contract. Where per-tick position writes are too expensive, the answer is rate limiting
plus client-side interpolation — which the reference workload already does, storing a *path* rather than a point
on `Creature` and `PlayerState` so the server writes only on decisions. That pattern is the supported solution,
not an unreliable channel.

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
- **Head-of-line blocking — the one that shows up as bad game feel.** A single socket carrying a 30MB terrain
  initial set *and* movement reducer responses lets terrain block movement. Chunk-and-interleave by priority is
  the simplest fix and is probably sufficient. But see the channel-tag constraint below: whichever fix ships,
  the *protocol* must not foreclose the better substrates.
- **How much log to retain for `Resume`.** Too little and every reconnect degrades to a full resync; too much
  and retention fights phase 07's compaction. Probably a time window rather than a transaction count, since what
  matters is surviving a plausible network outage.

### Don't assume one totally-ordered byte stream

The cheap-now, expensive-later decision in this phase. **Frames should carry a channel tag, with ordering
guaranteed only *within* a channel** rather than globally across the connection.

Cost now: a tag field and a slightly more careful client. What it buys is that every better substrate becomes an
implementation detail instead of a protocol break:

- **One socket, chunked and interleaved** — what ships in this phase.
- **Several sockets over HTTP/2** — available today. Multiple WebSocket connections multiplex onto a single TCP
  connection, so a second socket is cheap rather than wasteful. "Bulk terrain on one channel, interactive on
  another" becomes practical, which is a real answer to head-of-line blocking and not a future one.
- **WebTransport streams** — the eventual principled answer, mapping channels onto genuine QUIC streams. Status
  unconfirmed: it does not appear in mainline ASP.NET Core documentation and seems still experimental, so
  **nothing here plans around it** — the point is only that the protocol shouldn't rule it out.

Design the protocol assuming global frame ordering and adding multiplexing later is a breaking change for every
client. Add the tag now and it never is. Note also that QUIC requires UDP, which plenty of corporate networks
block, so HTTP/3 must always be able to degrade to HTTP/2 — automatic with Kestrel negotiation, but a client
must never assume it got the transport it asked for.

## Done when

- The sample worker serves a websocket; a `MelangeDB.Client` console app connects, calls a reducer, and sees
  the resulting row change arrive as a delta.
- All four query shapes above are covered by tests, including projection emitting partial rows.
- A range subscription re-scoped as a simulated player "moves" (changing `:lo`/`:hi`) correctly emits inserts
  for newly-visible rows and deletes for newly-invisible ones — the terrain-streaming pattern.
- Subscribing to a private table returns an explicit error.
- An unbounded subscription to a mandatory-predicate table is rejected before any rows are read.
- A range subscription exceeding the maximum span is rejected with an actionable error naming the limit.
- Killing and reconnecting a client restores its subscriptions and converges to correct state.
- A test asserts no gap and no duplicate across the initial-set/delta boundary under concurrent writes.
- A client disconnected for a few seconds during active writes reconnects via `Resume` and converges **without**
  refetching its initial set — asserted by measuring bytes transferred, since the whole point is the saving.
- A client disconnected past the retention window is told to full-resync rather than silently diverging.
- A large initial set does not delay a concurrent reducer response beyond a stated bound.
- One-shot HTTP reducer invocation and bulk ingestion work without opening a websocket.
- A client connects over **HTTP/2** (via `CONNECT`) as well as HTTP/1.1, with the negotiated version asserted —
  not merely "it still worked," since silent fallback is precisely the failure mode.
- Every frame carries a channel tag, and a test asserts ordering is preserved within a channel while frames on
  different channels may interleave.
- A dropped connection (killed process, no close frame) is detected by heartbeat within the configured timeout.

## Risks

- **The initial-set race is the classic bug here.** Computing a snapshot while transactions commit will
  produce a missed or doubled row unless the snapshot and the delta stream are anchored to the same LSN.
  Write that test first.
- **Terrain-scale initial sets.** A range subscription over chunk blobs can be tens of megabytes. Chunked
  delivery may be needed sooner than expected; note it and measure before designing for it.
