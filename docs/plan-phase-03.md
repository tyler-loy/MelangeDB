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
  Versioned handshake. **Every frame carries a channel tag from version one** (see the ordering
  constraint below). `CallReducer` carries a `traceparent` (see [OBSERVABILITY.md](OBSERVABILITY.md)).
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

An LSN is meaningful only within one commit log, and a clustered client (phase 09) holds attachments to more
than one — hub plus shard, with the shard log changing entirely on handoff. So the resume cursor is
**per attachment**, and the `Resume` frame names the **log epoch id** it is resuming against, never a bare LSN.
A stale or unknown epoch is an explicit **failure**, answered with full resync — never a partial answer, never
guessed at by the client. This costs one field now; retrofitting it in phase 09 would be a protocol break for
every client, same argument as the channel tag.

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
- ~~**Delta granularity for projections.**~~ **Settled: a projected subscription never emits when only
  non-projected columns change.** The fan-out runs as a commit observer *before* the hot store applies, so the
  store still holds each row's pre-image; a projected update whose projected column slices are byte-identical
  between pre- and post-image is suppressed. The same pre-image is what turns an update that crosses a
  predicate boundary into the correct insert or delete. One caveat, documented in code: resume replay reads
  the log, which has no pre-images, so replayed gaps are conservative (an update that no longer matches emits
  a delete the client may no-op) — correctness holds, suppression applies only on the live path.
- ~~**Backpressure.**~~ **Settled: bounded per-connection delta buffer (`Subscriptions:MaxBufferedBytes`);
  on overflow the policy applies — `DropAndResync` (default), `Buffer`, or `Disconnect`.** `DropAndResync`
  discards the queued delta stream, forgets the connection's subscriptions server-side, and sends one small
  connection-scoped error telling the client to re-establish — bounded memory, kept connection, and the client
  converges through the same path a rejected `Resume` uses. `Buffer` (unbounded past the trigger) is an
  explicit opt-in for trusted links; `Disconnect` is the last resort. This changed the registered default in
  CONFIGURATION.md from `Buffer` to `DropAndResync`: a default that buffers without bound past the bound's own
  trigger would make the trigger meaningless. Bulk initial sets are exempt by construction — chunks are
  generated lazily as the sender drains, so they occupy no buffer. The drop is synchronous with the overflow,
  under the engine lock the fan-out already holds: the connection's registrations are gone before the error
  frame exists, so a prompt re-subscribe always takes the fresh-registration path. The deferred sweep this
  replaced raced exactly that re-subscribe (observable under CPU starvation): the stale registration made the
  re-subscribe re-scope — no initial set — and the sweep then unregistered the replacement, leaving a silently
  dead subscription.
- ~~**Fan-out cost.**~~ **Settled: indexed by table now; key-range indexing within a table deferred with a
  measurement.** A commit tests only the subscriptions registered on the tables its write set touches. Within
  one table the per-op predicate test is an encode plus a byte-compare (~tens of nanoseconds); at the reference
  workload's scale — tens of subscriptions per hot table, single-digit row ops per commit — that is thousands
  of comparisons per second against a fan-out budget of millions. The race-test suite hammers 400 commits
  against eight live whole-table subscriptions without measurable fan-out cost. Key-range interval indexing
  earns its complexity only when per-table subscription counts reach the hundreds; revisit with phase 10's
  load rig.
- ~~**Head-of-line blocking.**~~ **Settled: chunk-and-interleave by priority on one socket, shipped.** The
  sender drains lanes in order — control/results, then one committed delta, then one bulk chunk of at most
  `Transport:MaxInitialSetChunkBytes` — so a reducer response waits at most one chunk behind a 30MB initial
  set. Asserted by a wire-order test. One exception, added with phase 10's swap in hand: the **first-chunk
  rule** — the first chunk of a not-yet-started initial set outranks committed deltas, because until that
  chunk is on the wire a subscription re-issued by a gateway swap is still judged by the client against the
  previous log's anchor (the phase 10 soft-spot record has the full story). The channel tag on every frame
  keeps multi-socket HTTP/2 and QUIC streams open as substrates with no protocol change.
- ~~**How much log to retain for `Resume`.**~~ **Settled: a time window, `Resume:RetentionWindowSeconds`
  (default 300).** What matters is surviving a plausible network outage, which is measured in seconds, not
  transactions. A resume whose oldest missed record is older than the window answers full resync. Interaction
  with phase 07 noted here deliberately: compaction's log truncation must treat the retention window as a
  floor — truncating inside it silently converts every reconnect in flight into a full resync — and the
  truncated-log case must keep answering full resync explicitly, which the epoch/LSN check already does.
  Implementation note recorded for the same reason: the log epoch id lives in a `melange.epoch` sidecar
  beside the log file, minted whenever the log file itself is freshly initialized — the phase-01 header
  format needed no version bump, and pre-epoch logs are adopted under a minted epoch exactly once.

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
- A `Resume` naming a stale or unknown log epoch fails cleanly into full resync — the cross-log case
  phase 09's handoff will rely on.
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
