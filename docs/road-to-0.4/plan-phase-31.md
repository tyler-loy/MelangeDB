# Phase 31: throughput, the ceiling re-measured, then shared fan-out

**Status:** step 1 measured 2026-11-22; step 2 planned, its decisions settled the same day (owner took every leaning). Phase 31 is the last
phase of 0.4 (owner, 2026-11-21). The ideas and their order are in the
[0.4 README](README.md#ordering); this plan records what step 1 found and cuts step 2 into changes.

## Step 1: the ceiling, re-measured as a game runs

Measured on 2026-11-22; full tables in
[LOAD-TESTING.md](../LOAD-TESTING.md#re-measured-as-a-game-runs-phase-31-2026-11-22). The load tool
gained `--aoi R` (each player subscribes to the chunks within R of it, as 2R+1 `BETWEEN` strips per
table, rescoped on every step) and now runs Server GC by default (`--workstation-gc` reproduces the
old runs).

- **Most of the 200-player wall was workstation GC** (in hindsight, collecting garbage the gateway
  made; see step 2's outcome below). Same shape, Server GC on: p50 call-to-delta
  went from 5.4 s to 0.10–0.13 s, and delivered rows from 90k/s to 206–221k/s. The reference
  game's host is a Web SDK project and already runs Server GC; only the tool did not. The README
  now tells console-project hosts to opt in.
- **In the game's shape the knee is between 600 and 800 players** on this one machine: 16 shards of
  about 12 players at 200, each player seeing a 5x5-chunk square. At 200 players that shape
  delivers a tenth of the rows the ladder's shape does, at a p50 of 14 ms.
- **Fan-out under the write lock is still the wall, at about half the cost.** About 2–3 µs per
  subscriber per commit under Server GC, against 5.5 µs. One shard at 750 commits/s saturates at
  about 400 subscribers instead of 200. Above roughly 180–220k delivered rows a second, more
  players become queueing.
- **The driver on a second machine was not done.** There is one machine. Every number is a lower
  bound, and the bound is tighter at high player counts, where the driver is also busy.
- **Rescope through the gateway works**, which this step had to check before relying on it: a
  rescope is a `Subscribe` frame with an existing id, and the gateway replaces the frame it replays
  at each shard swap.

## Step 2: shared fan-out, still under the lock

Identical subscriptions are one query. Evaluate the group once per op, build its row ops once,
encode them once, and give each member the shared bytes under its own subscription id. The
consistency guarantee stays exactly as it is: deltas are still computed under the write lock,
against the pre-image, in LSN order.

### What a subscriber costs today, per commit, under the lock

Read from `SubscriptionEngine.Fanout` and `MelangeSocketConnection.EnqueueDelta`:

- the predicate and policy verdicts (`Matches`, `PolicyAdmits`, `VisibleColumns`): per subscriber;
- a `WireRowOp`, a `List<WireRowOp>`, two dictionary entries, a `SubscriptionUpdate`, and a
  `TransactionUpdateFrame`: allocated per subscriber;
- `Measure(frame)`, which walks the frame to size it for the backpressure ledger: per subscriber;
- an enqueue and a `SemaphoreSlim.Release` on the connection's send signal, which can wake a
  waiting send loop from inside the lock: per subscriber.

Then, outside the lock, every send loop serializes its own copy of the same rows.

The row bytes and decoded columns are already shared (`WireRowMemo`, `DecodedRow`). What is not
known is how the 2–3 µs divides among the four items above. That decides which of them step 2
builds first, so it is measured before anything is built.

### Changes

- **2a. Apportion the cost.** An in-process benchmark of `Fanout`: one table, N subscribers on
  counting sinks, a fixed commit stream, timed per commit. Then the same with each item stubbed
  out in turn. No product change; the numbers go in this plan. *Measured 2026-11-22, below.*
- **2b. Subscription groups.** At registration, a subscription with no row policy and static wire
  columns joins the group keyed by (table, predicate kind, column, operand values, projection).
  The fan-out evaluates each group once per op and hands the resulting op list to every member.
  Subscriptions with a row or per-row column policy keep the per-subscriber path. A rescope moves
  a subscription between groups under the lock it already holds.
- **2c. Encode once.** A group's ops for one commit are serialized once into a shared segment.
  Each member's frame is a short header (LSN, subscription id) plus that segment, written by the
  send loop. Its length is known without walking it, so the backpressure ledger is still debited
  under the lock, where the drop-and-resync path needs it. **The bytes on the wire stay identical** to
  what the per-subscriber path writes; the conformance corpus and the live suite check that.
- **2d. Wake-ups.** 2a shows the send signal matters most (below): release it once per connection per commit
  instead of per frame, or after the lock drops.
- **2e. Re-measure** the sweep and the ladder as in step 1, interleaved against the step 1 build.

### 2a: where a subscriber's share goes

`FanoutCostMeasurementTests` (Transport tests): 200 subscribers on one table, 2,000 in-place
updates per run, each layer run five times interleaved with the others, median kept. Two runs of
the whole test agreed within about 20 ns. Per subscriber per commit, on the load test's machine:

| Layer, added in turn | Whole table (`SELECT *`) | Equality on an indexed column |
| --- | --- | --- |
| Evaluate: verdicts, op list, frame | 103–108 ns | 181–186 ns |
| + `Measure` | 146–148 ns | 181–203 ns |
| + enqueue and send-signal release, a send loop waiting | **558–567 ns** | **564–572 ns** |
| Delta-rows counter with a listener attached | +3–4 ns | none measurable |
| Allocated | 865 B | 869 B |

Off the lock, each send loop then spends 324–466 ns serializing its copy of the frame.

- **Waking the send loops is about 70% of what a subscriber costs under the lock.** Releasing a
  `SemaphoreSlim` that an async send loop waits on queues that loop's continuation to the thread
  pool, from inside the write lock, once per subscriber per commit.
- **Each subscriber allocates about 865 bytes per commit.** At the sweep's 200 subscribers and 750
  commits a second that is about 130 MB/s of garbage from the fan-out alone, which fits Server GC
  having halved the cost in step 1.
- **The isolated total, about 0.56 µs, is a quarter of the 2–3 µs the load test measured.** The
  rest is what the load test adds: 200 connections' state out of cache, the woken send loops and
  the GC competing for the same cores. The benchmark ranks the parts; it does not reproduce the
  absolute cost.
- **Telemetry is not a cost.** The per-subscriber counter is a few nanoseconds even when listened to.

### 2d: built, and a small win in the load test

The fan-out now queues each commit's frames quietly (`IDeltaSink.EnqueueDeltaQuietly`) and wakes
every connection it reached with one thread-pool item (`IDeltaSink.Wake`). In the benchmark,
a subscriber's share under the lock fell from 575 to 369 ns (whole table) and 632 to 421 ns
(equality); the rest of the saving is eaten by the woken loops competing with the next commit.

In the load test (one shard, 50 movers, Server GC, arms interleaved before and after, 200 and 400
subscribers twice each), the post-commit share of locked time fell in all four pairs, by 3–4
points: 74.3–74.4% to 70.1–70.8% at 200, 84.8–85.2% to 80.9–82.7% at 400. That is about 6% of
post-commit time. Mean lock hold moved within run-to-run noise (775–833 µs at 200 either way),
and 400 subscribers still queue into seconds. As 2a said, the isolated benchmark ranks the parts
but does not reproduce the in-place cost, which is dominated by what scale adds. 2b and 2c go at
that directly: they remove the per-subscriber allocation and evaluation.

**So the build order is 2d, then 2b and 2c.** Moving the wake-ups out of the lock is the largest
single item and does not touch what is evaluated or sent. Groups and encode-once then remove the
allocation, `Measure`, and the evaluation.

### Decisions (settled 2026-11-22)

The owner took all four leanings as written.

1. **Wire format.** Leaning: no protocol change. 2c splices a header onto shared bytes and must be
   byte-identical to today's frames. A frame that names a group instead of a subscription would
   save more, and would be a protocol version and a client change in both runtimes, for a gain
   2a has not shown is needed.
2. **What may be grouped.** Leaning: only subscriptions with no row policy and static wire
   columns. Per-row policies are per subscriber by definition. Grouping them by policy identity
   is possible but is the kind of sharing where a mistake leaks a row to the wrong player.
3. **A startup signal for workstation GC.** The hosting guidance is in the README now. The engine
   could also log a warning at startup when the transport runs under workstation GC on a machine
   with many cores, since that is the single largest throughput effect step 1 found. Leaning: yes,
   one warning with an EventId, no setting.
4. **Where step 2 stops.** Leaning: build 2a–2c, then decide on conflation (step 3) from the
   re-measure. Conflation changes what a client sees (fewer, coalesced updates), so it is a
   protocol choice for the owner, not a tuning step.

### 2b and 2c built; then the wall turned out to be the gateway

Shared fan-out (groups and encode-once) cut the benchmark's per-subscriber share under the lock to
about 250 ns and 293 bytes, from 575 ns and 865 bytes. In the load test it took 14% off lock hold at
200 subscribers and did not move the knee: 400 still collapsed. The per-subscriber slope in place
stayed at about 2 µs, eight times the benchmark's.

The load tool gained a CPU line (process cores, GC pause share, CPU by thread kind, read from the
runtime's thread names, since attaching a profiler was not available). At 400 subscribers the serve
process used 18.7 cores and spent 25.6% of the window paused for GC. **The cluster gateway allocated
a fresh 64 KiB receive buffer for every message a node sent and decoded every row it relayed.** A GC
pause stops the thread holding the write lock too, so the gateway's garbage read as lock hold that
grew with subscribers, in phase 20's sweep and in step 1's alike.

Fixed (one buffer per session; frames relayed without decoding). The same run: 9.6 ms p50 instead
of 15 s, 1.5% GC, 6.1 cores, and every requested row delivered. A subscriber now costs the lock
about 0.5 µs per commit; one shard at 750 commits/s carries 1,600 subscribers with the lock busy
69% of the time. The 4-shard ladder's knee moved from about 150 players to about 600 (1.7 million
delivered rows a second). Full tables in
[LOAD-TESTING.md](../LOAD-TESTING.md#after-the-gateway-fix-2026-11-22).

Workstation GC, re-checked on the fixed gateway, keeps up at 200 players and still collapses at
400 (55% of the time paused), so decision 3's warning stands, with its wording updated to these
numbers.

### What step 2 leaves for the owner

- **The game's shape has a new wall that is not delivery.** At 1,200 players on 16 shards with a
  5x5 view it delivers a fifth of the rows the ladder does at the same CPU, and fails on rejected
  seam writes and resync errors. Candidates: rescopes (ten per player step, each a range diff under
  the write lock, about 6,000 a second at 1,200 players) and handoffs (10,000 in two minutes).
  Measuring which is the next step if the game's shape matters before 0.4 ships.
- **Conflation (step 3) is less pressing than it looked.** It was aimed at delivered rows; the
  whole-shard shape now delivers 1.7 million a second before its knee.

### The game's wall, measured (2026-11-22)

The owner asked for the measurement first. It ruled re-scoping out: re-scopes take 2-4% of locked
time at about 10 µs each, and no shard's write lock is more than 18% busy. With handoffs off
(`--stay-in-block`), 1,200 players pass with no rejections and deliver 585k rows a second. The
wall is handoffs, and the time goes in two places:

1. **Every saga round trip slows from about 45 ms to about 160 ms** at 1,200 players, waiting in
   a thread pool that holds thousands of queued items while the process uses 12 of 32 cores.
2. **The gateway's swap has a tail of tens of seconds:** it waits for a destination session when
   the pre-opened one is missing, and for the connection's attachment lock, which
   `EnsureShardUpstreamAsync` holds across a connect.

The client-visible failures follow from the long holds: replayed bursts trip the per-player rate
limit, players walk past the band (refused as a span violation, reported as `internal`), and the
held-call cap answers `internal` though it means "retry". Full tables in
[LOAD-TESTING.md](../LOAD-TESTING.md#where-the-games-shape-breaks-phase-31-2026-11-22).
The fix is not one contained change. Candidates, for the owner:

- **Small and clearly right:** answer the held-call cap with `transient`, not `internal`; and treat
  an owned entity's write past the band as a transient rejection (its handoff is behind) rather
  than a span violation.
- **The gateway:** never connect while holding the attachment lock, and make the swap not depend
  on a per-player connection succeeding in time (pre-open reliably, or share an upstream per node).
- **Replay:** held calls should not reach the rate limiter as a burst the player never sent.
- **The saga's waits:** answer handoff requests off the shared pool, as phase 30 did for the
  liveness ping, then re-measure to see how much of the pool backlog is the one-process harness.

### Fixes 1 and 2 built (2026-11-22)

The owner took the recommendation: fix 1 (transient codes) and fix 2 (the gateway), then
re-measure. Fix 2 found two more gateway bugs on the way (a mid-transfer attach that bypassed the
swap, and pre-opened sessions never closed). At 1,200 players rejections fell from 44,721 to 11,487
and resync errors from 2 to 1; at 1,600, rejections fell tenfold. Results and what remains in
[LOAD-TESTING.md](../LOAD-TESTING.md#where-the-games-shape-breaks-phase-31-2026-11-22).
The next levers are fix 3 (a client's frames should not wait behind one slow subscription) and an
upstream shared per node instead of one per player.

### Next: one upstream link per gateway and node (decided 2026-11-22)

The owner chose to remove per-player upstream sockets rather than tune around them. After the
gateway fixes, each handoff still closed one per-player socket and opened another, about 90 a
second at 1,200 players, and each closed socket held a local port for two minutes (TIME_WAIT):
11,000-14,000 ports against Windows' 16,384, so connects at swap time still ran to seconds. A real
gateway host has the same limit per destination node.

**Design.** A gateway keeps one websocket per shard node, at a new node endpoint
`{Transport:Path}/mux`, and carries every player's session over it as a numbered stream:

- **Each end sees a `WebSocket`.** A session is a virtual `WebSocket` on both sides, so the gateway's
  `UpstreamSession` and the node's `MelangeSocketConnection` run unchanged on top of it, Hello and
  the hub-minted assertion included. Authentication is exactly what it is today: the link carries
  sessions, and each session proves its identity as before.
- **Envelopes.** One binary websocket message per envelope: a kind (open, data, close, abort,
  credit) and a session id, then the payload. A session's messages are cut into chunks of at most
  16 KiB, so one player's large initial set does not hold the link for the others.
- **Flow control per session.** Each direction of each session has a 256 KiB window of unacknowledged
  bytes; the receiver grants credit as its reader consumes. A slow client therefore backs up only
  its own session: the node's send loop for that player waits on credit, its queue grows, and the
  existing slow-consumer policy (`Subscriptions:MaxBufferedBytes`, `overflow_resync`) applies, as
  it did when TCP backpressure did the same job per socket. No one player can stall the link.
- **Failure.** If the link drops, every session on it reads as a dropped connection, exactly what
  the node's death looks like today; the gateway reconnects the link on next use.
- **Fallback.** `Cluster:GatewayUpstream` = `Shared` (default) or `PerSession` (the old behavior),
  so a deployment can step back without a release.

**Steps.** (A) the link and the virtual socket, with unit tests over real websockets; (B) the node
endpoint, the gateway's link pool and the option, with the cluster tests run both ways; (C)
re-measure the game shape and update the docs.

## Steps 3 to 5

Unchanged from the [README](README.md#ordering): conflation for position-like tables, read-side
fan-out only if step 2 is not enough, and binary node-link frames. Step 1 did not reorder them. The
node link carried every handoff and border batch in these runs (up to 8,227 handoffs in two
minutes) without showing up as a limit, so binary frames stay last.
