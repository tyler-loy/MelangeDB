# Load testing

`tools/MelangeDB.LoadTest` load-tests a spatial MelangeDB cluster with **real clients over real
sockets** — the same path the cluster acceptance tests' end-to-end client uses, at populations no
test suite runs at. It exists to answer the question the in-process hotspot ceiling
([CLUSTERING.md](CLUSTERING.md)) deliberately does not: what does the *whole* path sustain — gateway,
shard commit, border replication, handoff sagas, and above all **subscription fan-out**, where every
commit is delivered to every subscriber on the shard and the cost grows with the square of players
per shard.

## The workload

The spatial test app's shape, sized for crowds: a contiguous world of chunks, blocks of WxH chunks
per shard (`SpatialShardStrategy`), one `Terrain` row per chunk, one `PlayerPos` row per player.
Each simulated player is one websocket client through the gateway that:

- subscribes to `Terrain` and `PlayerPos` the way the seamless-walk tests do — the server scopes
  both to the player's current shard plus its border band, so **delta fan-out is part of the load**;
- issues a `Move` reducer call every tick (default 15 Hz), stepping one chunk every
  `--chunk-every` ticks (default 30 — a chunk step every 2 s, roughly the reference workload's
  sprint of 8 m/s across 64 m chunks) and re-committing in place on the other ticks;
- walks with a direction bias (roamers keep a heading for 6–16 chunk steps) so seam crossings
  actually happen — or, for the `--seam-fraction` of players that are **seam walkers**, oscillates
  across a block boundary one chunk past the hysteresis margin each way, so handoff and border
  traffic is continuously exercised rather than left to chance.

Player spawns are spread uniformly across the world (each client places its spawn shard on the hub
before its first move), so the configured population loads every shard from the start.

## Running it

The tool is a console app; `dotnet run --project tools/MelangeDB.LoadTest -- <subcommand>` or run
the built exe (`all` re-launches its own executable as the serve child, so run the apphost
`MelangeDB.LoadTest.exe`, not `dotnet MelangeDB.LoadTest.dll`). Three load subcommands, because
**the measurement is contaminated if server nodes and the load driver share a thread pool**:

| Subcommand | What it does |
| --- | --- |
| `serve` | Hosts the hub plus N shard nodes (real Kestrel hosts, real TCP node links) in this process, seeds the world's terrain, then prints `GATEWAY <uri>`, `STATS <uri>`, and `READY`. Runs until Ctrl+C (or `--serve-seconds`). |
| `drive` | Connects `--players` clients to `--address` and generates the workload for `--warmup-seconds` + `--duration-seconds`, printing a sample every `--sample-seconds` and a final summary. |
| `all` | Spawns `serve` as a **separate child process**, waits for `READY`, then drives against it. The one-command local run. |

A fourth, `failover`, is a different measurement: it kills the node that owns a crowded shard and
times the reassignment window. See [The reassignment window](#the-reassignment-window).

A fifth, `link-bench`, measures the node link alone over loopback: per message shape (a border batch
of 500 rows of 1 KiB, one of 50 rows of 64 B, a heartbeat), from one sender and from 32, in both
frame formats, it reports the round trip, bytes allocated across both ends, gen-0 collections,
payload bytes on the wire, and managed heap still held after a full collection. `--repeat` sets the
rounds. See [The node link's framing](#the-node-links-framing--json-against-binary-2026-11-24).

```
# Small and fast (under 30 s, 2 nodes, 8 players):
MelangeDB.LoadTest all --smoke

# A capacity run: 200 players, 4 shard nodes, 2x2 blocks of 8x8 chunks, interval fsync:
MelangeDB.LoadTest all --players 200 --nodes 4 --out capacity.csv

# A soak (5 min measured window, watching the server memory trend):
MelangeDB.LoadTest all --players 100 --soak --out soak.csv

# Cross-machine: serve on the server host, drive from another machine.
serverhost>  MelangeDB.LoadTest serve --listen 0.0.0.0 --port 5300 --nodes 4
driverhost>  MelangeDB.LoadTest drive --address ws://serverhost:5300/gateway --players 200
```

For cross-machine runs the driver's world flags must match the server's; the driver validates them
against the stats endpoint's geometry echo and refuses loudly on a mismatch. Shard nodes always bind
loopback — only the gateway (and stats) need to be reachable, and only the hub talks to shard nodes.

### Flags

These are the **tool's** flags, documented here rather than in [CONFIGURATION.md](CONFIGURATION.md),
which registers MelangeDB option keys. Where a flag maps onto a MelangeDB option, the mapping is
noted.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--players` | 200 | Concurrent clients, each a real websocket. |
| `--tick-hz` | 15 | Move calls per player per second. |
| `--chunk-every` | 30 | Ticks per one-chunk step; the rest re-commit in place. |
| `--seam-fraction` | 0.25 | Fraction of players that are seam walkers. |
| `--duration-seconds` | 120 | Measured window (excludes warm-up). |
| `--warmup-seconds` | 10 | Excluded from every reported statistic. |
| `--nodes` | 4 | Shard node count (shards assign least-loaded-first). |
| `--world-blocks` | 2x2 | World size in blocks; one block = one shard. |
| `--block-chunks` | 8x8 | Block size in chunks. |
| `--band` | 3 | `Cluster:BorderBandChunks`. 3 rather than the library's 2, applying the documented derivation to this workload's step speed. |
| `--margin` | 1 | `Cluster:HandoffMarginChunks`. |
| `--handoff-min-ms` | 2000 | `Cluster:HandoffMinIntervalMs`. |
| `--fsync` | interval | `CommitLog:FsyncPolicy`: `interval` or `commit`. Interval is the capacity default; per-commit measures the disk's fsync ceiling instead. |
| `--fsync-interval-ms` | 50 | `CommitLog:FsyncIntervalMs` when interval. |
| `--hot-store` | inmemory | `HotStore:Engine`: `inmemory` or `faster`. |
| `--paged-engine` | melangelog | `HotStore:PagedEngine` under `--hot-store faster`: `faster` or `melangelog`. |
| `--budget-mb` | 128 | `HotStore:MemoryBudgetBytes` in MiB. The walkers' own rows are a few hundred bytes each and update in place, so they never leave memory, even at 8 MiB. To make the paged store page, and, under `melangelog`, append and compact, add `--ballast-rows`. |
| `--ballast-rows` | 0 | serve/all: seed this many paged `Ballast` rows of `--row-bytes` across every block at start, for a working set larger than the budget. Seeding goes through the shards' reducers, so rows near a block edge are mirrored into the neighbouring shards like any other partitioned row. |
| `--ballast-churn` | 0 | serve/all, with `--ballast-rows`: rewrite this many ballast rows a second for the whole run, 50 to a transaction, so a paged log accrues superseded versions. The serve console's 30-second line adds the hot store's summed log, live and reclaimed bytes, segment files and compaction steps over the primary shards, and the rewrite count. |
| `--heartbeat-ms` | 250 (`failover`: 1000) | `Cluster:HeartbeatIntervalMs`. `failover` defaults to the library's value, because the window it measures is the one a deployment on defaults gets. |
| `--failure-timeout-ms` | 15000 (`failover`: 10000) | `Cluster:FailureTimeoutMs`. Same reasoning. |
| `--fast-failover` | off | `failover`: sets `Cluster:FastFailoverOnLinkLoss`, so a killed node is declared dead when its link drops rather than at the failure timeout. The harness kills a process, the case the option is for. |
| `--lock-split` | — | serve: trace every Move and sum its write-lock split, locked time and the post-commit part of it where subscription fan-out runs, into the stats endpoint; the driver reports the measured window's post-commit share. Sets `Telemetry:SlowReducerMs` to 0, because the split rides the slow-reducer event, and tracing every transaction costs a little. |
| `--movers` | everyone | drive: only the first N players move; the rest connect, subscribe, and receive. Holds the commit rate fixed across a subscriber sweep. |
| `--aoi` | — | drive: subscribe to an **area of interest** instead of the whole shard: the chunks within R chunks of the player (a square of side 2R+1), rescoped on every chunk step. Subscription SQL has equality and `BETWEEN`, and a chunk id packs x above y, so each table gets 2R+1 `WHERE ChunkId BETWEEN :lo AND :hi` strips, one per chunk column. R is at least 1, so a walker's next step stays in view. |
| `--stay-in-block` | off | drive: every player roams inside the block it spawned in, so no one crosses into another shard and the run has no handoffs. Seam walkers are off with it. Set against the same run without it, it separates handoff cost from stepping and re-scoping. |
| `--workstation-gc` | off | all: start the serve process with workstation GC (`DOTNET_gcServer=0`). The tool is built with Server GC, which is what a host on the Web SDK runs; this reproduces runs from before phase 31, which all used workstation GC. For a standalone serve, set `DOTNET_gcServer` yourself. The summary records the serve side's GC mode and core count either way. |
| `--port` / `--listen` | 0 / 127.0.0.1 | Hub bind (gateway + stats). `--listen 0.0.0.0` accepts a remote driver. |
| `--data` | temp dir | Data root for logs and hot stores; the temp default is deleted on exit. `all` passes it to the serve process it starts. |
| `--out` | — | Time-series file; `.json` writes JSON, anything else CSV. |
| `--sample-seconds` | 10 | Periodic console/series sample interval. |
| `--no-stats` | — | Don't poll the serve side's stats endpoint. |
| `--smoke` | — | Preset: 2 nodes, 2x1 world of 4x4 blocks, 8 players all seam walkers, 10 Hz, 12 s measured. |
| `--soak` | — | Preset: 300 s measured, 15 s warm-up. Combine with `--players`. |
| `--in-process-server` | — | `all` only: serve on a background task in the driver's process. For the smoke test and debugging; it contaminates the measurement and the tool says so. |
| `--rows` | 10000,100000,1000000 | `failover`: ballast rows on the measured shard, one sweep entry each. |
| `--row-bytes` | 256 | `failover`, and serve/all with `--ballast-rows`: payload bytes per ballast row (minimum 8; 16 under churn). |
| `--tail` | 10000 | `failover`: records committed after the pre-kill snapshot, which recovery replays. |
| `--replicas` | 1 | `failover`: `Cluster:ShardReplicas`. With 1 the survivor promotes its warm replica, and the run reports how many kills promoted and how far behind the replica was; 0 measures the cold reopen. |
| `--repeat` | 3 | `failover`: kills per sweep entry. `link-bench`: rounds of every shape. |
| `--clients` | 20 | `failover`: gateway clients on the measured shard, each calling Move at `--tick-hz`. |

## What each metric means

- **Reducer calls, attempted vs acked** — attempted counts sends at the tick cadence; acked counts
  results that came back OK. Rejections (`MelangeCallException`) and transport errors are separate.
  Outcomes are epoch-gated: a call attempted during warm-up that acks inside the measured window
  counts nowhere, so acked can never exceed attempted.
- **Call-to-delta latency** (p50/p90/p99/max) — the headline number. Each Move embeds a sequence
  number into the player's row; the clock starts at the call site and stops when the row carrying
  that sequence arrives back **on the caller's own subscription**. That is the full pipeline —
  gateway routing, the shard's serialized durable commit, fan-out, and the wire — where call-to-ack
  would stop at the commit. Timestamps never cross machines, so the metric is honest cross-machine.
- **Absorbed / lost self-deltas** — a handoff swap atomically replaces the client's row cache with
  the destination's initial set; a self-delta landing inside that set produced no delta event and
  its latency sample is unmeasurable. Such samples are counted **absorbed**, not silently dropped;
  entries older than 30 s count **lost**. Both are printed beside the sample count so the
  percentiles' denominator is visible.
- **Seam crossings and crossing continuity** — a crossing is a Move whose target chunk lies in a
  different block than the previous one; continuity is the time from that call until self-deltas
  resume on the client's subscription. This is the *client-observed* seam cost — it includes the
  gateway's invisible call-queueing during a transfer — and it is deliberately measured whether or
  not the crossing triggered an actual transfer, because "does the player feel the boundary?" is
  the question. Handoff *counts* (started/completed/aborted/rate-limited) come from the hub's
  `ClusterMetrics` via the stats endpoint; the server keeps no duration histogram, so no server-side
  duration is invented.
- **Delta traffic** — row events per second (inserts+updates+deletes across both subscriptions,
  aggregate and per client) and websocket payload bytes per second received by the drivers.
- **Server process metrics** — working set, GC heap, gen0/1/2 collection counts, sampled from the
  serve process via `/loadtest/stats` every sample interval. Hub and shard nodes share the serve
  process, so these numbers describe the whole server side of the run. On a soak run the time
  series (`--out`) is the leak watch: a healthy run's working set flattens; a leak trends.
- **Liveness** — the longest heartbeat gap the hub saw, the longest lease-renewal gap any shard
  node saw, how many gaps exceeded 0.5/1/2/3/5 s, and the nodes declared dead and leases lapsed.
  Whole-run counters (`ClusterMetrics`, see OBSERVABILITY.md). A drive run kills nothing, so any
  death or lapse is a false one. Run at a long `--failure-timeout-ms`, the gap counts say how many
  false deaths each shorter timeout would have caused, from one run.
- **`LOADTEST RESULT=PASS|FAIL ...`** — one machine-parseable line of key=value pairs. FAIL means:
  zero acked calls, any client disconnect, any resync error, or seam walkers configured on a
  multi-shard world with zero completed handoffs, or any node declared dead or lease lapsed (these
  two only checked when the stats endpoint is reachable). Latency and throughput are **reported, never asserted** — thresholds belong to your
  hardware, per the repo's measurement discipline (`HotspotMeasurementTests`).

## Methodology and caveats

- **Warm-up is excluded from everything** — counters reset, latency reservoirs cleared, and
  in-flight outcomes epoch-gated out.
- **Same-machine runs share the CPU.** `all` puts serve in a separate process, which removes
  thread-pool contention, but on one machine the driver still competes for cores. Around 100
  clients ≈ 1,500 calls/s and ~60k delta rows/s the driver side is real work; numbers from a
  one-machine run are a *lower bound* on what separated hardware would show. Driver saturation
  shows up as latency inflating while acked stays ≈ attempted.
- **Comparing two builds is a different measurement from measuring one, and working set is the
  wrong number for it.** Working set includes whatever the GC has not handed back, so it moves with
  collection timing rather than with the program. The soak below records the size of that: GC heap
  oscillating between ~50 and ~230 MiB with **no trend at all**. Divide a swing that size by a bot
  count and it dwarfs the effects people go looking for — a reference-workload run reported a
  consistent ~10% per-client regression across two package pins, from two samples against three,
  before an interleaved re-run put settled managed heap flat to one decimal on both and working set
  *favouring* the arm it had accused. To compare arms: interleave them in one session rather than
  running one and then the other, force a collection at the same point in each, and compare
  **settled managed heap**, with working set reported beside it rather than instead of it. Also
  record whether Server GC is on and the core count, since heap count tracks cores and moves the
  resident figure without anything in the program changing.
- **Before attributing a regression to a range of builds, check whether the code under test changed
  in it.** The same episode spent its effort deciding which of nine merged PRs was responsible; the
  range contained *no* change to the client assembly or its dependency closure at all, which a diff
  answered in a couple of minutes and no amount of bisecting would have. Declining to guess which
  change caused it is not the same as establishing that one did.
- **Call-to-delta reported "no samples" from protocol v2 (2026-08-06, shipped in 0.2.0) until phase
  20.** Protocol v2 decodes an `Identity` column as `Identity`, and the driver still matched its own
  row as `byte[]`, so it never recognized one. Every other number in such a run is unaffected, and the
  results below predate the break (2026-07-31). The smoke test now asserts the metric has samples,
  which is the check that would have caught it.
- **What the tool refuses to guess:** with the stats endpoint unreachable (or `--no-stats`),
  handoff counters and server memory are printed as *unavailable*, and the summary says
  `handoffs_completed=unavailable` rather than 0.
- **Cache inconsistencies** is the client library's own counter (an insert for a key already held,
  an update for one never seen). It should be zero; building this tool found and fixed two server
  bugs that inflated it (multi-op-per-key border batches fanning out as repeated inserts, and
  re-scoped subscriptions applying deltas against the old anchor mid-swap). What remains is the
  narrow window phase 10 documented — a delta arriving between the swap and the first chunk of the
  replacing initial set — which self-heals and is reported, not asserted.
- **Overload is a valid result.** The tool does not back off; if the configured population exceeds
  what the topology sustains, call-to-delta latency grows into seconds (delta queues backlog) and
  rejections appear as handoffs outrun the band. That *is* the measurement — see the 300-player
  row below.

## Results on this machine

Measured 2026-07-31, Release build, single machine: Windows 11 Pro, AMD Ryzen 9 9950X3D
(16C/32T), 128 GiB RAM, Samsung 990 EVO Plus NVMe. Serve and drive as separate processes on this
one machine (see the caveat above). Defaults unless stated: 4 shard nodes, 2x2 blocks of 8x8
chunks, 15 Hz, chunk step every 2 s, 25% seam walkers, band 3, interval fsync (50 ms), 120 s
measured after 10 s warm-up.

### Capacity ladder — where the fan-out wall is

**Measured under workstation GC, which no real host runs, and through a gateway that allocated
64 KiB and decoded every row for each frame it relayed (fixed in phase 31). See
[After the gateway fix](#after-the-gateway-fix-2026-11-22) for this shape today.** Four runs, identical topology, only the population moved. Call-to-delta in milliseconds; "acked"
is over the whole 120 s window.

| Players | Calls/s attempted → acked | p50 | p90 | p99 | max | Delta rows/s | Handoffs | Rejected |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | 1,516 → 1,515 | 24.8 | 43.9 | 257 | 1,352 | 55,900 | 828 | 4 |
| 150 | 2,270 → 2,262 | 102 | 229 | 1,365 | 2,184 | 101,900 | 1,285 | 8 |
| 200 | 3,036 → 2,606 | 4,214 | 10,698 | 17,528 | 27,986 | 110,500 | 1,715 | 45 |
| 300 | 4,631 → 3,248 | 10,427 | 23,741 | 30,809 | 35,552 | 58,300 | 1,741 + 79 aborted | 45,801 |

The reading:

- **100 players is comfortable.** Latency in the tens of milliseconds end to end, every call acked,
  ~830 handoffs completed invisibly (crossing continuity p50 26.8 ms — the seam costs roughly one
  ordinary delta), 0 aborted.
- **150 is the knee.** Acks still keep up, but the delivered delta rate has nearly doubled to
  ~102k rows/s and p99 crosses a second: the fan-out pipeline is running at its limit.
- **200 is past it.** Attempted rate holds (the driver does not back off) but delta queues backlog;
  call-to-delta grows to seconds while acked falls behind attempted. Note delta rows/s barely rose
  from 150 — ~110k rows/s delivered is this machine's fan-out ceiling, and extra load turns into
  queueing, not throughput.
- **300 is collapse, reported honestly.** Median latency 10 s; 45,801 rejections appear because
  handoffs can no longer keep up with walkers, so seam writes overrun the band and the freeze
  window (`beyond the band the write fails loudly` — the guard doing its job under overload); even
  delivery throughput drops as the server thrashes.

Commit throughput never approached the in-process loop ceiling: even at 300 players the cluster
commits only ~4,600/s across four shards. **The socket path's wall is delta fan-out, not the
commit loop** — which is the number this tool exists to produce.

One-machine caveat applies throughout: driver and server shared the same 16 cores, so the absolute
knee (~150 players ≈ 100k delivered rows/s here) is a lower bound; the *shape* — linear commits,
quadratic fan-out, backlog past the knee — is the durable result.

### Re-measured as a game runs (phase 31), 2026-11-22

**Every table in this section still ran through the gateway bug described in the next section,
which inflated all of them; they are kept because they are what the step 1 decisions were made
from.** The ladder above has three things in it that a deployed game does not: workstation GC, 50 players to
a shard, and every player subscribed to its whole shard. Phase 31 changed them one at a time before
changing any code. Same machine; the driver shares it, so these are still lower bounds.

**At 200 players, arms interleaved A B C D E E D C B A** (ranges are over the two runs of each):

| Arm | Shape | p50 | p99 | Delta rows/s | Rejected |
| --- | --- | --- | --- | --- | --- |
| A | The ladder's: workstation GC, 4 shards (2x2 blocks of 8x8), whole-shard subscriptions | 5.4 s | 18.8–19.4 s | 90–91k | 17,105–20,703 |
| B | A with Server GC | 102–131 ms | 476–503 ms | 206–221k | 13–936 |
| C | B with 16 shards (4x4 blocks of 8x8, about 12 players each) | 26–28 ms | 345–750 ms | 89k | 4–5 |
| D | B with `--aoi 2` (each player sees a 5x5-chunk square) | 15.5–15.9 ms | 177–185 ms | 57k | 3–5 |
| E | B, C and D together: the game's shape | 13.7–13.8 ms | 272–300 ms | 19k | 0–7 |

- **Most of the ladder's wall was the garbage collector.** Arm A ran about 19,000 gen0 collections
  in two minutes, against about 6,500 under Server GC, and Server GC alone took 200 players from
  collapse to a p50 of about 0.1 s while delivering more than twice the rows. The tool now runs
  Server GC by default (`--workstation-gc` reproduces the ladder). A host built on the Web SDK
  already does; a host built as a plain console app does not, and should set
  `<ServerGarbageCollection>true</ServerGarbageCollection>`.
- **Smaller shards and a bounded view each cut the delivered rows,** which is the cost that is
  quadratic: 16 shards about halve them, a 5x5 view cuts them to a quarter, and together to a
  tenth. The area-of-interest arm rescoped about 95 steps a second (five strips per table each)
  without a cache inconsistency.

**The ladder again, under Server GC** (driver also on Server GC):

| Shape | Players | p50 | p90 | p99 | Delta rows/s | Rejected | Handoffs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B (4 shards, whole-shard) | 300 | 5.6 s | 14.4 s | 25.2 s | 165k | 37,933 | 2,101 + 103 aborted |
| B | 400 | 10.1 s | 21.8 s | 30.4 s | 147k | 154,014 | 1,647 + 398 aborted |
| E (game shape) | 400 | 30 ms | 129 ms | 355 ms | 65k | 5 | 4,064 |
| E | 600 | 74 ms | 211 ms | 398 ms | 123k | 15 | 6,274 |
| E | 800 | 514 ms | 918 ms | 1.5 s | 181k | 9,752 | 8,227 |
| E | 1,200 | 3.7 s | 9.2 s | 15.3 s | 183k | 722,250 | 6,315 + 2,030 aborted |

- **The knee in the ladder's shape moved from about 150 players to between 200 and 300; in the
  game's shape it is between 600 and 800** on this one machine, at about 9,000–12,000 commits a
  second across 16 shards. No healthy node was declared dead in any run; the longest heartbeat
  gap was 589 ms.
- **The ceiling is now about 180–220k delivered rows a second,** about twice the ladder's. Past it,
  extra players become queueing, the same shape as before. The 300, 400 and 1,200 rows each ended
  with 1 to 11 resync errors, the overload behaviour described under replica lag below.

**The fan-out sweep again, under Server GC** (one shard, 50 movers, about 750 commits/s, as in the
sweep below; arms 100, 200, 400, 800, 200, 400, 200):

| Subscribers | Post-commit share | Mean lock hold per Move | Lock demand | Call-to-delta p50 / p99 | Delta rows/s |
| --- | --- | --- | --- | --- | --- |
| 100 | 65.6% | 442 µs | 0.33 | 14 / 155 ms | 75,800 |
| 200 | 73.8–75.5% | 793–848 µs | 0.60–0.64 | 40–43 / 211–647 ms | 152,000 |
| 400 | 85.7–86.5% | 1.16–1.19 ms | 0.89–0.91 | 16.9–17.7 s | 194–201k |
| 800 | 92.3% | 1.46 ms | over 1 | 19.7 s | 193,000 |

- **Fan-out costs about 2–3 µs per subscriber per commit under Server GC,** against 5.5 µs under
  workstation GC: post-commit time is about 290 µs at 100 subscribers, 600 µs at 200 and 1.0 ms at
  400.
- **It is still the wall.** The shard saturates at about 400 subscribers at this commit rate
  instead of 200, and past that the lock queues the way it did before. This is what shared fan-out
  (phase 31's next step) is aimed at: the per-subscriber work left inside the lock.

### After the gateway fix, 2026-11-22

Shared fan-out (phase 31 step 2) cut the fan-out's own cost to about a quarter in the benchmark,
yet the shard still collapsed at 400 subscribers. The load tool's new CPU line explained why: at
400 subscribers the **serve process used 18.7 cores and spent 25.6% of the window paused for GC**,
nearly all of it in thread-pool workers. The gateway's upstream session allocated a fresh 64 KiB
receive buffer for every message a node sent, copied it through a new stream, and decoded every row
of every frame, although it only forwards a delta's bytes. A pause stops the thread holding the
write lock too, so this showed up as lock hold growing with subscribers, which is what the sweeps
above read as the cost of fan-out. Every capacity number in this document before this section ran
through it.

The session now reuses one buffer and relays without decoding (it peeks the frame type). Same
machine, Server GC, defaults otherwise. Interleaved against the build before the fix:

| Run | Before | After |
| --- | --- | --- |
| One shard, 400 subscribers, 50 movers: p50 / p99 | 15.3 s / 29.4 s | 9.6 ms / 29.4 ms |
| Same: delivered rows/s, mean lock hold per Move | 212k, 1,091 µs | 303k (all of it), 233 µs |
| 4 shards, 300 players: p50 / p99 | 4.7 s / 23.8 s | 11.2 ms / 194 ms |
| Same: delivered rows/s, rejected calls | 188k, 10,592 | 639k, 0 |

**The fan-out sweep again** (one shard, 50 movers, about 750 commits/s):

| Subscribers | Serve CPU | GC paused | Mean lock hold per Move | Lock demand | p50 / p99 | Delta rows/s |
| --- | --- | --- | --- | --- | --- | --- |
| 200 | 3.6 cores | 1.4% | 171 µs | 0.13 | 5.9 / 29 ms | 152k |
| 400 | 6.1 cores | 1.5% | 233 µs | 0.17 | 9.6 / 29 ms | 303k |
| 800 | 9.4 cores | 1.5% | 393 µs | 0.29 | 17 / 41 ms | 607k |
| 1,600 | 17.7 cores | 3.1% | 925 µs | 0.69 | 40 / 134 ms | 1.21M |

- **A subscriber now costs the lock about 0.5 µs per commit,** from 2–3 µs. One shard at 750 commits
  a second carries 1,600 subscribers with the lock idle a third of the time.
- **The cost that grows now is delivery, off the lock:** about 15 µs of serve CPU per delivered row
  at 1,600 subscribers, gateway relay included.

**The ladder again** (4 shards of 8x8 chunks, whole-shard subscriptions, Server GC):

| Players | p50 | p90 | p99 | Delta rows/s | Rejected | Serve CPU, GC paused |
| --- | --- | --- | --- | --- | --- | --- |
| 200 | 6.2 ms | 12.7 ms | 99 ms | 285k | 0 | 4.1 cores, 1.3% |
| 400 | 18 ms | 40 ms | 179 ms | 1.13M | 0 | 7.3 cores, 1.7% |
| 600 | 71 ms | 329 ms | 8.1 s | 1.74M | 2,751 | 10.2 cores, 7.5% |

**The knee in this shape moved from about 150 players to about 600,** at 1.7 million delivered rows
a second on one machine with the driver beside it.

**The game's shape again** (16 shards, `--aoi 2`):

| Players | p50 | p90 | p99 | Delta rows/s | Rejected | Result |
| --- | --- | --- | --- | --- | --- | --- |
| 800 | 24 ms | 176 ms | 1.8 s | 236k | 16,965 | pass |
| 1,200 | 50 ms | 420 ms | 989 ms | 382k | 36,852 | 6 resync errors |
| 1,600 | 90 ms | 471 ms | 15.3 s | 353k | 520,460 | 1,294 resync errors |

- **This shape now hits a different wall, and delivery is not it:** it delivers a fifth of the rows
  the whole-shard ladder does at the same CPU. What grows is rejections (seam writes past the band
  while a transfer catches up) and, at 1,200 and up, resync errors. Each player step rescopes ten
  subscriptions, and a rescope diffs its range under the write lock; at 1,200 players that is about
  6,000 rescopes a second across 16 shards, plus 10,000 handoffs in two minutes. Which of these is
  the limit is not measured yet.

**Workstation GC, re-checked after the fix** (4 shards, interleaved):

| Players | GC | GC paused | p50 | p99 | Rejected |
| --- | --- | --- | --- | --- | --- |
| 200 | workstation | 8.8% | 6.6 ms | 135 ms | 0 |
| 200 | Server | 1.4% | 6.4 ms | 144 ms | 0 |
| 400 | Server | 1.9% | 19 ms | 269 ms | 1 |
| 400 | workstation | 54.8% | 149 ms | 15.4 s | 9,713 |

With the gateway's garbage gone, workstation GC keeps up at 200 players, and still collapses at 400.
The startup warning (EventId 1108) stands.

### Where the game's shape breaks (phase 31), 2026-11-22

The runs above left the game's shape (16 shards, `--aoi 2`) failing at about 1,200 players without
saying why. The suspects were re-scoping (ten re-scopes per player step, each diffing under the
write lock) and handoffs. The driver now reports both: each shard's write-lock busy time with the
re-scopes' share of it (`melange.subscription.rescope.duration`), rejections and client errors by
code, each step of a handoff, and the serve process's thread-pool queue. `--stay-in-block` keeps
every player inside its spawn block, so a run has no handoffs and changes nothing else.

**Re-scoping is not it, and neither is the write lock:**

| Players | Handoffs | Busiest shard's lock busy | Re-scopes' share of locked time | Rejected | Delta rows/s | Result |
| --- | --- | --- | --- | --- | --- | --- |
| 800 | on | 14.5% | 1.7% (8 µs each) | 20 | 244k | pass |
| 800 | off | 12.2% | 1.9% | 0 | 278k | pass |
| 1,200 | on | 14.5% | 2.6% (10 µs each) | 44,721 | 379k | 2 resync errors |
| 1,200 | off | 18.1% | 2.2% | **0** | **585k** | pass |
| 1,600 | on | 15.2% | 3.8% (13 µs each) | 560,585 | 333k | 1,384 resync errors |

Without handoffs, 1,200 players pass clean and deliver half again as many rows. The lock is idle
more than 80% of the time on every shard in every run.

**What the failures are.** Rejections by code at 1,200 players were 42,555 `rate_limited`, 2,164
`internal` and 2 `transient`. All three trace back to handoffs that take seconds:

- `rate_limited` is the per-player reducer limit (`RateLimit:ReducerCallsPerSecond` 20, burst 60).
  Players call at 15 Hz, under it. They trip it when the gateway replays the calls it held through
  a long handoff in one burst.
- `internal` is mostly `ShardSpanException`. A player walks past the border band before its
  handoff lands, and with `Cluster:ShardSpanCheck` on, the write is refused as a span violation
  and logged as a reducer failure. The rest is the gateway refusing calls past its held-call cap
  (256), which it also answers with `internal` although its message says "retry".
- `overflow_resync` is a gateway swap that failed outright: it could not connect to the
  destination node, so the client re-establishes its subscriptions.

**Where a handoff's time goes:**

| Step | 800 players | 1,200 players |
| --- | --- | --- |
| Trigger: crossing signal queued in the boundary monitor | 9.9 ms mean | 45 ms mean; none dropped |
| Freeze on the origin | 40 ms | 147 ms |
| Import on the destination | 49 ms | 154 ms |
| Release on the origin | 43 ms | 181 ms |
| Whole saga | 133 ms (max 0.6 s) | 476 ms (908 over 1 s, max 2.4 s) |
| Gateway swap: wait for the destination session | 0.1 ms | 432 ms mean, 243 over 5 s, max 41 s |
| Gateway swap: wait for the connection's attachment lock | 0 | 17 ms mean, 11 over 5 s, max 29 s |
| Calls held by the gateway, start to flush | 90 ms (max 0.6 s) | 693 ms (254 over 5 s, max 42 s) |
| Serve thread pool, queued work items | mean 468, p99 3,532 | mean 2,560, p99 7,987 |

- **Each round trip of the saga gets four times slower,** from about 45 ms to about 160 ms. The
  steps themselves are small (a freeze and a durable marker), and the lock is mostly idle, so the
  time is waiting: the serve process's thread pool holds thousands of queued items at 1,200
  players while the process uses 12 of 32 cores. Node-link request handlers wait in that queue.
- **The tail is the gateway's swap, not the saga.** The longest saga was 2.4 s, but calls were
  held up to 42 s. A swap uses the session the gateway pre-opened to the destination when the
  player approached; 431 swaps at 1,200 had none and connected then. Connecting runs through the
  same queued pool, retries on timeout, and at 1,600 sometimes fails ("Unable to connect").
  `EnsureShardUpstreamAsync` holds the connection's attachment lock while it connects, so a swap
  can wait for that too.

**Caveat:** this harness runs the hub, the gateway and all four shard nodes in one process, so one
thread pool carries everything that a real deployment spreads over five. The pool backlog per
process would be smaller there; whether the gateway's per-player upstream sessions and the
calls-held-then-replayed shape hold up is the question either way.

**After the handoff fixes, 2026-11-22.** The first two fixes shipped, and fixing the gateway's swap
surfaced two more gateway bugs: a subscription routed mid-transfer attached the destination
without the swap (so the swap found itself "already there" and never flushed its held calls), and
pre-opened sessions were never closed unless used, about ten upstream sockets a player at 1,200.
With those fixed (each run started with the previous run's closed sockets drained, which earlier
back-to-back runs did not wait for):

| Players | Rejected | Resync errors | Swaps over 5 s | Calls held, mean | Delta rows/s | Result |
| --- | --- | --- | --- | --- | --- | --- |
| 800 | 1 | 0 | 0 | 67 ms | 246k | pass |
| 1,200 | 11,487 (was 44,721) | 1 (was 2) | 62 (was 236) | 342 ms (was 693) | 382k | 1 resync error |
| 1,600 | 56,300 (was 560,585) | 76 (was 1,384) | 307 (was 807) | 1.2 s | 475k | 76 resync errors |

Cache inconsistencies stayed at zero. What remains at 1,200:

- **Rate-limited calls (10,722).** The gateway routes a client's frames one at a time, in order,
  so a subscription waiting on a slow swap stops the client's calls behind it; they reach the shard
  together and trip the per-player limit. Held calls never exceeded 50, so it is not the replay.
- **Sockets.** Each player still holds its own upstream session per shard it is attached to or
  approaching (about six sockets a player at 1,200, and thousands of closes a minute in TIME_WAIT),
  and a few hundred swaps a run still connect when they arrive.
- **The saga's round trips** vary with the thread-pool queue (a mean saga of 190 ms in one run,
  630 ms in the next). Raising the pool's minimum to 256 threads did not change the swap tail, so the
  pool is busy, not starved.

**On the shared upstream link, 2026-11-22.** With `Cluster:GatewayUpstream = Shared` (now the
default), each node gets one websocket from the gateway and a handoff opens a session on it rather
than a connection. Same runs, ports drained between them:

| Players | Rejected | Resync errors | p50 / p99 / max | Delta rows/s | Sockets mid-run | Result |
| --- | --- | --- | --- | --- | --- | --- |
| 800 | 3 | 0 | 23 ms / 590 ms / 1.3 s | 242k | 3.4k (TIME_WAIT 133) | pass |
| 1,200 | **0** (per-session: 11,487) | **0** | 47 ms / 1.05 s / 1.9 s | **483k** (382k) | 5.0k (14.5k) | **pass** |
| 1,600 | 167,090 | 0 | 2.9 s / 14.5 s / 32 s | 467k | 6.7k | passes, degraded |

A handoff at 1,200 players now takes 95 ms (631 ms per-session) and holds calls for 70 ms, with a
longest hold of 0.6 s; per-session swaps had a 38 s tail. **The game's shape now carries 1,200 players
cleanly on one machine,** where it failed before phase 31 started.

At 1,600 the new limit is the swap waiting for its destination session (mean 1.9 s, 2,163 over 5 s):
the session's handshake answer travels the link behind the other sessions' deltas. Sending control
envelopes first and taking data from sessions in turn looked like the fix and measured far worse
(1,200 players: 212k rows/s, call-to-delta p50 2.5 s, half the calls unacknowledged inside the
window), so the link keeps its first-in, first-out writer; why the fair writer collapsed is not
understood yet. A 2,000-player run could not start: the driver, on the same machine, ran out of
socket buffer space connecting its clients (`WSAENOBUFS`), which is this harness's ceiling rather
than the server's.

**Batched link messages and four links per node, 2026-11-22.** The 1,600-player symptom was swaps
waiting for their destination session behind other sessions' traffic. Counters on the link's writer
and reader (now in the driver's `Upstream links` line) showed the writers were the limit: one
websocket message per envelope, about 546,000 sends a second, with the node-to-gateway writers
busy all the time. Two changes, measured at 1,600 players in order:

| Link | Envelopes per message | Delta rows/s | Rejected | p50 / p99 call-to-delta | Swap wait, mean |
| --- | --- | --- | --- | --- | --- |
| One per node, one envelope per message | 1 | 398k | 222,710 | 3.9 s / 14.1 s | 2.8 s |
| One per node, batched (up to 64 KiB) | 347 | 619k | 24,451 | 1.4 s / 8.0 s | 0.66 s |
| Four per node, batched (three runs) | 214-296 | 680k-743k | 173-1,357 | 0.19-0.71 s / 5.1-5.7 s | 39-201 ms |

Every run had zero resync errors and zero cache inconsistencies; the remaining rejections are
`rate_limited`. At 1,200 players the same build delivers 462k rows/s with 30 transient refusals,
p50 42 ms and p99 1.6 s. **The game's shape now carries 1,600 players on one machine with a tail of
about 5 s,** where phase 31's first measurement of this shape put its knee at 600-800 players.

**Not kept: fewer relay allocations.** Pooling the link's envelopes and delivered chunks and
forwarding relayed frames from the receive buffer instead of a copy measured far worse at 1,600
players in two interleaved runs against the same build without it: 392k-393k rows/s against
741k-743k, about 254,000 rejected calls against about 200, and swaps waiting 2.4 s against
39-64 ms. GC pauses barely moved (9.7-10.2% against 8.7-8.9%), so allocation was no longer the
limit anyway. Which half caused the slowdown was not isolated.

### Relating to the in-process hotspot ceiling

CLUSTERING.md publishes the *commit loop's* ceilings, measured in-process on one crowded shard:
~500 commits/s under per-commit fsync from a sequential caller (the disk), ~4,000 under the same
durability from 16 concurrent callers (phase 17's group commit — the disk's flush rate times the
batch contention forms), ~12,000 under interval fsync (the loop). The socket path never gets near
the loop's ceiling, because it hits **delta fan-out first**: with S
subscribers on a shard each committing at rate R, the server must deliver S x R row events per
second per shard — quadratic in players per shard. The ladder above shows exactly that shape:
commits scale linearly and stay cheap; the deliverable delta rate is what saturates.

Single-shard runs with the fsync knob make the comparison direct (50 players, one shard, one node,
no seams, 60 s, ~758 calls/s — one commit per call):

| Fsync | Acked | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| `interval` (50 ms) | 45,500 / 45,500 | 11.4 ms | 19.7 ms | 34.5 ms | 113 ms |
| `commit` (default durability) | 45,550 / 45,550 | 46.4 ms | 522 ms | 1,294 ms | 2,855 ms |

758 commits/s sat under the sequential per-commit fsync ceiling of the day, so throughput held
either way — but the *latency distribution* tells the story: under per-commit fsync every commit
waits its turn behind the disk's fsync queue, and p90 goes from 20 ms to half a second at only
~70% of that ceiling. Choosing the fsync policy is choosing this distribution, exactly as
CLUSTERING.md says at the strategy-choice point.

That table was measured before phase 17's group commit, with a prediction attached: the concurrent
callers this tool generates are exactly the shape that shares fsyncs, so the per-commit-fsync tail
should collapse toward the batched ceiling. **Re-run 2026-11-20** (road-to-0.3 phase 20), same
machine and shape, arms interleaved interval → commit → commit → interval:

| Fsync | Acked | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| `interval` (50 ms), two arms | 45,500 / 45,500 each | 10.7–10.8 ms | 17.6–18.1 ms | 35.0–35.8 ms | 143–253 ms |
| `commit`, two arms | 45,499–45,500 / same | 12.8–13.0 ms | 18.9–19.4 ms | 166–172 ms | 251–291 ms |

**The prediction held for the bulk of the distribution, and only partly for the tail.**
- Per-commit fsync's p90 fell from 522 ms to 19 ms, and now sits level with interval fsync.
- p99 fell 7.6×, from 1,294 ms to about 170 ms, but is still about 5× interval's.
- The max fell an order of magnitude.

So since phase 17, per-commit durability costs about 2 ms at the median and a fatter p99 tail, not
a different latency class. The fsync-policy choice CLUSTERING.md describes is now mainly a choice
about that tail.

### Fan-out under the write lock — one shard, subscribers swept

Under workstation GC and through the gateway bug; the same sweep today is in
[After the gateway fix](#after-the-gateway-fix-2026-11-22). Measured 2026-11-20 for road-to-0.3 phase 20, as [read-side fan-out](idea-bin/read-side-fanout.md)'s
reopening metric. One shard on one node, **50 movers at 15 Hz (a fixed ~750 commits/s)**, and the
subscriber count swept by adding passive subscribers (`--movers 50 --players N --lock-split`). Every
subscriber holds the same `PlayerPos` and `Terrain` subscriptions. Arms ran interleaved 50 → 100 →
200 → 400 → 400 → 200 → 100 → 50, each 60 s measured after 10 s of warm-up.

| Subscribers | Post-commit share of locked time | Mean lock hold per Move | Lock demand (750/s × hold; over 1 cannot be served) | Call-to-delta p50 / p99 | Delta rows/s |
| --- | --- | --- | --- | --- | --- |
| 50 | 66.1%, 67.2% | 339–342 µs | 0.26 | 11.1 / 35–37 ms | 37,900 |
| 100 | 74.3%, 75.2% | 793–805 µs | 0.60 | 24–26 / 100–178 ms | 75,800 |
| 200 | 86.2%, 87.7% | 1.20–1.36 ms | 0.90–1.02 | 9.8–10.8 s | 122,400 |
| 400 | 90.5% | 2.46 ms | 1.85 | 20.7–21.6 s | 107,800–118,600 |

(The second 400 arm's final stats read did not complete under the overload, so it has a latency but
no split.)

The reading:

- **Fan-out is most of the lock already at 50 subscribers.** Phase 20 set the bar at 25% of locked
  time at 200 subscribers. Measured, it's 86–88%, and two-thirds at a quarter of that population.
- **It grows linearly in subscribers**, at about 5.5 µs per subscriber per commit of post-commit
  time (225 µs → 600 µs → 1.1 ms → 2.2 ms). That cost is paid under the write lock, so every other
  writer on the shard waits for it.
- **The shard saturates at about 200 subscribers at this commit rate.** Lock-held time per second
  reaches about 1.0 (750 × 1.3 ms), and call-to-delta goes from tens of milliseconds to ten seconds.
  This is the same wall as the capacity ladder above (about 110–120k delivered rows/s), now located:
  it's the subscription evaluation inside the commit, not the socket writes after it.
- The share itself stays under 100%, because the body and append are a fixed cost. The absolute
  post-commit time is what scales, and it does so linearly, not worse.

`--lock-split` traces every Move (every transaction carries the slow-reducer split), which costs
some throughput, so the absolute lock holds are a slight overstatement and the shares are not
affected. One machine, driver and server in separate processes on the same 16 cores: past 200
subscribers the driver is also saturated, which inflates latency but not the server-side split.

### The reassignment window

`failover` measures what node death costs a crowded shard. It runs a hub and two shard nodes in one
process, fills the measured shard with `--rows` ballast rows, attaches `--clients` gateway clients
that call Move at `--tick-hz`, and then for each of `--repeat` kills:

1. Takes a snapshot on the owner and commits `--tail` records after it, the log tail recovery will
   replay.
2. **Kills the owner the way a crash would.** The node runtime is disposed at once, so heartbeats
   and engines stop, and the host is then aborted with no grace, so its port refuses connections
   immediately. The hub learns of the death only from heartbeat silence.
3. Times each part on a spinning poll thread. **Detect** runs from the kill until the hub names a
   new owner. **Reopen** runs until the shard is open on the survivor: the heartbeat that carries
   the assignment, plus recovery. **First write** runs until a reducer commits there. **Client
   restore** runs from the kill until each client's first acked call that it sent after the node
   was dead.
4. Restarts the killed node, so the next kill moves the shard back.

After the kills, the shard directory is opened by a bare engine three times ("isolated open"), so the
heat-dependent part can be read without the heartbeat wait in it. The run also records, before each
kill, how stale the owner's heartbeat already was. A live owner that has gone quiet shortens
*detect* below the timeout, and the line says so with a WARNING rather than report it as speed.
The summary line also counts **false deaths** (declarations beyond the one each kill causes) and
lease lapses on the nodes still running, with the longest heartbeat and lease-renewal gaps: an
adopter whose recovery stalls its heartbeat shows up there, and either one fails the run.

**Synthetic by construction.** The reference workload has no cluster deployment, so this cannot come
from it. One process hosts hub, both nodes, and the clients, so GC and thread-pool load are shared;
treat sub-second differences as noise.

#### Results, 2026-11-20

Same machine as above. Release build, workstation GC, defaults `Cluster:HeartbeatIntervalMs` 1000
and `Cluster:FailureTimeoutMs` 10000 (hub sweep every 5 s), 256 B rows, 10,000-record tail, 20
clients at 15 Hz, 3 kills per size. Ranges are over the kills.

| Store | Rows (shard dir) | Detect | Reopen | Isolated open | Server window | Client restore p50 / max |
| --- | --- | --- | --- | --- | --- | --- |
| in-memory | 10k (12 MiB) | 10.8–11.7 s | 0.6–1.4 s | 116–183 ms | 11.9–13.1 s | 12.1 / 14.0 s |
| in-memory | 100k (62 MiB) | 10.6–12.2 s | 0.9–2.7 s | 392–753 ms | 12.0–13.9 s | 13.3 / 14.1 s |
| in-memory | 1M (565 MiB) | see below | 4.9–6.1 s | 3.2–4.5 s | 11.3–18.5 s | 12.0 / 19.1 s |
| FASTER | 10k (12 MiB) | 11.2–11.5 s | 0.6–1.3 s | 97–121 ms | 11.9–12.8 s | 12.6 / 13.0 s |
| FASTER | 100k (62 MiB) | 10.6–11.1 s | 0.7–2.8 s | 415–703 ms | 11.7–13.4 s | 12.6 / 14.1 s |
| FASTER | 1M (284 MiB) | 12.1–13.2 s | 6.0–7.0 s | 3.2–4.0 s | 19.1–19.2 s | 20.2 / 24.7 s¹ |

¹ Measured later, after the [#208](https://github.com/tyler-loy/MelangeDB/issues/208) fix, with 2
clients and 2 kills rather than 20 and 3. Before it, this row could not be measured: a border reset
scanned the paged shard under the engine lock and the hub fenced the live owner before the kill.
Each pre-kill snapshot of this shard took 4–5.5 min (one synchronous random read per row); through
them the owner's heartbeat staleness peaked at 1.0–1.35 s. With paged scans reading a block of rows
at a time ([#212](https://github.com/tyler-loy/MelangeDB/issues/212)), the same snapshot takes 29–41 s, and the 300k-row one 4.5 s (from 19.4 s).

The reading:

- **Detection is the window, at every size measured.** It took 10.6–13.4 s at defaults, because the
  hub's death sweep runs every `FailureTimeoutMs/2`, so a node is declared dead 1.0–1.5× the timeout
  after its last heartbeat ([#201](https://github.com/tyler-loy/MelangeDB/issues/201)). It doesn't
  depend on shard heat. A warm replica can't shorten it either: fencing forbids reassigning before
  the old owner's lease has expired.
- **Reopen is the part that grows with heat:** about 0.1 s of recovery at 10k rows, 0.5 s at 100k,
  and 3.5 s at 1M (isolated). In the cluster it costs more, because the assignment waits for a
  heartbeat and the recovery competes with everything else in the process. At 1M rows it's 5–6 s on
  top of detection.
- **Clients come back with the server,** within about 0.1 s of the first write at most sizes. Calls in
  the window fail or wait, depending on how the gateway's retry budget lines up with detection
  ([#200](https://github.com/tyler-loy/MelangeDB/issues/200)). This is process death, where the dead
  port refuses connections at once. A dead *machine* doesn't refuse; the gateway's upstream connect
  has no timeout, so each client stalls for the OS TCP connect timeout. That's 21 s measured on this
  Windows machine, and about 127 s on stock Linux by its documented SYN retry schedule (#200).
  **Since fixed:** the handshake and the attachment retry are bounded by the failure timeout; these
  rows were measured before the fix.
- **At 1M rows, detect is not a clean number.** Two of the three kills found the owner's heartbeat
  already 7.7–8.3 s stale before the kill. That owner had adopted the shard on the previous kill and
  went quiet for the whole recovery, because **a node opens an adopted shard inside its heartbeat
  loop** ([#198](https://github.com/tyler-loy/MelangeDB/issues/198)). With the failure timeout set
  below the recovery time (`--failure-timeout-ms 3000`), the hub declares the *adopting* node dead
  mid-recovery, a cascade from a single kill. **Since fixed:** opens run off the heartbeat loop.

#### After phase 30 step 1 (detection), 2026-11-21

Same machine, defaults, 100k rows, 4 clients, 3 kills each. The run line now also lists failed
calls by reason.

| Detection | Detect | Reopen | Server window | Client restore p50 / max |
| --- | --- | --- | --- | --- |
| At the deadline ([#201](https://github.com/tyler-loy/MelangeDB/issues/201)) | 9.0–9.9 s | 1.0–1.9 s | 10.1–11.7 s | 10.3 / 12.0 s |
| `--fast-failover` | 1.0–1.2 s | 0.9–1.9 s | 1.9–3.2 s | 3.1 / 3.3 s |

- **Detection is now a timeout after the last heartbeat,** checked every twentieth of the timeout.
  It used to be up to half a timeout later. Measured from the kill it reads 9–10 s, because the last
  heartbeat landed up to a second before the kill.
- **With fast failover a process death costs about a heartbeat plus the reopen.** The reopen is
  now the larger share of the window. That share is what phase 30's warm replica removes.
- **Failed calls in these windows are the rate limiter, not the failover.** The probe fires 15
  calls/s without waiting. The gateway holds them through the window and they reach the shard in
  one burst, and `RateLimit:*` turns the excess away.
- **At the phase 30 defaults (heartbeat 250 ms, timeout 5 s),** same runs:

  | Detection | Detect | Reopen | Server window | Client restore p50 / max |
  | --- | --- | --- | --- | --- |
  | At the deadline | 4.9–5.1 s | 0.8–1.3 s | 5.9–6.2 s | 6.1 / 6.4 s |
  | `--fast-failover` | 0.36–0.39 s | 1.2–1.3 s | 1.5–1.6 s | 1.8 / 1.8 s |

  No false death or lease lapse in either; the longest heartbeat gap was 305 ms.
- **These runs found a gateway bug.** About one kill in twelve tore down a client's connection:
  a send to a node dying under it was mistaken for the client leaving. Fixed separately; with the
  fix, 30 of 30 kills restored every client.

#### After phase 30 step 2e (promotion), 2026-11-21

Same machine, defaults (heartbeat 250 ms, timeout 5 s), `--fast-failover`, 4 clients. `--replicas 1`
is the library default: the survivor keeps a warm replica and the hub hands it the shard.
`--replicas 0` is the cold reopen it replaces. Before each kill the run waits until the replica
has caught up to where the owner stood, then writes the 10,000-record tail at full speed and kills
at once, so the replica is a few thousand records behind when the owner dies, as live traffic
would leave it.

| Store | Rows | Replicas | Kills promoted | Replica behind at kill | Promote / reopen | Server window |
| --- | --- | --- | --- | --- | --- | --- |
| in-memory | 100k | 0 | — | — | 0.74–1.12 s | 1.20–1.51 s |
| in-memory | 100k | 1 | 5 / 5 | 5.1–9.9k records | 0.49–0.54 s | 0.74–1.05 s |
| FASTER | 100k | 0 | — | — | 0.94–1.40 s | 1.40–1.69 s |
| FASTER | 100k | 1 | 5 / 5 | 1.9–10.1k records | 0.31–0.85 s | 0.72–1.14 s |
| in-memory | 1M | 0 | — | — | 4.62–6.05 s | 5.08–6.48 s |
| in-memory | 1M | 1 | 3 / 3 | 6.7–10.3k records | 1.63–3.00 s | 1.96–3.44 s |
| FASTER | 1M | 0 | — | — | 4.97–6.06 s | 5.30–6.51 s |
| FASTER | 1M | 1 | 3 / 3 | 2.1–10.1k records | 2.38–4.74 s | 2.68–5.08 s |

The 1M rows were measured after the border-reset fix below; before it, a 1M run could not get
past its first kill. At the default deadline detection, 100k in-memory with replicas: detect
4.8–5.1 s, promote 0.42–0.49 s, server window 5.2–5.5 s (from 5.9–6.2 s). No false death or lease
lapse in any run; the longest heartbeat gap was 320 ms.

- **Promotion takes the reopen out of the window.** What remains is the heartbeat that carries the
  assignment and the replay of what the replica had not applied. At 100k rows a promoted shard was
  writable half a second after the hub named it.
- **At 1M rows promotion costs 1.6–4.7 s,** about half the cold reopen, but more than the
  replay alone explains at 100k. Where the rest goes is not measured yet.
- **The 1M runs found a border bug, since fixed.** A border reset was sent as one node-link frame,
  and the link drops frames over 64 MiB. Against a side neighbour, a million-row shard's band is
  about 375k rows, a 152–170 MiB frame. The hub's reader dropped the link, and with fast failover
  on, the hub declared a healthy node dead and moved its shards, again after each reopen. It
  predates phase 30: the first replica-off runs showed it as "detect 0 ms", the shard already
  moving before the kill. Resets now travel in parts of about 16 MiB.
- **The first runs found a promotion bug, fixed before these numbers.** The survivor's first open
  often fails because the dying owner has not released the log yet, and the next heartbeat
  retries. The open took the replica before trying the log, so the failed attempt threw the
  replica away and the retry went cold. The replica is now taken only once the log is open.
- **Clients come back about 1.5 s after the kill however fast the server is.** This is the test
  machine: on Windows a refused loopback connect takes about 2 s to fail (measured), and the
  gateway's first reattach attempt goes to the dead node's port. On Linux a refused connect fails
  at once.

#### Replica lag under the soak (phase 30), 2026-11-21

Same soak as the liveness tables below (`all --nodes 4 --soak`, heartbeat 250 ms, 15 s timeout),
now with replicas, the default. The serve process samples every replica every 100 ms: the owner's
durable LSN, read from the owner's engine in the same process, minus what the replica has applied.
The hub's relay delay therefore counts as lag, where the `melange.cluster.replica.lag` gauge
measures against the ceiling the hub last relayed and does not see it. *Oldest unapplied* is how
long the oldest record the replica had not applied had been durable on its owner, in samples
where it was behind: what a promotion at that instant would have replayed, in time.

| Players | Hot store | Lag p50 / p99 / max (records) | Oldest unapplied p99 / max | Acked calls | Deaths / lapses |
| --- | --- | --- | --- | --- | --- |
| 100 | in-memory | 6 / 263 / 480 | 0.51 / 0.70 s | 454,686 | 0 / 0 |
| 100 | FASTER | 6 / 254 / 442 | 0.51 / 0.71 s | 454,771 | 0 / 0 |
| 200 | in-memory | 0 / 4,891 / 7,206 | 7.5 / 9.8 s | 792,513 | 0 / 0 |
| 200 | FASTER | 0 / 4,599 / 6,294 | 6.7 / 8.8 s | 776,434 | 0 / 0 |

- **At sustainable load a replica is about two heartbeats behind:** half a second at p99, never
  more than 0.71 s. The ceiling rides the heartbeat (250 ms) and the follower polls every 50 ms.
- **Past the knee it falls up to ten seconds behind, in bursts.** The median stays at zero. The
  ceiling travels in the heartbeat's load sample and comes back in its reply, and the heartbeat
  loop runs on the thread pool, which is what starves at 200 players. The liveness ping, which
  moved off the pool for the same reason, does not carry it. Lag costs a promotion replay time
  (7,000 records is about a second), never data: the replay reads the log, not the replica.
- **Replicas cost no measurable throughput here:** 776–793k acked calls at 200 players, against
  770–800k before phase 30's step 2.
- **The 200-player FASTER run ended with 2 resync errors** (3 in a first run), where the runs
  without replicas had none below 300 players. Each is the gateway failing to move a client to a
  shard whose pre-opened socket had already been aborted, which tells the client to resubscribe.
  It is overload behaviour past the knee, and it is the run's only failure.

#### Liveness soak (phase 30), 2026-11-21

How close a healthy cluster comes to its failure timeout: `all --nodes 4 --soak` with heartbeat
250 ms and a 15 s timeout, long enough that no death disturbs the run. Every gap the hub saw
between two heartbeats from one node is counted against each candidate timeout at once. The hub
and all four nodes share one process, so a pause stalls both ends of every link together; a real
deployment's hub does not share its node's thread pool, so treat these as pessimistic.

| Players (capacity ladder) | Hot store | Longest gap | > 1 s | > 2 s | > 3 s | > 5 s | Deaths / lapses |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100 (comfortable) | in-memory | 432 ms | 0 | 0 | 0 | 0 | 0 / 0 |
| 100 | FASTER | 428 ms | 0 | 0 | 0 | 0 | 0 / 0 |
| 200 (past the knee) | in-memory | 4.2 s | 428 | 35 | 2 | 0 | 0 / 0 |
| 200 | FASTER | 4.3 s | 425 | 43 | 9 | 0 | 0 / 0 |
| 300 (collapse) | FASTER | 10.0 s | 378 | 238 | 120 | 24 | 0 / 0 |

About 4,800 heartbeats per run at 100 players, 2,750 at 200, 1,400 at 300: overloaded, the
heartbeats themselves slow down. (The in-memory 300-player run lost its stats endpoint in the
collapse and has no row.) The lease-renewal gaps the nodes measured track the hub's within a few
milliseconds.

- **At sustainable load there is room for a 2 s timeout:** the longest gap was under half a second.
- **Past the knee, heartbeats stalled with everything else.** At 200 players a 2 s timeout would
  have declared a healthy node dead 35–43 times in five minutes, and 3 s 2–9 times. Each one moves
  a shard onto nodes that are just as overloaded. Timing each hop showed almost all of it was
  waiting for thread-pool threads: the heartbeat loop's 250 ms delay fired up to 2.4 s late, and
  the hub waited up to 1.3 s for a thread to handle the beat.

**With the liveness ping** (a dedicated pinger thread on the node, answered on the hub's link
reader thread) and synchronous link sends under a plain lock, the same sweep:

| Players | Hot store | Longest gap | > 1 s | > 2 s | > 3 s | > 5 s | Deaths / lapses |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | in-memory | 272 ms | 0 | 0 | 0 | 0 | 0 / 0 |
| 200 | in-memory | 1.1 s | 2 | 0 | 0 | 0 | 0 / 0 |
| 200 | FASTER | 1.8 s | 5 | 0 | 0 | 0 | 0 / 0 |
| 300 | FASTER | 3.2 s | 4 | 4 | 4 | 0 | 0 / 0 |

- **The ping alone was half the fix.** Its first version still waited up to 1.5 s for the link's
  send lock, a `SemaphoreSlim` that handed itself on release to queued async waiters whose
  continuations needed pool threads; no write held it longer than 23 ms. Sends are now synchronous
  writes under a plain lock, so the waiters are threads already running.
- **What remains is a few discrete multi-second pauses at collapse,** four in five minutes at 300
  players, likely full collections of a 3 GiB heap. A dedicated thread sleeping 10 ms never woke
  more than 54 ms late at 200 players.
- **Throughput rose too:** 200 players acked ~770–800k calls in five minutes, from ~690k, now that
  the pool no longer cycles through send-lock handoffs.
- **Recovering a big shard does not stall the adopter.** `failover --rows 1000000 --hot-store
  faster --heartbeat-ms 250 --failure-timeout-ms 15000 --fast-failover`: the longest gap across two
  7 s recoveries was 335 ms, with no false death or lapse (#198's fix holding).
- **The first pass found two gateway bugs** that refused 11% of calls at 100 players: shards for
  negative-x blocks were unreachable, and the retries exhausted the port range; and a failed swap
  wedged its client. Fixed in #228 before these numbers were taken.

### The node link's framing — JSON against binary, 2026-11-24

Node links carried row data as base64 inside JSON, serialized twice and staged in the shared
`ArrayPool`. A dotMemory snapshot of a paging soak put 322 MB of the server's 647 MB in that pool's
per-thread caches: 700 arrays averaging 435 KB across about 380 threads. Row data now rides in a
binary attachment after the JSON (see the glossary's *Node link*).

`link-bench --repeat 3`, best and worst of three where they differ:

| Shape | | Before | After, binary | After, JSON (a mixed-version link) |
|---|---|---|---|---|
| Border batch, 500 × 1 KiB | Round trip, 1 sender | 4.29–4.72 ms | **0.46–0.52 ms** | 1.73–1.95 ms |
| | Per message, 32 senders | 2.28–2.65 ms | **0.56–0.57 ms** | 1.63–1.68 ms |
| | Allocated per message | 4.51 MB | **1.13 MB** | 4.72 MB |
| | Wire bytes per message | 763 KB | **520 KB** | 710 KB |
| | Held after a full collection | 1–64 MiB | **0–1.5 MiB** | 0–3 MiB |
| Border batch, 50 × 64 B | Round trip, 1 sender | 115–121 µs | **31–39 µs** | 82–85 µs |
| | Allocated / wire per message | 67 KB / 7.4 KB | **20 KB / 4.1 KB** | 37 KB / 7.1 KB |
| Heartbeat | Round trip, 1 sender | 40–46 µs | 37–39 µs | 33–40 µs |
| | Allocated per message | 9.8 KB | **6.8 KB** | 6.6 KB |

In a cluster: `all --players 100 --nodes 4 --hot-store faster --paged-engine melangelog --budget-mb 8
--ballast-rows 400000 --row-bytes 1024 --ballast-churn 1000 --duration-seconds 360`, before (A) and
after (B) alternated twice, then the 200-walker capacity run alternated twice, then the paging run
once each under `DOTNET_GCHeapHardLimit=2 GiB`:

| | A, JSON link | B, binary link |
|---|---|---|
| Paging soak, call → delta p99 | 224 / 210 ms | 196 / 194 ms |
| Paging soak, GC heap / working set at the end | 3.30 / 8.25 GiB, 3.99 / 8.69 GiB | 2.87 / 7.46 GiB, 3.63 / 8.05 GiB |
| Walker run, p99 (no row batches to speak of) | 261 / 362 ms | 728 / 363 ms |
| Walker run, GC heap at the end | 275 / 275 MiB | 164 / 219 MiB |
| Paging soak under a 2 GiB heap limit | **out of memory while seeding; 0 calls acked** | seeds, serves 218k calls, then runs out in the key directory |

- **Where the batches are, the link was the cost.** The paging soak's tail fell ~10% and its heap
  ~10%; the walker runs, whose link traffic is small batches, moved within their own noise (one
  728 ms p99 against 363 ms on the repeat).
- **Under the heap limit the bottleneck moved.** The old link could not seed 400k rows in 2 GiB; the
  new one seeds them, and what then runs out is the paged tables' key directory: about 320 bytes
  per stored row copy, times about three border copies, times two for the replica. That cost is
  documented in [the bake-off](design/hot-store-bakeoff.md) with its follow-up, moving the
  directory off the managed heap.

### Soak — memory over five minutes

`all --players 100 --nodes 4 --soak`: 300 s measured after 15 s warm-up, the stable point from the
ladder held for five minutes.

- **Steady state held**: 1,515 calls/s attempted and acked for the full window; p50 27.2 ms /
  p99 69.1 ms with no upward trend (the last sample's p50, 24.5 ms, was the run's best); 2,100
  handoffs completed, 0 aborted, 0 unresolved; 0 disconnects; 0 lost self-deltas.
- **Managed heap is flat.** GC heap oscillated between ~50 and ~230 MiB with no trend across the
  window; gen2 collections ticked at a constant ~2.7/s. No managed-leak signal.
- **Working set grew**: 502 MiB (first sample) → 795 MiB (last), fastest early and flattening —
  +75 MiB in the first minute, +28 MiB in the last. With the heap flat, the growth is file-backed
  and native memory: four shards' commit logs, snapshots, and store files growing under
  ~380 commits/s/shard. A judgment call, stated as one: this reads as data growth, not a leak —
  but five minutes cannot prove that, and a longer soak (`--soak --duration-seconds 1800`) watching
  whether the flattening completes is the follow-up the time series exists for.
- 315 cache inconsistencies over 2,100 swaps (~0.15 per swap) — the residual swap-window artifact
  described under caveats.

### Paging soak — MelangeLog against FASTER under constant rewrites, 2026-11-24

`all --players 100 --nodes 4 --soak --hot-store faster --paged-engine <engine> --budget-mb 8
--ballast-rows 400000 --row-bytes 1024 --ballast-churn 1000`: 400,000 rows of 1 KiB, about 50 times
the budget, 1,000 of them rewritten a second, under the walker load. MelangeLog ran 2.7 h, then
FASTER 1.8 h, back to back on the same machine.

| | MelangeLog | FASTER |
|---|---|---|
| Primary shards' paged log | **flat at ~2.55 GiB** from minute 20 to the end | 1.4 GiB → **22 GiB**, linear |
| Hot-store disk, primaries and replicas | 5.4 → 5.5 GiB | 0 → **53.5 GiB** (~29 GiB/h) |
| Reclaimed by compaction | 36.7 GiB over 44,530 steps | — (FASTER does not compact) |
| Ballast rewrites failed | 0 of 9.87 M | 0 of 6.56 M |
| Call → delta p50 / p99 / max | 2.9 / 211 / 2,352 ms | 3.1 / 470 / 19,417 ms |
| Rejected calls | 4,244 | 34,325 |
| Replica staleness, max | 1.1 s | 14.9 s |
| Disconnects / inconsistencies / nodes declared dead | 0 / 0 / 0 | 0 / 0 / 0 |

- **Live bytes are three times the raw ballast, and that is right.** 400,000 rows at 1,104 B a
  record (main plus blob, headers and alignment) is 421 MiB; the primaries report 1,297 MiB live.
  With a 3-chunk band on 8-chunk blocks, a row within three chunks of a block edge is mirrored into
  that neighbour: six columns and six rows in eight qualify, so a row is stored
  (1 + 6/8)² = 3.06 times on average. `CompactionTests.Live_bytes_count_each_row_once` checks a
  single store counts each row once.
- **Disk is the live bytes times the trigger ratio times the replicas**: 1.3 GiB × 2 × 2 ≈ 5.2 GiB,
  against 5.4–5.5 GiB measured.
- **The commit log grew about 5.7 GiB/h on both engines**, the same either way, and is under
  investigation.
- **Memory**: with churn, both servers held 4–5 GiB of GC heap and about 10 GiB of working set from
  the first minutes, flat after that; without churn the heap is ~250 MiB. Whether that is Server
  GC's latitude on a 32-core host or retention is being checked under a heap limit.
- Both runs ended with one `token_expired` and one overflow resync per client, the same counts on
  both engines and on the walker-only soaks; under investigation, and not storage.

Re-run these on your hardware; the shapes hold even where the numbers move.
