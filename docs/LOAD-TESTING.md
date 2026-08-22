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
the built exe. Three subcommands, because **the measurement is contaminated if server nodes and the
load driver share a thread pool**:

| Subcommand | What it does |
| --- | --- |
| `serve` | Hosts the hub plus N shard nodes (real Kestrel hosts, real TCP node links) in this process, seeds the world's terrain, then prints `GATEWAY <uri>`, `STATS <uri>`, and `READY`. Runs until Ctrl+C (or `--serve-seconds`). |
| `drive` | Connects `--players` clients to `--address` and generates the workload for `--warmup-seconds` + `--duration-seconds`, printing a sample every `--sample-seconds` and a final summary. |
| `all` | Spawns `serve` as a **separate child process**, waits for `READY`, then drives against it. The one-command local run. |

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
| `--port` / `--listen` | 0 / 127.0.0.1 | Hub bind (gateway + stats). `--listen 0.0.0.0` accepts a remote driver. |
| `--data` | temp dir | Data root for logs and hot stores; the temp default is deleted on exit. |
| `--out` | — | Time-series file; `.json` writes JSON, anything else CSV. |
| `--sample-seconds` | 10 | Periodic console/series sample interval. |
| `--no-stats` | — | Don't poll the serve side's stats endpoint. |
| `--smoke` | — | Preset: 2 nodes, 2x1 world of 4x4 blocks, 8 players all seam walkers, 10 Hz, 12 s measured. |
| `--soak` | — | Preset: 300 s measured, 15 s warm-up. Combine with `--players`. |
| `--in-process-server` | — | `all` only: serve on a background task in the driver's process. For the smoke test and debugging; it contaminates the measurement and the tool says so. |

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
- **`LOADTEST RESULT=PASS|FAIL ...`** — one machine-parseable line of key=value pairs. FAIL means:
  zero acked calls, any client disconnect, any resync error, or seam walkers configured on a
  multi-shard world with zero completed handoffs (only checked when the stats endpoint is
  reachable). Latency and throughput are **reported, never asserted** — thresholds belong to your
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

Four runs, identical topology, only the population moved. Call-to-delta in milliseconds; "acked"
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
CLUSTERING.md says at the strategy-choice point. (Measured before phase 17's group commit; the
concurrent callers this tool generates are exactly the shape that now shares fsyncs, so a re-run
should show the per-commit-fsync tail collapse toward the batched ceiling.)

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

Re-run these on your hardware; the shapes hold even where the numbers move.
