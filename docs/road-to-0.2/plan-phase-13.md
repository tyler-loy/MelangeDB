# Phase 13 — Clustering III: elastic assignment

**Goal:** the shard → node map follows load. A hot shard moves to an underloaded node while its
players stay connected; the whole world consolidates onto one node when the night shift arrives.
No new nodes appear in this phase — elasticity across the fleet that exists.

**Depends on:** [09](../road-to-0.1/plan-phase-09.md) (membership, fencing, node-death
reassignment), [10](../road-to-0.1/plan-phase-10.md) (the gateway swap),
[design/elastic-rebalancing.md](../design/elastic-rebalancing.md) — the design record this phase
implements half of.

## Why here

The design record settled the model: shard boundaries are fixed at strategy registration, and the
elastic layer is the grouping of shards onto nodes. Everything load-bearing already exists —
per-shard logs on shared storage, fencing tokens, the node-death recovery path, the gateway's
queue-and-reswap — and this phase composes them into three missing pieces: a load signal, a
graceful drain, and a rebalance loop. It ships before the provisioner seam (phase 14) because it
needs no external dependency, and because the drain alone is the payoff: the day it lands, an
operator watching the load view can move the hot island off the busy box by hand.

## Deliverables

**Per-shard load on heartbeats.** Each shard node's existing heartbeat carries, per owned shard:
commit-loop utilization over the reporting window (the busy fraction of the engine's write lock —
the saturation signal that matches the published hotspot ceiling), applier lag, and resident bytes.
The hub aggregates the feed into a load view readable through `MelangeClusterCoordinator` and
exported as gauges. No new clock: the signal rides `Cluster:HeartbeatIntervalMs`.

**The planned drain** — `MelangeClusterCoordinator.DrainShardAsync(shard, destination)`, the
node-death reassignment path made polite, per the design record's six-step sequence:

1. Intent recorded in the membership store (the hub is its sole writer; no second decider).
2. **Preflight snapshot** on the shard, so the destination's recovery tail is short — the handover
   window is bounded by snapshot-plus-tail, and the drain gets to choose how fresh the snapshot is.
3. Quiesce by fencing-token bump; the origin's refusals are `transient`, and the gateway queues that
   shard's calls exactly as it does mid-handoff.
4. Destination recovers the shard from its log on shared storage, reports its applied LSN.
5. Gateway re-issues affected sessions' subscriptions on the destination and flushes the queue —
   the phase 10 swap, verbatim. Neighbours' border cursors reset through the existing 1715 path.
6. Origin drops the engine; hub marks the move settled.

Every step idempotent; an interrupted drain resolves through the phase 09 reconciler pattern in
favour of the higher fencing token. The drain is an operator-facing API in its own right, not just
the loop's internal move.

**The rebalance loop.** A hub background service over the load feed, off by default
(`Cluster:RebalanceEnabled`). One move in this phase: drain a sustained-hot node's shard to the
least-loaded node whose projected load stays under the hot threshold. Hysteresis at every layer,
imported from handoff: a *sustained* window (`Cluster:RebalanceWindowSeconds`), a per-shard floor
between moves (`Cluster:ShardMoveMinIntervalMs`), and a dead zone between the hot and cold
thresholds so the loop never chases its own wake. Every decision — including the decision not to
move — logs its arithmetic at debug; every move logs loudly.

**The granularity guardrail.** At strategy registration, when the hub can already see that one
shard's declared extent makes it an indivisible majority of a node's measured capacity, say so —
a startup log line with the arithmetic in it, in the spirit of EventId 1723. The trap it catches:
island-sized shards discovered at peak to be the unit of shedding.

**Observability.** Per-shard load gauges, a moves counter with reason, a drain-duration histogram
split by step (preflight, quiesce-to-recovered, swap), queue depth during drains, and the loop's
considered/acted tick counters. Recorded in [OBSERVABILITY.md](../OBSERVABILITY.md) with the
change, per the standing convention.

**Configuration** (registered as planned rows in [CONFIGURATION.md](../CONFIGURATION.md), same
change as this plan): `Cluster:RebalanceEnabled`, `Cluster:RebalanceWindowSeconds`,
`Cluster:RebalanceHotUtilization`, `Cluster:RebalanceColdUtilization`,
`Cluster:ShardMoveMinIntervalMs`, `Cluster:DrainQueueTimeoutMs`.

## Out of scope

Provisioning and decommissioning — the fleet is fixed; phase 14 owns `INodeProvisioner`. Scale-in
as a *policy* (the cold threshold exists in this phase only as the dead zone's floor; nothing acts
on it). Dynamic boundary splitting — deferred with its trigger stated in the design record.
Predictive or scheduled scaling (move at 1:45 p.m. because 2 p.m. is coming) — the loop is
reactive; prediction can layer on the same drain primitive later without new machinery.

## Decisions to settle

### Settled: commit-loop utilization is the load metric

Leaning: the busy fraction of the engine's write lock over the window. It is the resource the
hotspot ceiling is made of, it is cheap to measure at the source, and unlike commit rate it does
not need a per-hardware calibration to mean "near the ceiling". Applier lag and resident bytes
ride along as tie-breakers and diagnostics, not triggers. To settle: whether fsync-bound shards
(`OnCommit` deployments, where the lock is busy *waiting*) need the wait split out of the busy
fraction to avoid draining a shard that a faster disk, not a second node, would fix.

**Settled as leaning** — `MelangeEngine.WriteLockBusyTicks`, cumulative work inside the lock,
lock *waits* excluded (a queue is evidence of saturation, not more capacity spent). The
fsync-wait caveat is recorded rather than built: append time — fsync included — counts as busy,
so an `OnCommit` disk-bound shard reads hot. That is arguably honest (the ceiling it is near is
real) and the split ships when a consumer demonstrates the misdiagnosis.

### Settled: the drain's queue cap is its own key

Leaning: `Cluster:DrainQueueTimeoutMs`, defaulting well above the handoff queue's patience —
recovering a large shard is slower than importing one player, and the cap exists to bound a
*wedged* drain, not a normal one. The alternative — reusing the handoff cap — reads as simpler and
would make every large-shard drain trip it. To settle: the default, once the drain-duration
histogram exists to inform it.

**Settled as leaning** — shipped at 60 s, and it grew a second job: the floor of the drain's
per-step node-link timeout, because quiesce and recovery scale with shard size while the link's
default request timeout does not. One key states the deployment's whole drain patience.

### Settled: quiesce is the fencing bump, not a new freeze

Leaning: reuse. The bump is already what stops a wrongly-suspected-dead node from writing, the
refusal surface already exists (`transient`), and a second freeze mechanism would be a second thing
the reconciler must reason about. To settle: whether the origin needs a distinct "draining" state
in membership purely for legibility (operator asks "why is this shard refusing writes"), even if
the mechanism underneath is the same token.

**Settled as leaning, with one addition the plan missed**: a node-local *draining mark* (not
membership state — the hub's store stays a pure ownership registry). It exists because the node's
own heartbeat would otherwise reopen a quiesced shard while the hub sits between quiesce and
reassign — the race that puts two writers on one log. The mark clears when the assignment moves,
on an explicit abort, or by expiry after 2 × `Cluster:FailureTimeoutMs` (EventId 1728) — the
self-healing bound for a hub that died mid-drain, resolving the interruption in the origin's
favour.

### Settled: destination choice is least-projected-load, not round-robin

Leaning: place the moved shard where the load view says it fits, using the shard's own measured
contribution as the projection. To settle: whether resident-bytes headroom vetoes a placement the
utilization numbers would allow — a shard that fits by CPU and not by memory is a worse outcome
than not moving.

**Settled with a stronger rule than planned.** The plan's test — projected target under the hot
threshold — turned out to admit a degenerate move: relocating a node's single hotspot to an
emptier node "fits" whenever the threshold is generous, and accomplishes nothing. Shipped instead:
move the largest-load shard for which `target + shard < origin` — the pair's *maximum strictly
improves* — which both refuses the hotspot shuffle and is threshold-free, so it cannot be
mis-tuned. The resident-bytes veto is deferred to phase 14 with the rest of memory-aware
placement; utilization is the only axis in 13.

## Shipped notes

Landed as three stacked changes — the load signal, the drain, the loop — each green on the full
cluster and core suites before the next began. Boundaries drawn during implementation, beyond the
settled decisions above:

- **The granularity guardrail moved from registration time to runtime.** The plan wanted a warning
  at strategy registration when one shard's extent exceeds a share of node capacity — but capacity
  is not knowable at registration, and any registration-time number would be a guess. Shipped
  instead as EventId 1732, fired (rate-limited) when a node is *measured* sustained-hot and owns a
  single shard: the same trap, caught with real arithmetic at the moment it matters. Worth an
  alert; the docs say so.
- **The gateway mutes before the quiesce, not at the swap.** The handoff precedent mutes the
  origin at the destination-authoritative moment; a drain's origin dies earlier — its transport
  closes under the quiesce — so `OnMoveStarted` mutes immediately, and the closure reads as
  machinery rather than a client-visible resync error.
- **Mid-drain asymmetry, recorded honestly:** reducer *calls* queue (bounded by
  `Cluster:DrainQueueTimeoutMs`); a *fresh subscription* issued mid-drain instead rides the
  gateway's existing connect-retry window (~5 s). Calls are the hot path and the promise; new
  subscriptions mid-drain are rare and converge.
- **Done-when deltas.** The no-disconnect drain, the mid-drain call, the least-loaded pick, the
  refusals, the post-quiesce fault handback, the loop's corrective move, the no-flap window, and
  the single-shard refusal are tests. Interruption coverage is by injected fault at the
  quiesce/reassign boundary plus the expiry design for a dead hub — a scripted kill-the-hub-mid-
  drain test is left for the phase 14 cycle, where the reference provisioner makes process-level
  chaos cheap. The full five-shard night-shift script is likewise subsumed piecewise (drain by
  hand, loop spreads back out) rather than run as one scenario test.

## Done when

- A live shard with connected, subscribed clients drains to another node with **no disconnect**: the
  test asserts every client's cache converges to the destination's state, queued reducer calls all
  execute in order, and the observed stall is bounded by the measured drain window.
- A drain interrupted at every step boundary (origin killed, destination killed, hub killed)
  resolves — the reconciler either completes or aborts it, and exactly one node ends up owning the
  shard, asserted by fencing token.
- The loop, in a two-node test with a synthetic load skew, moves the hot shard within one window and
  then does nothing — and under an oscillating skew, the move count is bounded by the rate limit,
  never proportional to the oscillation.
- The night-shift scenario runs end to end: five shards on two nodes under load, load removed,
  operator drains everything to one node by hand (scale-*in* policy is phase 14; the primitive must
  already make it possible), load reapplied, loop spreads back out.
- The configuration rows flip to shipped with defaults verified, and the load view, drain, and loop
  are in OBSERVABILITY.md and GLOSSARY.md ("drain" is a noun now).

## Risks

- **The drain window on a big shard.** Snapshot-plus-tail recovery of a multi-GB shard could hold
  players in the queue past any reasonable cap. The preflight snapshot bounds the tail but not the
  snapshot load itself; if that proves slow, the fallback shape is a warm standby recovery
  (destination replays *before* the quiesce, then catches up the delta) — more machinery, so it is
  the risk's contingency, not the plan.
- **A metric that lies under `OnCommit`.** A fsync-bound shard looks saturated while the CPU idles;
  draining it to an identical disk buys nothing. The busy/wait split in the first decision is the
  hedge.
- **Flapping.** The loop's failure mode is not wrongness but oscillation — every hysteresis layer
  exists for this, and the oscillating-skew test is the regression guard.
- **Shared-storage latency.** Phase 09's reassignment assumes the destination can open the shard's
  directory; a drain makes that path routine rather than exceptional, so slow shared storage
  becomes a routine cost. The drain histogram's step split is what makes this visible.
