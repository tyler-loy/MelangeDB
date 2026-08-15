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

### Commit-loop utilization is the load metric

Leaning: the busy fraction of the engine's write lock over the window. It is the resource the
hotspot ceiling is made of, it is cheap to measure at the source, and unlike commit rate it does
not need a per-hardware calibration to mean "near the ceiling". Applier lag and resident bytes
ride along as tie-breakers and diagnostics, not triggers. To settle: whether fsync-bound shards
(`OnCommit` deployments, where the lock is busy *waiting*) need the wait split out of the busy
fraction to avoid draining a shard that a faster disk, not a second node, would fix.

### The drain's queue cap is its own key

Leaning: `Cluster:DrainQueueTimeoutMs`, defaulting well above the handoff queue's patience —
recovering a large shard is slower than importing one player, and the cap exists to bound a
*wedged* drain, not a normal one. The alternative — reusing the handoff cap — reads as simpler and
would make every large-shard drain trip it. To settle: the default, once the drain-duration
histogram exists to inform it.

### Quiesce is the fencing bump, not a new freeze

Leaning: reuse. The bump is already what stops a wrongly-suspected-dead node from writing, the
refusal surface already exists (`transient`), and a second freeze mechanism would be a second thing
the reconciler must reason about. To settle: whether the origin needs a distinct "draining" state
in membership purely for legibility (operator asks "why is this shard refusing writes"), even if
the mechanism underneath is the same token.

### Destination choice is least-projected-load, not round-robin

Leaning: place the moved shard where the load view says it fits, using the shard's own measured
contribution as the projection. To settle: whether resident-bytes headroom vetoes a placement the
utilization numbers would allow — a shard that fits by CPU and not by memory is a worse outcome
than not moving.

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
