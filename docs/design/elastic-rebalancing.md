# Elastic capacity: fixed shard boundaries, dynamic assignment

**Goal:** the cluster grows and shrinks with load by **regrouping shards onto nodes** — never by
reshaping the shards themselves. At 2 a.m. one node owns every shard; at 2 p.m. the hub notices one
shard running hot, obtains a second node through a provisioner seam, and moves that shard onto it;
overnight the loop runs in reverse and consolidates back down to one box. Shard boundaries are drawn
once, at strategy registration; the shard → node map is the layer that breathes.

**Status:** **built** — shipped as [road-to-0.2](../road-to-0.2/README.md) phases
[13](../road-to-0.2/plan-phase-13.md) (elastic assignment: the load signal, the drain, the
rebalance loop) and [14](../road-to-0.2/plan-phase-14.md) (provisioned capacity and scale-in: the
`INodeProvisioner` seam, provision-then-reassign, drain-and-decommission); each plan's Shipped
notes record the deviations. [CLUSTERING.md](../CLUSTERING.md) shipped static assignment in phase
09 (shards created at runtime, assigned least-loaded-first, reassigned only on node death) and
left rebalancing as an open question. This record resolved the *assignment* half of that question
and deliberately re-deferred the other half — dynamic boundary splitting — because the
fixed-boundary version captures the load-following behaviour at a fraction of the cost.

**Depends on:** [plan-phase-09](../road-to-0.1/plan-phase-09.md) (membership store, fencing tokens,
node-death reassignment, per-shard logs on shared storage), [CLUSTERING.md](../CLUSTERING.md)
(gateway call-queueing and re-subscription during handoff, border cursor resets).

## Why

Player load is not static and it is not uniform. The motivating shape is a world of five islands on
a North American schedule: at 2 a.m. the whole world's load is five night owls and fits comfortably
on one node; at 2 p.m. one island is packed while the others idle. A statically-provisioned cluster
must be sized for the 2 p.m. peak of the hottest region and then burns that capacity all night.

The tempting frame — "the overloaded shard resizes" — is the expensive frame. Resizing a live
shard's boundaries is the quadtree subsystem CLUSTERING.md already flags as substantial: re-homing
rows across a moving line, re-deriving interest, resetting border streams, all while handoff sagas
are in flight. The observation this record rests on is that the motivating scenario never needed it:

> **What reshapes at 2 p.m. is not any shard's boundary. It is the grouping of shards onto nodes.**

A shard node already runs one engine per owned shard, so "one node handles all five islands" is one
node owning five shards — that works today. The only missing motion is the *graceful* move of one
owned shard to another node while both are alive.

## The rule that decides everything

> **Draw the split lines once, at the finest grain a node might ever need to shed. Elasticity only
> regroups them.**

Granularity is the whole design burden the developer carries. If an island could ever outgrow a
node, it must be registered as several shards (four blocks instead of one), and then "splitting the
island under load" collapses back into reassignment — the hub moves two of the four blocks. A region
registered as a single shard makes that shard the indivisible unit of placement forever: no amount
of provisioning splits it later, because splitting is exactly the subsystem this design declines to
build.

The trade is more shards than nodes in the common case, which is already the shipped topology
(engines are per-shard, assignment is per-shard) and costs little: an idle shard is an idle engine.

## The mechanism is mostly built

| Piece | Status | Where |
| --- | --- | --- |
| Shards created at runtime, assigned least-loaded-first | shipped (phase 09) | hub membership store |
| Reassignment with fencing: bump token, new owner recovers shard from its own log on shared storage | shipped, **on node death only** | membership + shard recovery |
| Gateway holds a session's shard-routed calls, re-issues subscriptions on the new owner, flushes in order | shipped (phase 10 handoff swap) | gateway |
| Border cursor invalidation answered with a full band reset, never silently resumed past | shipped (EventId 1715) | border stream |
| Node → hub heartbeats | shipped (failure detection) | membership |
| **Planned drain** — the node-death path, minus the death | shipped (phase 13) | `MelangeClusterCoordinator.DrainShardAsync` |
| **Per-shard load metrics carried on heartbeats** | shipped (phase 13) | load view (`LoadView()`, `melange.cluster.shard.*`) |
| **The rebalance loop** | shipped (phase 13) | hub, `Cluster:RebalanceEnabled` |
| **The provisioner seam** | shipped (phase 14) | `INodeProvisioner` + `Cluster:MaxNodes`/`MinNodes`/`ScaleInEnabled` |

The load-bearing property underneath all of it: **each shard has its own log on shared storage, so
moving a shard is an ownership transfer, not a data copy.** The new owner recovers the shard the
same way a node-death successor does — snapshot plus log tail — and the recovery-time work already
done (bulk-mode replay) directly shrinks the handover window.

## The planned move

A drain is the node-death reassignment made polite. In order:

1. The hub picks a (shard, destination) pair and records the intent in the membership store — the
   hub is that store's sole writer, so there is no second decider to race.
2. The origin quiesces the shard: the fencing token is bumped, so the origin's engine refuses
   further commits for it (this is the same refusal a wrongly-suspected-dead node already makes —
   a **transient** rejection, and the gateway begins queueing that shard's calls exactly as it does
   during a player handoff).
3. The destination recovers the shard from its log on shared storage and reports its applied LSN.
   Scheduled tables travel with the log — a timer is a row in the shard engine's log, so the shard's
   timer set arrives with it, and the fresh-versus-recovered distinction ("the log has no head")
   already prevents re-seeding.
4. The gateway re-issues the affected sessions' shard subscriptions on the destination and flushes
   the queued calls in order — the phase 10 swap, verbatim. Re-subscribing under an existing id
   re-scopes it, so each client's cache is atomically replaced with no disconnect and no gap.
5. Neighbouring shards' border cursors against the moved shard are invalidated by the ownership
   change and answered with the existing full band reset. Disjoint regions — islands with water
   between them — have empty bands and skip this cost entirely; island-shaped worlds are the
   best case for this whole design.
6. The origin drops the shard's engine and the hub marks the move settled.

The cost, stated honestly: **that shard's writes pause for the handover window** — quiesce plus
recovery plus re-subscribe — and its players see one hitch, the same order of event as a player
handoff. Every other shard sees nothing. An interrupted drain is recovered by the same shape that
recovers an interrupted handoff: both sides' state is durable (membership intent, shard log), every
step is idempotent, and the reconciler pattern resolves a stranded move in favour of whoever holds
the higher fencing token. A drain is *simpler* than a player-handoff saga, not harder — it moves
ownership of a log, not rows between logs.

## The signal and the loop

Heartbeats already flow node → hub on a clock for failure detection; the extension is that each
heartbeat carries per-shard load — commit-loop utilization, applier lag, resident memory. The hub
runs one rebalance loop over that feed with exactly two moves, tried in order:

1. **Reassign** a hot node's shard to an existing underloaded node.
2. **Provision, then reassign** — only when every live node is hot.

And the reverse for consolidation: when the fleet's aggregate load fits comfortably on fewer nodes,
drain the emptiest node's shards onto the rest and decommission it.

The lesson to import wholesale is handoff's: **hysteresis at every layer.** Provisioning has minutes
of latency and real money attached, and a drain has a player-visible hitch, so the loop acts on
*sustained* load (a window, not a sample), rate-limits moves per shard (a shard that just moved does
not move again for a long interval), and keeps a wide dead zone between the scale-out and scale-in
thresholds. Flapping here is not jitter — it is orphaned cloud instances and a world that hitches on
a timer. Scale-in is the genuinely harder half (drain everything, confirm, decommission, do not
oscillate against the scale-out threshold) and may ship a phase behind scale-out without weakening
it; a cluster that only grows still solves the 2 p.m. problem, just not the 2 a.m. bill.

## The provisioner seam

MelangeDB never talks to a cloud API. The hub asks a **capacity provider** — the same seam serves
AWS, a rack of bare metal, and a stack of pre-warmed standby processes:

```csharp
public interface INodeProvisioner
{
    /// <summary>Ask for one more shard node. Slow — minutes, not milliseconds. The hub
    /// tracks the ticket; the node announces itself by joining membership as usual.</summary>
    Task<ProvisionTicket> RequestNodeAsync(CapacityRequest request, CancellationToken ct);

    /// <summary>Release a drained node. Called only after the hub has confirmed it owns
    /// no shards.</summary>
    Task DecommissionAsync(string nodeName, CancellationToken ct);
}
```

Shipped as sketched (phase 14), with the node identity settled as the membership node-name string
and the ticket carrying the name the new instance will join under — the provisioner configures the
instance's `Cluster:NodeName`, and that name is the entire correlation mechanism.

Three contract clauses do the safety work:

- **Fire-and-track, never awaited inline.** The rebalance loop records the ticket and moves on; a
  provisioned node *joins membership like any other node* and the loop assigns to it when it
  appears. There is no special "provisioned node" path to keep correct.
- **At-least-once, made safe by fencing.** A node that comes up after the hub gave up on its ticket
  is indistinguishable from a zombie — which is a solved problem: it joins membership, owns nothing
  until assigned something, and can write nothing it does not own. Duplicate provisioning wastes
  money, not correctness; `DecommissionAsync` on the surplus node is the remedy.
- **Shared-storage access is part of the contract.** A node that cannot reach the shard logs is not
  capacity, and the provisioner — not MelangeDB — is the party that knows how to grant it. A
  provisioned node that joins but cannot open the log store must fail its first assignment loudly,
  not sit in membership looking assignable.

For a real game the recommended shape is **pre-warmed standbys**: a player surge is precisely when
minutes of cold-provision latency are least affordable, and a standby that is already in membership
(owning zero shards) turns "provision, then reassign" into just "reassign". This costs no
machinery at all — a standby is an ordinary shard node you start ahead of time:

```jsonc
// The standby's appsettings: an ordinary shard node. It registers, owns nothing, and the
// rebalance loop prefers assigning to it over provisioning by construction — move one is
// always tried before the seam.
"MelangeDb:Cluster": {
  "Role": "Shard",
  "NodeName": "standby-1",
  "HubAddress": "hub.internal:7100",
  "PublicAddress": "http://standby-1.internal:5000",
  "ShardDataPath": "/mnt/shard-logs"   // the same shared storage as every other node
}
```

A deployment that cannot afford provision latency at all runs its standby pool this way and
registers no provisioner; the seam never fires and the fleet's ceiling is the pool.

## What this deliberately does not fix

- **The single-shard hotspot ceiling is untouched.** A crowd standing in one chunk is one writer no
  matter how the map is cut, and no node count changes that — CLUSTERING.md publishes the measured
  ceilings. Elastic assignment spreads *breadth* (many warm shards); it cannot buy *depth*.
- **Dynamic boundary splitting stays deferred.** The quadtree remains where CLUSTERING.md left it:
  a substantial subsystem, now with a narrower customer — only the workload whose load concentrates
  inside a single registered shard *and* whose developer cannot redraw granularity, *and* which
  cannot use instancing. The Vibe Shaft port demonstrating such a workload is the trigger to reopen
  it; nothing in this design forecloses it.
- **Instancing already has this for free.** Fresh instances land least-loaded on whatever nodes
  exist, including freshly provisioned ones; the instancing strategy's elasticity work is zero.

## Decisions to settle

All three were settled where they belonged — in the phase plans, in place: the metric is the
write-lock busy fraction with `Cluster:RebalanceWindowSeconds` / `RebalanceHotUtilization` /
`RebalanceColdUtilization` naming the window and the thresholds; quiesce is queue-with-timeout
under its own cap (`Cluster:DrainQueueTimeoutMs`); and the granularity guardrail landed at runtime
rather than registration (EventId 1732, joined in phase 14 by its capacity-shaped sibling 1741).
The original questions, kept for the reasoning:

- **The load metric and its thresholds.** Commit-loop utilization is the honest saturation signal
  (it is the ceiling that matters), but the window length, the hot/cold thresholds, and the dead
  zone between them need names under `Cluster:` and defaults measured against the reference
  workload, not guessed.
- **Quiesce semantics during step 2.** Queue-with-timeout at the gateway (the handoff precedent)
  versus transient-rejection passthrough to the client — the handoff answer (queue, capped, then a
  retryable error) is presumably right, but the cap for a drain may want to be longer, since
  recovery of a large shard is slower than a player import.
- **Granularity guardrail.** Whether registration should warn when a spatial strategy's block size
  makes one shard larger than some share of a node's measured capacity — the trap is choosing
  island-sized shards and discovering at peak that the unit of shedding is too big. Probably a
  startup log line with the arithmetic in it, in the spirit of EventId 1723.
- **Scale-in shipping order.** Ship scale-out first and consolidate manually, or hold the feature
  until both directions exist. Leaning ship-out-first: the 2 p.m. problem is the outage; the
  2 a.m. problem is a bill.
- **Ticket lifecycle on provisioner failure.** How long the hub waits on a `ProvisionTicket` before
  re-requesting, and whether it re-requests at all versus surfacing an operator alert — money is
  involved, so the default should probably be one retry then alert, never an unbounded loop.
