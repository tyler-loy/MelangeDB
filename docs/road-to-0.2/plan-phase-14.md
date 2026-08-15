# Phase 14 — Clustering IV: provisioned capacity and scale-in

**Goal:** the fleet itself follows load. When every node is hot, the hub obtains one more through a
capacity seam and the loop spreads onto it; when the fleet is cold, the hub drains its emptiest
node and gives it back. The 2 p.m. problem and the 2 a.m. bill, both.

**Depends on:** [13](plan-phase-13.md) — the drain and the loop are the primitives this phase
composes; nothing here touches a shard except through them.

## Why here

Phase 13's loop can only rearrange the nodes it has. This phase adds the two fleet-level moves —
*provision, then reassign* and *drain, then decommission* — behind a seam, because which cloud (or
rack, or stack of warm processes) supplies a node is the deployment's business, not MelangeDB's.
It is a separate phase because the failure modes change character: phase 13's worst case is a
hitch; this phase's worst cases are money-shaped (a runaway loop provisioning unbounded instances,
an orphaned node billing forever) and the design must be conservative in a way a same-fleet
rebalancer need not be.

## Deliverables

**The capacity seam.** Per the design record:

```csharp
public interface INodeProvisioner
{
    Task<ProvisionTicket> RequestNodeAsync(CapacityRequest request, CancellationToken ct);
    Task DecommissionAsync(NodeId node, CancellationToken ct);
}
```

A DI registration, not a configuration string — the membership-store precedent: a provisioner is a
component with credentials, not a name. No registration means the fleet is fixed and phase 13
behaviour is unchanged. The contract clauses that do the safety work, tested as behaviours:
fire-and-track (the loop records the ticket and moves on; a provisioned node announces itself by
joining membership like any other node — there is no special path to keep correct); at-least-once
made safe by fencing (a node arriving after its ticket expired owns nothing and can write nothing;
the surplus is decommissioned, a cost, never a correctness problem); shared-storage access is part
of the contract (a node that joins but cannot open the shard-log store fails its first assignment
loudly rather than sitting in membership looking assignable).

**Provision-then-reassign.** The loop's second move, taken only when the first is unavailable:
every live node sustained-hot *and* no ticket already outstanding. Bounded twice over —
`Cluster:MaxNodes` is a hard ceiling the loop never crosses, and it must be set explicitly for the
loop to provision at all: a registered provisioner with the ceiling unset is refused loudly at
startup, because unbounded-by-default is this phase's defining failure mode.

**Ticket lifecycle.** One outstanding ticket at a time per scale-out decision; expiry after
`Cluster:ProvisionTicketTimeoutMs` triggers exactly one re-request, and a second expiry raises an
operator alert (log + health check, the applier-stall precedent) and stops asking. Money is
involved: the default posture on repeated failure is *tell a human*, never *keep trying*.

**Standbys, for free.** A pre-warmed node is just a node in membership owning zero shards; the loop
prefers assigning to it over provisioning by construction (move one is always tried first). The
deliverable is the documentation stating this shape and a sample, not new machinery — a game that
cannot afford minutes of provision latency during a surge runs its own standby pool and the seam
never fires.

**Scale-in.** Behind its own switch (`Cluster:ScaleInEnabled`), because giving nodes back is the
half with sharp edges. When the fleet's aggregate sustained load fits under the cold threshold on
`N − 1` nodes with headroom to spare, the loop drains the emptiest node's shards onto the rest
(phase 13 drains, one at a time, rate-limited) and calls `DecommissionAsync` only after membership
confirms the node owns nothing. `Cluster:MinNodes` floors the fleet. The dead zone between the
scale-out and scale-in thresholds is deliberately wide, and a node the loop just provisioned is
exempt from consolidation for a long cooldown — the two moves must never take turns.

**A reference provisioner for tests and samples.** An in-repo `ProcessNodeProvisioner` that spawns
and kills local shard-node processes — enough to test every lifecycle behaviour end to end and to
show a deployment what implementing the seam looks like. Explicitly not a cloud integration;
shipping one would make MelangeDB opinionated about exactly the thing the seam exists to stay out
of.

**Observability.** Fleet-size gauge, tickets outstanding, provision latency histogram,
decommissions counter, scale-decision log lines carrying the arithmetic, and the ticket-failure
alert. Recorded in [OBSERVABILITY.md](../OBSERVABILITY.md) with the change.

**Configuration** (planned rows in [CONFIGURATION.md](../CONFIGURATION.md)): `Cluster:MaxNodes`,
`Cluster:MinNodes`, `Cluster:ScaleInEnabled`, `Cluster:ProvisionTicketTimeoutMs`.

## Out of scope

Any concrete cloud-provider integration (AWS/Azure/GCP packages) — the seam is the product; the
reference implementation is process-local and test-shaped. Autoscaling the hub — the hub's ceiling
is its Postgres, per CLUSTERING.md's open question, and remains one. Dynamic boundary splitting —
still deferred, still recorded. Bin-packing optimality — the loop places shards greedily; a
provably optimal packing is not worth the moves it would take to reach it.

## Decisions to settle

### `Cluster:MaxNodes` has no default on purpose

Leaning: refuse to start the provisioning half when a provisioner is registered and the ceiling is
unset. Every default here is wrong: low silently caps a deployment that meant to scale, high is a
silent spending authorization. The one-line refusal names the key and the reasoning. To settle:
whether the refusal is startup-fatal or degrades to phase 13 behaviour with a loud health check —
leaning fatal, because a deployment that registered a provisioner meant to use it.

### What `CapacityRequest` carries

Leaning: the hub's view of why — the sustained load summary and the shard it intends to move — so
a provisioner can size the instance. To settle: how much of that is contract versus opaque
diagnostic payload; a provisioner that *branches* on load numbers is coupling its instance types to
MelangeDB's metric definitions, which the seam should discourage rather than enable.

### Decommission of a node that will not drain

A node whose shards cannot move (destination refuses, drains repeatedly wedge) blocks scale-in
forever. Leaning: that is correct — scale-in is an optimization, and the alert (drain failures are
already loud from phase 13) is the escalation path. The alternative — force-fencing a stubborn node
and letting node-death reassignment clean up — converts a billing annoyance into a player-visible
outage on purpose, which the loop should never do on its own authority.

### Does consolidation prefer the newest node or the emptiest?

Leaning: emptiest (fewest moves, least disturbance), with the provision-cooldown exemption
preventing the pathological case (newest is emptiest by definition). To settle: whether operators
need a per-node `no-consolidate` pin for nodes that exist for reasons the load view cannot see —
leaning yes, as membership metadata rather than configuration.

## Done when

- With the reference provisioner: an all-hot two-node fleet grows to three within one window plus
  provision latency, the hot shard lands on the new node, and — load removed — the fleet drains
  back to `MinNodes` and the surplus processes exit. The whole curve runs as one test.
- A ticket that never materializes re-requests once, then alerts and stops; the health check
  surfaces it; a node arriving late is decommissioned without ever owning a shard.
- The loop provably cannot exceed `MaxNodes` or dip under `MinNodes` — asserted under an
  adversarial load script that begs for both.
- An oscillating load curve at the dead-zone boundary produces zero provision/decommission churn
  over a simulated day — the scale-out/scale-in flap test, this phase's equivalent of phase 13's
  oscillating-skew guard.
- A registered provisioner with `MaxNodes` unset is refused at startup with the reasoning in the
  message; no provisioner registered leaves phase 13 behaviour bit-identical.
- Configuration rows flip to shipped; seam, ticket, standby, and decommission enter GLOSSARY.md;
  the design record's status updates to built.

## Risks

- **Money-shaped bugs.** The ceiling, the single-ticket rule, the one-retry-then-alert posture, and
  the flap test are all aimed at the same failure: a loop that spends autonomously must be bounded
  by construction, not by review.
- **The provisioner is user code on the hub's critical path.** A `RequestNodeAsync` that blocks or
  throws must not stall the rebalance loop — calls are isolated the way event-bus handlers are
  (timeout, no loop-thread execution), and a poisoned provisioner degrades the fleet to fixed, not
  the hub to dead.
- **Scale-in racing real load.** The 2 p.m. crowd arriving mid-consolidation is the nasty
  interleaving: a drain in flight when the fleet turns hot should complete (drains are short) but
  the *decommission* must re-check membership and load at the last moment — decommissioning a node
  the loop now needs is the one mistake players see.
- **Seam design lock-in.** `INodeProvisioner` is public API; once a deployment implements it,
  reshaping it is a break. The reference implementation and the phase 13 load view existing first
  are what give the contract a real consumer before it freezes.
