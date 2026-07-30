# Phase 09 — Clustering I: placement, hub/shard roles, instancing

**Goal:** more than one node, with tables declaring where they live and the developer defining what a shard
means. Instancing works end to end.

**Depends on:** M1 complete (01–08).

## Why here

First multi-node phase, and it deliberately implements the **easy** sharding strategy. Instanced shard
transitions are explicit and discrete — a portal, a loading screen — so this phase needs no border overlap, no
interest computation, and no seamless transfer. Phase 10 adds those for continuous worlds. Same mechanism, a
fraction of the machinery, and a working cluster much sooner.

## Deliverables

**The four placements** — the entire developer-facing mental model, one per table:

| Placement | Lives where | Written by |
| --- | --- | --- |
| `Partitioned` | Split across shard nodes by shard key | The one node owning that shard |
| `Replicated` | Full copy on every node | Hub only; shards hold read-only copies |
| `Global` | Hub node only | Hub only |
| `Local` | One node, never leaves | That node |

- `Partitioned` tables have **one writer per shard, many readers** — other nodes may hold read-only slices,
  which is what lets a node see entities it may not mutate.
- `Replicated` converges with `Residency.Resident` and with the 52 `.Iter()` scan targets: replicate it
  everywhere, pin it in RAM, scan it freely.
- `Global` converges with the Postgres tier — the hub's `Global` tables *are* the relational tier.

**Node roles.** Hub (identity, `Global` + `Replicated` tables, Postgres tier, shard assignment) and shard
node (`Partitioned` tables for its shards, scheduled reducers for its shards). A client holds a permanent hub
attachment plus a moving shard attachment.

Placement rule to document prominently, because it prevents the expensive mistake:

> A table belongs on the hub only if it is **not** written in the same transaction as shard-local world state.
> More generally: place tables so transaction boundaries fall inside a node.

The trap this catches: `InventoryItem` looks like hub data, but gathering decrements a `ResourceNode` (shard)
and adds an inventory row in one transaction. Hub placement would force distributed commit onto the hottest
path in the game, thousands of times a minute. Player-owned tables are `Partitioned` and follow the player.

**`IShardStrategy`** — MelangeDB supplies the mechanism, the developer supplies the meaning:
```csharp
public interface IShardStrategy
{
    ShardKey ShardForRow(TableId table, in RowRef row);
    ShardKey ShardForSession(SessionContext session);
    IReadOnlyList<ShardKey> InterestOf(ShardKey shard);
}
```
Ship the **instancing** strategy: shard key is an explicit instance id column, instances are causally
disjoint, `InterestOf` is empty.

**Per-shard commit logs.** One log per shard. No global total order — only per-shard order plus causal
ordering via handoff and the event bus. Stated plainly as a documented trade, because it is what makes writes
scale.

**Gateway.** Terminates client connections and routes reducer calls and subscriptions to owning nodes, so the
client sees one endpoint and never learns the topology.

**The shard-span check.** MelangeDB cannot statically verify the one contract the developer must uphold —
*rows mutated in the same transaction must resolve to the same shard.* So a debug-mode check fails loudly when
a write set spans shard keys. Without it, violations surface as mysterious latency under load instead of as a
test failure.

**Explicit handoff.** Freeze on origin → append on destination → confirm → release on origin, as a small saga
recoverable because both logs record their half. A fencing token prevents a wrongly-suspected-dead node from
continuing to write a player it no longer owns.

**Distributed `IEventTransport`** for cross-shard events and sagas, replacing phase 06's in-process transport
with no change to handler code.

## Out of scope

Spatial partitioning and seamless handoff (10). Dynamic rebalancing — static shard assignment only. Sharding
the hub.

## Decisions to settle

- **Cluster membership.** Ownership registry, failure detection, and reassigning a dead node's shards. Could
  be Postgres-backed (the hub already needs it) rather than a new consensus dependency — worth taking, since
  it avoids introducing Raft one phase early.
- **Client protocol during dual attachment.** A client holds a hub session and a shard session; the gateway
  must present that as one endpoint, including which node answers which subscription.
- **Does `Replicated` need write support from shards?** Say no as long as possible.
- **How do reducers that touch only `Global` tables get routed** — hub-executed, or shard-executed with a
  remote call? Hub-executed is simpler.

## Done when

- A hub plus two shard nodes run; a client connects to the gateway and cannot tell how many nodes exist.
- A reducer touching only its own shard commits locally with no cross-node traffic — asserted by counting
  network calls, not by inspection.
- `Replicated` reference data is identical on all nodes and updates propagate from the hub.
- A `Global` write from a shard-attached client reaches the hub and is visible cluster-wide.
- Moving a player between instances transfers their partitioned rows; the player is never writable on two
  nodes at once, and a kill mid-transfer recovers to exactly one owner.
- A deliberately shard-spanning transaction trips the debug check with a clear message.
- Scheduled reducers fire once per shard on the owning node — never twice, never zero times.
- Killing a shard node reassigns its shards; the fencing token prevents the revived node from writing.
- A single-node deployment ignores placement entirely and behaves exactly as in M1.

## Risks

- **The mental model is the deliverable.** If a developer can't predict which node runs their reducer, the
  feature fails regardless of correctness. Documentation and the shard-span diagnostic are not follow-up work.
- **Instancing may reveal that the gateway is the real bottleneck**, since every client's traffic crosses it.
  Measure early; the answer may be that clients connect directly to shard nodes with the gateway only
  directing.
