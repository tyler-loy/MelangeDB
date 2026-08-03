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

**Id allocation under partitioning.** Phase 01's AutoInc contract (unique-not-dense, originator-prefixed
64-bit) gets exercised here: the membership store assigns each shard owner an originator id, and two shards can
never mint the same value with no coordination on the hot path. Relatedly, **`[Unique]` is restricted to
non-partitioned tables** (compile-time diagnostic, phase 02) — a unique index is a single-writer guarantee, and
globally-unique claims over partitioned data live in a `Global` claims table instead.

**Explicit handoff.** Freeze on origin → append on destination → confirm → release on origin, as a small saga
recoverable because both logs record their half. A fencing token prevents a wrongly-suspected-dead node from
continuing to write a player it no longer owns.

**Distributed `IEventTransport`** for cross-shard events and sagas, replacing phase 06's in-process transport
with no change to handler code.

## Out of scope

Spatial partitioning and seamless handoff (10). Dynamic rebalancing — static shard assignment only. Sharding
the hub.

## Decisions to settle

Each settled when the phase shipped; the subsections are the record.

### Settled: cluster membership is Postgres-backed, not Raft

The ownership registry (`IMembershipStore`) — registered nodes, per-shard owner, fencing token, and
originator id — is owned and written exclusively by the hub, and persists in the hub's own Postgres
(`PostgresMembershipStore`, opted in with `AddPostgresClusterMembership()`; an in-memory store serves tests
and single-process clusters). Rationale: the hub already has Postgres for its Global tier, membership
mutations are rare (register, heartbeat, create-shard, reassign-on-death) so an exclusive table lock is
plenty, and what actually must survive a hub restart is small and relational-shaped — fencing tokens (a
restarted hub must never re-mint an old one) and originator ranges. Introducing Raft one phase early would
have bought availability of the *hub* — a non-goal until "does the hub shard?" is answered — at the price of
a consensus dependency in every deployment. Failure detection is heartbeat silence past
`Cluster:FailureTimeoutMs`; reassignment bumps fencing tokens; shard nodes learn assignments over their node
links, never by reading the store.

### Settled: inter-node identity is a hub-minted signed assertion over a shared cluster secret

Exactly as the plan sketched. The gateway validates a client's IdP JWT once; every upstream node session
authenticates with an `InternalIdentityAssertion` — HMAC-SHA256 over `Cluster:Secret`, carrying identity,
guest/owner claims, expiry (capped by `Cluster:AssertionTtlSeconds`, never outliving the client token), and
whether the session fires lifecycle reducers (only the hub attachment does — one real session start per
client). Node links mutually authenticate with the same secret over exchanged nonces, so neither a rogue
dialer nor a fake hub passes the handshake, and assertions are refused at the gateway itself so a client can
never present one. The trust boundary is stated in docs/THREAT-MODEL.md rather than left as an accident: any
holder of the cluster secret can assert any identity — a compromised shard node impersonates every player in
its shards — accepted because nodes are your infrastructure, with the operational consequences (internal
network for node endpoints, secret rotation invalidates all assertions) written down.

### Settled: the dual attachment is server-internal; the client protocol does not change

The client speaks the ordinary one-socket protocol to one endpoint (the gateway, on the hub). The gateway
holds the hub session and the moving shard session, routes `CallReducer` by the descriptor's execution site
and `Subscribe` by the named table's placement, tracks which upstream owns each subscription id, and forwards
node frames verbatim — the client cannot tell how many nodes exist, which is the acceptance test. Three
protocol consequences, chosen deliberately: the client's `Welcome` carries the hub log's epoch; `Resume`
through the gateway always answers full resync (a resume cursor counts against one log, and a gateway session
spans several — the client converges through the same path a rejected resume always used); and when a shard
attachment is re-established under the client (handoff, node death), the gateway sends the existing
`overflow_resync` error, telling the client to re-establish subscriptions — under instancing that moment is a
portal with a loading screen, which is the whole reason instancing shipped first.

### Settled: `Replicated` takes no writes from shards

No, as the plan hoped, and enforced rather than assumed: a shard engine refuses a `Replicated` write at the
point of access ("written only by the hub — route the write through a hub-executed reducer"), and the
commit-point guard backstops the bulk path. Replication is strictly hub log → node, applied per shard engine
with a persisted cursor that travels with the shard's directory.

### Settled: policies evaluate where the subscription fans out, and may read only tables present there

Row and column policies for a `Partitioned` table's subscription evaluate on the owning shard node, against
that shard engine's committed view — so they may read `Replicated`, `Partitioned`, and `Local` tables, and a
read of a hub-only `Global` table throws at the point of access with the fix in the message (make the table
`Replicated`, or hub-execute the reducer). The other options lost on their merits: evaluating at the gateway
would re-serialize every candidate row across a node boundary to apply a predicate, and pushing policy state
with the assertion turns a token into a cache with invalidation problems. The flagship `AdminIdentity` case
is precisely `Replicated`-shaped data — small, bounded, read-mostly. DESIGN.md §7's "policies may freely read
private tables" now carries the qualifier: private is not the constraint; placement is.

### Settled: reducers touching only `Global`/`Replicated` tables are hub-executed

Hub-executed, resolved at **compile time**: the generator analyzes each reducer body's table touches through
`ctx.Db` and emits the execution site into the descriptor — `Hub` when every touch is `Global` or
`Replicated` (and for lifecycle reducers, which are session events on the hub attachment), `Shard` otherwise.
A body the analysis cannot see through (passes `ctx` to a helper, inferred-generic `IDbView` calls) resolves
to `Shard`, where a genuinely hub-shaped reducer fails loudly with the stated fix:
`[Reducer(Site = ReducerSite.Hub)]`, which always wins. Conservative-plus-loud beats clever-and-wrong here —
a misrouted reducer is a clear error message, never silent empty reads.

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

## Shipped notes

Boundaries drawn while shipping, recorded so they read as decisions rather than surprises:

- **A shard is an engine.** "One commit log per shard" is implemented literally: each shard is a full engine
  instance (own log, own hot store, own scheduler, own reducer host, own subscription fan-out) opened by
  whichever node the membership store names. Per-shard order, single writer, and timers-follow-their-shard
  all fall out of that one identity instead of being separately engineered.
- **Reassignment assumes shared or re-attachable storage** (`Cluster:ShardDataPath`): the shard's directory
  *is* the shard, and the new owner opens it and recovers from the shard's own log. Log shipping for
  non-shared storage is a later phase.
- **Cluster events are handled on the hub.** Shard-published events forward from each shard's log
  (at-least-once, acked cursor) and dispatch to the hub's handlers; handler code is unchanged, but foreign
  events have no per-subscriber durable checkpoint and no dead-letter file — a handler that exhausts retries
  is a loud log (EventId 1704), and the source log still holds the event. Shard-side handler execution
  (interest-scoped delivery) is phase 10 territory alongside interest itself.
- **The freeze covers the collected row set.** Handoff freezes exactly the rows the `IHandoffSet` selector
  named, atomically with collecting them; rows created *for that player* by a concurrent reducer during the
  (short, explicit) transfer window are not retroactively frozen. Instancing's discrete transitions make the
  window a loading screen; the spatial strategy will need more.
- **Log truncation respects live cluster state** (hardened in the review round; details in
  docs/CLUSTERING.md). Pending handoff markers pin their shard's log until the saga resolves — an unresolved
  freeze or unsettled import can never be snapshotted away — and each node's reconciler resolves stranded
  saga halves idempotently, including the unknowable-import case, where the player deliberately stays frozen
  (unavailable beats duplicated). Event forwarders floor truncation at their forwarded cursor. A replica
  cursor below the hub log's truncation base triggers a full-state bootstrap (upserts plus
  absent-row deletes, EventId 1711) instead of silently skipping the gap — the phase 08 silent-gap bug
  class, killed in its phase 09 habitats.
- **`ShardBy` must not be the primary key** (MELANGE0018, mirrored at schema registration): handoff rewrites
  the shard column while the stored row key stays fixed, so a primary-key shard column would silently
  diverge from its key on the first transfer.
- **The gateway forwards frames sequentially per client** — no lane prioritization at the gateway hop (each
  node's own transport still interleaves bulk and interactive traffic on its leg). Revisit when the phase 10
  measurement says the gateway is the bottleneck, which the risk register already predicts.

## Risks

- **The mental model is the deliverable.** If a developer can't predict which node runs their reducer, the
  feature fails regardless of correctness. Documentation and the shard-span diagnostic are not follow-up work.
- **Instancing may reveal that the gateway is the real bottleneck**, since every client's traffic crosses it.
  Measure early; the answer may be that clients connect directly to shard nodes with the gateway only
  directing.
