# Clustering: placement, node roles, and user-defined shards

[DESIGN.md](DESIGN.md) §10 deferred the clustering model. This is the resolution.

The technical mechanism is the easy half. **The hard half is conveying multi-node in a way a developer
can reason about**, and that is a real risk to the project: a clustering model nobody can hold in their
head gets used wrong, produces mystifying bugs, and gets blamed on the database. So this document leads
with vocabulary and only then describes machinery.

Two commitments shape everything below:

1. **The developer declares placement per table**, from a small fixed vocabulary — four options, not a
   configuration language.
2. **The developer defines the sharding function itself.** MelangeDB does not decide what a shard *means*.
   A contiguous open world and an instanced MMO city are different answers, and both are correct.

## The four placements

Every table declares exactly one. This is the whole mental model.

| Placement | Lives where | Written by | Use for |
| --- | --- | --- | --- |
| **`Partitioned`** | Split across shard nodes by shard key | The one node owning that shard | World entities: terrain, creatures, buildings, drops |
| **`Replicated`** | Full copy on every node | Hub only; shards get read-only copies | Small bounded reference data: item defs, recipes, species |
| **`Global`** | Hub node only | Hub only | Accounts, registration, social, world statistics |
| **`Local`** | One node, never leaves it | That node | Per-node caches, scratch state, telemetry buffers |

```csharp
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(ChunkId))]
public partial struct Creature { public ushort ChunkId; /* ... */ }

[Table(Public = true, Placement = Placement.Replicated)]      // pinned resident everywhere
public partial struct ItemDefinition { /* ... */ }

[Table(Placement = Placement.Global, Tier = StorageTier.Relational)]
public partial struct Registration { /* ... */ }
```

`Replicated` converges with two earlier decisions: it is exactly the set that wants
`Residency.Resident` (DESIGN.md §8) and exactly what the audited game's 52 `.Iter()` scans run over.
One declaration, three problems solved — replicate it everywhere, pin it in RAM, scan it freely.

`Global` converges with the Postgres tier: **the hub's `Global` tables are the relational tier.** The
"servicey" split you'd already built by hand is the same split the cluster needs.

### Partitioned tables are readable beyond their owner

"Some tables will be alive *in part* on all nodes" — yes, and this is a distinct thing from `Replicated`.
A `Partitioned` table has exactly one **writer** per shard, but other nodes may hold **read-only** slices
of it. That's what lets a node render players and creatures just across a boundary without being allowed
to mutate them. Interest — which foreign slices a node subscribes to — is derived from the shard strategy,
not hand-configured.

The invariant to teach: **one writer per shard, many readers.**

## Node roles: hub and shard

The **hub** / **shard node** split (originally framed as master/daughter — see
[GLOSSARY.md](GLOSSARY.md)) is the right topology. A player holds **two** attachments at once: a permanent one
to the hub, and a moving one to whichever shard node owns where they are.

```
                    ┌─────────────┐
   client ──────────│     HUB     │   Global + Replicated tables, identity,
        │           │             │   Postgres tier, shard assignment
        │           └──────┬──────┘
        │                  │ replicates reference data, owns Global writes
        │        ┌─────────┴─────────┐
        └────────│    SHARD NODE     │   Partitioned tables for its shards,
                 │   (shard 12,13)   │   scheduled reducers for its shards
                 └───────────────────┘
```

The hub is *not* a bottleneck by construction, because its tables are the ones not touched by
moment-to-moment gameplay. That is also the test for what belongs there:

> **A table belongs on the hub only if it is not written in the same transaction as shard-local world state.**

### The trap this rule exists to catch

`InventoryItem` looks like player-owned hub data. It is not — put it on the hub and you have broken the
system.

Gathering is the single most common action in a survival game: decrement a `ResourceNode` (partitioned,
shard node) and add an `InventoryItem` (hub). That is a cross-node transaction on the hottest path in
the game, thousands of times a minute, and it forces distributed commit into the common case — the exact
thing this design avoids everywhere else.

So `InventoryItem`, `PlayerSkill`, `PlayerAttribute`, `EquipmentSlot`, `PlayerCombatState`, and
`PlayerState` are **`Partitioned` and follow the player**, sharing the player's current shard key. They
transfer on handoff. Handoff is comparatively rare; gathering is not.

What genuinely belongs on the hub: `Registration`, accounts and auth, `AdminIdentity`, `WorldStat`,
social/guild/friends state, and trade *history*. None of those are written by a gather, a swing, or a
step.

The general form of the rule: **place tables so that transaction boundaries fall inside a node.** If two
tables are mutated together, they belong in the same place. That single sentence is most of what a
developer needs to get placement right.

## Sharding is user-defined

MelangeDB supplies the mechanism — one writer per shard, per-shard logs, handoff, interest — and the
developer supplies the meaning:

```csharp
public interface IShardStrategy
{
    // Which shard owns this row?
    ShardKey ShardForRow(TableId table, in RowRef row);

    // Which shard is this session currently attached to?
    ShardKey ShardForSession(SessionContext session);

    // Which foreign shards must this shard hold read-only slices of?
    IReadOnlyList<ShardKey> InterestOf(ShardKey shard);
}
```

### Strategy A — spatial partitioning (Vibe Shaft)

A contiguous 10km world of 157×157 chunks. Shard = a rectangular block of chunks; shard key derives from
`ChunkId`. Interest = the eight neighbouring blocks, narrowed per row to the border band
(`Cluster:BorderBandChunks` deep). Handoff is **continuous and implicit** — triggered by walking.

Shipped in phase 10 as `SpatialShardStrategy`: the developer supplies the geometry (`SpatialGeometry` —
block dimensions in chunks and the chunk-id decoder, because the chunk encoding is the game's, not ours) and
the strategy supplies block math, eight-neighbour interest, band membership, and boundary assessment. Three
contracts worth knowing:

- **The shard key packs two full 32-bit block coordinates**, so the world can grow in any direction without a
  key migration. The chunk-id *column* must be at least 32 bits wide, enforced at registration: the reference
  workload's `cx * 157 + cy` in a `ushort` tops out at 65,535, so a 20 km world overflows it — a trap that
  must fail at startup, not surface as a migration under load.
- **Ownership widens at the seam.** The strict rule — a shard commits only rows resolving to itself — would
  freeze the world at every boundary line, because an entity mid-handoff stands *across* the line while its
  origin still owns it. `MayCommit` therefore admits rows up to the band's depth inside a neighbouring block;
  beyond the band the write fails loudly, because an entity that deep into foreign territory means handoffs
  are not keeping up and the band was sized too shallow.
- **Transferred rows re-home by content** (`RowRehoming.ByContent`): a spatial row's chunk id *is* its shard,
  so the import asserts the row already resolves to the destination instead of rewriting anything —
  instancing's column rewrite would corrupt a chunk id.

Splitting a single crowded location makes no sense here: everyone in the town square interacts with
everyone else, so they must share a writer. **This is the hard case** — see the hotspot ceiling below.

### Strategy B — instancing (WoW-style)

Shard = an explicit instance id, already a column on the row. No geometry, no interest overlap between
instances (they are causally disjoint by definition), and handoff is **explicit and discrete** — you
enter a portal, and the loading screen *is* the handoff window.

```csharp
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(InstanceId))]
public partial struct Creature { public uint InstanceId; /* ... */ }
```

Here "200 players in one city" is solved by putting 100 in city instance 1 and 100 in instance 2 —
precisely the option unavailable to a single-world game. **This is the easy case**, and it should ship
first: it needs no border overlap, no interest computation, and no seamless transfer.

### They compose

Real MMOs are both at once: a continuous open world *plus* instanced dungeons and city shards. So a
deployment registers more than one strategy, and each table group names the one it uses. Strategy is a
property of a table group, not of the cluster.

### The one contract the developer must uphold

MelangeDB cannot verify this, so it has to be stated loudly:

> **Rows that are mutated in the same transaction must resolve to the same shard.**

Get this right and virtually every transaction is single-shard and commits locally. Get it wrong and you
get either correctness bugs or a system that silently degrades into distributed commits. A debug-mode
check should fail loudly when a transaction's write set spans shard keys, so violations surface in
development rather than under load.

## Handoff

Two shapes, following from the strategy:

- **Explicit** (instancing, portals, zone transitions): the transition is a discrete player action with a
  natural pause. Freeze, transfer, resume. Simple, and no overlap machinery required.
- **Continuous** (open world): the destination node begins streaming border chunks before the player
  arrives, the player's partitioned rows transfer, then the origin drops the band. Seamless, and the
  reason interest overlap exists.

Either way, transfer is the one unavoidable distributed transaction: the player must not be writable on
two nodes at once, and must not vanish if a node dies mid-transfer. It runs as a small saga — *freeze on
origin → append on destination → confirm → release on origin* — recoverable because both logs record
their half, so an interrupted handoff replays. A fencing token prevents a wrongly-suspected-dead node
from continuing to write a player it no longer owns.

Three consequences of "recoverable from both logs," shipped in phase 09 and worth stating because each is
the fix for a silent failure mode:

- **Live saga markers pin log truncation.** A pending freeze pins the origin's log from the marker onward;
  an import pins the destination's log until the origin is known settled. Without the pin, the shard's own
  routine snapshot could erase the marker mid-transfer — a restarted origin would silently unfreeze a
  half-transferred player, and a restarted destination would answer "never imported" while holding the rows:
  two owners either way. The pin is bounded, because every saga resolves.
- **A reconciler, not just crash recovery, resolves stranded sagas.** Each shard node periodically resolves
  every saga half it holds: a pending freeze asks (via the hub) whether the destination's import became
  durable — release if yes, abort if definitively no, wait otherwise — and an unsettled import asks whether
  the origin's freeze is still pending, settling (and unpinning) once it is not. Every step is idempotent,
  which is what makes it correct across any combination of crashes, and an in-flight saga always answers
  "wait" so the reconciler never races its own coordinator.
- **An unknowable import failure leaves the player frozen, deliberately.** If the import request times out
  or the link dies, the destination may or may not hold the import — an ack lost in transit looks identical
  to a dead node. Aborting blind could mint two owners, so the freeze stays (the player is writable
  *nowhere*) until the reconciler learns the truth from the destination's log. Unavailable beats duplicated.
  Only an error *reply* from the destination — a definitive "did not happen" — aborts immediately.

One structural rule falls out of how re-homing works: **`ShardBy` must not be the primary key** (compile
error MELANGE0018, mirrored at schema registration). Handoff rewrites the row's `ShardBy` column while the
stored row key — the encoded primary key — stays fixed; a primary-key shard column would silently diverge
from its key on the first transfer. The shard id is its own column.

## What holds regardless of strategy

- **Each shard is internally single-writer** — one serialized transaction loop, one commit log, exactly
  the semantics that make reducers pleasant. Multi-writer is a property of the *cluster*. The reducer
  programming model does not change.
- **One commit log per shard.** No global total order across the cluster — only per-shard order plus
  causal ordering via handoffs and the event bus. You cannot ask "what was the whole world's state at
  instant T." Games don't need that; ledgers do. Accepting it is what makes writes scale.
- **`[Unique]` is a single-writer guarantee.** A unique index is enforceable only inside one shard's
  writer, so unique columns are restricted to non-partitioned tables (`Global`, `Replicated`, `Local`) —
  a compile-time diagnostic, not a runtime surprise. A globally-unique *claim* over partitioned data
  (player names) lives in a small `Global` claims table on the hub. `[AutoInc]` stays coordination-free
  for the same structural reason: its contract is unique-not-dense, so each shard allocates from an
  originator-prefixed range and no cross-shard sequence exists.
- **Scheduled reducers partition with their rows.** Timers are rows (DESIGN.md §3), so a timer fires on
  whichever node owns its shard — no global timer wheel, no leader election. Vibe Shaft's single global
  `CreatureAiTick` row becomes one row per shard, and the hand-written "only chunks near a player"
  filtering becomes implicit in the partition.
- **A replication gap that truncation erased is bootstrapped, never skipped.** A node whose replica cursor
  fell below the hub log's truncation base (down while the hub snapshotted) cannot be served from the log —
  the gap's records are gone. The hub sends the full current `Replicated` state at one LSN instead
  (EventId 1711), and the node applies it as upserts *plus deletion of local rows absent from the snapshot*,
  because a pure upsert bootstrap would resurrect rows the hub deleted during the gap. The same rigor the
  Postgres tier's phase 08 bootstrap established; silently resuming past a truncated gap is the bug class
  both exist to kill.
- **Policies evaluate where the subscription fans out, and may read only tables present there** (settled,
  phase 09). Subscription fan-out for a `Partitioned` table runs on its shard node, so a row policy there may
  read `Replicated`, `Partitioned`, and `Local` tables; a read of a hub-only `Global` table fails loudly with
  the fix in the message rather than answering empty. The flagship "admins see everything" policy reads an
  `AdminIdentity` table — which is exactly the small, bounded, read-mostly shape `Replicated` exists for.
  DESIGN.md §7's "policies may freely read private tables" carries this placement qualifier now: private is
  not the constraint; placement is.
- **Reducers touching only `Global` and `Replicated` tables are hub-executed** (settled, phase 09). The
  execution site is resolved at compile time from the body's table touches into the reducer descriptor —
  `Hub` when every touch is `Global`/`Replicated`, `Shard` otherwise, including bodies the analysis cannot
  see through (which fail loudly on the shard if they were really hub-shaped, with
  `[Reducer(Site = ReducerSite.Hub)]` as the stated fix). The gateway routes calls by it.
- **A shard may itself be replicated** (Raft across nodes) for HA later, independently of this design.

## Two axes of scale, not one

Sharding alone does not solve the N² memory problem — it re-bills it as more machines. The world's cost
has two terms that grow differently:

| Term | Grows with | What it is | Fixed by |
| --- | --- | --- | --- |
| **Cold world** | **Area (N²)** | Terrain, flora, water, LOD blobs — ~24.6k chunk rows, nearly all far from any player | **Paging** (DESIGN.md §8) |
| **Live simulation** | **Player density** | Creatures, combat, buildings being ticked | **Sharding** (this doc) |

Vibe Shaft's own `CreatureAiTick.cs` already says it: *"everything else in the world is inert, which is
what makes a persistent world-wide population affordable."* Most of a 10km world is cold at any instant.

Doubling that world to ~20km means 314×314 = 98,596 chunks, 4× the memory — the wall you hit. (It also
overflows `ushort ChunkId`, since `cx * 157 + cy` tops out at 65,535; the key encoding widens regardless.)
Paging is what lets one node own a shard far larger than its RAM. **Paging ships first** — it attacks the
bigger term and needs no coordination layer at all.

## Open questions

- **Shard assignment and rebalancing.** Static assignment shipped in phase 09: shards are created at
  runtime, assigned least-loaded-first by the hub's membership store, and reassigned only on node death
  (fencing tokens bumped; the new owner recovers the shard from the shard's own log on shared storage).
  Player density is wildly uneven, so fixed shards will hotspot; dynamic splitting (a quadtree subdividing
  under load) is where the spatial strategy ends up, and it's a substantial subsystem. Instancing sidesteps
  it — another reason it shipped first.
- **The hotspot ceiling is strategy-dependent, and worth telling users plainly.** Spatial partitioning
  cannot split a single crowded location; instancing can. A developer choosing a strategy is choosing
  which failure mode they get, and that should be documented at the point of choice.
- ~~**Cluster membership.**~~ **Settled in phase 09: Postgres-backed, not Raft.** The ownership registry —
  nodes, per-shard owner, fencing token, and originator id — lives in the hub's own Postgres
  (`AddPostgresClusterMembership()`; in-memory for tests), the hub is its sole writer, failure detection is
  heartbeat silence past `Cluster:FailureTimeoutMs`, and a dead node's shards reassign under bumped fencing
  tokens while the suspect self-fences on the same clock. See docs/plan-phase-09.md for the rationale.
- ~~**Client protocol during dual attachment.**~~ **Settled in phase 09: the dual attachment is
  server-internal.** The wire protocol stays one socket, one session; the gateway owns the hub-plus-shard
  mapping and the client never learns it. See docs/plan-phase-09.md.
- **Does the hub shard?** For a very large deployment the hub's `Global` tables become the ceiling. Since
  they're the Postgres tier, the answer is probably "Postgres's problem, not ours" — but it needs saying.
  Explicitly out of phase 09's scope; still open.
