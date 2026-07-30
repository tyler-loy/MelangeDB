# Phase 10 — Clustering II: spatial strategy and seamless handoff

**Goal:** a contiguous world split across nodes, with players walking between shards and never noticing.

**Depends on:** [09](plan-phase-09.md).

## Why here

This is the hard sharding case, and it exists because a single continuous world cannot use instancing. In
Vibe Shaft, everyone in the town square interacts with everyone else, so they must share a writer — splitting
a crowded location is not available. Phase 09 built the mechanism against explicit, discrete transitions; this
phase makes transitions implicit, continuous, and invisible.

It's also the phase that finally lifts the N² ceiling in combination with phase 07: doubling the reference
world to ~20km means 314×314 = 98,596 chunks, 4× the memory. Paging lets one node own a shard bigger than its
RAM; sharding spreads live simulation across nodes.

## Deliverables

**Spatial `IShardStrategy`**
- Shard = a rectangular block of chunks; shard key derives from a chunk id.
- `InterestOf` returns the eight neighbouring blocks, one or two chunks deep — the border band.
- Configurable geometry, since chunk size and world dimensions are the developer's, not ours.
- Note for the reference workload: `cx * 157 + cy` in a `ushort` tops out at 65,535, so a 20km world overflows
  the key encoding. Shard keys must be wide enough (`uint`/`ulong`) that a world can grow without a migration.

**Interest-driven read-only replication.** A shard node subscribes to its neighbours' border slices and holds
them read-only, so it can serve entities just across the boundary to its clients without being allowed to
mutate them. This is `Partitioned`'s many-readers property from phase 09, now actually exercised.

**Seamless handoff.** As a player approaches a boundary:
1. The gateway opens a session to the destination node and begins streaming its border chunks, so the client
   already holds the terrain and entities it is about to need.
2. The player's `Partitioned` rows transfer; the destination becomes authoritative.
3. The origin keeps serving the band briefly, then drops it.

Built on phase 09's saga and fencing token — the difference is *when* it triggers and that the client must
observe no interruption. **Hysteresis is required**: a player pacing across a boundary must not trigger a
handoff per step. Trigger on crossing plus a margin, and rate-limit.

**Cross-shard interaction, in priority order:**
1. **Co-locate, then transact locally.** Interaction in a game is proximity-gated — you trade with someone
   adjacent, you attack what's in range — so interacting entities are already in one shard. This covers nearly
   everything and is why spatial sharding is tractable at all.
2. **Ownership transfer** for an entity crossing a boundary (a creature chasing a player, a vehicle driven
   across) — the handoff protocol with a different entity class.
3. **Saga over the event bus** for the rare genuine case. Eventually consistent, compensating actions,
   explicitly not ACID, and documented as such.

**Observability.** Shard ownership map, handoff rate, handoffs in flight, border-band bandwidth, per-shard
transaction rate and lag. Debugging a distributed world without these is guesswork.

## Out of scope

Dynamic rebalancing / quadtree splitting — still static assignment. Region-level Raft for HA. Cross-shard
distributed transactions beyond the handoff saga.

## Decisions to settle

- **Border band depth.** Deeper is smoother and costs bandwidth and memory on every node. Should be tunable,
  with a documented default derived from movement speed and tick rate rather than guessed.
- **Who decides a handoff — gateway, origin node, or client position?** The client cannot be trusted;
  origin-decides is authoritative but adds latency to the decision.
- **What happens to a player mid-handoff who calls a reducer?** Queue on the destination, reject and retry, or
  serve from origin until release. Queueing is invisible to the player and hardest to get right.
- **Do border-band rows count against residency?** They inflate every node's footprint by its neighbours'
  edges; with eight neighbours that is not negligible.
- **Creature AI across boundaries.** A creature pathing toward a player in the next shard cannot read that
  player's authoritative row. Either creatures don't chase across boundaries (cheap, visibly wrong) or
  ownership transfers on aggro (correct, more traffic).

## Done when

- A world spans three or more shard nodes; a client walks a continuous path across every boundary with no
  disconnect, no visible hitch, and no missing terrain.
- Entities in the border band are visible to clients on the neighbouring node and **not** mutable there —
  asserted, since a violated read-only invariant is silent state divergence.
- A player pacing back and forth across a boundary triggers a bounded number of handoffs, not one per step.
- Killing the destination node mid-handoff leaves the player owned by the origin, alive and playable.
- Killing the origin node mid-handoff leaves the player owned by the destination with no duplicate.
- A creature chasing a player across a boundary behaves per the decision above, with a test encoding it.
- Scheduled reducers tick each shard's own entities; a creature never ticks twice or stops ticking after a
  boundary crossing.
- The hotspot limit is measured, not assumed: put N players in one location, record where it degrades, and
  publish the number.

## Risks

- **Handoff is where distributed-systems bugs live**, and they reproduce under load rather than in tests.
  Invest in a deterministic simulation harness — virtual clock, scripted movement, injected node failures —
  rather than relying on manual playtesting.
- **The hotspot ceiling is real and unfixable by this strategy.** 200 players in one town square is one shard
  on one node no matter how large the cluster. Document it at the point where a developer chooses a strategy,
  so they know they're choosing which failure mode they get.
- **Scope creep toward dynamic rebalancing.** Static assignment will visibly hotspot, and the temptation to
  fix it here is strong. It's a separate subsystem; resist.
