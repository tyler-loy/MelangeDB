# Phase 10 — Clustering II: spatial strategy and seamless handoff

**Goal:** a contiguous world split across nodes, with players walking between shards and never noticing.

**Depends on:** [09](plan-phase-09.md).

## Why here

This is the hard sharding case, and it exists because a single continuous world cannot use instancing. In
the reference workload, everyone in the town square interacts with everyone else, so they must share a writer — splitting
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

Dynamic rebalancing / quadtree splitting — still static assignment. Shard-level Raft for HA. Cross-shard
distributed transactions beyond the handoff saga.

## Decisions to settle

Each settled when the phase shipped; the subsections are the record.

### Settled: border band depth is tunable, defaulting to a derived 2 chunks

`Cluster:BorderBandChunks`, default 2 — derived, not guessed. The band must cover two things: the hysteresis
margin (an entity is still origin-owned up to `HandoffMarginChunks` past the line, and its writes must stay
legal), plus the distance an entity travels during one handoff window (crossing detection is one commit
observation; the saga plus the gateway swap is comfortably under a second in-process and budgeted at ~1 s
over a network). For the reference workload — 64 m chunks, ~8 m/s sprint —
`margin (1) + ceil(8 m/s x 1 s / 64 m) = 1 + 1 = 2`. The band is also the ownership slack at the seam, so
the strategy validates `margin < band ≤ block dimension` loudly at construction; the acceptance walk, which
steps far faster than 8 m/s, runs at band 3 — the derivation applied to its own speed, which is exactly how
a developer should size it.

### Settled: the origin node decides a handoff

The origin's committed rows are the only trusted position: the client's claim never is, and the gateway
cannot see positions at all. A boundary monitor per owned shard assesses every committed Partitioned write
of an anchored entity (`IMigrationAnchors` — the application names its migratable rows), notifies approaches
(the gateway pre-opens the destination session on them, hiding the decision latency the plan worried about),
and requests a transfer once the entity crosses past the margin. The hub still owns admission — in-flight
dedupe, the per-entity rate limit, and the stale-origin check (a request from a shard that is not the
entity's recorded owner is dropped, EventId 1722) — so a confused origin can trigger nothing worse than a
dropped message. A sweep re-signals standing strays, so a rate-limited trigger with no further movement is
never stranded.

### Settled: a mid-handoff reducer call is queued at the gateway, invisibly

Queue — the invisible option, taken deliberately with its costs stated. From saga start the gateway holds
the client's shard-routed calls; at the destination-authoritative moment it re-issues the client's shard
subscriptions on the destination (re-scoping atomically replaces the client's row cache, border band
included) and flushes the held calls in order — to the destination on success, back to the origin on a
definitive abort. The trades: held calls wait out the transfer window (bounded latency, no visible error); a
wedged transfer caps the queue at 256 with a retryable error (EventId 1720); and calls held when the
connection dies die with it, exactly as in-flight calls always have. Reject-and-retry was rejected because
it makes every boundary a visible stutter; serve-from-origin was rejected because the origin's rows freeze
mid-saga, so "serving" would mean failing.

### Settled: border-band rows count against residency

Yes. They are ordinary rows in the observer engine's store — paged, budgeted, and reported like everything
else — because a separate unaccounted cache is a second storage path with a second failure mode, and an
"honest footprint" that excludes the band would be neither. The inflation is bounded by geometry:
perimeter x band depth, against an area — for the reference geometry (39x39-chunk blocks, band 2) about
20% of a shard's chunks, and it shrinks as blocks grow. Size `HotStore:MemoryBudgetBytes` for owned area
plus band; the residency report shows what the band actually costs.

### Settled: creature AI transfers ownership on crossing

Ownership transfer — correctness over cheapness — with the reads made cheap first: a creature *chases* by
reading its target's border copy (pathing toward a player in the next shard needs only the band, no
ownership machinery), and *transfers* only when it physically crosses, as an immediate anchor: no hysteresis
margin, because its AI ticks only rows resolving to its own block, and a creature waiting out a margin would
stand unticked at the line. The don't-chase option was rejected as visibly wrong at every boundary; the
traffic cost of transfer-on-crossing is one saga per actual crossing, which the chase test measures as
exactly one. The convention the AI must keep — tick only rows resolving to your own block — is enforced
loudly by the read-only border guard.

## Shipped notes

- **Interest replication is a border stream per (owned shard, interesting neighbour) pair** — owner node →
  hub → observer node, at-least-once, observer's durable cursor as the truth, full-band reset (1715) for any
  cursor the owner's log can no longer serve. Copies land as *borrowed rows*: readable and subscribable,
  refused at every commit (`BorderReadOnlyException`, always on). The borrowed registry survives
  snapshot+truncate+restart via a sidecar beside the shard's log (snapshot-plus-tail, refreshed at
  truncation time; content-scan fallback, 1716).
- **Ownership rules were the hard part**, and the walk test flushed out the phase's worst bug: a stale
  trigger starting a second saga from a shard that no longer owned the entity, re-importing pre-transfer
  bytes over the destination's newer state. The fix is layered, each layer independently load-bearing: the
  hub's entity-owner map drops stale-origin requests (1722); a freeze aborts on an empty transfer set and
  refuses borrowed rows; the monitor never signals frozen or borrowed rows; publishers never publish rows
  they merely borrow; and a released origin's zombie row is *adopted* as a marked copy by the new owner's
  publication instead of silently skipped into sent-set desync.
- **Shard-side interest-scoped event delivery stayed out.** Cross-shard interaction landed as
  co-locate / ownership-transfer / hub-driven saga (`ExecuteOnShardAsync`), and none of the three needs
  shard-side handler execution — the saga's remote steps are ordinary local transactions driven from the
  hub, where handlers already run. It remains future work if an application needs handlers with shard-local
  reads.
- **The hotspot ceiling is measured and published** in docs/CLUSTERING.md at the strategy-choice point:
  ~1,100 commits/s per crowded shard under default per-commit fsync (~55 players at a 20 Hz budget),
  ~52,000 commits/s under interval fsync (~2,600 players at 20 Hz), Windows/NVMe dev machine, methodology in
  `HotspotMeasurementTests`. No cluster size changes either number; that is the trade spatial partitioning
  is.
- **One known protocol soft spot, documented rather than hidden — since closed behaviorally:** during the
  swap, a delta committed on the destination could be dropped by the client if its LSN happened to fall at
  or below the *previous* attachment's anchor — the anchor comparison is not epoch-qualified across logs.
  Under CPU starvation this was not a self-healing rarity: the walk test lost the player's own position 5
  runs in 6 (`--cpus=2`, the starved sender let a post-registration delta reach the wire ahead of the
  replacement set's first chunk, and the test's awaited row never changed again). Closed in two steps, both
  below the protocol: the client flips back to buffering on the first chunk of any new initial set and
  replays against the anchor that set names; and the node's sender enforces the **first-chunk rule** — a
  freshly registered subscription's first chunk precedes any delta for it on the wire (deltas otherwise
  outrank bulk), asserted by a wire-order test that forces the schedule with a TCP-wedged sender. The
  epoch-qualified anchor in the subscription protocol remains the deferred *protocol-level* hardening, with
  this note still its record: the wire-order guarantee is server behavior, and a client speaking to a server
  without it — or a future substrate that reorders the data and bulk channels — would reopen the window.
  A second reason the protocol change matters: initial-set chunks carry no set identity, so a client hit by
  two swaps in quick succession concatenates the abandoned set's partial rows with the replacement's
  (`MelangeSubscription.AcceptInitialChunk` accumulates until an `IsLast`), and stale rows scoped only to the
  abandoned attachment can linger in the cache until touched. Unobserved in tests — it needs a second handoff
  inside one set's streaming time — and undetectable client-side without a set marker on the wire.

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
