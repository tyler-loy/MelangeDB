# Identity-sharded hub

**Shape:** split the hub's per-player `Global` tables (wallet, journal, reputation, profile and
similar) across several hub nodes by player identity. Every node has the same schema, and the gateway
routes each call to the node holding that identity. Identity becomes a second shard space alongside
the world one.

**Status:** undecided. It is also **against a live deferral**: [ROADMAP.md](../ROADMAP.md) lists
sharding the hub as deferred, and that decision stands until the trigger below fires. This entry is
the proposal, not a reversal. See [the bin's rule on overlap](README.md#what-this-directory-must-not-do).

## What exists today

The hub owns every `Global` table and is a single writer. Its load is bounded by construction, because
nothing written in the same transaction as world state may live there
([CLUSTERING.md](../CLUSTERING.md)'s placement rule). CLUSTERING.md lists "Does the hub shard?" as an
open question and guesses the answer is "Postgres's problem". That guess holds only for hubs whose
`Global` tables sit on the relational tier. A hub that keeps them in the hot store, as the consumer's
does, has its own commit loop as its ceiling.

## The shape

```csharp
[Table(Public = true, Placement = Placement.Partitioned, ShardBy = nameof(PilotId),
       Space = ShardSpace.Identity)]
public partial struct Wallet { public Identity PilotId; /* ... */ }
```

- **Fixed buckets.** An identity hashes to one of a fixed number of buckets (say 256), and the
  membership store assigns buckets to hub nodes. This is the
  [elastic-rebalancing](../design/elastic-rebalancing.md) model applied to a second space: fixed
  boundaries, with buckets moving between nodes. No consistent hashing is needed, because the
  membership store is authoritative.
- **A smaller central hub.** Membership, the gateway's routing table, unique and one-per-account
  claims, anything spanning players (groups, broadcasts, economy overrides, global rollups), and
  `Replicated` writes all stay on one central hub.
- **No client-side negotiator.** The gateway and the membership directory are the negotiator, and
  the dual attachment becomes a triple: world shard, identity shard, central hub. The phase 09 rule
  holds: **the client protocol does not change** and the client never learns the topology. If clients
  learned it, every rebalance would become a client-protocol problem.
- **Redundancy is separate.** Losing data is already handled by the log on shared storage. Extra
  copies buy availability, which is [shard-ha-warm-replica.md](shard-ha-warm-replica.md)'s question
  and trigger. Identity buckets are small, which shortens the reassignment window that entry worries
  about.

## What breaks

The consumer's `Global` tables were audited against this, and the split is less clean than "per-player
data" suggests. Each item below is a transaction that would become cross-node, so it either stays
central or becomes a saga:

- **A transfer between two players** (a give, and later a trade or contract) debits one identity and
  credits another in one transaction.
- **An operation over everyone at one place**: an admin redraw of a location's per-player offers, or
  pruning every player's index rows for a location.
- **Rollups over all players by time** (the money supply), which today assume one clock and one log.
- **The connect lifecycle** writes identity rows and central rows (account, group membership)
  together.
- **Player purge** writes the player's rows plus group, mute, bookmark and account rows.
- **One-per-account links**, enforced by an index, must stay central.
- **The gateway's routing table** (which world shard a player is in) is read on every call and must
  stay central.
- **Per-player rows that shards read**, such as bookmarks a shard checks a warp against, are
  `Replicated`, not `Global`. Partitioning them by identity breaks the shard-side read, and `AutoInc`
  ids issued across nodes would need the originator-prefixed ranges partitioned tables already use.

None of these makes the shape impossible, but together they make it a migration of the consumer's
transaction boundaries, not a placement flag.

## The argument for building it early

That migration is also why waiting until it's needed is the expensive choice. A saturated hub caps
every player at once, and the fix is not a config change. It means moving tables, turning
cross-identity writes into sagas, and re-testing the flows that touch them. None of that can be done
during an outage. Once a game knows it is aiming for a population one hub can't carry, building the
mechanism before that population arrives is cheaper than building it under load. So the trigger below
is a *projection*, not a saturation event.

## Considered and rejected: lending identity rows to the world shard

The tempting variant: while a player is docked, hand ownership of their wallet row to the station's
shard, so a purchase becomes one local transaction. Credits can be applied in any order and could be
routed to wherever the row lives. It was rejected for reasons that generalise beyond one game:

- **Moving rows off a node looks like deletion to subscribers.** A client that reads a journal delete
  as "archived" sees its history corrupted on every move.
- **Idempotency moves with the row.** If the journal is the dedupe set, the whole journal and its
  archive move on every dock, or the dedupe set splits.
- **The frequencies are backwards.** Docking happens far more often than buying, and a reserve step
  already shows the debit at once.
- **In-flight credits** (bounties, shares, refunds) land mid-move and need forwarding.

[Ownership transfer](../CLUSTERING.md#cross-shard-interaction-in-priority-order) stays the right tool
for entities that move with the player. It is the wrong tool for a ledger the player carries
everywhere.

## What it costs and what it touches

A subsystem:

- a second shard space in the strategy, membership store and gateway, plus triple attachment and its
  handoff interactions;
- placement checks that know two spaces (a transaction may not span an identity bucket and a world
  shard);
- rebalancing and drain for identity buckets;
- scatter-gather or event-fed aggregation for every query over all players.

It also depends on [engine-sagas.md](engine-sagas.md): every cross-identity write in the list above
becomes a saga, and sharding the hub before sagas are cheap would push that cost onto each
application.

## Reopening trigger

**A load test or clustered telemetry projects the hub saturating before a consumer's target
population.** That means the hub's write lock is held for a large share of wall time, or its p99
`melange_commit_duration` is rising with hub commit rate, on a curve that crosses the ceiling before
the player count the game is planning for. Acting on the projection, not on saturation itself, leaves
the lead time the migration needs.

The projection should be taken after two cheaper fixes land, so it measures the hub rather than
something else:

1. single-row commit latency that fsync doesn't explain, which currently dominates lock-held time on
   its own and which sharding wouldn't fix;
2. the application's unindexed periodic scans on the hub.

No clustered measurement exists yet, so the deferral stands for now. Taking that measurement is the
next step, not a reason to stop looking.
