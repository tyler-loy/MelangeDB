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

Shipped in phase 10 as the **border stream**: each shard node keeps one subscription per (owned shard,
interesting neighbour) pair, and the owner ships its border-relevant ops — rows in the observer's band, plus
its own entities strayed into the observer's block mid-handoff — through the hub, in LSN order, at least
once. The copies land in the observer's engine as **borrowed rows**: ordinary rows to every read and
subscription, refused at every commit (`BorderReadOnlyException`, always on — a copy silently diverging from
its owner is the failure no test surfaces). Four properties worth knowing:

- **Owner wins.** A border op never touches a row the observer holds authoritatively (a completed import
  supersedes any stale copy in flight), and a trailing delete from a previous owner cannot erase the new
  owner's fresh copy — during a transfer two neighbours briefly publish the same entity, and the rules make
  that window harmless.
- **Out-of-scope means retracted.** An update that moves a row out of the observer's band ships as a delete,
  so the observer stops seeing what walked away rather than keeping a stale ghost.
- **Nothing pins the owner's log.** A border cursor the log can no longer serve — truncation, epoch change, a
  changed band depth — is answered with a full band reset (upserts plus deletion of absent borrowed rows,
  EventId 1715), never silently resumed past. Staleness is tolerable for a read-only cache; a pinned log is
  not.
- **The borrowed registry survives restarts** via a sidecar beside the shard's log (state at an LSN plus log
  tail replay — the engine's own snapshot pattern), because border records below a truncation base are gone
  while their rows survive in the snapshot. A missing sidecar rebuilds from row content, loudly (EventId
  1716).

One honesty note: `MayCommit`'s seam (an owner writing its strayed entity inside a neighbour's band) is a
*debug net*, not a security boundary — the strategy cannot distinguish "my entity strayed across" from "I
invented a row in my neighbour's territory" by content alone. The borrowed-row guard catches mutation of
every replicated copy; creating fresh rows in a neighbour's near-band is the one contract the application
keeps by convention, the same way it keeps the same-shard transaction contract.

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

### The HTTP surface is node-local

`MapMelangeSocket` maps `/call/{reducer}`, `/bulk`, `/sql`, and `/ticket` against the engine
registered in DI — **this node's engine, not a shard's.** A shard node's `ShardRuntime` builds its
own engine per owned shard, so the HTTP endpoints on a shard node reach none of them; the shard
engines are reachable only over the internal `/{path}/shard/{shardKey}` sockets, which are
hub-assertion authenticated and never exposed to clients.

The consequence worth stating plainly, because it is not what a single-node deployment trains you
to expect:

> **Ad-hoc SQL against a `Partitioned` table is refused in a cluster** (`partitioned_elsewhere`),
> on hub and shard nodes alike. `Partitioned` is the default placement, so this covers every table
> that has not declared otherwise. `Global` and `Replicated` tables answer on the hub as usual, and
> nothing changes single-node, where `Cluster:Role` is `None` and placement is inert.

It is a refusal rather than an empty result on purpose. The endpoint *could* return zero rows and a
200, and that is what it used to do — but an operator console cannot tell "no rows" from "wrong
node", and the empty answer reads as fact. A refusal naming the placement is information; a
successful lie is not.

To read partitioned rows, go through a reducer or a subscription: the gateway routes both to the
owning shard.

**Bulk ingestion is the exception, and it is routed rather than refused.** Refusing would have left
a clustered deployment with no way to seed a world except routed reducer calls, forfeiting bulk's
measured 44× advantage over per-row transactions on every bake — a real cost, unlike ad-hoc SQL,
where the alternative is a subscription that works. So `/bulk` on the **hub** fans the batch out;
see [Bulk ingestion in a cluster](#bulk-ingestion-in-a-cluster). On a shard node it is still the
wrong engine, and now says so at the commit point rather than accepting the rows.

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

### Strategy A — spatial partitioning (the reference workload)

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

### The seam: what a transaction may write across a block boundary

The contract above is stated strictly, and the spatial strategy relaxes it in exactly one place. The
question that keeps coming up — *must a transaction touching several adjacent chunks be split when
those chunks straddle a block seam?* — has the answer **no, within the band**:

> `MayCommit` admits a row whose chunk is in the executing shard's block, **or** within
> `Cluster:BorderBandChunks` of it. A write set that stays inside that envelope is one shard's
> business and commits normally. Beyond it, the span check has something to say.

Per-chunk splitting is therefore not required, and a refactor to achieve it is wasted work.

**But the widening is about ownership, not geometry**, and its docstring is doing real work that the
signature hides:

> the owner may also commit a row standing up to the band depth inside a neighbouring block — the
> entity it still owns whose handoff has not completed yet

The distinction that resolves it, and the one worth carrying away:

> **`MayCommit` answers "may this shard write this row?" — not "should this row live here?"**

For an entity mid-handoff those coincide: the row is the executing shard's to write, and it will be
transferred. For **durable world data the neighbour owns**, they come apart. A row committed on
shard S that homes to T stays authoritative in **S's log**; T only ever sees it as a *borrowed*
copy, which T may not mutate. The authoritative copy is in the wrong engine, and nothing later
moves it.

So: gameplay writes near a seam are fine within the band. Terrain and world rows homed to a
neighbour are not — for those, the executing shard must be the home shard.

### What actually guards a commit, and what is on by default

`ShardCommitGuard.Validate` runs five checks in order. **Only the last one is configurable**, which
is the opposite of the impression `Cluster:ShardSpanCheck` alone gives:

| Check | Catches | On |
| --- | --- | --- |
| Lease — `ShardFencedException` | Committing on a shard this node no longer holds a live lease for | Always |
| Freeze — `TransientRejectionException` | Writing a row frozen mid-handoff | Always |
| Borrowed — `BorderReadOnlyException` | Writing a row this node holds as a read-only border-band copy | Always |
| Placement — `InvalidOperationException` | Writing a `Global` or `Replicated` table on a shard node | Always |
| Span — `ShardSpanException` | A write set resolving to more than one shard, per `MayCommit` | `Cluster:ShardSpanCheck`, default `DebugOnly` |

Commits with `CommitOrigin.Internal` — replication, handoff imports, saga markers — skip all of it
by design: the write set was validated where it originated, and the applying node holds it precisely
because its own placement rules say it may not produce it. `Bulk` is *not* internal, and is checked
like any reducer.

**The asymmetry that matters.** The borrowed-row guard is a lookup by key in the borrowed registry,
so it only fires for a row this node is *currently holding a copy of*:

- **Updating** a row homed to a neighbour and already borrowed → `BorderReadOnlyException`, always,
  Release builds included.
- **Inserting** a new row homed to a neighbour → nothing borrowed, nothing to look up → falls
  through to the span check → `DebugOnly` → **silent in a Release build.**

A world generator writing terrain into a neighbouring chunk is an insert, which is precisely the
case the always-on guard does not catch. If a deployment writes world data anywhere near a seam,
set `Cluster:ShardSpanCheck` to `Always` and pay the dictionary probe.

### Reaping a shard that holds nothing

`EnsureShard` creates a shard the first time anyone arrives in it, and for a long time had no
counterpart: a world where players can wander anywhere accumulated a membership row and a data
directory per shard key ever visited, permanently. `MelangeClusterCoordinator.ReapShardAsync` is the
counterpart, beside `EnsureShard` on the same coordinator.

It is a **host API and not an endpoint**, deliberately. This is the only cluster operation that
destroys durable state, and a new authenticated wire surface for it would drag the whole gating
ladder behind a call an operator makes by hand; direct callers are the host's own code, the same
line `BulkInsert` draws.

The shape is *drain, then do not hand it anywhere*.

**Emptiness and the decision to stop accepting rows are one decision, taken under the engine write
lock.** They cannot be separated: a shard found empty and then closed can take a row in between,
from a reducer call or a bulk group that resolved the shard before the reap began and commits after
it. The node's shard-set lock does not close that window — it guards the map of open shards, and is
released before the commit runs — and neither does the heartbeat's authoritative-row gauge, which is
throttled to ten seconds and is documented as advisory for exactly this reason. So the owner counts
its partitioned rows *and* installs a commit guard refusing every subsequent write in one hold of
the engine write lock. A write is therefore either counted, or refused with an explanation. The seal
lifts if a later check refuses the reap, and does not survive the shard runtime: a shard that
reopens, reopens writable.

**The destruction goes last, in three steps:** the owner seals and closes the shard; the hub removes
the membership row; the owner deletes the directory. Everything before the removal is undoable, so
an interruption leaves a shard still owned and still openable — the owner's reap mark expires and
the shard reopens from its untouched directory, which is what "the reap did not happen" has to mean.
The order matters in the other direction too: deleting first would leave a window where the
membership row outlives its data, and a stale assignment would open a fresh empty engine **under the
retired originator**, re-minting ids that transfers have already carried to other shards. After the
removal a lost delete strands a directory nobody owns — garbage, logged as garbage, and never a
collision.

A reap is refused unless all of these hold, and a refusal is the ordinary answer rather than an
error:

- **No rows of its own.** Border-band copies do not count — they are a neighbour's, rebuilt by a
  band reset — and neither do `Local` timer rows, which the shard's init reducer writes again the
  next time the key is visited.
- **Nothing holding its log.** Judged by which truncation floors are *present*, never by where they
  sit: a floor whose provider has nothing outstanding returns null and is omitted from the report
  entirely. Position carries no intent here — `PinTruncation` pins at the current base, so a
  streaming backup and a cluster-event cursor that has never forwarded anything report the very same
  LSN meaning opposite things.
- **Its events are shipped.** The cluster-event cursor only advances when the pump runs, and the
  pump only wakes on an event-bearing commit, so a resting cursor means "not asked lately" rather
  than "nothing to send". The reap kicks the forwarder and waits for it to reach the durable
  watermark instead of reading a floor; a cursor that will not get there means the hub is not taking
  the events, and they would go with the directory.
- **Not mid-drain.** A drain and a reap both decide where the shard ends up, so they cannot overlap.

**The shard key is not reserved.** Arriving there again simply creates a new shard — with a new
fencing term and a **new originator**, because originators are allocated from a high-water mark and
a reaped shard's prefix retires with it. Ids minted under it may still be alive in a neighbour, and
`AutoInc`'s contract is unique-not-dense.

### Bulk ingestion in a cluster

A loader keeps posting one batch to one endpoint. The hub groups the rows by
`IShardStrategy.ShardForRow`, keeps `Global`, `Replicated`, and `Local` rows on its own engine, and
forwards each group to the node owning that shard. Topology stays hidden, the same way the gateway
hides it for reducers and subscriptions. The alternative — a per-shard bulk endpoint with the loader
sharding its own writes — works, and puts the deployment's sharding function into every tool that
loads data.

**Atomicity is per engine, and that is the honest statement rather than a weakening.** Bulk has
always been "one write set, one transaction, one log record"; fanned out, one batch becomes N
commits on N logs. A single-node deployment has one engine, so nothing there changes. Nothing is
promised across shards, and nothing ever was. The response reflects it: `results` is an array of
`{shard, lsn, rows}`, because there is no such thing as *the* LSN of a batch that spanned three
logs.

Three properties are worth knowing about, because each is load-bearing:

- **The hub encodes each row before routing it.** `RowRef` carries both the serialized bytes and a
  by-name column accessor, and the bundled spatial strategy reads only columns — so it is tempting
  to route a dictionary through a `RowRef` with empty bytes and skip the encode. A strategy that
  reached for the bytes would then read *empty rather than throw*: rows silently routed to the wrong
  shard, authoritative in the wrong engine. The encode costs one serialization the owning shard
  redoes; bulk's advantage is transaction overhead, not encoding, so paying it twice is noise.

- **Each receiving shard re-resolves every row, unconditionally.** A forwarded group is single-shard
  by the hub's reckoning, and the hub's reckoning is exactly what can be wrong — a drifted shard
  map, a strategy that differs between hub and node. It is tempting to lean on the shard-span check
  here, which does include the executing shard in what it compares; two things make that not enough.
  `Cluster:ShardSpanCheck` defaults to `DebugOnly`, so in a Release cluster — the only kind with a
  bake to run — it is off. And a spatial strategy's `MayCommit` deliberately admits the seam, so a
  row a band's depth across the line is admitted by design. The guard that would catch a misroute is
  absent exactly where it would be needed, so the receiver checks for itself, and refuses the whole
  group naming both shards.

- **`[AutoInc]` is allocated by the owning shard**, never by the hub, so ids keep their
  originator prefix and two shards allocating "the first row" cannot collide. What travels is the
  hub's routing preimage plus the list of columns the caller actually supplied; the shard
  reconstitutes the caller's row from the two and stages it through the ordinary bulk path.

**Bulk does not create shards by default.** `EnsureShard` creates on demand, so a world generator
touching thousands of shard keys would otherwise turn one POST into thousands of shards, their
originators, and their data directories. Reaping exists now, but it is a deliberate operator action
rather than something that happens on its own, and code is revertible where durable directories are
not. A batch routing to a shard that does not exist is refused **whole and before any engine
writes**, naming the shards to pre-declare with `MelangeClusterCoordinator.EnsureShard`. Set
`Bulk:CreateShards` to accept create-on-demand; the hub then creates *and opens* every missing
destination up front, because a shard's owner learns of an assignment on its next heartbeat and a
bake creates and writes in the same breath.

**Re-posting a partly-applied bake is sound**, which is what keeps all of the above from needing to
be a distributed transaction: rows are upserts and `[AutoInc]` is originator-prefixed, so the
results array only has to be good enough to tell an operator what landed.

One thing to keep an eye on: this makes the hub a **data path**. It is a control plane plus
`Global`/`Replicated` data the rest of the time, and pushing tens of MB of bake traffic through it
makes it a throughput participant, which is the assumption [ROADMAP.md](ROADMAP.md) leans on when it
defers sharding the hub. Two honest caveats on the current implementation: the hub **buffers the
whole batch**, since the endpoint parses the request body into rows before anything is routed, and
it forwards groups **one at a time**, so a batch spanning many shards pays them in sequence rather
than in parallel. Neither is inherent — the round-trip count is the number of destination shards
rather than the number of rows, and a bake is rare — but both are the first things to change if hub
load or bake wall-clock ever becomes interesting.

## Handoff

Two shapes, following from the strategy:

- **Explicit** (instancing, portals, zone transitions): the transition is a discrete player action with a
  natural pause. Freeze, transfer, resume. Simple, and no overlap machinery required.
- **Continuous** (open world): the destination node begins streaming border chunks before the player
  arrives, the player's partitioned rows transfer, then the origin drops the band. Seamless, and the
  reason interest overlap exists.

Continuous handoff shipped in phase 10 on top of phase 09's saga, and its moving parts are worth naming
because each is the answer to a specific failure mode:

- **The origin decides** (settled). Its committed rows are the only trusted position — the client's claimed
  position never is, and the gateway cannot see positions at all. A boundary monitor per owned shard assesses
  every committed write of an anchored entity (`IMigrationAnchors`): entering the band notifies an
  *approach* (the gateway pre-opens the destination session on it); crossing past the margin requests a
  transfer. A sweep re-signals standing strays, so an entity that stops just past the margin after a
  rate-limited trigger is never stranded.
- **Hysteresis is layered** (settled): the margin (`Cluster:HandoffMarginChunks` — after a transfer the
  entity must walk back through the whole margin before the reverse can fire), the hub's per-entity rate
  limit (`Cluster:HandoffMinIntervalMs`), and a local notify cooldown. Pacing across a boundary triggers a
  bounded number of transfers, never one per step.
- **The gateway swap is invisible** (settled: mid-handoff reducer calls are *queued*). From saga start the
  gateway holds the client's shard-routed calls; at the destination-authoritative moment — after the import
  is durable, before the release is requested — the saga synchronously lets the gateway mute the origin (so
  the release's row deletions never reach the client), then the gateway re-issues the client's shard
  subscriptions on the destination and flushes the held calls in order. Re-subscribing under an existing id
  re-scopes it: the destination's initial set (border band included, which is why the terrain behind the
  player is already there) atomically replaces the client's cache. No disconnect, no resync error, no gap.
  The trade: those calls wait out the transfer window, and a wedged transfer caps the queue with a retryable
  error.
- **Expected refusals are typed `transient`, never `internal`** (settled: [#22](https://github.com/tyler-loy/MelangeDB/issues/22)).
  The guards that fire for conditions the cluster itself designed — a row frozen mid-handoff, a write routed
  to a border copy just after the shard map flips, a fenced node awaiting re-registration — reach the client
  as error code `transient` carrying the precise reason, and the server logs nothing: a seam walker crossing
  shards is the product working, and an error log per unlucky crossing is noise that trains operators to
  ignore the log. The retry contract is named on the client as `MelangeCallException.IsTransient`: retry the
  call unchanged on the next tick. `rejected` stays reserved for what reducer code itself decided; `internal`
  for genuine faults, which still log.
- **Stale origins cannot transfer** (defense in depth, each independently sufficient for its case): the hub
  drops a request whose sender is not the entity's recorded owner (1722); a freeze refuses to collect
  borrowed rows and aborts on an empty transfer set; the monitor never signals frozen or borrowed rows. A
  saga built on a stale copy would re-import the past over the present — the one lesson of this phase's
  hardest bug.
- **Creatures transfer on crossing** (settled: ownership-transfer, not don't-chase). A creature chases by
  reading its target's border copy — pathing toward a player in the next shard needs only the band — and
  when it crosses, it migrates immediately (no margin: its AI only ticks rows resolving to its own block, so
  a margin would leave it standing unticked at the line). Scheduled AI ticks the shard's own entities and
  must skip rows resolving elsewhere — the read-only guard makes a violation loud, and one throwing row
  would abort the whole tick.

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

### Cross-shard interaction, in priority order

Settled in phase 10, and the order is the design:

1. **Co-locate, then transact locally.** Interaction in a game is proximity-gated — you trade with someone
   adjacent, you attack what's in range — so interacting entities already share a shard and the transaction
   is one ordinary local commit with zero per-transaction cross-node messages (asserted by counting them).
   This covers nearly everything and is why spatial sharding is tractable at all.
2. **Ownership transfer** for an entity crossing a boundary — the handoff protocol with a different entity
   class (a creature chasing, a vehicle driven across).
3. **A saga over the event bus** for the rare genuine remote case. The initiating shard commits its half
   locally and publishes the fact; a hub-side handler drives the remote steps with
   `MelangeClusterCoordinator.ExecuteOnShardAsync` — each step one ordinary local transaction on the owning
   shard, fencing-checked — and compensates on a definitive failure. **Eventually consistent, explicitly not
   ACID**: between a debit and its remote credit (or compensating refund) the value is simply in flight, and
   delivery is at-least-once, so steps must be idempotent-enough for the game's semantics. If a flow cannot
   tolerate that, the answer is placement (put the tables together), not a distributed transaction.

One structural rule falls out of how re-homing works: **`ShardBy` must not be the primary key** (compile
error MELANGE0018, mirrored at schema registration). Handoff rewrites the row's `ShardBy` column while the
stored row key — the encoded primary key — stays fixed; a primary-key shard column would silently diverge
from its key on the first transfer. The shard id is its own column.

## What holds regardless of strategy

- **Each shard is internally single-writer** — one serialized transaction loop, one commit log, exactly
  the semantics that make reducers pleasant. Multi-writer is a property of the *cluster*. The reducer
  programming model does not change. The lock behind that loop covers each whole reducer body, not just
  its commit ([DESIGN.md §4](DESIGN.md)), so a slow reducer stalls writes for its shard — which is also
  the good news: it stalls *only* its shard, and sharding is what turns one global write stall into a
  local one.
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
  whichever node owns its shard — no global timer wheel, no leader election. The reference workload's single
  global `CreatureAiTick` row becomes one row per shard, and the hand-written "only chunks near a player"
  filtering becomes implicit in the partition.

  The mechanism is `Placement.Local`, which a scheduled table always has (declaring another is compile error
  MELANGE0022). That reads like "one per cluster" and is the opposite: a shard node runs one engine per shard
  it owns, and a timer is a row in *that* engine's log, so node-local on a per-shard engine is shard-local.
  One declared timer table is one independent timer set per shard.

  **Those rows have to be seeded, and only the shard itself can do it.** A shard is created the first time a
  session resolves to it, and its engine opens empty; no application code holds a handle on it. So a
  shard-executed **`[Reducer(ReducerKind.Init)]`** fires once inside each shard's fresh engine, before its
  scheduler starts — the hook the first player to walk into a never-visited block depends on. Without it that
  block's shard serves reads and writes correctly and simply never ticks: creatures inert, nothing growing,
  nothing decaying, no error anywhere. Fresh is "the log has no head", so reassignment and restart recover
  rather than re-seed, while a crash between creating a shard and its first commit still seeds on the retry.
  A shard that opens holding no rows in any scheduled table is logged as a warning (EventId 1723) — the state
  is almost always a seeding mistake, and it is otherwise completely silent.
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

The reference workload's own creature-tick code already says it: *"everything else in the world is inert,
which is what makes a persistent world-wide population affordable."* Most of a 10km world is cold at any
instant.

Doubling that world to ~20km means 314×314 = 98,596 chunks, 4× the memory — the wall you hit. (It also
overflows `ushort ChunkId`, since `cx * 157 + cy` tops out at 65,535; the key encoding widens regardless.)
Paging is what lets one node own a shard far larger than its RAM. **Paging ships first** — it attacks the
bigger term and needs no coordination layer at all.

## Open questions

- **Shard assignment and rebalancing.** Static assignment shipped in phase 09: shards are created at
  runtime, assigned least-loaded-first by the hub's membership store, and reassigned only on node death
  (fencing tokens bumped; the new owner recovers the shard from the shard's own log on shared storage).
  **The assignment half is design-settled, not built:** load-following ships as *fixed shard boundaries,
  dynamic shard → node assignment* — heartbeat-carried load metrics, a hub-side rebalance loop, a graceful
  drain (the node-death path minus the death), and a provisioner seam for obtaining capacity — see
  [docs/design/elastic-rebalancing.md](design/elastic-rebalancing.md). Dynamic *splitting* (a quadtree
  subdividing under load) stays deferred: it's a substantial subsystem, its customer narrows to workloads
  whose load concentrates inside a single registered shard, and choosing fine-enough boundaries up front
  turns "split the hot region" back into reassignment. Instancing sidesteps all of it — another reason it
  shipped first.
- **The hotspot ceiling is strategy-dependent, and worth telling users plainly.** Spatial partitioning
  cannot split a single crowded location; instancing can. A developer choosing a strategy is choosing
  which failure mode they get. **Measured in phase 10, re-measured for phase 17's group commit**
  (in-process, one crowded shard on a real shard node — engine, guards, and durable log in the path;
  100 players in one chunk, movement reducers over a 2 s window; Windows 11 dev machine with NVMe,
  Release build, each row measured in isolation on the same day — absolute numbers are the machine's
  and drift with it, which is why all three are re-measured together; the methodology and the live
  measurement are `HotspotMeasurementTests`):
  - Under the default `CommitLog:FsyncPolicy = OnCommit` with a **single sequential caller**, one shard
    sustains **~500 commits/s** — each commit pays the disk's full fsync, ~25 players in one square at a
    20 Hz per-player update budget. This row is the phase-10 measurement re-run, and group commit leaves
    it unchanged by design: a lone caller fsyncs for itself at the old inline latency.
  - Under the same per-commit durability with **16 concurrent callers** — the shape a real crowd has,
    since every player's reducer arrives on its own transport thread — the same shard sustains
    **~4,000 commits/s at a mean of 8 commits per fsync** (phase 17's group commit: while one caller's
    fsync is in flight the others run their bodies and park, and the next flush covers them all).
    ~200 players at 20 Hz, ~400 at 10 Hz. Every commit is still individually durable before its call
    returns; the disk does the same work per flush and answers for eight commits instead of one.
  - Under `FsyncPolicy = Interval` (50 ms), the shard sustains **~12,000 commits/s** — the serialized
    transaction loop's own ceiling, with no durability wait at all. ~600 players at 20 Hz, ~1,200 at 10 Hz.

  Run the measurement on your hardware; the shape holds even where the numbers move. The point of publishing
  it: the per-commit-fsync ceiling is no longer the arithmetic of one fsync per commit — it is the disk's
  flush rate times however many commits contention packs behind each flush — but a crowded location is
  still one shard on one node by construction, and *no cluster size changes any of these numbers*.
  Choosing spatial partitioning is choosing this ceiling; instancing trades it for the inability to have
  one shared world.

  These are the *commit loop's* ceilings, measured in-process on purpose. The full path — real clients,
  real sockets, subscriptions fanning every commit out to every subscriber on the shard — hits a different
  and lower wall first: delta fan-out, which grows with the *square* of players per shard. That path is
  measured by the standalone load-testing tool, with numbers and methodology in
  [LOAD-TESTING.md](LOAD-TESTING.md).
- ~~**Cluster membership.**~~ **Settled in phase 09: Postgres-backed, not Raft.** The ownership registry —
  nodes, per-shard owner, fencing token, and originator id — lives in the hub's own Postgres
  (`AddPostgresClusterMembership()`; in-memory for tests), the hub is its sole writer, failure detection is
  heartbeat silence past `Cluster:FailureTimeoutMs`, and a dead node's shards reassign under bumped fencing
  tokens while the suspect self-fences on the same clock. See docs/road-to-0.1/plan-phase-09.md for the rationale.
- ~~**Client protocol during dual attachment.**~~ **Settled in phase 09: the dual attachment is
  server-internal.** The wire protocol stays one socket, one session; the gateway owns the hub-plus-shard
  mapping and the client never learns it. See docs/road-to-0.1/plan-phase-09.md.
- **Does the hub shard?** For a very large deployment the hub's `Global` tables become the ceiling. Since
  they're the Postgres tier, the answer is probably "Postgres's problem, not ours" — but it needs saying.
  Explicitly out of phase 09's scope; still open.
