# Distributed reducers

**Shape:** a reducer that declares a small, fixed set of participants (say a player's identity shard
and a station's world shard) and commits atomically across all of them. One function, one outcome,
nothing in flight. It is the step above [engine-sagas.md](engine-sagas.md) for flows whose invariants
cannot tolerate a visible in-between.

**Status:** undecided. It also **reverses a stated stance**. [CLUSTERING.md](../CLUSTERING.md)'s
cross-shard ladder ends "if a flow cannot tolerate that, the answer is placement (put the tables
together), not a distributed transaction". This entry argues that placement is not always available,
and that a narrowly built distributed commit is better than a hand-made saga pretending to be one.
Adopting it means rewriting that sentence, not working around it.

## What exists today

- **Handoff** is already described as "the one unavoidable distributed transaction". It is a fenced
  saga (freeze, import, confirm, release) whose halves are recorded in both logs. It pins truncation
  while a marker is live, and a reconciler resolves stranded halves by asking the other side.
  Unavailable beats duplicated: when the outcome is unknowable the player stays frozen. That is
  in-doubt resolution, built and tested once already, for one fixed transaction shape.
- **Snapshot-isolation reducers** ([design/snapshot-isolation.md](../design/snapshot-isolation.md))
  run their body lock-free against a stable view, then apply the write set under the lock. They do
  **not** validate what the body read, which is why they are documented unsafe for read-modify-write.
- **Reducer bodies already do no I/O** and build an in-memory write set. The commit is a separate
  step, so splitting execution from commit needs no change to the programming model.

## Why the naive version is wrong here

The obvious implementation holds the write lock on every participant while the body runs, reads
remote rows mid-body, then runs two-phase commit at the end. In this engine that is worse than usual:

- **One writer per shard, holding the lock for the whole body** ([DESIGN.md §4](../DESIGN.md)). Every
  round trip in the body would freeze *every* write on every participant.
- **A coordinator lost between prepare and decide** leaves participants holding locks they cannot
  release on their own. In a single-writer engine, that blocks the whole shard.
- **Availability multiplies.** The transaction runs only when every participant is reachable.

## The shape that fits

```csharp
[Reducer(Spans = new[] { nameof(buyer), nameof(stationSystem) })]   // participants from arguments
public static void ProcessMarketTransaction(
    DistributedContext ctx, Identity buyer, ShardKey stationSystem, ItemId item, int quantity)
{
    var wallet = ctx.On(buyer).Find<Wallet>(buyer);             // read from a pinned snapshot
    var offer  = ctx.On(stationSystem).Find<StoreOffer>(item);
    // ... ordinary reducer logic; every write lands in its participant's write set
}
```

1. **Participants are declared, resolved from arguments, and capped** at a small number (two is the
   case worth building). The engine knows the scope before the body runs, and a write outside it fails
   loudly, as the single-shard span check does today.
2. **The body runs lock-free** against a snapshot of each participant pinned at a log position,
   fetched once up front. It yields one write set per participant plus a **read set**: the rows it
   read and the versions it read them at.
3. **Prepare is short and doesn't hold the shard.** Under its writer lock, each participant checks
   that the read set is unchanged, appends a *prepare* record to its own log, takes row-level intents
   on the rows it will write, and **releases the writer**. Other transactions keep running; only those
   touching an intent-held row wait or get `transient`, the existing retry contract.
4. **One decision record, durable and fenced.** The coordinator appends commit or abort to its own
   log. Participants apply on the decision, and an in-doubt participant asks the coordinator, reusing
   handoff's reconciler shape. **Presumed abort:** no decision past a deadline means abort, so a dead
   coordinator costs a timeout, not a wedged shard.
5. **Projections wait for the decision.** Subscription fan-out, the Postgres applier, border streams
   and handoff all treat prepared writes as invisible until committed. No client sees half a purchase.
   A row holding an intent cannot hand off; handoff waits for it like any other pending saga.

### What the node links must grow

No broker is needed (see "Messaging" below). But prepare and decide are latency-critical round trips
between participants, and today's node links are hub-and-spoke: every shard node holds one link to
the hub, and shard-to-shard traffic is relayed through it. Routing a two-participant commit through a
relay doubles the window intents are held for. **Direct node-to-node links** are part of this
entry's cost.

## What it costs and what it touches

This is the largest entry in the bin.

- **Log:** prepare and decision record types, truncation pins while either is live, and recovery of
  in-doubt state across restarts on both sides.
- **Engine:** read-set capture and validation (optimistic concurrency, which snapshot reducers do not
  have), row-level intents in a single-writer engine, and the `transient` path for intent conflicts.
- **Projections:** every applier, the subscription engine, and border publishing learn
  "prepared, not yet visible".
- **Cluster:** direct links, coordinator election per transaction (the caller's node), fencing on
  every leg, and the handoff rule for intent-held rows.
- **Generator:** the participant analysis, and the compile-time span check across declared
  participants.

**Hot rows abort each other.** A wallet row held by a purchase makes a concurrent bounty credit
retry. That's fine at purchase rates and corrosive on a frequent path. So the guardrail is part of
the design: a compile-time warning when a distributed reducer is reachable from a scheduled reducer
or a high-frequency client call, and a metric for intent-conflict retries.

## Relationship to sagas

Sagas come first and stay the default: they never hold anything, never block, and cover most
cross-node flows. A distributed reducer earns its cost only where a flow needs to be *invisible in
between*:

- a ledger whose totals must be exact at every instant, with nothing in flight;
- an invariant across nodes, such as "the item and its price never both exist";
- a flow whose compensation is impossible, not merely awkward.

## Messaging: no bus or queue is needed

Inter-node messaging does not need an external broker for this entry or for sagas:

- **The durable queue already exists.** Each shard's commit log is ordered and replayable, and every
  consumer keeps its own checkpoint. The event bus is an outbox over that log, and the appliers and
  border streams are consumers of it.
- **The wire can stay simple.** Node links are request/response plus notifications over
  authenticated TCP. Durability comes from logs on both ends, never from the wire, which is what lets
  a lost message be harmless.
- **A broker would be a second source of truth** beside the log, which DESIGN.md exists to avoid,
  and an infrastructure dependency for a library whose zero-infra deployment is a goal.

A broker becomes reasonable only for consumers *outside* the cluster (analytics, other services), and
then as one more projection of the log, an applier like the Postgres one, not as the internal bus.

## Reopening trigger

**A flow with a named consumer whose compensation is impossible or whose in-flight state is
unacceptable, after [engine-sagas.md](engine-sagas.md) has shipped.** A saga that merely needs too
much code is that entry's problem; a saga that cannot be correct at any amount of code is this one's.
Engine sagas are a prerequisite in practice too: the saga's record, receipt and redrive machinery is
most of what a presumed-abort coordinator needs.
