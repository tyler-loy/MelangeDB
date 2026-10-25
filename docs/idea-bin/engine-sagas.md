# Engine-owned sagas

**Shape:** make the cross-node saga a declared engine primitive, not a pattern every consumer
rebuilds. The application writes the step that debits, the step that delivers, and the step that
compensates. The engine owns everything between them: the outbox, durable retry, idempotency, the
receipt that decides the outcome, the redrive loop, and a parked state you can see.

**Status:** undecided. Of the entries in this bin, it has the most evidence behind it. A consuming
game has hand-written the same pattern six times, and that game's maintainers have said what the
primitive must cover before they would delete their version rather than wrap it. That list is below,
and it is the acceptance bar.

## What exists today

[CLUSTERING.md](../CLUSTERING.md)'s cross-shard ladder puts sagas third, after co-location and
ownership transfer, and describes them as the application's job. The engine provides two pieces:

- the event bus, a transactional outbox over the commit log, which forwards cluster events from
  shards to the hub at least once;
- `MelangeClusterCoordinator.ExecuteOnShardAsync`, which is one fenced reducer call on the shard's
  owner, from the hub.

Everything else is the application's. A handler that runs out of `Events:HandlerRetries` is
dead-lettered on the hub, and for a forwarded event it is only logged, after which delivery moves on.
The engine has no durable retry, no idempotency key, no receipt and no reconciler. A shard reducer
also has no way to call the hub: the shard publishes an event and hopes for the best.

The consumer's version shows what the application has to build in place of each missing piece. Take
an NPC store purchase:

1. A hub reducer debits the wallet, writes a pending journal line and a reservation row, and
   publishes a request event.
2. A hub handler calls the station shard's delivering reducer. That reducer checks a receipt table
   first, so a redelivery does nothing twice.
3. A second handler settles or refunds on the shard's answer, routing by matching saga-id string
   prefixes.
4. A hosted reconciler re-drives overdue reservations every five seconds, backing off. After eight
   attempts it asks the shard to cancel. The cancel records a failure unless the delivery already
   happened, so the receipt is the single place the outcome is decided.

The reverse direction (credit earned on a shard) needs its own outbox row, a retry timer, an ack
reducer and a per-shard sequence for idempotency keys. The same structure appears again for mission
accept, turn-in, remote sites and release, industry fees, and registration-block top-ups.

## The shape

```csharp
// In any reducer: the saga's record is written in this transaction's write set, so it commits or
// rolls back with the debit. Nothing leaves the node until the commit.
ctx.Sagas.Start<StorePurchase>(
    key: SagaKey.New(ctx),                      // or app-chosen: $"bounty:{rat}:{pilot}"
    target: ShardKey.Of(stationSystem),         // or SagaTarget.Hub
    payload: new ItemDelivery(item, quantity),  // stored with the saga, not passed as arguments
    state: reservationRow);                     // any row of this engine: the saga's visible state

public sealed class StorePurchase : ISaga<ItemDelivery>
{
    // Runs on the target, as one ordinary local transaction. The engine writes the receipt in the
    // same transaction, so a redelivery after commit finds the receipt and runs nothing.
    public SagaStep Deliver(ReducerContext ctx, ItemDelivery payload) =>
        IsDocked(ctx, payload) ? SagaStep.Done() : SagaStep.Refuse("not docked");

    // Run back on the originator, typed, in the transaction that deletes the saga's record.
    public void OnDone(ReducerContext ctx, Reservation state) => Settle(ctx, state);
    public void OnRefused(ReducerContext ctx, Reservation state, string reason) => Refund(ctx, state, reason);
}
```

What the engine does for each requirement the consumer named:

| Requirement | Engine behaviour |
| --- | --- |
| Starts inside the originator's transaction, with any row as state | `ctx.Sagas.Start` writes a saga record in the write set. It is a row like any other, so it inherits the originator's placement and travels with it on handoff. |
| Both directions, and chains | The target is a `ShardKey` or `SagaTarget.Hub`. That opens the missing shard-to-hub direction through the same routed, fenced call. A step may return `SagaStep.Then<Next>(target, payload)`. |
| Refusal is terminal, throw is transient | `Refuse(reason)` ends the saga with a reason. An exception is retried, including "content hasn't reached this shard yet", which replaces in-process retry loops. |
| Cancel at the receiver without racing delivery | Past `Deadline`, the engine sends the target a cancel that writes a refused receipt unless a receipt already exists. The receipt stays the single decision point. |
| Idempotency keys with defined scope and retention | Scope is (saga type, key). The dedupe set is an engine table with a declared retention, not the application's journal forever. Keys are engine-made or app-chosen. |
| Receipt garbage collection | A receipt is deleted once the originator's outcome reducer commits plus a retention window, so a late redelivery inside the window is still recognised. |
| Parked, visible state instead of dead letters | After `MaxAttempts` the saga parks in its record. It appears in metrics and `ISagaAdmin`, never only in a log line. |
| Introspection and purge | `ISagaAdmin.Query(...)` for "credits in flight"-style views, and `PurgeFor(...)` for deleting a player, so admin tooling stops deleting outbox rows by hand. |
| Fan-out | `StartMany` starts one saga per recipient in the same transaction (one kill, N shares). |
| Payloads larger than reducer arguments | The payload is stored in the saga record and read at the target, not encoded as call arguments. |
| Same code path on one node | On a single node the target is an engine in the same process. The record, receipt and retry are identical; only the transport is shorter. |
| Pending state the client can see | The saga record is an ordinary table and may be public with a row policy. |

The redrive loop is a scheduled table per engine (timers are rows), so it partitions with the sagas
it drives and needs no global wheel, the same argument CLUSTERING.md makes for timers in general.

### Candidates beyond money

The consumer named flows that would move onto the primitive, and these are part of whether it pays
for itself:

- **Application-level handoff follow-ups.** A lost handler today leaves a ship online until the next
  boot's sweep, or a respawn waiting for the player to reconnect. The engine's own handoff protocol is
  not in scope; the application work around it is.
- **Multi-node purge.** Backup, then one purge per shard, then the hub purge. Today this cannot resume
  partway. As a chain of steps it can.
- **Mission accept, turn-in and remote sites, and registration-block top-ups.** These are the same
  pattern, written again.

## What is explicitly not proposed

**Distributed transactions (two-phase commit).** Each shard has one writer, and the write lock covers
the whole reducer body ([DESIGN.md §4](../DESIGN.md)). A prepared transaction waiting on another node
would stall every write on that shard for a network round trip, and indefinitely if the coordinator
dies. Sagas stay **eventually consistent, explicitly not ACID**, exactly as CLUSTERING.md states.
This entry makes them cheap and safe to write; it does not make them atomic.

## What it costs and what it touches

This is a subsystem, not a helper.

- **Core:** engine-owned saga, receipt and dedupe tables (with placement inherited from the
  originator), `ctx.Sagas`, the redrive timer, and parked state. The new tables come under the
  hot-tier migration rules.
- **Cluster:** a shard-to-hub execute path (only hub-to-shard exists), routing a step to wherever its
  target lives *now*, and fencing on both legs.
- **Generator:** step and outcome methods become reducer descriptors with resolved sites, and
  compile-time checks keep a step from writing outside its target's placement.
- **Testing:** saga-aware helpers in `MelangeDB.Testing` (drive to completion, inject a lost
  delivery, force a park).
- **Open edges:**
  - an originator handed off mid-saga (its record travels, so the outcome reducer must route to the
    new owner);
  - a target entity handed off between the step and its receipt;
  - retention numbers that suit both a store purchase and a multi-minute purge.

## Reopening trigger

**A design pass that expresses every flow listed above — store purchase, earned credit with fan-out,
mission accept and turn-in, multi-node purge — in the primitive, each with less application code than
today's hand-written version.** If any of them only fits through a wrapper, the primitive is the wrong
shape and stays here. If all of them fit, it moves to [design/](../design/) and becomes a phase.

This is a prerequisite for [identity-sharded-hub.md](identity-sharded-hub.md): sharding the hub turns
every write that spans two identities into a saga, so it should not be considered before sagas are
cheap.
