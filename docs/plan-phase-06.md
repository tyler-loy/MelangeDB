# Phase 06 — The event bus

**Goal:** a reducer can publish domain events that are delivered exactly when its transaction commits, never
when it rolls back.

**Depends on:** [01](plan-phase-01.md).

## Why here

Small phase, high leverage. The commit log already *is* an outbox, so the bus is mostly wiring — and it's
needed before phase 09, where cross-shard sagas and world events ride on it. Doing it now also gives
integrations (metrics, external services, the admin console) a supported extension point instead of everyone
polling tables.

## Deliverables

```csharp
[Reducer]
public void Attack(ReducerContext ctx, Identity target, int weaponId)
{
    // ... row mutations ...
    if (health <= 0)
        ctx.Publish(new PlayerDied(target, ctx.Caller, ctx.Timestamp));
}
```

- **`ctx.Publish` performs no I/O.** The event lands in the write set; publication happens after the log
  append. This is the transactional outbox pattern with the log as the outbox, and it buys two properties:
  - An event is **never** observed for a transaction that rolled back. The classic
    "notification escaped but the state change didn't" failure is structurally impossible.
  - Delivery is **at-least-once and replayable**, because a subscriber is just another log consumer with its
    own checkpoint — the same mechanism as the storage appliers. A subscriber that was down catches up rather
    than losing events.
- Handlers resolved from DI, running **outside** the emitting transaction:
  ```csharp
  public sealed class DeathHandler(ILogger<DeathHandler> log) : IEventHandler<PlayerDied>
  {
      public Task HandleAsync(PlayerDied e, CancellationToken ct) { /* ... */ }
  }
  ```
- `IEventTransport` with an in-process implementation. The seam exists so phase 09 can drop in a distributed
  transport without touching handler code.
- Handler failure policy: retry with backoff, then a dead-letter path. A handler must not be able to wedge the
  log's applier pipeline.

## Out of scope

Distributed transport (09). Ordered cross-shard delivery. Exactly-once semantics — at-least-once plus
idempotent handlers is the contract, and saying so plainly is better than implying a guarantee we can't keep.

## Decisions to settle

- **Are events in the log record, or derived from it?** Storing them makes replay trivially correct and the
  contract obvious, but grows the log for data that is often transient. Deriving them from row deltas avoids
  the bloat but couples handlers to schema. Leaning "in the record" for clarity; note the cost.
- **May a handler call a reducer?** Yes — but that's a new transaction, and an event → reducer → event cycle
  is an infinite loop. Depth limiting or cycle detection needed.
- **Do handlers block the applier?** They must not. Which means a queue, which means bounded buffers and a
  policy for what happens when they fill.
- **Is the bus visible to clients?** Pushing events to subscribed clients is tempting (combat feed, chat) but
  it's a second delivery channel next to subscriptions. Defer — subscriptions to an append-only table cover
  it with one mechanism.

## Done when

- An event published by a committed reducer reaches its handler.
- An event published by a reducer that then **throws** reaches nobody — asserted, since this is the property
  the whole design exists for.
- A handler that throws is retried per policy and eventually dead-lettered without stalling the log pipeline
  or blocking later transactions.
- A subscriber stopped for N transactions and restarted receives all missed events from its checkpoint.
- Two handlers for the same event both receive it, and one failing does not prevent the other.
- Publishing from inside a handler is either rejected or depth-limited, with a test either way.

## Risks

- **This looks trivial and isn't.** The failure modes (handler wedges the applier, unbounded queue growth,
  event→reducer cycles) all appear under load rather than in tests. Bound everything, and write the
  slow-handler test on day one.
- **Log growth.** If events live in the log and handlers are the only consumer, retention is now driven by the
  slowest subscriber. Phase 07's compaction has to account for event retention, not just row state.
