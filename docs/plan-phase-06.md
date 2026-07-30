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
- **Subscriber checkpoints expire.** A checkpoint belonging to a subscriber that no longer exists — handler
  deleted from the code, service retired — would otherwise pin log truncation at a frozen LSN forever, which
  is a full disk on a timer. Checkpoints idle past `Events:SubscriberExpirySeconds` are evicted with a loud
  log; a subscriber returning after eviction has lost its place and starts from current state rather than
  silently resuming. Deliberate, bounded data loss, chosen over unbounded disk growth.

## Out of scope

Distributed transport (09). Ordered cross-shard delivery. Exactly-once semantics — at-least-once plus
idempotent handlers is the contract, and saying so plainly is better than implying a guarantee we can't keep.

## Decisions to settle

- ~~**Are events in the log record, or derived from it?**~~ **Settled: in the record.** Commit-record format
  version 2 appends an event section (type name, publish depth, opaque payload) after the write set; version-1
  records read back with no events, so every pre-phase-06 log stays readable with no migration — the same bar
  the phase-03 epoch sidecar set. Deriving events from row deltas was rejected because it couples every handler
  to table schema and cannot express facts that aren't row-shaped (`PlayerDied` is not an update). The cost is
  real and stated: events grow the log, and a publish-only transaction now appends a record where before it
  appended nothing. The retention interaction is bounded by design: events pin retention only up to the slowest
  *live* subscriber checkpoint, because `Events:SubscriberExpirySeconds` evicts abandoned ones (phase 07 reads
  the floor from `MelangeEventBus.MinimumLiveCheckpointLsn`). Serialization is reflection-based JSON
  (`System.Text.Json`, in the framework — no package) with converters for `Identity` and `Timestamp`; the
  record format treats the payload as opaque bytes plus a type name, so a schema-registered binary codec can
  supersede it later without a format change.
- ~~**May a handler call a reducer?**~~ **Settled: yes, as a new transaction, depth-limited.** Each event
  carries the publish depth it was born at — one more than the event whose handler (transitively) published it,
  stamped from an ambient `AsyncLocal` that flows through the handler's reducer calls. A publish at
  `Events:MaxPublishDepth` (default 4) throws, aborting the publishing reducer; the handler's failure then
  follows the ordinary retry → dead-letter path, so a cycle ends loudly with a durable record instead of
  spinning. Depth is persisted in the event record, so the guard holds across restarts and replays. Chosen over
  cycle *detection* because detection needs event identity across hops, which handlers can trivially defeat by
  rewrapping payloads; a depth bound cannot be defeated.
- ~~**Do handlers block the applier?**~~ **Settled: never — the log is the buffer.** The commit path only hands
  envelopes to the transport (which enqueues) and wakes the per-subscriber dispatch loops; no user code ever
  runs under the write lock. The in-memory delivery window is bounded by `Events:MaxQueueDepth`; on overflow the
  oldest entries are evicted and a subscriber that needed them replays from the commit log, its checkpoint lag
  saying honestly how far behind it is. This is the phase-03 backpressure precedent (`DropAndResync` over
  unbounded buffering) applied where it costs nothing: the log already holds every event durably and
  checkpoints already model lag, so "drop from the window" loses nothing at all. A slow or retrying subscriber
  delays only itself — each subscriber has its own loop and its own checkpoint.
- ~~**Is the bus visible to clients?**~~ **Settled: deferred, unchanged.** Handlers are the only consumers in
  this phase. A client-visible feed is a subscription to an append-only table a handler (or the reducer itself)
  writes — one delivery channel, not two. Revisit only if phase 09's world events produce a concrete need.

### Implementation decisions recorded

- **Checkpoint durability**: a JSON sidecar beside the log (`<CommitLog:Path>/melange.events.json`), per the
  epoch-sidecar precedent — the log format is untouched, and losing the file costs only redelivery, which
  at-least-once already permits. Each entry holds the subscriber's LSN, its last-active timestamp (the expiry
  clock), and, after eviction, a tombstone — which is how a returning subscriber is *told* it lost its place
  (EventId 1404) rather than silently resuming. Writes replace the file atomically (temp + move).
- **Subscriber identity**: the handler type's full name. Renaming a handler class is therefore a new subscriber
  that starts from current state — documented behavior, not an accident.
- **New subscribers start at the current head.** A handler deployed for the first time does not replay world
  history into itself; catch-up is for subscribers that *have* a checkpoint. (A returning-after-eviction
  subscriber is the same rule plus the loud log.)
- **Dead letters**: one JSON line per poisoned event in `<Events:DeadLetterPath>/melange.deadletter.ndjson` —
  subscriber, event type, LSN, depth, attempt count, error, and the payload as raw JSON, fsynced per append.
  Delivery then advances past the event; the checkpoint never wedges on a poison message.
- **Retry backoff** is exponential: `Events:RetryBackoffMs`, doubling per retry, capped at 30 s, driven by the
  injected `TimeProvider` so tests hand-crank it.

## Done when

- An event published by a committed reducer reaches its handler.
- An event published by a reducer that then **throws** reaches nobody — asserted, since this is the property
  the whole design exists for.
- A handler that throws is retried per policy and eventually dead-lettered without stalling the log pipeline
  or blocking later transactions.
- A subscriber stopped for N transactions and restarted receives all missed events from its checkpoint.
- Two handlers for the same event both receive it, and one failing does not prevent the other.
- Publishing from inside a handler is either rejected or depth-limited, with a test either way.
- A subscriber checkpoint idle past expiry stops pinning compaction and is evicted loudly; the returning
  subscriber is told it lost its place rather than silently resuming from a truncated log.

## Risks

- **This looks trivial and isn't.** The failure modes (handler wedges the applier, unbounded queue growth,
  event→reducer cycles) all appear under load rather than in tests. Bound everything, and write the
  slow-handler test on day one.
- **Log growth.** If events live in the log and handlers are the only consumer, retention is now driven by the
  slowest subscriber. Phase 07's compaction has to account for event retention, not just row state.
