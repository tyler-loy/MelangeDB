# Phase 05 — Scheduled and lifecycle reducers

**Goal:** the world ticks. Timers stored as rows fire reducers on schedule, and session transitions run
lifecycle reducers.

**Depends on:** [01](plan-phase-01.md); benefits from [03](plan-phase-03.md) for lifecycle testing.

## Why here

The audited reference workload has **14** scheduled reducers, and they are not peripheral — they *are* the
simulation (creature AI, population, flora growth, resource respawn, breath, station heat, work progress,
trade expiry, project expiry, three decay reducers, two compaction reducers). Without scheduling, MelangeDB
can host a chat app but not a game. This was the largest omission in the original design.

## Deliverables

**Timers as rows**
```csharp
[Table(Scheduled = nameof(TickCreatures))]
public partial struct CreatureAiTick
{
    [PrimaryKey, AutoInc] public ulong Id;
    public ScheduleAt ScheduledAt;      // Instant (one-shot) | Interval (repeating)
}
```
- `ScheduleAt` as a discriminated shape: a one-shot instant, or a repeating interval.
- Scheduling is **transactional** — inserting a timer row commits with the work that scheduled it, so a
  rolled-back reducer schedules nothing. This is the whole reason timers are data rather than a
  `[Cron("...")]` attribute, and the property that makes them partition in phase 09.
- Timers survive restart because they live in the log like any other row.
- A timer table is implicitly private and implicitly `Local` until phase 09 gives it a placement.

**The scheduler**
- A timer wheel driven by the timer tables; fires the named reducer with the timer row as its argument.
- **Overrun policy** must be explicit: if a tick takes longer than its interval, does the next fire
  immediately, skip, or coalesce? Skip-and-log is the sane default; whatever is chosen must be documented,
  because silent pile-up is how a simulation death-spirals under load.
- One-shot timers delete their own row on fire, transactionally with the work.
- Repeating timers reschedule as part of the same transaction.

**Lifecycle reducers**
```csharp
[Reducer(ReducerKind.ClientConnected)]    public void OnConnected(ReducerContext ctx) { }
[Reducer(ReducerKind.ClientDisconnected)] public void OnDisconnected(ReducerContext ctx) { }
```
- **A session beginning must be distinct from a query being run.** In SpacetimeDB, owner SQL over HTTP fires
  `ClientConnected` with a fresh ConnectionId, forcing the reference module to detect tooling identities to
  avoid creating ghost player rows and inflating login counts. Don't reproduce that: an admin query is not a
  session.
- `ClientDisconnected` must fire on ungraceful drops too, which means a heartbeat/timeout, not just socket
  close.

## Out of scope

Cluster-wide scheduling (09). Cron expressions — intervals and instants cover the reference workload, and
cron strings are a parsing liability for no gain here.

## Decisions to settle

- ~~**Are scheduled reducers callable by clients?**~~ **Settled: no — rejected at the pre-transaction
  gate.** Any reducer named by a `Scheduled` table is refused for client-originated calls (websocket and
  HTTP one-shot alike) before any transaction opens, with the same "unknown reducer" answer lifecycle
  reducers give — a probing client cannot even confirm the tick exists. Scheduled reducers are likewise
  excluded from the unpoliced-reducer report, since a policy on an uncallable reducer is dead weight.
  In-process dispatch and the scheduler itself are unaffected.
- ~~**Timer identity across restart.**~~ **Settled: fire-once-and-resume (`Scheduler:CatchUpAfterDowntime =
  FireOnce`, the default).** An overdue repeating timer fires once at recovery and resumes its cadence from
  there — the simulation reading of downtime is "the world was paused." `CatchUpAll` exists for
  billing-shaped work: one fire per missed interval, back to back. The contrast matters because repeating
  timers persist **no per-fire bookkeeping** (see the write-amplification settlement below), so downtime is
  measured from the recovered log's tail record — the moment the world last moved — an approximation that is
  exactly right whenever the world was active at shutdown. A workload needing *exact* catch-up accounting
  should use a self-rescheduling one-shot instead: each fire transactionally inserts its successor's
  instant, buying precise bookkeeping at the cost of one deliberate row write per fire.
- ~~**Do lifecycle reducers get a transaction each?**~~ **Settled: yes, one transaction per fire.**
  `ClientConnected` fires on the completed websocket handshake (on the read loop, so the client's first
  `Subscribe` already sees what it committed); `ClientDisconnected` fires on graceful close and on
  heartbeat-detected drops, paired one-to-one with the connect. HTTP one-shots, ad-hoc SQL, and ticket
  minting fire nothing — an admin query is not a session. The **thundering-herd risk stands recorded
  rather than knobbed**: a reconnect storm after a network blip runs one `ClientConnected` transaction per
  socket, serialized on the engine write lock. `Auth:MaxConnectionsPerIdentity` and the IdP already bound
  the inflow, a cheap load-shedding knob here would just trade the herd for silently missing session state,
  and a game whose `OnConnected` is expensive should move that work to a scheduled batch. A throwing
  lifecycle reducer is logged (EventId 1205) and never takes the session down with it.
- ~~**Scheduler fairness.**~~ **Settled: a single-threaded dispatch loop over one `TimeProvider` timer
  armed at the earliest due entry, with a linear scan for "earliest" — the simplest correct thing at the
  reference workload's 14 timers** (a heap or wheel is a drop-in later; the scan is an implementation
  detail, not API). No worker pool, because reducer invocations serialize on the engine's single-writer
  lock regardless — parallel tick dispatch would parallelize only argument encoding.
  `Scheduler:MaxConcurrentTicks` stays registered (default 1, values above 1 accepted and reserved). The
  failure mode is deliberate and visible: one slow tick delays every other timer, surfaced by
  `melange.scheduler.overruns` and the slow-reducer warning rather than hidden by concurrency the lock
  would nullify.

## Done when

- A repeating timer fires on interval, and its work commits atomically with its own reschedule.
- A one-shot timer fires exactly once and removes itself; process restart does not re-fire it.
- A reducer that inserts a timer row and then throws leaves **no** timer scheduled.
- Restarting the process mid-schedule resumes all timers from the log with no duplicates and no losses.
- A tick that overruns its interval follows the documented overrun policy, proven by a test.
- `ClientConnected` fires on connect and `ClientDisconnected` on both graceful close and dropped socket.
- An admin/ad-hoc query does **not** fire `ClientConnected`.
- A client attempting to call a scheduled reducer directly is rejected.

## Risks

- **Timers plus fsync-per-commit is a write-amplification trap.** Fourteen timers rescheduling themselves
  every few seconds is a steady stream of transactions doing nothing but bookkeeping. Consider whether a
  repeating timer needs to write a row at all on each fire, or can be derived from its interval.

  **Settled when shipped: a repeating timer's next fire derives from its interval; its row is written only
  when created, changed, or deleted.** The scheduler keeps the pending fire in memory, rebuilt on restart
  from the rows plus the log-tail anchor, so a repeating fire commits only what its reducer wrote — and a
  fire that writes nothing appends **nothing**: fourteen idle simulation timers cost zero log records and
  zero fsyncs (asserted by test). One-shot timers are the deliberate exception: the row *is* the schedule,
  so the fire deletes it transactionally with its own work — one commit record carrying both. What this
  trades away is per-fire durability of a repeating timer's phase: after a restart the cadence re-anchors
  rather than resuming mid-interval, which is precisely the fire-once-and-resume semantic settled above.
- **The reference workload's global-timer pattern won't survive phase 09.** Today one `CreatureAiTick` row
  drives a reducer that scans for creatures near online players. Under partitioning that becomes one timer
  row per shard. Worth designing the API so that change isn't a rewrite.

  **How the shipped API keeps that true:** the timer table is an ordinary table and the scheduler holds one
  pending entry *per row*, with no exactly-one-row assumption anywhere — a table already schedules any
  number of concurrent timers, each firing the same reducer with its own row as the argument. "Per-shard
  timers" is therefore purely data (one row per shard, a shard key column on the row) plus phase 09 letting
  timer tables declare a `Placement` other than the implicit `Local` and running each shard's scheduler
  over its own log. The reducer signature, `ScheduleAt`, and the transactional fire contract don't change.
