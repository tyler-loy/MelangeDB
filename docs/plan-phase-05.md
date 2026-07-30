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

- **Are scheduled reducers callable by clients?** They must not be, or a client can force a world tick.
  Default to rejecting client calls to any reducer named by a `Scheduled` table.
- **Timer identity across restart.** After downtime, a repeating timer is overdue by however long the process
  was down. Fire once and resume, or catch up N times? Fire-once is almost always right for a simulation;
  catch-up is right for billing. Pick and document.
- **Do lifecycle reducers get a transaction each?** Yes, for consistency — but `ClientConnected` doing real
  work (spawn, state creation) on a connection storm is a thundering-herd risk worth knowing about.
- **Scheduler fairness.** With 14 timers on one loop, a slow tick starves the rest. Single-threaded and
  documented, or a bounded worker pool per timer table?

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
- **The reference workload's global-timer pattern won't survive phase 09.** Today one `CreatureAiTick` row
  drives a reducer that scans for creatures near online players. Under partitioning that becomes one timer
  row per shard. Worth designing the API so that change isn't a rewrite.
