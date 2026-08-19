# Scheduler lanes: player reducers ahead of simulation work

**Goal:** a player's `Move` does not wait behind `GrowFlora`. Simulation work yields to
client-initiated work when the two contend for one shard's write lock, instead of being ordered by
whichever happened to ask first.

**Status:** **open, and possibly unnecessary.** Three shipped mechanisms already attack most of what
this proposal is usually asked for, and the residue they leave has never been measured. The
measurement that decides whether this becomes a feature is a named deliverable of
[road-to-0.3 phase 20](../road-to-0.3/plan-phase-20.md). This record exists so the decision is made
against evidence rather than re-argued from intuition every few months — which is what has been
happening.

**Depends on:** [plan-phase-01](../road-to-0.1/plan-phase-01.md) (the single write lock),
[plan-phase-05](../road-to-0.1/plan-phase-05.md) (timers are rows; the scheduler and its overrun
policy), [snapshot-isolation.md](snapshot-isolation.md) (the body-out-of-the-lock axis),
[plan-phase-17](../road-to-0.2/plan-phase-17.md) (group commit).

## Why

An engine has one write lock, and [DESIGN.md](../DESIGN.md) §4 is blunt about what it covers: the
reducer body, the commit guards, the buffered log append, and the commit observers. The question a
module author is answering is not *"is this reducer slow for its caller"* but **"how long may this
hold the entire world still."**

The reference workload has fourteen scheduled reducers, and they are the simulation. They contend
for that lock with `Move`, `Attack`, and `Gather`. The failure mode people describe is a hitch in
the town square every time a sweep runs — a cost paid by players, caused by work no player asked
for.

## What is already answered

This is the part that has to come first, because the proposal is routinely argued as though none of
it existed.

**Windowing.** [DESIGN.md](../DESIGN.md) §4 already prescribes it: a long sweep runs as many short
transactions carrying a cursor, so the world freezes in slices rather than in one block. Both of the
reference workload's expensive sweeps already do this — `FloraChunkWindowPerTick` and
`CreatureChunkWindowPerTick` are exactly that mechanism, and `MELANGE0017` pushes authors toward it.

**Snapshot isolation.** [snapshot-isolation.md](snapshot-isolation.md) is **built**, and it was
built for precisely the reducers this proposal names. `GrowFlora` is its worked example. A body
declared `Isolation.Snapshot` runs against a stable read view *outside* the write lock; only
reconcile, the commit guards, and the append serialize. A sweep that spends 200 ms reading and
0.2 ms writing stops charging the other 199.8 ms to every writer.

**Group commit.** Phase 17 removed the durability wait from the critical section — the append
buffers, the lock releases, and the caller waits for a shared fsync afterwards. The
often-quoted case for a scheduler lane is built on phase 10's ~1,100 commits/s figure, which phase
17 superseded.

So the honest framing of the remaining problem is much narrower than the usual pitch: **for a sweep
that is windowed and declared `Isolation.Snapshot`, what is left under the lock is reconcile, the
guards, the append, and the observers.**

## What is actually left

Two things, and only the second is interesting.

**The residual serialized portion.** Even a perfectly-behaved sweep still takes the lock, and fan-out
runs under it as a commit observer. Windowing makes each slice short; it does not make slices free,
and fourteen timers produce a lot of slices. Whether that residue is a millisecond or a hitch is
unmeasured.

**There is no policy at all — only arrival order.** This is the real gap. `MelangeEngine` holds a
`System.Threading.Lock`, which offers no priority and guarantees no fairness ordering whatsoever. A
scheduled fire that reaches the lock first goes first, always, no matter how many players are
queued behind it. Nothing in the system expresses the idea that a player's transaction is worth more
than a tick's — and that is a policy the engine could reasonably hold, because unlike most database
workloads, **here the engine knows which is which**.

It already knows, in fact. Scheduled fires run as `Identity.Hash("melange/scheduler")`, a distinct
caller established in phase 05. The discriminator this proposal needs already exists and is already
on every scheduled transaction.

## The rule that decides everything

> **A reducer is a transaction, so it cannot yield. Anything described as "the simulation yields
> mid-tick" is either windowing under a different name, or a correctness bug.**

This kills the most common phrasing of the proposal outright. "Simulation work has a per-tick
millisecond budget and yields" is not implementable as stated: a reducer holding the write lock is
inside a transaction, and suspending it to let a player through would mean either releasing the lock
mid-transaction (abandoning atomicity) or holding it while idle (worse than the problem). What *is*
implementable is deciding, **between** transactions, whether the next slice of simulation work runs
now or later.

Which means every viable shape below is about **admission**, not preemption.

## Shapes considered

**1. Priority admission at the lock.** Two queues; player-initiated transactions admitted ahead of
scheduler fires. Directly expresses the policy. It also means replacing the engine's `Lock` with a
hand-rolled admission gate on the hottest path in the system, and it introduces starvation as a new
failure mode — a busy shard could defer simulation indefinitely, which turns a latency problem into
a world that stops evolving.

**2. Adaptive windowing driven by contention.** The scheduler shrinks or skips its window when
players are waiting. Entirely scheduler-side: no change to the lock, no new failure mode in the
engine's core, and it composes with windowing rather than competing with it.

**3. A contention signal, and let the existing policy respond.** The smallest shape, and the one
this record favours. **The scheduler already implements deferral of simulation work** —
`SchedulerOverrunPolicy.Skip` is the default precisely because *"silent pile-up is how a simulation
death-spirals under load."* Skip triggers when a tick is late against its own cadence. The proposal
becomes: let it also trigger when **players are waiting**, by giving the scheduler a contention
reading it does not currently have.

That reframing is the most useful thing in this record. The machinery for "defer simulation work
under load" is shipped, tested, and has a documented default. What is missing is not a lane; it is a
signal, plus permission for an existing policy to act on it.

## Why this may not be a feature at all

Stated plainly, because the project's habit is to record refusals with reasoning rather than build
on momentum:

- The workload it targets is already windowed and already has snapshot isolation available. If the
  reference workload's sweeps are not declared `Isolation.Snapshot`, **the first action is to
  declare them**, not to build a scheduler lane — and that is a configuration finding, not a phase.
- `Telemetry:SlowReducerMs` already alarms on exactly this, and is documented as *"how long is it
  acceptable to freeze the world."* If it is not firing, the hitch is somewhere else.
- Starvation is a real cost. A world that stops growing flora because the square is busy is a
  different bug report, and arguably a worse one.
- The measurement does not exist. Every version of this argument to date has been built on
  arithmetic and on phase 10 numbers that phase 17 replaced.

## Decisions to settle

### Does the residual actually cost a player anything

**The gating question.** Phase 20 measures the p99 of a player-initiated `Move` on a shard running
the fourteen scheduled reducers, and the share of lock-held time the scheduled work accounts for.

**Leaning:** if scheduled work is a small share of lock-held time, or if player p99 is unaffected by
its presence, this record closes as a recorded refusal and the idea stops being re-argued. That is a
legitimate and probably likely outcome.

### If it is real, signal or lane

**Leaning:** shape 3, then shape 2 if 3 is insufficient. Shape 1 last and probably never — the cost
is a bespoke admission gate on the engine's hottest path, and the benefit over "the scheduler
declines to start a slice right now" is small.

**Open:** what the contention reading actually is. `WriteLockBusyTicks` already exists and is already
accumulated per engine, but it measures *utilization*, not *queue depth* — and the policy wants to
know whether anyone is waiting, not how busy the lock has been. A waiter count is the honest signal
and it is not currently tracked.

### Whether the discriminator is identity or declaration

Scheduled fires already run as `Identity.Hash("melange/scheduler")`, so a policy could key on the
caller with no new API. Alternatively the reducer's own declaration says it — the scheduler knows it
is firing a scheduled reducer without inspecting the caller.

**Leaning:** the declaration. Keying policy on an identity value invites a client to be mistaken for
the scheduler if that identity ever becomes reachable, and it conflates *who called* with *what kind
of work this is*. **Open:** whether lifecycle reducers (`ClientConnected` and friends) count as
player work or simulation work — they are client-caused but transport-fired, and the answer is not
obvious.

### Whether any of this is configurable

`Scheduler:MaxConcurrentTicks` is the precedent worth noting: it is accepted-and-reserved at 1
because a worker pool would parallelize nothing that serializes on the write lock anyway.

**Leaning:** if this ships, it ships as a policy enum on `SchedulerOptions` beside `OverrunPolicy`,
defaulting to today's behaviour, rather than as a millisecond budget. A budget invites tuning a
number nobody can derive; a policy invites choosing between named behaviours, which is the shape
every other decision in this system takes.
