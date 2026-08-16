# Phase 17 — Group commit

**Status: Shipped.**

**Goal:** raise the per-shard durable-commit ceiling by coalescing fsyncs across concurrent
commits, at **unchanged `OnCommit` semantics** — a reducer that returns has committed durably,
exactly as today. No new durability mode, no relaxation, ideally no knob.

**Depends on:** nothing in phases 13–16. It changes the inside of the commit path only.

## Why here

Phase 10 measured and published the hotspot ceiling: **~1,100 commits/s per crowded shard at
per-commit fsync, ~52,000 at interval fsync** — and noted that no cluster size changes either
number, because a hotspot is one shard by definition. The 47× gap is not the cost of durability;
it is the cost of *serialized* durability. Today the whole chain holds locks through the fsync:
the reducer body, reconcile, and append run under the engine's write lock, and
`FileCommitLog.Append` holds the log's lock through `Flush(onCommit: true)`. At ~1 ms per fsync,
1,100/s is simply the arithmetic.

Group commit is the classic answer, as old as ARIES: many commits buffer, one fsync makes them
all durable, every waiter whose LSN the flush covered acks. The disk does the same work per
flush; it just answers for ten commits instead of one. This is the single largest per-shard
throughput lever left that changes no semantics — and the reference port is exactly the workload
shape (many concurrent player reducers on a crowded shard) that collects the win.

## The design in one sentence

**Append buffered under the locks, wait for durability outside them, and let whoever finds the
flusher idle fsync everything buffered so far — batches form from contention itself, with no
timer and no delay.**

Sync piggybacking, concretely: a committing thread appends its record (buffered, no fsync),
releases the engine write lock, then waits until the durable LSN covers its record. If no flush
is in flight it performs one itself — a lone caller fsyncs immediately and pays exactly today's
latency. If a flush *is* in flight, it parks; the flush that just finished picks up everything
that accumulated behind it in one fsync. Under contention the batch size is however many commits
arrive per fsync duration — self-tuning, with no `MaxBatchSize` or `MaxDelayMs` to mis-set, and
**no added latency in the uncontended case**, which is most engines most of the time.

## Deliverables

**The split commit path.** `FileCommitLog` grows an append-without-flush path and a
`WaitDurable(lsn)` (names to taste) with the piggybacking flusher behind it. The engine's
serialized path becomes: reducer + guards + reconcile + append under the write lock; durability
wait after release; **fan-out, observers, and the caller's return only after durability**. That
last ordering is load-bearing and must be preserved from today: a subscriber must never see an
LSN that a crash could untell — `Resume` names epoch + LSN, recovery truncates torn tails
without an epoch mint, and a cursor past the truncated head would strand. Fan-out stays
LSN-ordered per subscriber; the flusher releases waiters in LSN order so batches cannot
interleave deliveries.

**Failure semantics unchanged in kind, widened in blast radius.** A failed group fsync fails
every commit in the covered range: the file is truncated back to the last durable length (the
`RollBackPartialAppend` contract, batch-wide), every parked waiter gets the failure, and the log
poisons exactly as it does today for the single-append case — a partially durable batch must not
let a later append land after orphaned records or re-mint their LSNs. The phase's hardest tests
live here: fault injection at every point of the batch lifecycle, the poison-then-restart
recovery already proven for single appends re-proven for batches.

**Policies other than `OnCommit` untouched.** `Interval` and `OsBuffered` never waited for
durability and still don't; `FlushToDisk`, the backup fence's `FlushBuffers`, and snapshot
capture interact with the flusher but not with waiters.

**Telemetry.** `melange.log.group_commit.batch_size` (histogram — its shape *is* the feature
working); the existing per-append fsync attribution (`LastAppendFsyncMilliseconds`, which feeds
the commit span) needs an honest answer for a shared fsync — see decisions. Recorded in
[OBSERVABILITY.md](../OBSERVABILITY.md).

**The re-measurement.** `CommitPathBenchmarks` extended with a concurrent-committers benchmark;
the phase 10 hotspot numbers re-run and republished wherever they are quoted. The deliverable is
not "group commit exists" but a new, honest ceiling sentence with the same rigor as the old one.

## Out of scope

**Raising `Interval` throughput** — it has no fsync wait to coalesce. **Parallel reducer
execution** — the single-writer transaction is the design (phase 02); snapshot-isolation
reducers (which already run bodies outside the write lock) compose with this phase but are not
extended by it. **Cross-engine batching** — every engine owns its log file; a hub and its shards
do not share fsyncs. **Platform-specific I/O** (io_uring, `F_FULLFSYNC` tuning) — the win here
is batching, not syscall selection.

## Decisions to settle

### Settled: the committing thread blocks

The engine's `Invoke` is synchronous end to end, and the transport calls it from its own
concurrency. Blocking the calling thread on the durability wait is simple, preserves every
signature, and wins whenever concurrent callers exist — which is precisely the hotspot scenario;
a lone caller blocks for its own fsync, same as today. Plumbing an async ack through the engine
would free those threads but touches every caller of `Invoke` and the reducer contract's
synchronous story. **Settled as leaned: blocking wait** — `FileCommitLog.WaitDurable(lsn)`, called
by every engine append path after the write lock releases and before the commit returns. Revisit
only if thread-pool pressure shows up in the port's numbers, with evidence.

### Settled: the attribution reports the wait the caller experienced

`LastAppendFsyncMilliseconds` currently answers "what did durability cost *this* transaction" —
under group commit the honest answers diverge: the batch's fsync duration (what the caller
waited, roughly), the amortized share (what the caller *cost*), or null-when-piggybacked (the
`Interval` precedent: no inline fsync, nothing to charge). **Settled as leaned:**
`melange.fsync_ms` reports the wait the caller actually experienced (near-zero when an in-flight
flush already covered the record — group commit doing its job), `melange.commit_ms` becomes the
append alone, and the `melange.log.group_commit.batch_size` histogram beside them makes the
amortized view derivable without either metric lying. The `melange.fsync` span belongs to
whichever transaction performed the shared flush. `LastAppendFsyncMilliseconds` is gone;
`WaitDurable`'s return value replaced it.

### Settled: the escape hatch already existed, so it is honored

Leaning: none. The semantics are identical and the uncontended latency is identical, so a
`CommitLog:GroupCommit=false` flag would exist only to express distrust of the implementation —
which is what the fault-injection suite is for. If a real incident ever demands it, adding a
flag is an afternoon; shipping one preemptively is a knob that documents a fear.

**Settled the other way, because the leaning was written against the wrong baseline:**
`CommitLog:GroupCommit` has existed since phase 07, shipped accepted-and-reserved at `true` for
exactly "a future concurrent commit path" — this phase is that path, and silently ignoring a
documented, live-reloaded option is worse than the knob. It is honored: `false` restores the
phase-01 inline fsync under the write lock, single-append rollback contract and all. The
distrust argument stands — the fault-injection suite is still the real answer — but a reservation
kept is not a fear documented.

## Shipped notes

- **The ordering contradiction, and where it resolved.** The deliverable said "fan-out, observers,
  and the caller's return only after durability" — but the pre-image guarantee pins observers
  before the store applies, and serializability pins the store apply inside the write lock, so
  observers cannot run after a durability wait that happens outside it. The resolution is the
  walsender's: fan-out stays where it was (under the lock, computing against the pre-image), the
  caller's return gates on durability in the engine, and everything that *leaves the process*
  gates at the edges on the new durable watermark. Recovery's prefix-truncation makes same-log
  consumers (the scheduler's fires, saga markers) safe with no gate at all: an effect appended
  after an untold record is untold with it.
- **`ReadFrom` caps at `DurableLsn`, which gated most edges for free.** Every pull-shaped consumer
  — Postgres dispatch, resume replay, replica and border streams, the event forwarders, the event
  bus — reads the log through `ReadFrom`, so one cap covered them all; only the push-shaped paths
  (live deltas, initial-set anchors, resume heads, replica bootstrap) needed explicit
  `WaitDurable` gates, and the online backup a fence. Two costs surfaced and were paid: in-process
  projections need the uncapped read (their catch-up runs under the write lock ahead of the
  fsync), and every loop that judged "more to do" by `HeadLsn` had to judge by `DurableLsn`
  instead or it busy-spun through the un-fsynced gap.
- **The fsync needs its own file handle.** Measured, not theorized: on Windows a
  `FlushFileBuffers` serializes against writes on the same handle, so fsyncing through the append
  handle stalled the next append — write lock and all — behind the in-flight flush, and the first
  concurrent measurement produced a mean batch of exactly 1.0. Through a second handle to the same
  file (probe: mean append 0.1 ms against 2.4 ms same-handle), batches formed immediately: mean
  8.0 at 16 callers. The log file's share mode widened to admit the second handle; the
  `melange.lock` sidecar has been the actual second-writer refusal since phase 15.
- **Crash recovery now accepts multi-record torn tails.** Buffered appends mean the un-fsynced
  region can span several records, and the OS persists pages in no particular order — so an
  intact-looking record beyond a tear is an expected crash artifact, and everything past the first
  invalid record is truncated (loudly). What keeps deep corruption fatal is the durable floor: the
  snapshot's LSN, which `CompleteSnapshot` now forces the log durable through before the file is
  written — under every policy, which also closed a pre-existing `Interval`/`OsBuffered` window
  where a crash could boot a snapshot ahead of its own log and re-mint LSNs it already held.
- **A failed batch fsync poisons; it does not apologize.** The covered commits already ran their
  fan-out and applied to the store, so the only honest outcome is: roll the file back to the last
  durable length (left in place, the OS could persist the bytes later and failed transactions
  would materialize at the next boot — the fsyncgate lesson), fail every parked waiter with the
  original fault attached, and refuse appends until a restart reconverges from the durable prefix.
  The divergence window — in-memory state ahead of the durable log, readable until restart — is
  documented on `Invoke`, and the poison is what bounds it.
- **Re-measured ceilings** (same box, same day, Release, each in isolation; the absolute numbers
  are the machine's): ~500 commits/s per-commit fsync sequential (unchanged by design — a lone
  caller pays its own flush), **~4,000 commits/s at 16 concurrent callers with a mean batch of
  8.0**, ~12,000 commits/s interval. Published in CLUSTERING.md, CONFIGURATION.md, and
  LOAD-TESTING.md; the live measurement is `HotspotMeasurementTests`, now with a concurrent-caller
  variant, and `GroupCommitBenchmarks` isolates the engine commit path in the bench suite.
