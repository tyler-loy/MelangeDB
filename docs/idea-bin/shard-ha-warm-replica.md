# Shard HA as a warm replica

**Shape:** a second node per hot shard that is already applying that shard's log into its own
projection, holds no write lock, and can take one on fence without rebuilding anything.

**Status:** **measured, and moving to a design record** (2026-11-20). [Phase 20](../road-to-0.3/plan-phase-20.md)
timed the window this entry's trigger asked for (see *The measurement* below). And the project owner
settled the framing the trigger left open: **production systems get HA, and the bar for not building
it is high.** So the measurement shapes [phase 30](../road-to-0.4/README.md)'s design; it doesn't
decide whether phase 30 happens. ROADMAP.md's shard-HA deferral is lifted by that decision, and this
entry stays as the proposal the design record starts from.

## What exists today

Node-death reassignment shipped in phase 09 and is correct: the hub fences the dead owner, assigns
the shard to a new node, and that node rebuilds its projection from the newest snapshot plus the log
tail before serving. Phase 13's planned drain added the graceful version of the same ownership
transfer — fence, hand over, resume — between two live nodes.

Both are **recovery**. Neither is availability: in both cases the projection is built *after* the
decision to move, and the shard is unavailable for writes while it builds.

## The argument for

A crowded shard is by definition the one with the most players on it, the largest hot set, and
therefore the longest rebuild. The recovery window is not an abstraction to the people in it — it is
a loading screen for everyone on that island, and it scales the wrong way: the more successful the
shard, the longer the outage.

The mechanism is mostly assembled. A shard is already an engine with its own log, and the applier
pipeline already exists and is already exercised by every node that owns a shard. A warm replica is
an applier running against a log it does not own, plus the ownership transfer phase 13 built, minus
the rebuild. Described that way it is a recombination of shipped parts rather than a new subsystem.

## The argument against

Three things, and the first is the one that matters.

**Nobody had measured the window.** When this entry was written, the case above was "the recovery
window is too long" with no number behind it. Phase 20 supplied one; see *The measurement*. (Kept as
written: the other two points below are still the design's real costs.)

**It doubles the resident footprint of the shard you least want to double.** Residency is opt-in and
computable on purpose (phase 07) — a warm replica makes the honest footprint of a hot shard two
nodes' worth. That interacts directly with the elastic layer, which is trying to *reduce* how many
nodes are held.

**Fencing gets a third state.** Today a shard is owned or it is being transferred. A replica that is
applying but not authoritative adds a role that every fencing path, heartbeat, and the membership
store must now reason about — and split-brain in a replica that thinks it took the lock is a
data-loss shape, not a downtime shape.

## What is *not* proposed

Dynamic boundary splitting stays deferred, with its reasoning in
[design/elastic-rebalancing.md](../design/elastic-rebalancing.md). Fixed grains plus reassignment
remains the right elasticity story; this idea is orthogonal to it.

## The measurement (phase 20)

Process death of a shard's owner, default timing (heartbeat 1 s, failure timeout 10 s), 20 clients on
the shard. Full method and table in [LOAD-TESTING.md](../LOAD-TESTING.md).

| Rows on the shard | Detect | Reopen (in cluster) | Recovery alone | Clients back |
| --- | --- | --- | --- | --- |
| 10k | 10.8–11.7 s | 0.6–1.4 s | ~0.1 s | 12–14 s |
| 100k | 10.6–12.2 s | 0.9–2.8 s | ~0.5 s | 12–14 s |
| 1M | (contaminated, see below) | 4.9–6.1 s | 3.2–4.5 s | 12–19 s |

What it says about the design:

1. **The window is a loading screen at every size.** It's 12–19 s before any write lands. Phase 28's
   automatic reconnect doesn't hide it, because the client never disconnects: the gateway holds or
   fails its calls.
2. **Detection, not rebuild, dominates.** That part is 10.6–13.4 s, and a warm replica doesn't
   touch it. Fencing forbids promotion before the dead owner's lease expires, and the hub's sweep
   adds up to half the timeout on top ([#201](https://github.com/tyler-loy/MelangeDB/issues/201)).
   **A warm replica alone removes 1–6 s of a 12–19 s window.** Phase 30's design has to own
   detection too: shorter leases, a faster sweep, or an explicit failure signal from the link. The
   fencing third state below is where the shorter-lease risk lives.
3. **The cold path could cascade. Fixed.** A node opened an adopted shard inside its heartbeat
   loop, so a recovery longer than the failure timeout got the *adopting* node declared dead
   ([#198](https://github.com/tyler-loy/MelangeDB/issues/198)). Opens now run off the heartbeat
   loop. The fix also closed a Replicated-stream gap a newly opened shard could fall into.
4. **Machine death was worse than process death. Fixed.** The gateway's upstream connect had no
   timeout, so on a dead machine each client stalled for the OS TCP connect timeout: 21 s on
   Windows, about 127 s on stock Linux ([#200](https://github.com/tyler-loy/MelangeDB/issues/200)).
   Handshakes and the attachment retry are now bounded by the failure timeout.
5. **FASTER at 1M rows could not be measured. Fixed, and now measured.** The "missing payload"
   errors ([#199](https://github.com/tyler-loy/MelangeDB/issues/199)) turned out to be a read view
   outliving its disposed store, now a clean `ObjectDisposedException`. Underneath it, a border
   reset scanned the whole paged shard under the engine lock, silencing the heartbeat until the hub
   fenced the live node ([#208](https://github.com/tyler-loy/MelangeDB/issues/208)); it now scans a
   pinned view off the lock. The 1M-row window is about 19 s, the same shape as the rest. What
   remains is that paged scans are slow, not blocking
   ([#212](https://github.com/tyler-loy/MelangeDB/issues/212)): a replica bootstrap reads through
   that path, so phase 30 inherits it.

## Reopening trigger (fired)

Kept as it was written, for the record. The measurement ran, and the decision it gated was then made on
other grounds: HA by default.

**The reassignment-window measurement, on a crowded shard, under realistic residency.** That number
is a named deliverable of [phase 20](../road-to-0.3/plan-phase-20.md) — added to the outstanding
phase 11 measurement pass precisely so this decision has evidence instead of intuition, and placed
there rather than in this idea's own design work because a trigger that lives inside the thing it
gates never fires.

- If the window is inside a client reconnect backoff: the deferral holds, and now it holds for a
  recorded reason rather than an assumption.
- If it is user-visible and scales with shard heat: this becomes a phase, and the design work starts
  with the fencing third-state problem above, because that is where the data-loss risk lives.
