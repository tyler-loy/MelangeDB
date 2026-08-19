# Shard HA as a warm replica

**Shape:** a second node per hot shard that is already applying that shard's log into its own
projection, holds no write lock, and can take one on fence without rebuilding anything.

**Status:** undecided, and **against a live deferral** — [ROADMAP.md](../ROADMAP.md) records
shard-level HA as deferred, and that decision stands until the trigger below fires. This entry is
the proposal, not a reversal. See [the bin's rule on overlap](README.md#what-this-directory-must-not-do).

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

**Nobody has measured the window.** The entire case above is "the recovery window is too long," and
that number does not exist in this repo. If reassignment on a realistic crowded shard completes
inside a reconnect backoff, this feature buys a loading screen nobody sees, and it buys it at the
price below. Arguing for it before measuring is exactly the move the project's phase-11 risk
register warns about.

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

## Reopening trigger

**The reassignment-window measurement, on a crowded shard, under realistic residency.** That number
is a named deliverable of [phase 20](../road-to-0.3/plan-phase-20.md) — added to the outstanding
phase 11 measurement pass precisely so this decision has evidence instead of intuition, and placed
there rather than in this idea's own design work because a trigger that lives inside the thing it
gates never fires.

- If the window is inside a client reconnect backoff: the deferral holds, and now it holds for a
  recorded reason rather than an assumption.
- If it is user-visible and scales with shard heat: this becomes a phase, and the design work starts
  with the fencing third-state problem above, because that is where the data-loss risk lives.
