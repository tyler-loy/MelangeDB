# Read-side fan-out off the writer

**Shape:** move subscription fan-out out of the engine's write lock, so the number of subscribers on
a shard stops being a cost the write path pays.

**Status:** **trigger fired, 2026-11-20** ([phase 20](../road-to-0.3/plan-phase-20.md)); undecided,
and still the one idea in this bin that trades away a stated guarantee. Read the consistency section
before treating it as a throughput knob. The trigger reopens the question; it doesn't settle it, as
[the bin's rule](README.md#what-this-directory-must-not-do) says.

## What exists today

Fan-out runs **under the write lock**, as a commit observer, before the hot store applies the write
set. `SubscriptionEngine` is explicit that this is deliberate:

> every fan-out runs under the engine's write lock, which is the whole consistency story

The cost is already well attacked from other directions. Subscriptions are indexed by table, so a
commit touching table T tests only T's subscriptions. Protocol v2 made the shared unit a span of
bytes rather than a decoded dictionary, and row wire bytes are memoized across the subscriptions a
single fan-out visits them for.

## The argument for

On a crowded shard the write lock is the serialization point for everything, and fan-out is work
done inside it that is proportional to **subscriber count**, not to write-set size. Two hundred
players watching one town square means every commit in that square pays two hundred subscriptions'
worth of matching before the lock releases — and phase 17's group commit, which raised concurrent
throughput substantially, did nothing about this because it batches fsyncs rather than shortening
the critical section.

If subscriber count turns out to be the binding constraint on a hot shard, this is the lever that
addresses it and no other shipped lever does.

## The argument against

**It buys throughput with the consistency property the design was built around.** Under the lock,
the world a subscriber is told about and the world the store holds advance together; a subscriber
cannot observe a delta ordering that no transaction produced. Fan-out on the read side has to
reconstruct that ordering explicitly, and the failure mode is not a slow client — it is a client
that saw an impossible world.

**It may be solving a problem that doesn't exist.** The per-subscription cost has already been cut
twice (table indexing, protocol v2 byte sharing). Whether fan-out is a meaningful share of
lock-held time at realistic subscriber counts is unmeasured. This is a rewrite of the consistency
core justified, at present, by arithmetic rather than by a profile.

## Reopening trigger

**[Phase 20](../road-to-0.3/plan-phase-20.md) shows subscriber count, not commit rate, is the
crowded-shard limit** — specifically, fan-out measured as a significant share of lock-held time at
realistic player density on one shard.

Until that profile exists this stays here. The hotspot numbers that get quoted in this project have
a habit of being re-measured and changing the argument: phase 10's ceiling was superseded by phase
17's, and any case built on the older figures is a case built on a machine that no longer describes
the system.

## The profile (phase 20)

The trigger asked for a profile instead of arithmetic. The profile:

- **Setup:** one shard, a fixed ~750 commits/s from 50 movers, and subscribers swept 50 → 400
  (interleaved arms; [LOAD-TESTING.md](../LOAD-TESTING.md)).
- **Share:** post-commit, where fan-out runs, is **66% of locked time at 50 subscribers, 75% at
  100, 87% at 200, and 90% at 400**. The bar was 25% at 200.
- **Cost:** it grows linearly, at about 5.5 µs per subscriber per commit, all under the write lock.
- **Wall:** at about 200 subscribers the lock is busy for the whole of each second (750 × 1.3 ms),
  and call-to-delta jumps from tens of milliseconds to ten seconds. That's the capacity ladder's
  ~110–120k delivered rows/s wall, now attributed to the subscription evaluation inside the commit.

What the profile does **not** say:

- **That moving fan-out off the lock is the right lever.** A cheaper per-subscriber evaluation, for
  example sharing the verdict across subscribers whose predicates are identical, which the sweep's
  subscriptions all were, attacks the same 5.5 µs without touching the consistency story. Measure
  that first.
- **That 200 subscribers on one shard is the reference workload's shape.** The reference workload's
  production deployment has had no player traffic in 30 days, so its real density is unmeasured.

The argument against stands unchanged. What changed is the second paragraph of it: the problem
exists, and it is the dominant cost of a crowded shard's write lock.
