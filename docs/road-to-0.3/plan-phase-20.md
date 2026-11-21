# Phase 20 — The measurement pass

**Status: Measured (2026-11-20).** The decision-gating half is recorded; findings are issues [#198](https://github.com/tyler-loy/MelangeDB/issues/198)–[#204](https://github.com/tyler-loy/MelangeDB/issues/204). See [As built](#as-built).

**Scope note:** this phase is the *decision-gating* half only. The head-to-head comparison against SpacetimeDB is deferred, and whether it runs at all is its own open decision — see [Decisions to settle](#settled-this-is-two-phases-and-only-the-first-is-scheduled).

**Goal:** the numbers [phase 11](../road-to-0.1/plan-phase-11.md) called its deliverable are
recorded in this repo, including the ones that came out worse — and with them, the two measurements
that other decisions are now explicitly waiting on. At the end of this phase, "treat every benchmark
in these docs as a dev-machine measurement" comes out of [ROADMAP.md](../ROADMAP.md), because it is
no longer the honest thing to say.

**Depends on:** the reference workload being live, which it is — an ASP.NET host on
`0.1.2-ci.*` prereleases, developed on daily. Nothing in this phase requires a code change to
MelangeDB, which is the point and also the risk (see *Out of scope*).

## Why here

Phase 11 shipped its port and did not ship its numbers, and its own shipped notes say so without
softening it: *a port that reports only wins is not evidence, and "it runs in production" is a fact
about the port, not a measurement of it.* That note has been standing since 0.1. This is the phase
that closes it.

Putting it first in 0.3 is not tidiness. Three separate decisions in and around this milestone are
currently blocked on numbers that do not exist:

- **Phase 24 (scheduler lane) cannot be sized, or ruled out.** The case for it is that fourteen
  scheduled reducers contend with player reducers for one write lock, and nobody has measured what
  that contention costs a player. [design/scheduler-lanes.md](../design/scheduler-lanes.md) records
  the shapes and concludes the measurement below decides whether the phase happens at all — a
  recorded refusal is one of its two expected outcomes.
- **[Shard HA](../idea-bin/shard-ha-warm-replica.md) cannot be argued.** Its entire case is "the
  reassignment recovery window is a loading screen," and the window has never been timed.
- **[Read-side fan-out](../idea-bin/read-side-fanout.md) cannot be justified.** It proposes trading
  the consistency property the design is built around for throughput, on the strength of arithmetic
  rather than a profile.

There is a fourth reason, and it is the one that makes this a phase rather than a chore. The
performance claims in this repo have already been superseded once without the arguments built on
them being revisited: phase 10's hotspot ceiling (~1,100 / ~52,000) is still what gets quoted, while
phase 17 re-measured the same machine class at ~500 sequential, ~4,000 concurrent, ~12,000 interval.
Stale numbers do not merely age; they keep winning arguments after they stop being true.

## What the reference workload can actually produce

Phase 11 named its numbers before anyone checked whether the workload could still produce them. An
audit of the consumer repo, done while planning this phase, says most of them survive and two do
not. This section exists so the phase is scoped against reality rather than against the 0.1 wish
list.

| Number | Status |
| --- | --- |
| 10 km memory vs SpacetimeDB | **Decaying** — the baseline has drifted; see below |
| 20 km memory | **Fine** — one-sided capability claim, needs no SpacetimeDB; no longer a claim about what SpacetimeDB cannot do (see below) |
| Reducer p50/p99 | **Two of four reducers are not exercised** |
| Terrain streaming | **Partial** — crossings counted, throughput needs the OTel metrics |
| Concurrent players per node | **Harness-bound, not server-bound** |
| Reassignment window | **Not obtainable from the reference workload at all** |
| Scheduled-reducer cost | **Probably already observable** |

**The SpacetimeDB baseline has drifted, and drifts further every week.** The old module is still in
the consumer's tree and still buildable, but it froze at the port while the game kept moving:
**82 table files and 39 reducer files, against the live module's 97 and 48** — roughly three weeks
apart. A comparison run today measures fifteen tables that have nothing to do with either engine.
The drift is not theoretical or slow: the live count moved *between two measurements taken an hour
apart* while this section was being written, because the game ships almost daily. The port was also
explicitly *not* a data migration — the world regenerates from terrain-gen — so both sides need
generating from the same seed regardless.

**Attack and craft are not exercised.** The load fleet moves, digs, and fills. It is unarmed and it
does not craft, so half of the four named reducers have no load behind them. That is bot work, and
it is work on a measurement tool rather than on the product.

**Concurrent players per node is currently a fact about the driver.** The fleet costs tens of MB per
bot *in the bench process* — most recently ~38 MB of settled managed heap and ~60 MB resident, on a
world whose largest client-held table has itself grown — so a 200-bot fleet is many GB before the
server is considered. The consumer's own documentation says the honest fix is more processes;
multi-process fleet orchestration does not exist yet.

The per-bot figures in that sentence used to be stated precisely (45 MB) and are now stated as a
range on purpose, because the instrument that produced them turned out not to support the precision.
Working set was being compared across arms that were never interleaved, and the GC swing over a bot
count is larger than the differences it was being read for; a ~10% "regression" measured that way
evaporated under an interleaved re-run with settled managed heap. **This is the phase's own failure
mode, caught in miniature before the phase started** — precisely the thing the honesty rule below
exists for, and a useful reminder that the risk is not only stale numbers but numbers that were
never as sharp as their digits implied. [LOAD-TESTING.md](../LOAD-TESTING.md) now carries the
methodology this phase inherits: interleave arms, force a collection at the same point in each,
compare settled managed heap with working set beside it, and record the GC mode and core count.

**The reassignment window cannot come from the reference workload.** Its clustering phase is
planned, not built — the deployment is single-node and carries no shard configuration. This
measurement moves to MelangeDB's own cluster tests, which weakens it: it becomes a synthetic number,
and [shard HA](../idea-bin/shard-ha-warm-replica.md)'s trigger is gated on synthetic data rather
than on the real workload. Recorded as a known weakness of that trigger rather than papered over.

**The scheduled-reducer cost may already be answered.** The consumer's Grafana stack has a
write-lock dashboard whose panels are, almost verbatim, this measurement: *who trips the threshold*,
*lock hold distribution over time*, *body vs commit vs fsync — worst call per bucket*, the
slow-reducer warning stream, and a flora-sweep panel showing saplings per tick against the window
that produced them.

**So the phase starts with the dashboard, not with the rig.** Load the world, read those panels,
and see whether phase 24's gating question is already answered. It is the cheapest possible path to
closing or confirming an entire phase, and doing it after building bots would be a waste of the
bots.

## Deliverables

**The phase 11 numbers that gate other work.** The struck ones below moved to the deferred half, and
are kept here rather than deleted because the shape each would have taken is itself the argument
against running it now. What is struck is what needs a second system to compare against or a tool
that does not exist yet; what is not struck needs neither, which is the whole basis of the split.

- ~~**Memory for the 10 km world versus SpacetimeDB's**~~ — **deferred.** It would have needed **both sides pinned at the port
  commit** — the only point at which they were the same game. Everything else in this phase measures
  current MelangeDB; this one number does not, and says so where it is published. The alternative,
  live-against-frozen, biases *against* MelangeDB by thirteen tables, which is the safe direction for
  a self-run benchmark and also the direction that makes the number mean nothing.
- **Memory for the 20 km world** — the full 20 km world (98,596 chunks) on one node within a fixed
  budget. This is complaint 2, the RAM ceiling, either demonstrated or not. It used to be billed as
  the world SpacetimeDB cannot host, which held at the port: SpacetimeDB 2.7 kept every table in
  memory. SpacetimeDB 2.16.1 (2026-11-20) added opt-in per-table tiered storage — rows can leave
  memory, indexes cannot yet — so whether a current SpacetimeDB hosts this world is an open
  empirical question this phase does not answer. Publish the number as what MelangeDB does, not as
  what SpacetimeDB cannot.
  **Kept in this half**, unlike its sibling above: it is a capability claim about MelangeDB alone,
  it needs no baseline, and so none of the drift that deferred the head-to-head touches it. Report
  it against [LOAD-TESTING.md](../LOAD-TESTING.md)'s methodology — settled managed heap, GC mode and
  core count recorded — and not as working set.
- **Reducer latency p50/p99 for gather and move**, from the `melange.reducer.duration` histogram
  rather than from the driver — the load bench deliberately does not print latency, on the grounds
  that a reducer's span duration *is* every player frozen for that long.
- ~~**The same for attack and craft**~~ — **deferred with the comparison.** Both need the fleet
  armed and crafting, which is bot work: a measurement tool, not the product, and the split's whole
  point was that tool-building must not gate the cheap numbers.
- **Terrain-streaming throughput** as a player crosses chunk boundaries. The driver already counts
  crossings; the throughput half comes from the subscription metrics.
- ~~**Concurrent players per node**~~ — **deferred with the comparison**, because it requires
  multi-process fleet orchestration first. Until that exists, any number produced is a measurement
  of the driver's memory, not the server's capacity, and publishing it as the latter would be the
  exact error this phase exists to correct.

**The reassignment window**, from MelangeDB's own cluster tests. Time from fence to the new owner
serving writes, for a *crowded* shard under realistic residency — not an empty one. Reported as a
distribution, not a single number, and alongside the resident footprint that produced it, because
the claim being tested is that the window scales with shard heat. This is the trigger for
[shard HA](../idea-bin/shard-ha-warm-replica.md) and belongs here rather than in that design; a
trigger that lives inside the thing it gates never fires. **It is a synthetic number and must be
labelled one** — the reference workload's clustering phase is planned rather than built, so no real
deployment can produce it.

**The fan-out share of lock-held time**, which is [read-side fan-out](../idea-bin/read-side-fanout.md)'s
reopening trigger and until now has been named without being specified. The idea is a rewrite of the
consistency core, so the bar for reopening it has to be a number rather than an intuition:

- **Metric.** `melange.post_commit_ms` over `melange.locked_ms`, from the same `melange.reducer`
  span — the fan-out runs in the post-commit half of the locked window, so the ratio *is* the share.
  `melange.subscription.delta_rows` and the sampled `melange.subscription.delta` spans (raise
  `Telemetry:DeltaSpanSampleRatio` for the run) attribute the share to tables and subscriber counts.
- **Harness.** The load rig, not the reference workload: the variable is subscriber count on one
  shard, and only the rig can hold everything else still. One crowded shard, a fixed commit rate,
  and a subscriber sweep — 50 / 100 / 200 / 400 subscribers on the same terrain-streaming
  subscription — each arm interleaved and reported per [LOAD-TESTING.md](../LOAD-TESTING.md).
- **Pass/fail.** Fan-out is a *significant* share if post-commit exceeds **25% of locked time** at
  realistic density (200 subscribers on one shard), or if the share grows faster than linearly in
  subscriber count across the sweep — the second matters more, because it is the shape that says the
  ceiling is subscribers rather than commits. Either result reopens the idea bin entry. Neither
  result closes it as refused: it stays where it is, with a profile attached instead of arithmetic.

**Per-tick scheduled-reducer cost against player-reducer latency.** With the reference workload's
scheduled reducers running, the p99 of a player-initiated `Move` on the same shard, and the share of
lock-held time the scheduled work accounts for. `melange.scheduler.tick.duration` and
`melange.scheduler.overruns` already exist per reducer (phase 05); what is missing is the
*correlation* — the player-visible cost, which is the number phase 24 is actually about. **Start
here, from the existing dashboard**, per the audit above.

**The post-group-commit re-run.** [LOAD-TESTING.md](../LOAD-TESTING.md)'s single-shard fsync latency
table is explicitly flagged as measured *before* phase 17, with a prediction attached: the
per-commit-fsync tail should collapse toward the batched ceiling, because the concurrent callers that
rig generates are exactly the shape that now shares fsyncs. Re-run it and record whether the
prediction held. A published prediction that is never checked is worse than no prediction.

**A recorded methodology, and the honesty rule.** Every number lands with the machine, the build
configuration, the world, the client count, and the version of anything it is compared against.
Phase 11 pre-committed to publishing measurements "including any that came out worse," and that
commitment is inherited here without renegotiation — a comparison run by the authors of one side is
worth exactly what its disclosed methodology is worth.

## Out of scope

**Fixing anything this phase finds.** This is the boundary that makes the phase trustworthy, and it
is easy to breach with good intentions: a measurement pass that also optimizes cannot report
honestly, because the thing it reports on changes underneath it. Findings become issues and, where
they deserve it, phases. If a number is bad enough to demand immediate action, that is a decision
made *after* it is recorded, not instead of recording it.

**A continuous performance-regression gate in CI.** Genuinely valuable and genuinely a different
phase. It needs stable hardware, a noise model, and a policy about what a red build means — none of
which this phase produces, and all of which it would have to invent badly under time pressure.
Recorded as the natural follow-up if the numbers here turn out to move often.

**New benchmark infrastructure beyond what the missing numbers require.** `tools/MelangeDB.LoadTest`,
`bench/MelangeDB.Benchmarks`, and `HotspotMeasurementTests` already exist and already have recorded
methodology. Extending them is in scope; replacing them is not.

**Re-measuring what phase 17 already measured on the same class of machine.** The group-commit
ceilings are recent and their methodology is published. Repeating them would produce a second set of
numbers to keep in sync, which is how the ~1,100 figure survived past its own supersession.

## Decisions to settle

### Where the numbers live

[LOAD-TESTING.md](../LOAD-TESTING.md) holds recorded numbers today, but it documents a *rig* — what
it measures, how to run it, what the ladder shows. The phase 11 comparison is a different artifact:
a one-time, version-pinned comparison against another system, which will age in a way a rig's output
does not.

**Leaning:** a new `docs/BENCHMARKS.md` for the comparison, dated and version-pinned at the top,
with `LOAD-TESTING.md` keeping the rig and its ladder and the two cross-linking. The
reference-workload-specific findings go in [REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md), which
is already the document that audits the design against the real game. The risk of a third
performance document is real; the alternative — a rig document that is half a competitive comparison
— seems worse.

**Settled (2026-11-20): no `BENCHMARKS.md` yet.** It was for the head-to-head, which is deferred, and
creating an empty one would be the third document with nothing in it. The rig's numbers (failover,
fan-out, the fsync re-run) went into [LOAD-TESTING.md](../LOAD-TESTING.md) next to the ladder they
extend. The workload's numbers (scheduled-reducer cost, the 20 km world) went into
[REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md). If the comparison ever runs, it gets
`BENCHMARKS.md` as leaned.

### What makes the SpacetimeDB comparison fair, and whether it can be

This is a comparison run by the people who built one side, against the system they ported off,
published to argue that the port was correct. That is not disqualifying, but it is a position that
has to be handled explicitly rather than hoped past.

**Leaning:** pin and publish the SpacetimeDB version and configuration; run both at documented
defaults and tune neither; publish the world, the client count, and the machine; and state the
conflict of interest in the document rather than in a footnote. Where a number cannot be made fair
— the 20 km world is a capability claim about MelangeDB, not a benchmark — say which kind of claim
it is, and do not extend it to SpacetimeDB without running SpacetimeDB: since 2.16.1 its tables can
opt into tiered storage, so "cannot host" is no longer a safe default.

**The audit sharpens this considerably.** The baseline is not merely partial, it is *decaying*: 82
table files against 97, three weeks apart, widening every week the game ships. Pinning both sides at the
port commit is the only construction that compares the same game, and it means the headline memory
number describes a MelangeDB that is weeks old by publication.

**Open, and now the phase's central question:** whether this comparison is worth doing at all. The
case against is strong — it is structurally partial, it decays, it requires standing up a dead
baseline, and **nothing depends on it.** No phase, no idea-bin trigger, and no design decision waits
on the SpacetimeDB number; only the project's own promise does. The case for is that the promise was
made explicitly and is recorded as "not being quietly dropped." If it is dropped, it must be dropped
*loudly*, with this reasoning attached — that would discharge the debt honestly, which silence
would not.

### Settled: this is two phases, and only the first is scheduled

The audit split the deliverables along a line the original plan did not see, and they are now
scoped separately.

**Phase 20 is the decision-gating half:** the scheduled-reducer cost (start from the consumer's
existing write-lock dashboard), the fan-out share, the reassignment window from MelangeDB's own
cluster tests, and the 20 km memory claim. The first three unblock phase 24 and two idea-bin
triggers; the fourth unblocks nothing but costs nothing either — it is a capability claim about
MelangeDB alone, so the drift that deferred the head-to-head does not reach it, and there is no
reason for it to wait behind a decision it does not depend on. None of the four need new tooling,
and all four are days rather than weeks.

**The comparison half is deferred and undecided:** the head-to-head against SpacetimeDB, the
armed-and-crafting bot fleet, and multi-process fleet orchestration. It unblocks nothing, two of the
three are tool-building rather than measurement, and bundling them meant the cheap measurements that
gate real work waited behind a comparison that gates none.

**Whether the comparison runs at all is a separate decision, still open**, and the evidence has been
accumulating against it. The frozen baseline drifts every week the game ships — 82 table files at
the port against 97 and climbing, measured an hour apart during this plan's own audit. More
pointedly, the workload has since made changes its author judges would not have been feasible on
SpacetimeDB at all. A benchmark whose two sides have diverged in *capability* is not measuring an
engine, and "recorded why we are not running it" discharges phase 11's promise more honestly than a
comparison of two different games would.

What that costs, stated plainly so the choice is not made by drift: without it, the only head-to-head
evidence MelangeDB has is the port's existence, and [ROADMAP.md](../ROADMAP.md)'s warning narrows
rather than lifts.

### Live workload or synthetic rig

The reference workload is real and not reproducible by a reader. The load rig is reproducible and is
not a game.

**Leaning:** both, for different numbers, with each labelled as which. Memory for the 10 km and
20 km worlds and concurrent players per node come from the real workload, because the whole claim is
about a real world's shape. Reducer latency percentiles and the reassignment window come from the
rig, because they need repetition and controlled load. Terrain streaming probably needs both and
that is the one to decide with data in hand.

**Settled (2026-11-20), and differently from the leaning in one place.** The rig produced the fan-out
share and the reassignment window, as leaned. The scheduled-reducer cost came from **the reference
workload's production telemetry**, read with queries and no load added. That was cheaper than either
option and more real than the rig. The catch: the deployment had no players in its 30-day retention,
so it gives the cost side only (see [As built](#as-built)). The 20 km world came from the workload's
own host and generator on this machine. Terrain streaming stayed with the deferred half, since it
needs the fleet.

### What "concurrent players per node" counts

The rig's synthetic players are not the game's players — they call reducers at a chosen rate with no
client-side simulation, no rendering budget, and no human pacing. A number derived from them is a
server-side capacity figure wearing a gameplay word.

**Leaning:** report it as *sustained concurrent sessions at a stated call rate and delta budget*,
with the rate stated in the headline rather than the methodology, and give the reference workload's
observed per-player call rate next to it so a reader can convert. Resist the single round number;
it is the figure most likely to be quoted without its conditions, which is precisely how ~1,100
outlived its measurement.

### The one-machine caveat

[LOAD-TESTING.md](../LOAD-TESTING.md)'s ladder ran driver and server on the same 16 cores, and says
so — the knee it found is a lower bound.

**Leaning:** split driver and server for the headline capacity numbers, and keep one same-machine
run so the new figures remain comparable with the recorded ladder rather than silently replacing it
with numbers measured differently. If a second machine is not available, the caveat stays and the
headline number is stated as a lower bound in the headline, not in the caveat.

**Settled (2026-11-20): one machine, and the caveat stays.** No second machine was used. Every rig
number in this phase is a single-machine figure, and LOAD-TESTING.md says so at each table. None of
them is a headline capacity number, and the capacity number this caveat was written for (concurrent
players per node) is in the deferred half.

### Whether ROADMAP's blanket warning comes off entirely

The current text says to treat *every* benchmark in these docs as a dev-machine measurement. This
phase does not change the fact that these are dev machines; it changes the fact that the comparison
was missing.

**Leaning:** the warning narrows rather than disappears — the promised comparison exists and is
linked, and what remains is a plain statement of what hardware produced the numbers. The distinction
phase 11 was drawing is between *unmeasured* and *measured on a known box*, and only the first one
is debt.

**Settled (2026-11-20): narrowed, as leaned.** [ROADMAP.md](../ROADMAP.md) now points at the
recorded numbers and keeps the warning for what they are not: a characterization of the reference
workload under players, which its deployment has not had.

## As built

Measured 2026-11-20 on the machine LOAD-TESTING.md describes (Windows 11, Ryzen 9 9950X3D 16C/32T,
128 GiB, NVMe; Release builds; workstation GC). Each result's full table lives in the document named;
this is the record of what was done and what it decided.

### What was built to measure

`tools/MelangeDB.LoadTest` gained, per *Out of scope*'s rule that extending the rig is in scope:

- **`failover`**, the reassignment-window measurement. It kills a crowded shard's owner the way a
  crash would, then times detect, reopen, first write, and per-client restore. It also times
  recovery alone, and checks the owner's heartbeat staleness before each kill so a quiet owner
  can't masquerade as a fast hub. `--hot-store faster` measures the reference workload's store.
- **`--lock-split`** (serve) and **`--movers`** (drive): the fan-out share at a fixed commit rate.
- **A fix:** call-to-delta had reported "no samples" since protocol v2 (2026-08-06), because the
  driver matched its own row's `Identity` column as `byte[]`. The smoke test now asserts samples,
  and LOAD-TESTING.md records which recorded results predate the break (all of them).

### The four deliverables

**1. Scheduled-reducer cost: phase 24 is not needed on the measured workload.** Two six-hour windows
of production traces, a week apart. All scheduled work holds the write lock **0.180% / 0.178%** of
wall time, and the longest hold in twelve hours was **8.9 ms**. The 194 ms spans that looked like
contention are fsync after the lock released. Recorded in [REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md)
and in [design/scheduler-lanes.md](../design/scheduler-lanes.md)'s status, with the re-read trigger.
**Not delivered as planned:** the player-side p99 is a bound derived from the hold distribution,
not an observation, because the deployment had no player traffic in 30 days.

The query, for the re-read. Over OpenObserve's trace stream, pair each `melange.reducer` span with
its `melange.commit` child:

- **Hold** = commit `start_time` − reducer `start_time` + commit `duration`.
- A reducer with no commit is read-only, and its whole span is the hold.
- Sum per reducer, then divide by the window's wall time.

**2. Fan-out share: read-side fan-out's trigger fired.** One shard at ~750 commits/s. Post-commit is
**66% of locked time at 50 subscribers and 87% at 200** (bar: 25% at 200). It grows linearly, at about
5.5 µs per subscriber per commit, and saturates the lock at about 200 subscribers. Recorded in
[LOAD-TESTING.md](../LOAD-TESTING.md); the idea-bin entry is reopened with the profile and with the
cheaper lever to measure first.

**3. Reassignment window: 12–19 s, and detection is most of it.** At default timing, detect is
10.6–13.4 s, independent of shard size (the hub's sweep adds up to half the timeout: #201).
Reopen is 1–6 s and grows with size (recovery alone: 0.1 s at 10k rows, 0.5 s at 100k, 3.5 s at
1M). Clients come back with the server on process death. Phase 30's framing changed the same day
this was measured: the project owner decided production systems get HA, so this number now shapes
the design instead of gating it. See [idea-bin/shard-ha-warm-replica.md](../idea-bin/shard-ha-warm-replica.md)
and the [0.4 README](../road-to-0.4/README.md).

**4. The 20 km world: 294 MiB working set on one node,** under a 137.5 MiB declared footprint. It's a
*sparse* world, though: 23,020 of 98,596 chunks are land since the workload's archipelago work. A
land-dense bake needs a generator preset the workload doesn't have, and the settled managed heap was
not taken (attaching `dotnet-gcdump` was not permitted in the session). Recorded in
[REFERENCE-WORKLOAD.md](../REFERENCE-WORKLOAD.md).

**And the post-group-commit re-run:** the prediction held for p90 (per-commit fsync 522 ms → 19 ms,
level with interval fsync) and partly for p99 (1,294 ms → about 170 ms, still about 5× interval's).

### Findings, per *Out of scope*: recorded, not fixed

| Issue | Finding |
| --- | --- |
| [#198](https://github.com/tyler-loy/MelangeDB/issues/198) | A node opens an adopted shard inside its heartbeat loop. A 1M-row recovery silenced the adopter for 7 s, and with the timeout below the recovery time the hub declared the *adopter* dead mid-recovery. |
| [#199](https://github.com/tyler-loy/MelangeDB/issues/199) | FASTER at 1M rows: after reassignment churn, out-of-line payloads go missing, scans skip rows, and `TakeSnapshot` throws. It reproduces 2/2; three simpler hypotheses are ruled out in the issue. |
| [#200](https://github.com/tyler-loy/MelangeDB/issues/200) | The gateway's upstream connect has no timeout (machine death: 21 s on Windows, about 127 s on stock Linux), and its 20 × 250 ms retry budget is shorter than detection on a fast-refusing OS. |
| [#201](https://github.com/tyler-loy/MelangeDB/issues/201) | The hub's death sweep runs at `FailureTimeoutMs/2`, so detection is 1.0–1.5× the timeout. |
| [#202](https://github.com/tyler-loy/MelangeDB/issues/202) | Calls racing a shard engine's disposal log `ObjectDisposedException` as unexpected errors. |
| [#203](https://github.com/tyler-loy/MelangeDB/issues/203) | Production telemetry: about 125 scheduled spans over the 50 ms threshold in a week, but five `1003` warnings in 30 days. Cause not established. |
| [#204](https://github.com/tyler-loy/MelangeDB/issues/204) | A flaky `ScaleInTests` curve test under suite load (noticed during phases 27–28, filed here). |

Also observed and not filed as its own issue: during a 1M-row bulk write on the FASTER store, the
live owner went silent for 13.7 s and was declared dead. The in-memory store with the same seed did
not. The mechanism is not established (`Statistics()` is O(tables) on both stores, and a tight writer
delays a `ReadConsistent` caller by at most about 144 ms in a probe), so it's recorded inside #199,
where it's the first step of that repro.

**Afterwards (still 2026-11-20).** The table records the findings as filed. Since then #198 and
#200 are fixed, and #199 turned out to be a read view outliving its disposed store, also fixed. The
13.7 s silence above was then narrowed to its own issue,
[#208](https://github.com/tyler-loy/MelangeDB/issues/208), and fixed: a border reset scanned the
whole paged shard under the engine lock, and the commit the stall was first blamed on was only
waiting behind it. With that fixed the FASTER 1M row was measured, and no cold-path finding from
this phase is still open before phase 30.
