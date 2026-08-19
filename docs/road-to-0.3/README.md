# The road to 0.3

Post-0.2 phase plans, in the same form as [road-to-0.1/](../road-to-0.1/) and
[road-to-0.2/](../road-to-0.2/) and numbered continuously with them: written before the work,
"Decisions to settle" answered in place as each one resolves, and kept afterwards as the decision
record.

These are **not** user documentation — [ROADMAP.md](../ROADMAP.md) is the summary, and the reference
docs one level up describe the current system. Where a plan and the code disagree, the code is right.

## The theme

0.1 built the engine. 0.2 was capacity and operations — elastic assignment, backup and restore,
schema migration, group commit, retention observability. Both were about making the database do more.

**0.3 is the surface a team building on this every day still has to invent.** The engine is past
"needs more database." What a game team currently has to build for itself is: a way to look at what
the log already recorded, a way to test a tick without standing up a host, and a way to reach the
world from a browser. None of those are engine capabilities and all three are things every consumer
will otherwise write badly, once each.

One phase does not fit that theme and comes first anyway, because it is the debt that makes the rest
arguable.

## The phases

| | Phase | Status |
| --- | --- | --- |
| [20](plan-phase-20.md) | The measurement pass — phase 11's outstanding half, plus the two numbers 0.3 depends on | Planned |
| [21](plan-phase-21.md) | `melange inspect` — time-travel over the commit log | Planned |
| [22](plan-phase-22.md) | `MelangeDB.Testing` — the reducer test kit | Planned |
| [23](plan-phase-23.md) | The TypeScript client, client conformance, and `melange generate` | Planned |
| 24 | Scheduler lane — player reducers ahead of simulation work | [Design record](../design/scheduler-lanes.md) written; may not become a phase |

**Phase 20 comes first and gates two things.** It is not a feature; it is the measurement half
[phase 11](../road-to-0.1/plan-phase-11.md) called its deliverable and did not record. Everything
this project claims about performance is currently a dev-machine number, and
[ROADMAP.md](../ROADMAP.md) says so in as many words. Until phase 20 lands, phase 24 cannot be sized
and [read-side fan-out](../idea-bin/read-side-fanout.md) cannot be argued. It also carries two
measurements phase 11 did not ask for, because decisions elsewhere now wait on them — the
reassignment window that gates [shard HA](../idea-bin/shard-ha-warm-replica.md), and the per-tick
cost of the fourteen scheduled reducers that sizes phase 24.

**Phases 21, 22, and 23 are mutually independent** — any order, same as 16–19 were. Where they touch
at all it falls out of the designs: 21 wants a manifest to render argument and row bytes, which is
the same manifest 23 generates from, and 22's harness is the obvious place to assert against what 21
displays.

**Phase 24 gets a `design/` document before it gets a plan**, the way
[elastic-rebalancing.md](../design/elastic-rebalancing.md) preceded 13–14. That record —
[design/scheduler-lanes.md](../design/scheduler-lanes.md) — is written, and it does not conclude
that the phase should happen. Windowing, snapshot isolation, and group commit already answer most
of what the proposal is usually asked for; the residue has never been measured; and "the simulation
yields mid-tick" turns out not to be implementable as stated, because a reducer is a transaction.
Phase 20 decides whether 24 exists at all. **A milestone that ends with a recorded refusal here is a
successful outcome, not a dropped phase.**

## A note on the throughput argument

The case for phase 24 is routinely made with phase 10's hotspot ceiling (~1,100 commits/s at
per-commit fsync, ~52,000 at interval). **Those numbers were superseded by phase 17**, which
re-measured on one box at ~500 commits/s sequential, ~4,000 at 16 concurrent callers, and ~12,000 at
interval. Any argument built on the older figures is built on a machine that no longer describes the
system, and this has now happened often enough to be worth writing down here.

The surviving argument for phase 24 is narrower and does not depend on either set: **group commit
raised throughput; it did not raise fairness.** Batching fsyncs does nothing about fourteen
scheduled reducers contending with `Move` for one write lock.

## What 0.3 deliberately excludes

- **A stock admin console.** Recorded as a permanent refusal in [ROADMAP.md](../ROADMAP.md), with
  the signal-by-signal accounting of what already serves it. Publishing the signals is the engine's
  job; prescribing the tool that draws them is not.
- **Area-of-interest subscriptions** and **shard HA**, which are open questions rather than
  refusals and live in [idea-bin/](../idea-bin/) with the triggers that would promote them. Shard
  HA's trigger is a phase 20 measurement, so it may well be answered within this milestone.
- **Additional client languages beyond TypeScript.** Scoped and declined for 0.3 in
  [idea-bin/additional-client-languages.md](../idea-bin/additional-client-languages.md); phase 23
  pays the one-time cost that would make a later one cheap.

The standing conventions hold here exactly as they did for 0.1 and 0.2: every configuration item
goes in [CONFIGURATION.md](../CONFIGURATION.md) in the change that introduces it, every noun in
[GLOSSARY.md](../GLOSSARY.md), every signal in [OBSERVABILITY.md](../OBSERVABILITY.md).
