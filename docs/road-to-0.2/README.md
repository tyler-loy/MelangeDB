# The road to 0.2

Post-0.1 phase plans, in the same form as [road-to-0.1/](../road-to-0.1/) and numbered continuously
with it: written before the work, "Decisions to settle" answered in place as each one resolves, and
kept afterwards as the decision record.

These are **not** user documentation — [ROADMAP.md](../ROADMAP.md) is the summary, and the reference
docs one level up describe the current system. Where a plan and the code disagree, the code is right.

## The phases

| | Phase | Status |
| --- | --- | --- |
| [13](plan-phase-13.md) | Clustering III — elastic assignment | Shipped |
| [14](plan-phase-14.md) | Clustering IV — provisioned capacity and scale-in | Shipped |
| [15](plan-phase-15.md) | Backup and restore | Shipped |
| [16](plan-phase-16.md) | Hot-tier schema migration | Shipped |
| [17](plan-phase-17.md) | Group commit | Shipped |
| [18](plan-phase-18.md) | Truncation-floor observability | **Shipped** |
| [19](plan-phase-19.md) | Backup, second pass — check, clone, point-in-time | Planned |

Phases 13 and 14 implement [design/elastic-rebalancing.md](../design/elastic-rebalancing.md):
fixed shard boundaries, dynamic shard → node assignment. Phase 13 makes the shard map follow load
across the nodes that exist; phase 14 makes the set of nodes itself follow load. The split is
deliberate — 13 has no external dependency and delivers value alone (an operator can rebalance by
hand the day it ships), while 14 involves a capacity seam, money, and the genuinely harder
scale-in half.

Phase 15 is independent of both: `melange backup` / `restore` / `backup verify`, the operational
surface the commit-log-as-truth design has owed its operators since 0.1. It can land before,
between, or after 13–14.

Phases 16–19 were planned together after 13–15 shipped and the reference port went live, and are
mutually independent — any order works. Phase 16 closes DESIGN.md §10's open half (how schema
changes replay against an existing log); 17 is the largest per-shard throughput lever that
changes no semantics; 18 is the smallest phase in the set, naming what already exists; 19 builds
the three verbs phase 15's decision record explicitly deferred to next. Where they touch at all
(19's restored directories become 16's migration boots under newer code) the interaction falls
out of the designs rather than requiring sequencing.

The standing conventions hold here exactly as they did for 0.1: every configuration item goes in
[CONFIGURATION.md](../CONFIGURATION.md) in the change that introduces it, every noun in
[GLOSSARY.md](../GLOSSARY.md), every signal in [OBSERVABILITY.md](../OBSERVABILITY.md).
