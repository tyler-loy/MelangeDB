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
| [14](plan-phase-14.md) | Clustering IV — provisioned capacity and scale-in | Planned |
| [15](plan-phase-15.md) | Backup and restore | Planned |

Phases 13 and 14 implement [design/elastic-rebalancing.md](../design/elastic-rebalancing.md):
fixed shard boundaries, dynamic shard → node assignment. Phase 13 makes the shard map follow load
across the nodes that exist; phase 14 makes the set of nodes itself follow load. The split is
deliberate — 13 has no external dependency and delivers value alone (an operator can rebalance by
hand the day it ships), while 14 involves a capacity seam, money, and the genuinely harder
scale-in half.

Phase 15 is independent of both: `melange backup` / `restore` / `backup verify`, the operational
surface the commit-log-as-truth design has owed its operators since 0.1. It can land before,
between, or after 13–14.

The standing conventions hold here exactly as they did for 0.1: every configuration item goes in
[CONFIGURATION.md](../CONFIGURATION.md) in the change that introduces it, every noun in
[GLOSSARY.md](../GLOSSARY.md), every signal in [OBSERVABILITY.md](../OBSERVABILITY.md).
