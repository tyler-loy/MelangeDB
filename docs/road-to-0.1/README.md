# The road to 0.1

The twelve phase plans MelangeDB was built from, kept verbatim.

These are **not** user documentation — [ROADMAP.md](../ROADMAP.md) is the summary, and the reference
docs one level up are what you want if you're trying to *use* MelangeDB. What lives here is the
decision record: for each phase, the goal, the deliverables, what was explicitly ruled out, and — the
part worth keeping — the reasoning behind every decision it settled.

They're preserved because that reasoning exists nowhere else. `CONFIGURATION.md` records what a knob
is; only these record why it isn't a different knob, or why it isn't configurable at all. Several
decisions were settled by a measurement, and the number is here with the argument it won:

- Why GUIDs lost to originator-prefixed AutoInc ids, and why the contract is *unique, not dense* (01)
- Why reducers are synchronous and always will be able to become async, but not the reverse (02)
- Why every frame carries a channel tag from version one (03)
- Why rows compose as a UNION and columns as an INTERSECT — and the ~520 ns/row that bought
  "no policy cache, no invalidation bugs" (04)
- Why a repeating timer writes nothing per fire (05)
- Why event cycles are bounded by publish *depth* rather than detection (06)
- Why residency is opt-in rather than a size threshold — and the 44× bulk-load measurement (07)
- Why aggregates are owner-mode only (08)
- Why cluster membership is Postgres-backed rather than Raft (09)
- Why a mid-handoff reducer call is queued invisibly, and the measured hotspot ceiling (10)

If you're about to change something that looks arbitrary, it's worth checking here first — a fair
number of these are recorded refusals rather than omissions.

## The phases

| | Phase | Status |
| --- | --- | --- |
| [01](plan-phase-01.md) | Core engine — schema, write set, transactions, commit log | Shipped |
| [02](plan-phase-02.md) | Source generator and host integration | Shipped |
| [03](plan-phase-03.md) | Transport, subscriptions, and the C# client | Shipped |
| [04](plan-phase-04.md) | Identity, auth, and row-level policies | Shipped |
| [05](plan-phase-05.md) | Scheduled and lifecycle reducers | Shipped |
| [06](plan-phase-06.md) | The event bus | Shipped |
| [07](plan-phase-07.md) | Durable hot store — paging, residency, large values | Shipped |
| [08](plan-phase-08.md) | The Postgres tier and ad-hoc SQL | Shipped |
| [09](plan-phase-09.md) | Clustering I — placement, hub/shard roles, instancing | Shipped |
| [10](plan-phase-10.md) | Clustering II — spatial strategy and seamless handoff | Shipped |
| [11](plan-phase-11.md) | Reference workload port and validation | **Outstanding** |
| [12](plan-phase-12.md) | Typed client bindings | Shipped |

Phase 12 is numbered after 11 but landed first — the reasoning is in
[ROADMAP.md](../ROADMAP.md#m3--proven).

## A note on tense

These were written *before* the work, as plans, and most keep that voice — "Decisions to settle"
sections were struck through and answered in place as each one was resolved, rather than rewritten
afterwards. Phases 09 and 10 also carry "Shipped notes" recording boundaries drawn during
implementation. Read them as a log rather than as a description of the current system; where a plan
and the code disagree, the code is right and the reference docs one level up describe it.
