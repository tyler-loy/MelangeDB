# Roadmap

Phase plans live in `docs/plan-phase-NN.md`. Each is written to be executable on its own: goal,
dependencies, deliverables, what's explicitly out of scope, decisions still to settle, and a verifiable
definition of done.

## Standing conventions

- **Every configuration item goes in [CONFIGURATION.md](CONFIGURATION.md)**, in the same change that introduces
  it — not at the end of a phase. That document is the source of truth for key names, defaults, and reload
  semantics. Undocumented knobs are how a library becomes folklore.
- **Every phase instruments what it adds**, recorded in [OBSERVABILITY.md](OBSERVABILITY.md) in the same change.
  Span and metric names are public API — once a dashboard or alert depends on `melange.applier.lag`, renaming it
  is a breaking change. A phase is not done if its failure modes are invisible, which is why several phases name
  specific metrics in their done-criteria rather than saying "add telemetry."

## Ordering principles

Three constraints drove the sequence, and each one reverses an intuitive ordering:

1. **The in-memory projection comes before the real storage engine.** Because the commit log is the source
   of truth, an in-memory hot store is a *legitimate* projection rather than a stub. That lets the
   transaction, log, and subscription layers be built and tested end-to-end before any storage-engine work
   — which is the part most likely to eat months.
2. **Paging comes before clustering.** Cold world data grows with area (the N² term); live simulation grows
   with player density. Sharding alone just re-bills N² as more nodes holding cold terrain. Paging attacks
   the bigger term and needs no coordination layer.
3. **Instancing comes before spatial sharding.** Instanced shard transitions are explicit and discrete —
   the loading screen *is* the handoff window — so they need no border overlap, interest computation, or
   seamless transfer. Same mechanism, a fraction of the machinery.

## Milestones

### M1 — Single-node MelangeDB (phases 01–08)

A developer can `dotnet add package MelangeDB`, define tables and reducers in their own worker service,
and have real clients subscribe to live data. All three original complaints are answered at single-node
scale by the end of M1: DI in phase 02, the RAM ceiling in phase 07, and clustering *prepared for* by the
commit log in phase 01.

| Phase | Title |
| --- | --- |
| [01](plan-phase-01.md) | Core engine — schema, write set, transactions, commit log |
| [02](plan-phase-02.md) | Source generator and host integration |
| [03](plan-phase-03.md) | Transport, subscriptions, and the C# client |
| [04](plan-phase-04.md) | Identity, auth, and row-level policies |
| [05](plan-phase-05.md) | Scheduled and lifecycle reducers |
| [06](plan-phase-06.md) | The event bus |
| [07](plan-phase-07.md) | Durable hot store — paging, residency, large values |
| [08](plan-phase-08.md) | The Postgres tier and ad-hoc SQL |

### M2 — Cluster (phases 09–10)

| Phase | Title |
| --- | --- |
| [09](plan-phase-09.md) | Clustering I — placement, hub/shard roles, instancing |
| [10](plan-phase-10.md) | Clustering II — spatial strategy and seamless handoff |

### M3 — Proven (phase 11)

| Phase | Title |
| --- | --- |
| [11](plan-phase-11.md) | Vibe Shaft port and validation |

Phase 11 is written as one phase for planning purposes, but in practice porting will start much earlier —
a subset of Vibe Shaft's tables and reducers is the most honest integration test available from phase 03
onward, and should be used that way rather than saved for the end.

## The shortest path to something real

Phases 01 → 02 → 03 produce a running system: tables, reducers, a websocket, and a client receiving live
row deltas. That is the point at which the design stops being a document, and it is worth reaching before
building anything in phases 04–08.
