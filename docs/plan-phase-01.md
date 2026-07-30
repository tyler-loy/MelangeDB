# Phase 01 — Core engine: schema, write set, transactions, commit log

**Goal:** a reducer can be invoked in-process, mutate tables transactionally, and have its effects survive
a restart — with no storage engine, no network, and no codegen involved.

**Depends on:** nothing. This is the foundation.

## Why here

This phase builds the part every other phase rests on, and it can be completed without touching the two
hardest subsystems (storage engine, clustering). Because the commit log is the source of truth, an
in-memory projection is architecturally legitimate rather than a placeholder — so the transaction
semantics can be finished and proven correct while the store is still a dictionary.

Get this wrong and everything downstream inherits it. Get it right and phase 07 is a swap behind an
interface.

## Deliverables

**`MelangeDB.Abstractions`**
- `Identity`, `ConnectionId`, `Timestamp`, `ShardKey` value types.
- Attributes: `[Table]` (with `Public`, `Tier`, `Residency`, `Placement`, `ShardBy`, `Scheduled`),
  `[PrimaryKey]`, `[Index]`, `[Unique]`, `[AutoInc]`, `[Reducer]`.
- `ReducerContext` shape, `RejectedException`.
- `IHotStore`, `ICommitLog`, `ILogApplier`.

**`MelangeDB.Core`**
- **Schema model** — `TableSchema`, `ColumnSchema`, `IndexSchema`, built by reflection for now (phase 02
  replaces the reflection path with generated registration).
- **Write set** — ordered `Insert`/`Update`/`Delete` row ops keyed by `(TableId, PrimaryKey)`, with
  last-write-wins collapsing within a transaction.
- **Transaction with read overlay** — reads resolve write set first, then the store. This is what makes
  read-your-writes work inside a reducer body without any I/O.
- **AutoInc sequences** — durable, per-table, assigned into the write set *before* the log append, and
  recovered on startup. Getting this wrong breaks replay silently, which is why it belongs in phase 01
  rather than being bolted on.
- **Commit log** — append-only local file. One record per transaction: LSN, timestamp, caller identity,
  reducer name and args as metadata, and the write set as the authoritative payload. Configurable fsync
  policy. CRC per record; a torn trailing record is truncated on recovery, not fatal.
- **Applier pipeline** — `ILogApplier` with per-applier LSN checkpoints so appliers may lag independently
  and resume from their own position.
- **`InMemoryHotStore`** — dictionary-backed projection plus indexes, rebuilt from the log on startup.
- **Dispatcher** — invoke a reducer, build the write set, append, apply. Return means commit; throw means
  abort with nothing appended.
- **OpenTelemetry instrumentation** — established here, not retrofitted. A single `ActivitySource` and `Meter`
  named `MelangeDB`, with **no telemetry package reference in core at all** — both types are in the `net10.0`
  framework, so MelangeDB emits the built-in signals and the host chooses exporters, exactly as ASP.NET Core and
  EF Core do. Spans `melange.reducer`,
  `melange.commit` (with an `melange.fsync` child so durability cost is separable from serialization cost), and
  `melange.apply`; metrics for transaction count, reducer and commit duration, write-set size, log head LSN, and
  **`melange.applier.lag`**. See [OBSERVABILITY.md](OBSERVABILITY.md) for the full register and the
  cardinality rules — caller identity goes on spans and never on metric dimensions.

## Out of scope

Codegen (02), networking (03), auth (04), scheduling (05), events (06), FASTER and paging (07), Postgres
(08), anything cluster-related (09–10). Reducers are invoked by direct method call in tests.

## Decisions to settle

- ~~**Row representation in the log.** Reflection-based serialization is fine to start, but the format must
  be versioned from record one, since phase 02 replaces the serializer and existing logs must still read.~~
  **Settled: reflection-based, versioned from record one.** Rows serialize against the schema's declared
  column order (format v1: fixed-width little-endian primitives, null-flagged length-prefixed
  strings/blobs, enums as their underlying integer), and every log record payload begins with a format
  version. Phase 02's generated serializers implement the same format behind the same `TableSchema` seam,
  so existing logs keep reading.
- ~~**Primary key encoding.** A uniform comparable byte-key keeps the log and indexes simple; typed keys are
  faster. Pick one and write it down.~~ **Settled: uniform order-preserving byte-key (`RowKey`).**
  Big-endian for unsigned integers, big-endian with the sign bit flipped for signed ones, UTF-8 for
  strings, raw bytes for `Identity` — so byte-wise comparison compares values and the log, the store,
  and future range indexes share one key shape. Floats are not key-encodable; typed keys remain a
  phase 07 optimization if benchmarks demand it.
- ~~**Index maintenance ownership** — does the store own index updates, or does the applier drive them? This
  determines how much phase 07 has to reimplement.~~ **Settled: store-owned.** `IHotStore.Apply` consumes
  whole commit records and maintains its own secondary indexes; the applier is just a cursor. Phase 07's
  engine swap reimplements indexing behind the same interface without touching the applier pipeline.
- ~~**Nested reducer calls** — allowed (sharing one transaction) or forbidden? Forbidding is simpler and can
  be relaxed later; allowing it later is a breaking change to nothing, so default to forbidding.~~
  **Settled: forbidden — a nested invoke throws.** Relaxing later is backwards-compatible; sharing one
  transaction today would complicate abort semantics for no workload that needs it. Shared logic belongs
  in plain methods both reducers call.
- ~~**AutoInc id encoding must be cluster-proof from record one.**~~ **Settled: 64-bit
  originator-prefixed ids, allocated within 63 bits.** The documented contract is **unique, not dense** —
  never promise contiguity, because phase 09 gives each shard its own log and "the" per-table sequence
  stops existing. GUIDs were rejected: they double key size on the hottest tables and, unless
  time-ordered, destroy index locality. Layout: top bit always zero, 16-bit originator, 47-bit per-shard
  sequence — the sign bit stays clear because **Postgres `bigint` is signed** (as are Java/Kotlin longs),
  and an id must round-trip through the relational tier unchanged. Columns may be declared `long` or
  `ulong`; the allocator never mints a value above 2⁶³−1 either way. A single-node deployment allocates
  with originator zero and never notices any of this. The originator-assignment mechanics land in 09 via
  the membership store.

## Done when

- A reducer defined in the test assembly inserts, updates, and deletes rows through `ctx.Db`, and reads its
  own writes within the same call.
- A reducer that throws leaves **zero** trace: no rows changed, no log record, no consumed AutoInc value.
- Killing the process mid-run and restarting rebuilds identical state from the log alone. Asserted by
  comparing a full table dump before and after.
- Replaying a log into a fresh `InMemoryHotStore` produces byte-identical state — proving projections are
  rebuildable without re-executing reducers.
- A log with a deliberately truncated final record recovers to the last intact LSN.
- Two tables mutated in one reducer either both commit or neither does.
- `dotnet test` green; the core path has no dependency on ASP.NET Core, FASTER, Npgsql, **or any OpenTelemetry
  package** — asserted by a test over `MelangeDB.Core`'s resolved dependencies, since this is easy to violate
  accidentally and painful to walk back.
- A test host collecting `ActivitySource("MelangeDB")` sees a `melange.reducer` span per invocation with the
  correct outcome attribute, and a `melange.commit` span with an `melange.fsync` child.
- `melange.applier.lag` reports a non-zero value when an applier is deliberately paused, and returns to zero
  when it resumes.
- Instrumentation with no listener attached costs no measurable throughput — benchmarked, because "it's just a
  null check" is an assumption worth verifying once.

## Risks

- **Over-building the schema model.** It's tempting to design for migrations and every column type now.
  Support what the reference workload uses (integers, floats, strings, `byte[]`, `Identity`, enums) and
  defer the rest.
- **Fsync-per-commit will look catastrophically slow** in a naive benchmark. Group commit is the answer,
  but do not optimize it in phase 01 — just make the policy configurable so the number is explainable.
