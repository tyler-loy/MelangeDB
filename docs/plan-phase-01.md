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

## Out of scope

Codegen (02), networking (03), auth (04), scheduling (05), events (06), FASTER and paging (07), Postgres
(08), anything cluster-related (09–10). Reducers are invoked by direct method call in tests.

## Decisions to settle

- **Row representation in the log.** Reflection-based serialization is fine to start, but the format must
  be versioned from record one, since phase 02 replaces the serializer and existing logs must still read.
- **Primary key encoding.** A uniform comparable byte-key keeps the log and indexes simple; typed keys are
  faster. Pick one and write it down.
- **Index maintenance ownership** — does the store own index updates, or does the applier drive them? This
  determines how much phase 07 has to reimplement.
- **Nested reducer calls** — allowed (sharing one transaction) or forbidden? Forbidding is simpler and can
  be relaxed later; allowing it later is a breaking change to nothing, so default to forbidding.

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
- `dotnet test` green; the core path has no dependency on ASP.NET Core, FASTER, or Npgsql.

## Risks

- **Over-building the schema model.** It's tempting to design for migrations and every column type now.
  Support what the reference workload uses (integers, floats, strings, `byte[]`, `Identity`, enums) and
  defer the rest.
- **Fsync-per-commit will look catastrophically slow** in a naive benchmark. Group commit is the answer,
  but do not optimize it in phase 01 — just make the policy configurable so the number is explainable.
