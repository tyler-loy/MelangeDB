# MelangeDB performance findings

Deep pass over the engine, hot stores, commit log, subscription fan-out, wire path, and what is
already measured.

Date: 2026-08-05. Branch context: `feat/snapshot-isolation` (and current tree at time of review).

**Status: partly acted on.** The review below is kept as written — it is the record of what was
found and why it was ranked the way it was. What has since been done to it, including two defects
the review missed and one finding it overstated, is in [the outcomes section](#outcomes) at the
bottom. Read that first if you are looking for the current state of the code rather than the
history of the analysis.

---

## Architecture context (what the hot path is)

One commit roughly looks like:

```
reducer body → write-set collapse → commit guards
  → FileCommitLog.Append (+ optional fsync)
  → commit observers (subscription Fanout, scheduler bookkeeping, …)
  → Appliers.NotifyAppended (hot store Apply under store lock)
  → optional auto-snapshot under the same write lock
```

**Serialized reducers** hold the engine write lock for that entire sequence. **Snapshot-isolated
reducers** only take the lock for reconcile/guards/append/fan-out — already the right lever for
read-heavy sweeps.

Published ceilings ([CLUSTERING.md](../CLUSTERING.md)):

| Mode | Sustained commits/s (one shard) |
| --- | ---: |
| `FsyncPolicy.OnCommit` (default) | ~1,100 |
| `FsyncPolicy.Interval` (50 ms) | ~52,000 |

Those numbers already tell you where the first wall is. Clustering does **not** raise the
crowded-shard ceiling: one town square = one writer.

---

## What’s already strong

These are real wins, not aspirational:

| Area | Why it helps |
| --- | --- |
| Generated `RowCodec<T>` | No reflection/boxing on the typed reducer path |
| Struct rows + write-set overlay | Transaction work stays in-memory; no I/O in bodies |
| Write-set last-write-wins collapse | Small log records; fewer store ops |
| `Isolation.Snapshot` | Moves long reads off the write lock |
| Persistent containers for pins | Free LSN pin vs ~28.6 ms table clone |
| Out-of-line FASTER blobs + key directory | Scans/indexes avoid pulling large payloads |
| Directory-stored index values (FASTER) | Index maintenance doesn’t re-read hybrid log |
| Subscriptions indexed by table | Fan-out doesn’t scan all subscriptions |
| `ScanKeys` for PK ranges | Fixed “page whole table for a window” (~3s → ~5ms story) |
| `ProjectedEqual` on restricted subs | Avoids silent-column update spam |
| Postgres applier decoupled | Relational lag doesn’t sit on the commit path |
| Configurable fsync | Throughput vs durability is an explicit knob |

The design is aware of lock scope, fsync, and fan-out. The remaining wins are mostly **under-lock
work that doesn’t need to be there**, **allocation density**, and **data-structure costs that
compound at scale**.

---

## Priority findings

### P0 — Structural ceilings (biggest impact)

#### 1. Write lock still owns too much of “after body”

Even with snapshot isolation, **every commit** serializes:

- log encode + write + fsync
- **subscription fan-out** (predicate/policy, column maps, MessagePack)
- hot-store apply
- observers (scheduler)
- optional full snapshot

So reducer body is not the only global stall. At high player density, **fan-out + fsync dominate**,
which matches load-test framing (cost grows with subscribers × commits). See
[LOAD-TESTING.md](../LOAD-TESTING.md).

**Win shape:** shrink work under `_writeLock` after append — especially fan-out serialization and
store apply — or make more of it async with strict LSN ordering.

#### 2. Default fsync is the first hard wall

`CommitLog:FsyncPolicy = OnCommit` is correct for durability and caps you near disk fsync rate
(~1.1k commits/s). `GroupCommit` is **accepted-and-reserved** and cannot help under single-writer
([CONFIGURATION.md](../CONFIGURATION.md) already says this).

**Win shape (ops / product):** document workload guidance more aggressively (interval fsync for
sim ticks, OnCommit for money/auth).

**Win shape (engine, later):** only if you ever allow concurrent prepare + group flush; today that
fights the single-writer model.

#### 3. Fan-out builds wire payloads under the write lock

`SubscriptionEngine.Fanout` → `RowWire.ToColumns` (`Dictionary<string, object?>` per matching op) →
`IDeltaSink.EnqueueDelta` → **full MessagePack serialize** on the engine thread while holding the
lock (`MelangeSocketConnection.EnqueueDelta`).

At N players each subscribed to the same table, one Move is:

- N match evaluations
- up to N column maps + N MessagePack frames
- all before the next reducer can enter

**Win shape:**

- Compute wire ops under lock (needed for pre-image correctness), but **defer MessagePack +
  queueing** to a per-connection sender with a lightweight, lock-free handoff of already-owned
  buffers.
- Prefer **binary projected row slices** on the wire instead of named column maps (biggest
  bandwidth + CPU cut for games).
- Key-range / shard-scoped subscription indexes (docs already defer this until measured).

---

### P1 — Per-commit / per-write tax (under lock or on Apply)

#### 4. `ImmutableSortedDictionary` put cost on every Apply

Both stores publish a new version per put: rows + each secondary index. Measured **~1.78× put** vs
mutable (0.39 µs vs 0.22 µs) at 1M rows — small per op, multiplies as write sets and indexes grow.
Index updates do multiple `SetItem` path copies per column. See
[design/snapshot-isolation.md](snapshot-isolation.md).

**Win shape:** keep persistent structures for **pin**, but consider:

- mutable primary map + COW only when a read view is open, or
- chunked/versioned pages (fewer nodes touched per write), or
- batch Apply: one version publish per commit, not per op (especially important for multi-op
  records / border batches).

Bulk load already uses builders — the **online Apply path does not**.

#### 5. Index encoding deserializes the whole row (often repeatedly)

`RowCodec.EncodeColumnFromBytes` does:

```text
Deserialize(row) → EncodeColumn(name, typed)
```

Called once **per indexed column** in `EncodeIndexValues` / in-memory `IndexedValues`. A table with
3 indexes pays **3 full deserializes** per put for index maintenance alone.

**Win shape:** generate `EncodeAllIndexedColumns(ReadOnlySpan<byte>)` that walks the row once and
fills a `RowKey[]` (or encode from offsets without materializing the struct).

#### 6. Allocation density on the commit path

Hot allocations that fire every commit:

| Site | Cost |
| --- | --- |
| `LogRecordCodec.WritePayload` | `MemoryStream` + `BinaryWriter` + final `ToArray` |
| Generated `Serialize` | `RowWriter` + `ToArray` (copy) |
| `RowKey` ctor | always copies encoded bytes |
| `WriteSet.ToOps` | new `List<RowOp>` |
| `FileCommitLog` | payload buffer + CRC over full payload |
| Fan-out | column dictionaries + MessagePack buffers + `op.Key.ToArray()` |

Under interval fsync, **allocation + encode + fan-out** become the next wall after disk.

**Win shape:** `ArrayPool` / `IBufferWriter` for log and row bytes; `RowKey` intern or store
`ReadOnlyMemory` owned by the write set; `WriteSet` expose `IReadOnlyList` without re-list;
stackalloc/fixed buffers for small keys.

#### 7. `ScanMerged` materializes the full table

Any `Scan` / PK range filter while the write set has **any** pending op for that table builds a
`SortedDictionary` of the **entire store scan** + overlay. One insert then a full table scan in the
same reducer turns O(n) into O(n log n) + huge allocation.

**Win shape:** merge iterators (store scan + sorted pending keys) without materializing; or
“overlay only” path when pending is tiny (common case).

#### 8. Snapshots run under the write lock

`TakeSnapshotCore` scans every table, writes a full snapshot, fsyncs, optionally truncates — all
while holding `_writeLock`. Interval is 100k commits, so rare, but at large resident sets this is a
multi-second world freeze.

**Win shape:** pin a read view under lock (cheap), write snapshot **outside** the lock from that
pin; only truncation needs careful coordination with floors.

---

### P2 — Store / read path

#### 9. FASTER: single session + store lock on paged reads

Documented in [design/snapshot-isolation.md](snapshot-isolation.md): paged view reads take
`store._lock` per row; resident tables are lock-free from the pin. Snapshot-isolated sweeps over
**paged** tables still serialize with Apply.

**Win shape:** per-view or pool of FASTER sessions (called out in design, not done); ensure hot
sweep tables are `Residency.Resident`.

#### 10. FASTER fixed hash table size `1L << 16`

Main/blob KV constructed with 65k hash buckets regardless of `MemoryBudgetBytes` / expected row
count. Large paged tables will lengthen chains and increase pending I/O completions.

**Win shape:** size hash table from expected keys or budget (power of two, with an operator knob).

#### 11. Per-op key buffer allocation in FASTER

`StoreKey` / `BlobKey` allocate a new `byte[]` every upsert/delete/read. Under high apply rates this
is pure GC pressure.

**Win shape:** stackalloc for small keys, or thread-local/reusable buffers (session is
single-threaded under the store lock already).

#### 12. Secondary index range scans start at the leftmost key

`foreach (var (value, set) in index)` skips until `low` — correct, but **O(position of low)** on
`ImmutableSortedDictionary`, not O(log n + k). Moving windows near the high end of a large index pay
full walks of discarded keys.

**Win shape:** use a range enumerator / seek from lower bound if the immutable API allows, or a
different range structure (B-tree with seek).

#### 13. `WriteSet.OpsFor` is O(all ops)

Scans the whole write set for one table. Fine for tiny sets; painful if a reducer stages thousands
of ops and then repeatedly filters/counts.

**Win shape:** optional per-table linked lists or secondary index of slots.

#### 14. Unique-constraint check always hits the store index

Correct, but each insert/update with unique columns does index probe + full pending scan. Multiple
unique columns multiply this.

Lower priority than (5) and fan-out unless unique-heavy schemas dominate.

---

### P3 — Client, protocol, secondary systems

#### 15. Wire shape: named maps, not compact rows

MessagePack maps of `string → object` for every row/op:

- larger frames
- client-side dictionary + decode into struct
- server-side `ToColumns` allocation

For a game at 15 Hz × hundreds of players, this is likely **the #1 bandwidth and client CPU** issue
after fan-out itself.

**Win shape:** schema-ordered binary columns (row format v1 already exists) projected on the wire;
optional column mask bitset.

#### 16. Client cache merge is solid; apply path is map-heavy

Refcounting and rescope-by-diff are good. Remaining cost is decoding column maps per op and locking
per cache.

#### 17. Scheduler is single-threaded by design

Idle repeating timers write nothing — excellent. One slow tick delays others
(`melange.scheduler.overruns`). Windowing + snapshot isolation on heavy ticks is the mitigation; a
worker pool “buys nothing” for engine lock contention, which is true — but **snapshot ticks** could
theoretically arm more than one body concurrently (with care). Only worth it if overrun metrics show
multi-tick pileups.

#### 18. Recovery rebuilds FASTER every start

Intentional (log is source of truth). Startup cost ∝ snapshot size. Incremental/FASTER-native
recovery was deliberately rejected — only revisit if cold start becomes an ops problem.

#### 19. Rate limiter dictionary unbounded

One bucket per `(Identity, reducer)` forever. Not a hot-path CPU issue; long-lived servers with many
identities may want eviction.

#### 20. Telemetry

Spans on every reducer/commit are fine when sampled; delta spans are already sampled at 1%. Keep
that discipline if more instrumentation is added.

---

## Where time actually goes (mental model)

```
                    ┌─────────────────────────────────────────┐
 OnCommit fsync     │  DISK FSYNC  (~ms)                      │  ← often #1
                    └─────────────────────────────────────────┘
 Interval fsync     │  Fan-out × subscribers (CPU + alloc)    │  ← becomes #1
                    │  Log encode + CRC                       │
                    │  Store Apply (immutable + FASTER I/O)   │
                    │  Reducer body (unless Snapshot)         │
                    └─────────────────────────────────────────┘
 Large tables       │  ScanMerged / full scans / index encode │
                    │  Snapshot under lock (rare but long)    │
                    └─────────────────────────────────────────┘
```

---

## Measurement gaps (what to run before changing code)

Existing coverage is strong for containers and read views (`bench/MelangeDB.Benchmarks`) and for
cluster load (`tools/MelangeDB.LoadTest`). Missing for prioritization:

1. **Commit path breakdown under interval fsync**  
   Body / commit / fsync / post-commit telemetry already exists — run the reference or LoadTest and
   attribute **post-commit** into fan-out vs Apply vs observers (today post-commit is one blob).

2. **Fan-out cost vs subscription count**  
   Fix write-set size (1 row), vary active subs 1→500; measure locked duration and gen0. Confirms
   whether wire encode or match dominates.

3. **Apply cost: multi-op batch vs N single-ops**  
   Validates “one version publish per commit”.

4. **Index maintenance with multi-index tables**  
   `EncodeColumnFromBytes` × N vs single-pass encode.

5. **Wire size: column maps vs raw v1 projection**  
   Bytes/s from LoadTest’s delta traffic metrics with a prototype framing (even offline encode
   bench).

6. **FASTER hash occupancy / pending rate** at target row counts  
   Before resizing blindly.

7. **Snapshot duration vs resident bytes** under lock  
   Whether snapshot-out-of-lock is urgent.

---

## Recommended priority order (when implementing)

| Order | Item | Expected impact | Risk |
| ---: | --- | --- | --- |
| 1 | Move MessagePack (and maybe socket enqueue) off write lock; keep match under lock | High at multi-sub density | Ordering / backpressure |
| 2 | Compact wire rows (v1 bytes or fixed columns) instead of maps | High bandwidth + CPU | Protocol version |
| 3 | Single-pass index key extract from row bytes | Medium-high on write-heavy indexed tables | Correctness vs nulls |
| 4 | Batch immutable version publish per `Apply`/`commit` | Medium on multi-op commits | Pin/view semantics |
| 5 | Allocation trim: log codec, RowKey, StoreKey, RowWriter | Medium at 10k–50k commits/s | Careful pooling lifetime |
| 6 | `ScanMerged` streaming merge | High for scan+write reducers | Overlay edge cases |
| 7 | Snapshot from pinned view outside write lock | Removes rare multi-second stalls | Truncation floors |
| 8 | FASTER session-per-view / hash sizing / key buffers | Medium for paged + large sets | Complexity |
| 9 | Index range seek | Medium for large secondary indexes | Structure choice |

### Module-side guidance (no engine change)

- Window long sweeps across many short transactions.
- Mark hot sweep tables `Residency.Resident`.
- Use `Isolation.Snapshot` only for recompute-safe bodies (not read-modify-write).
- Prefer equality/range subscriptions over full-table.
- Match `FsyncPolicy` to durability needs (interval for sim ticks, OnCommit for durable money/auth).

---

## Summary

MelangeDB’s architecture already targets the right problems (log as truth, paging, DI, snapshot
isolation, table-scoped fan-out). The largest remaining performance wins are not exotic algorithms —
they are:

1. **Less work under the single writer lock after the body** (especially fan-out encode).
2. **Cheaper durability mode selection** for sim-style workloads (already available, still the first
   ceiling at default).
3. **Denser wire and less per-row allocation** (column maps are expensive at game tick rates).
4. **Write-path structure costs**: full-row deserialize for each index column, per-op immutable
   versioning, and key/buffer churn.
5. **A few sharp edges** (`ScanMerged` full materialize, snapshots under lock, paged FASTER lock,
   fixed hash size).

Highest-value next step: a short **measurement plan** (or focused prototype design) for items 1–3
only — those are where the published ceilings and load-test shape say the money is.

---

## Key source anchors

| Area | Primary locations |
| --- | --- |
| Engine invoke / lock | `src/MelangeDB.Core/MelangeEngine.cs` |
| Write set / overlay | `src/MelangeDB.Core/WriteSet.cs`, `TransactionDb.cs` |
| Commit log | `src/MelangeDB.Core/CommitLog/FileCommitLog.cs`, `LogRecordCodec.cs` |
| In-memory store | `src/MelangeDB.Core/Store/InMemoryHotStore.cs` |
| FASTER store | `src/MelangeDB.Storage.Faster/FasterHotStore.cs` |
| Subscriptions | `src/MelangeDB.Server/Subscriptions/SubscriptionEngine.cs`, `ServerSubscription.cs` |
| Wire rows / socket | `src/MelangeDB.Server/RowWire.cs`, `MelangeSocketConnection.cs` |
| Protocol | `src/MelangeDB.Protocol/MessagePackFrameSerializer.cs` |
| Snapshot isolation design | `docs/design/snapshot-isolation.md` |
| Measured ceilings | `docs/CLUSTERING.md`, `docs/LOAD-TESTING.md` |
| Benchmarks | `bench/MelangeDB.Benchmarks/` |

---

## Outcomes

What the sweep on `perf/findings-sweep` actually changed, and what it deliberately did not.

### Two defects the review missed

**`FilterRange` on a primary key never used `ScanKeys`.** The review lists `ScanKeys` as a strength
(the ~3s → ~5ms subscription fix) and `ScanMerged` as separate problem #7, and does not connect
them. `ScanKeys` had exactly one caller — `ServerSubscription` — so the reducer-facing window query
still routed through `ScanMerged` and read every row below the window to discard it. Worse than #7
describes: it happened **unconditionally**, not only when the write set had pending ops, because
`FilterRange` had no `hasPending` fast path. The same defect the store seam had already fixed once,
surviving on the other side of it.

**Fan-out recomputed identical column maps per subscriber.** `RowWire.ToColumns` was called from
`ComputeDelta`, once per subscription, so two hundred players on one table with the same projection
built two hundred identical dictionaries — and two hundred copies of the same key — under the write
lock. Not in the review at all, and strictly cheaper to fix than either of its top two items.

### One finding the review overstated

**#1's risk note has ordering and backpressure the wrong way round.** Ordering was never at risk:
the delta queue is FIFO per connection with a single sender. Backpressure was the whole problem —
the drop must stay synchronous under the engine lock, because a deferred sweep once raced a client
re-subscribe and left it silently dead. That constraint, not ordering, is what shaped the fix.

### Done

| # | Finding | What landed |
| --- | --- | --- |
| — | PK range ignored the key directory | `FilterRange` walks `ScanKeys`, seeks to the low bound, stops at the high one |
| — | Per-subscriber column maps | One decode and one key copy per op, shared across subscribers |
| 1, 3 | Fan-out encodes under the lock | Frames are **measured** under the lock and encoded on the sender; `MsgPackWriter` gained a counting mode so measuring runs the same write path |
| 4 | Per-op version publish | One `TableVersion` per record per table in the in-memory store |
| 5 | Index encode deserializes per column | `RowCodec.EncodeColumnsFromBytes` — one pass, positional `RowKey[]` |
| 7 | `ScanMerged` materializes | Two-way streaming merge; `First` no longer needs its own lazy path |
| 10 | Fixed hash table size | Derived from the memory budget, with `HotStore:HashBuckets` to override |
| 11 | Per-op key allocation | Composed into a reused buffer under the store lock |
| 13, 14 | `OpsFor` is O(all ops) | Slot positions indexed per table |
| 19 | Unbounded rate-limiter map | Refilled buckets evicted — safe because buckets are created full |
| 2 | Fsync guidance | Workload table and the two rules of thumb in [CONFIGURATION.md](../CONFIGURATION.md) |

### Not done, with reasons

| # | Finding | Why not |
| --- | --- | --- |
| 6 | Commit-path allocation trim | Untouched. Wants the interval-fsync commit breakdown (measurement gap 1) to say which of the six sites actually dominates, rather than pooling all of them on principle. |
| 8 | Snapshot under the write lock | Untouched. The pin is cheap and the write is not, but truncation has to stay ordered against the retention floors, and that coordination is the actual work. |
| 9 | FASTER session pool | Untouched. Paged reads still take the store lock per row. A structural change to session ownership, not a sizing fix. |
| 12 | Index range seek | Untouched. `ImmutableSortedDictionary` has no seek, so this needs the index container replaced — an `ImmutableSortedSet` of `(value, key)` entries would seek *and* make maintenance cheaper, but it is a refactor across both stores. |
| 15 | Compact wire rows | Untouched. The largest single item and a protocol break; wants measurement gap 5 (bytes/s for maps vs a v1 projection) before the client, the bindings generator, and the cache all move. |
| 16 | Client apply path | Follows 15; nothing to do independently. |
| 17 | Concurrent snapshot ticks | The review gates this on overrun metrics showing multi-tick pileups. They do not. |
| 18 | Recovery rebuilds FASTER | Deliberately rejected in phase 07; not a work item. |
| 20 | Telemetry sampling | "Keep this discipline" — not a work item. |

### Measurement gaps: still open

None of the seven were closed. Every "done" row above is a structural fix whose correctness is
tested but whose *magnitude* is unmeasured on this hardware — the sweep removed work that provably
happened, it did not demonstrate how much that work cost. Gaps 1 (commit breakdown), 2 (fan-out vs
subscription count), and 3 (batched vs per-op apply) now have code worth pointing at, and should be
run before anyone quotes a number.
