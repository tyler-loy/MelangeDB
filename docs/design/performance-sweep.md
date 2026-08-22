# MelangeDB performance findings

Deep pass over the engine, hot stores, commit log, subscription fan-out, wire path, and what is
already measured.

Date: 2026-08-05. Branch context: `feat/snapshot-isolation` (and current tree at time of review).

**Status: acted on, with one item left.** The review below is kept as written — it is the record of
what was found and why it was ranked the way it was. What has since been done to it is in [the
outcomes section](#outcomes) at the bottom: two defects the review missed, one finding it overstated,
one whose premise does not hold for this store, and three whose measured numbers changed the
decision rather than confirming it. Read that first if you want the current state of the code rather
than the history of the analysis.

The seven measurement gaps are closed, and every finding that was a work item is done — including
#15, compact wire rows, which the measurement both justified and re-described.

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

### Round two: what the measurements changed

The first round shipped with the caveat that none of the seven measurement gaps were closed. They
are closed now, by [eight benchmark suites](../../bench/README.md), and three of them changed a
decision rather than confirming one. That is the argument for measuring first, made concretely:

- **Finding #5 is not the unconditional win it was recorded as.** Single-pass index extraction is
  **2.9–3.3x faster and allocates 4.7x less** on rows carrying strings and byte arrays — and
  **1.3–1.7x *slower*** on rows of fixed-width scalars with fewer than eight indexes, because a
  scalar row costs almost nothing to deserialize and iterating a column list costs more than the
  repeat deserializes it saves. The fix is right for the shape real tables have; the regression is
  about 10 ns inside a ~500 ns write, and is recorded rather than optimised away.
- **Finding #6 named six allocation sites. Only one of them was worth doing.** The log payload is
  15–19% of everything a commit allocates, steady from one row to a hundred; framing and CRC add
  barely a hundred bytes on top. Pooling that one site alone took 14–20% off the whole commit's
  allocation.
- **Finding #15's headline is wrong, and its case is still strong.** The review calls compact wire
  rows "likely the #1 bandwidth and client CPU issue." Bandwidth: **1.18x (narrow) to 1.40x (wide)**
  — real, and not a protocol break's worth on its own. CPU and allocation: **4.6–12.4x on encode,
  2.4–2.9x on decode, 2.4–3.6x less allocation**. It was worth doing, for reasons other than the ones
  given — and it landed as protocol v2.
- **Finding #9's premise does not hold for this store.** See below.

### Done

| # | Finding | What landed |
| --- | --- | --- |
| — | PK range ignored the key directory | `FilterRange` walks `ScanKeys`, seeks to the low bound, stops at the high one |
| — | Per-subscriber column maps | One decode and one key copy per op, shared across subscribers |
| 1, 3 | Fan-out encodes under the lock | Frames are **measured** under the lock and encoded on the sender; `MsgPackWriter` gained a counting mode so measuring runs the same write path |
| 4 | Per-op version publish | One `TableVersion` per record per table in the in-memory store |
| 5 | Index encode deserializes per column | `RowCodec.EncodeColumnsFromBytes` — one pass, positional `RowKey[]`. See the caveat above |
| 7 | `ScanMerged` materializes | Two-way streaming merge; `First` no longer needs its own lazy path |
| 10 | Fixed hash table size | Derived from the memory budget, with `HotStore:HashBuckets` to override |
| 11 | Per-op key allocation | Composed into a reused buffer under the store lock |
| 13, 14 | `OpsFor` is O(all ops) | Slot positions indexed per table |
| 19 | Unbounded rate-limiter map | Refilled buckets evicted — safe because buckets are created full |
| 2 | Fsync guidance | Workload table and the two rules of thumb in [CONFIGURATION.md](../CONFIGURATION.md) |
| 6 | Commit-path allocation | The log payload writes into a bounded pooled buffer. **One** of the six sites, because the benchmark said the other five were not where the bytes were |
| 8 | Snapshot under the write lock | Captured under the lock (header plus a pinned view), written outside it, truncated under it again |
| 9 | FASTER reads take the store lock | Resident tables now read with no lock at all. Paged reads keep it — see below |
| 12 | Index range scans start at the leftmost key | Both stores hold indexes as one `ImmutableSortedSet` of `(value, key)` entries, so a range seeks |
| 15, 16 | Named column maps on the wire | Protocol v2: a wire descriptor per subscription, schema-ordered row bytes per row. See below |

### Not done, with reasons

| # | Finding | Why not |
| --- | --- | --- |
| 17 | Concurrent snapshot ticks | The review gates this on overrun metrics showing multi-tick pileups. They do not. |
| 18 | Recovery rebuilds FASTER | Deliberately rejected in phase 07; not a work item. |
| 20 | Telemetry sampling | "Keep this discipline" — not a work item. |

### Finding #15: what the break actually bought

The review's headline — "likely the #1 bandwidth and client CPU issue" — is half wrong, and the
correct half is the whole case.

**Bandwidth is not it.** A column map costs 1.18x the bytes on a narrow row and 1.40x on a wide one.
Real, and not a protocol break's worth on its own: MessagePack's map keys are short strings that a
websocket's deflate handles well, and the values dominate either way.

**CPU is it, and it is spent in the worst possible place.** Encoding a row as a map costs **4.6–12.4x**
what sending its bytes costs, and that cost is paid per subscriber per row on the fan-out path, under
the engine's write lock — so it is not one client's latency, it is the next reducer's. Decoding costs
**2.4–2.9x**, on every frame, on the client's frame thread. Allocation is 2.4–3.6x either way.

What made the fix tractable is that the win was already sitting there: the store holds rows in row
format v1, which is exactly what the wire wants. So the common case is not a cheaper encoding — it is
**no encoding at all**. A full row on an unprojected subscription is the committed bytes handed to
every subscriber: no decode, no dictionary, no copy. `FanoutSharingTests` asserts that by memory
identity rather than by equality, because equality would pass against an implementation that copied.

Three things the plan for this did not anticipate:

- **A per-subscription descriptor is not sufficient.** Column policies are evaluated per *row* —
  `ServerSubscription.VisibleColumns` takes the row bytes — so a hideout that hides a player's position
  narrows one row and not the next. The finding lists a "column mask bitset" as optional; it is what
  makes column policies expressible at all once names leave the wire. It costs one byte on rows that
  do not need it.
- **Schema drift needed a new home.** A map wire reports drift as a missing column; ordered bytes have
  no names in them, so a schema off by one column decodes into *plausible garbage* — an int read four
  bytes early is still an int. The check moved from per column per row to once per subscription,
  against the descriptor, and got stronger for the move: a rename, a reorder, and a changed kind are
  all one structural comparison, and the reorder is one the map wire could not have caught at all.
- **Row format v1 had to leave Core.** A client cannot reference the engine, and transcribing the
  format a second time on the client side is the drift hazard this codebase already knows about from
  the log payload. `RowWriter`, `RowReader`, and `ColumnKind` moved to Abstractions so both halves run
  the same code, and `RowWire`'s private `MeasureColumn` — a third transcription of the same widths —
  went with them.

The break is hard: there is no v1 encoder left, and a v1 peer is refused at the handshake rather than
accepted and failed later on the first row it cannot read. That is the honest failure mode for a
pre-1.0 break, and `HandshakeTests` pins it.

### Finding #9: the premise, corrected

The review reads the FASTER store's lock as protecting its single `ClientSession`, and asks for a
session pool. The code says otherwise, and the comment on the pinned read path says it outright: the
hybrid log **overwrites in place**, so the directory probe and the record read have to be atomic
against a concurrent write, or a reader can hold a directory entry whose record a concurrent delete
has already removed. The lock is a correctness mechanism for a log with no old versions. Pooling
sessions would admit more threads and every one of them would still need it.

What was available instead: a resident table's rows live in managed memory inside persistent
containers, reachable through one volatile read of an immutable version — no session, no hybrid log,
no overlay, and so no lock. That is now the path for `TryGetRow`, `ScanIndex`, `ScanIndexRange`, and
`Count`. `TryGetRow` is the one that matters, because the engine's fan-out calls it per op to fetch
a pre-image while holding the *engine* write lock.

Making **paged** reads concurrent needs pre-images captured with a version stamp readers can compare
— which is what a pinned read view already implements — not a change to session ownership.

### Measurement gaps: closed

All seven, by the suites in [`bench/`](../../bench/README.md). Numbers here come from one machine
(Windows 11, .NET 10, Release, `--job short`); ratios between rows of the same run travel, absolute
figures do not. Re-run before quoting.

| Gap | Suite | What it said |
| --- | --- | --- |
| 1 | `CommitPathBenchmarks` | Under `OnCommit` the fsync is over 95% of a commit and nothing else matters. Under `Interval` the log payload is 15–19% of commit allocation at every write-set size |
| 2 | `FanoutBenchmarks` | Fan-out cost against 1 to 500 subscribers, with shared and with distinct projections |
| 3 | `ApplyBenchmarks` | Batching a record's ops into one version publish is 1.3–1.9x faster; the gap is widest on tables with **no** secondary index, where index work does not dilute it |
| 4 | `IndexMaintenanceBenchmarks` | Single-pass extraction wins 2.9–3.3x on mixed rows and loses 1.3–1.7x on scalar ones |
| 5 | `WireFormatBenchmarks` | Maps cost 1.18–1.40x the bytes, 4.6–12.4x the encode time, 2.4–2.9x the decode time |
| 6 | `FasterHashBenchmarks` | Point-read cost against a derived hash size and a deliberately undersized one |
| 7 | `SnapshotBenchmarks` | A million-row snapshot held the write lock about 547 ms; pinning a view instead costs about 1 ms. This is what moved #8 from "worth considering" to done |

### Three tests that did not work, and what replaced them

All the same mistake in different clothes: **asserting the answer, when the answer was never what
changed.**

The index range seek was first tested by asserting the position a seek returns. A linear walk
returns the same position, so the test passed against a deliberately scanning implementation. What
separates a seek from a scan is how much work it does, so the test counts comparisons through the
set's own comparer: under 200 for a window a scan reaches in 3,997.

The batched apply (round one) was first tested by comparing a batched record against the same ops
applied one at a time. Both sides run the same accounting code, so a leak in that code appears on
both sides and cancels — confirmed by mutating the delete path and watching the test stay green.
What replaced it asserts an absolute: a table emptied by deletes weighs nothing.

The log payload rewrite avoided a third instance. A round trip through this codec's own reader
passes when the writer and the reader are wrong in the same direction, which is exactly the mistake
available when one change touches both. So the format test asserts byte equality against the
previous encoder, transcribed into the test file where changing the codec cannot change it.

### One regression the existing tests caught

Pooling the log payload through `ArrayPool<byte>.Shared` failed the residency test, and rightly. A
shared pool retains whatever it is handed, so one bulk load of hundred-kilobyte blobs parks
megabytes of buffers beside a memory budget this database reports as a computed artifact. The fix is
a bounded pool with a 256 KB ceiling: the steady state pools, and a rare oversized bulk record
allocates once and stays collectable. The memory report is one of the few numbers here that is a
promise rather than an observation, and a test that guards it earned its keep.

## Round three: the siblings of the range walk

The primary-key range walk (0.2.1, `ScanKeyRange`) was found by a production deployment, not by the
suite, and the reason it was not found earlier is structural: a walk and a seek return the same rows,
and every test table was small enough that the difference was noise. That is a *family* of defect,
not an instance — correct answers, cost proportional to the distance to the window rather than the
window — so the third round went looking for the rest of the family before the next deployment did.
The method was the one the first two rounds settled on: read the hot paths with one question (what
does this scale with, and should it?), then write the test that counts the work rather than checks
the answer.

### Found and fixed

**The commit log had no seek.** `FileCommitLog.ReadFrom(lsn)` scanned from the header, reading,
CRC-checking and decoding every record below `lsn` and throwing it away. Nine callers pay for that,
and all but one call it per batch with a cursor near the head: the resume replay (twice — once to
age the gap, once to replay it), the Postgres applier, the event bus's catch-up, the applier
pipeline's catch-up (under the write lock), and in a cluster the event forwarder, the border
publisher (per observer stream) and the hub replica pump (per node link). Every one re-read the
retained log from byte zero per batch — with a cursor at the head and a 300-second retention window,
that is "read the last five minutes of history to fetch the next hundred records", forever. The fix
is a sparse LSN→offset index (`IndexStrideBytes` apart, built by the recovery walk the log already
does, extended per append, rebased per compaction), a verified seek, and a frame-header hop for the
remainder of the stride. `LogSeekBenchmarks` is the gate: ten records at three positions in a
100,000-record log: before, 195 µs / 12.0 ms / 25.6 ms for Low / Middle / High; after, 219 µs /
262 µs / 218 µs. The dev box, short job; the ratio is what travels.

**Compaction held the engine for the size of the log.** `TruncateBefore` decoded every record to
learn its LSN and rewrote every survivor, under the write lock, the append lock and the fsync lock
— so the stall hit commits that had not started, commits mid-append, and commits parked on an fsync,
and it scaled with the retained log rather than with what was removed. The retention floor's scan
decoded every removable record under the same lock. Now: the floor decision stays under the write
lock (binary search for the retention boundary, one record per probe); the compaction seeks to the
first survivor, copies bytes off the lock, and takes the log's locks only for the tail appended
meanwhile and the swap. A pin taken during an in-flight compaction pins at its floor. The test
commits from another thread *inside* the compaction, through a hook between its phases, and asserts
the commit lands — a deadlock against the old code, which is the discriminating shape the first two
rounds taught.

### The test that counts instead of checks

`CommitLogSeekTests` pins the cost where it is observable: the log counts the frames a read passes
over (`SkippedFrames`), and reading the last ten of four thousand records must pass over fewer than
a hundred where the scan passed over 3,990. Disabling the index — making `TryFloor` return nothing —
fails exactly the two tests that claim a cost and none of the four that claim a result, which is the
property a cost test has to have. The truncation tests cover what an index can get wrong: every
survivor's offset moving under it, appends landing between the copy and the swap, and a reopen
rebuilding it from the compacted file.

