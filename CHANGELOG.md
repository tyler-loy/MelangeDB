# Changelog

All notable changes to MelangeDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the pre-1.0 caveat that **the
public API may break in any release** until 1.0.

All packages ship together at one version; there is no per-package versioning. See
[docs/RELEASING.md](docs/RELEASING.md).

## [Unreleased]

### Added

- **Eight benchmark suites** in [bench/](bench/README.md), closing the seven measurement gaps the
  performance sweep left open: commit-path attribution, fan-out against subscriber count, batched apply,
  index maintenance, wire format, FASTER hash sizing, snapshot duration, and index range position. The
  project now runs the source generator, so a suite measures the generated codec rather than the
  reflection fallback. Results, and the three decisions the numbers changed rather than confirmed, are in
  [docs/design/performance-sweep.md](docs/design/performance-sweep.md).

- **`HotStore:HashBuckets`.** Sizes the FASTER hash index, rounded up to a power of two; zero derives it
  from `HotStore:MemoryBudgetBytes`. It was a hardcoded 65,536 regardless of budget or row count — an
  index sized for roughly a quarter of a million records whatever the configuration said, past which
  chains lengthen and a lookup that should be one probe becomes several, each a candidate for a pending
  I/O completion on a paged table.
- **Fsync workload guidance** in [docs/CONFIGURATION.md](docs/CONFIGURATION.md): the ~47× spread between
  `OnCommit` and `Interval`, and the rule for choosing between them — simulation state takes `Interval`,
  anything a player can dispute takes `OnCommit`, and a database holding both takes `OnCommit`.
- **`IMelangeSerializer.Measure`.** The exact serialized length of a frame without producing it, so the
  delta path can judge backpressure under the engine's write lock and encode on the sender.

- **`1003 SlowReducer` now says which half was slow.** The warning and the `melange.slow_reducer` span
  event carry `BodyMs`/`melange.body_ms`, `CommitMs`, `FsyncMs`, `PostCommitMs`, and `Rows` alongside the
  total. A wide reducer body and a stalled disk produced identical warnings before, and telling them apart
  meant pulling child spans out of a trace store by `trace_id` — for a warning whose whole job is to say
  "look here". Body time is measured directly rather than derived as *total − commit*, so commit observers,
  applier notification, and an automatic snapshot land in `PostCommitMs` instead of being billed to the
  module's reducer body. Under a deferred `CommitLog:FsyncPolicy` the fsync field is **absent rather than
  zero** and the entry keeps id `1003` under the event name `SlowReducerDeferredFsync`.

- **Pinned reads on the hot store.** A store may now hand out an `IHotStoreReadView` — a read view
  fixed at one LSN that an `Apply` cannot disturb, however long it is held and however lazily its
  enumerations are consumed. The read surface moved to a shared `IHotStoreReader` (`IHotStore` still
  carries every member it did, so nothing implementing or calling it changes), and the capability
  itself is the optional `IReadViewSource`, in the manner of `IResidencyControl`. `InMemoryHotStore`
  implements it by holding each table's rows and indexes in persistent containers, so opening a view
  captures references rather than copying: measured at one million 96-byte rows, **identical
  container memory**, bulk build 0.57×, point reads 0.99×, full scan 1.24×, one put 0.39 µs against
  0.22 µs — and pinning a view costs nothing where cloning the table cost 28.6 ms. `FasterHotStore`
  implements it too, splitting the problem the way it splits storage: its managed-memory state (key
  directory, indexes, a resident table's rows) is captured by reference, while a **paged** row's
  payload — which a hybrid-log upsert overwrites in place, leaving no old version — is covered by an
  undo overlay that costs one pre-image read per paged row written *while a view is open*, and
  nothing at all while none is. Measured at 100,000 rows: opening a view is 37.9 ns (in-memory) and
  58.0 ns (FASTER), independent of row count in both; holding one open costs the in-memory store
  **nothing** (0.99× on a hundred-row apply) and the FASTER store **~188 ns per paged row written**.
  One contract suite runs against both stores, because a reducer must
  not behave differently for having been configured onto a different storage engine. Reading a paged
  row through a view still takes the store lock for that row (1.24× on a full scan); a resident table
  reads lock-free.
  Changing a table's residency invalidates open views loudly rather than answering from bookkeeping
  that no longer describes the data. This is the groundwork for snapshot-isolated reducers
  ([docs/design/snapshot-isolation.md](docs/design/snapshot-isolation.md)); no reducer uses it yet.

- **`[Reducer(Isolation = Isolation.Snapshot)]` — a reducer body that does not hold the write lock.**
  A third axis on `[Reducer]` next to `Site` and `Policy`. The body runs against a read view pinned at
  one LSN while other transactions commit underneath it; only reconcile, the commit guards, and the log
  append serialize. A sweep that spends 200 ms reading and 0.2 ms writing stops charging the other
  199.8 ms to every writer on the engine. `Isolation.Serialized` is the default and the honest name for
  what was always happening — one global lock around the whole body *is* serializable — and nothing
  about that path changes.

  **Read the eligibility rule before declaring it: snapshot isolation is safe for
  recompute-from-scratch and unsafe for read-modify-write.** A body that reads state, computes a value,
  and writes it is safe — two concurrent runs each write a defensible answer and the last one wins. A
  body that reads a value, adds a delta, and writes the sum is not: two runs read the same number and
  one increment is lost, silently and permanently. There is no read-set validation and no retry; the
  declaration is the contract. Both shapes routinely live in the same reducer, which is why this is
  opt-in per reducer and **never inferred** — the compiler cannot tell them apart and the module author
  can. The write set *is* reconciled against committed state before the guards see it, so an update of a
  row someone deleted becomes an insert and a delete of a row already gone drops; that fixes op shape,
  never op value, and cannot rescue a lost increment. There is a test that asserts the lost update, so
  the hazard is a pinned-down property rather than a warning in prose.

  Two consequences of a body no longer running alone. **AutoInc ids are reserved as they are allocated**
  rather than staged until commit, because two concurrent bodies staging against one sequence hand out
  the same id — so an aborted snapshot transaction leaves a gap, which is within the sequencer's
  standing "unique, not dense" contract. And a store that does not implement `IReadViewSource` runs
  these reducers **serialized, with a one-time `1004 SnapshotIsolationUnavailable` warning**: they stay
  correct, they are just not faster. Both shipped stores offer pinned reads. Design record and the
  guardrails still open in
  [docs/design/snapshot-isolation.md](docs/design/snapshot-isolation.md).

- **Prerelease packages from `main`.** Every push to `main` now publishes all eleven packages as
  `<VersionPrefix>-ci.<run-number>` to this repository's GitHub Packages feed, so a fix can be
  consumed before a release exists instead of only downloaded as a workflow artifact. nuget.org is
  unchanged and still carries releases only — a version there can never be deleted, only unlisted,
  which is why the per-commit stream lives on a feed whose versions can be. Restoring needs a token
  even though the repository is public; that, and the reasons prereleases are not a supported
  surface, are in [docs/RELEASING.md](docs/RELEASING.md).

- **A benchmark project**, `bench/MelangeDB.Benchmarks`, so measured claims in the design documents
  are reproducible rather than remembered — starting with the two that settled how a read view is
  pinned. It does not run in CI: these are minutes-long measurements, and shared runners would
  produce numbers not worth recording.

- **`ReducerKind.Init`** — a reducer fired once on an engine that has never committed anything,
  before its scheduler starts. Its `ReducerSite` picks the engine as for any other reducer:
  shard-executed init reducers fire on every per-shard engine as that shard opens, hub-executed ones
  on the hub, and both on the single engine of a deployment that is not clustered. Each fire is its
  own transaction; a thrower is logged (EventId 1106), not rethrown.

### Fixed

- **A slow reducer that aborted warned about nothing.** `1003` fired only on the commit path, so a reducer
  that spent 500 ms in its body and then threw was silent — while holding the write lock, and therefore
  every other writer, for exactly as long as one that committed. A validating reducer that does its
  expensive work before refusing, a transaction rejected by the cluster's span check, and a scheduled
  reducer that throws were all invisible to the alarm built to catch them. Aborts now warn with the same
  split, carrying `Outcome` (`abort`/`rejected`) and reporting only `DurationMs` and `BodyMs`, since nothing
  was appended; the event name is `SlowReducerAborted`. Rejections warn too — an alert that considers them
  routine can filter on `Outcome`.
- **`schemaHash` no longer depends on the MelangeDB version.** The manifest's `generator` field was
  inside the hashed content, so every MelangeDB release rotated every schema hash — and
  `conn.SchemaHash`, whose only job is detecting drift, reported drift against a schema nobody had
  touched. The hash is now taken over the manifest rendered with both `schemaHash` and `generator`
  empty, so it identifies the schema and not the build that emitted it. **Hash values change once
  in this release**; after that, upgrading MelangeDB leaves them alone.
- **A shard created on first visit had no timer rows, and nothing said so.** Shards are created
  lazily when a session first resolves to one, and a scheduled table is `Placement.Local` — so its
  timer rows live in that shard's own engine, which opened empty, and no `ReducerKind` could seed
  them. The shard served reads and writes correctly and simply never ticked: in a spatial world, the
  first player into a never-visited block found creatures inert, nothing growing and nothing
  decaying, with no error anywhere. `ReducerKind.Init` is the fix; a shard that opens holding no rows
  in any scheduled table is now also warned about (EventId 1723).

### Changed

- **Secondary index range scans seek to their lower bound.** Both storage engines held indexes as a
  dictionary of value → nested key set, which cannot seek: a range query started at the leftmost value and
  discarded everything below its window, so a ten-row window at the far end of a large index paid for the
  whole index. Both now hold one sorted set of *(value, key)* entries, where the lower bound is a binary
  search. Index maintenance got cheaper on the way past — adding a row is one entry insert rather than
  read-inner-set, rebuild, write-back.

- **Snapshots write outside the engine's write lock.** `TakeSnapshot` scanned every table, wrote the file,
  fsynced, and truncated while holding the lock, so nothing committed for the duration — measured at about
  547 ms for a million rows. It now captures the header and pins a read view under the lock (about a
  millisecond), writes from the pin outside it, and re-takes the lock only to truncate. Evaluating the
  retention floors after the capture is safe in the only direction that matters: floors advance, and the
  result is capped by the snapshot's own LSN. Two snapshots never write at once; an overlapping automatic
  trigger is skipped and logged at Debug as `1509 SnapshotAlreadyRunning`. A store with no pinned-read
  capability keeps the old behaviour, having no way to offer a consistent view outside the lock.

- **A resident FASTER table reads with no store lock.** `TryGetRow`, `ScanIndex`, `ScanIndexRange`, and
  `Count` answer from one volatile read of an immutable version when the table is resident. `TryGetRow` is
  the one that mattered: the engine's fan-out calls it per op, to fetch a pre-image, while already holding
  the engine write lock. Paged reads keep the store lock, and it is not there for the session's sake — the
  hybrid log overwrites in place, so the directory probe and the record read must be atomic against a
  concurrent write.

- **The commit-log payload writes into a pooled buffer.** `MemoryStream` plus `BinaryWriter` plus a final
  `ToArray` becomes a span writer over a bounded pool, and reducer and event-type names encode straight into
  it. Measured under interval fsync: a hundred-row payload drops from 44,488 B and 7,961 ns to 72 B and
  2,524 ns, and the whole commit allocates 14–20% less at every write-set size. The pool is bounded at
  256 KB rather than `ArrayPool<byte>.Shared`, so a bulk load of large blobs cannot park megabytes of
  retained buffers beside the memory budget this database reports as a computed artifact. The record format
  is byte-for-byte unchanged; logs written by earlier builds read as before.

- **A row is decoded once per fan-out, not once per subscriber.** `RowWire.ToColumns` ran per
  subscription, so a commit on a table with N subscribers built N identical column dictionaries — and N
  copies of the same key — on the engine thread while holding the write lock. Both are computed once per
  op and shared; equal projections converge on one wire-column set at registration so the memo can key on
  reference identity.
- **Delta frames are measured under the write lock and encoded on the sender.** The full MessagePack
  encode used to run on the engine thread, once per connection, before the next reducer could enter.
  `Subscriptions:MaxBufferedBytes` keeps its exact meaning and its default: measuring runs the writer's
  own path against a counting sink, so it cannot drift from what serialization produces.
- **`FilterRange` on a primary key walks the key directory.** It filtered a full merged scan, reading
  every row below the window to discard it — the same defect `ScanKeys` fixed for subscriptions (~3s
  against ~5ms on a 24k-row table of 9KB blobs), which had survived on the reducer-facing side because
  `ScanKeys` had exactly one caller. It now seeks to the low bound and stops at the high one.
- **The merged overlay scan streams.** It built a `SortedDictionary` of the entire store scan whenever
  the write set held any pending op for the table, so a reducer that inserted one row and then took
  `First` read the whole table — and on a paged store faulted all of it in.
- **Index maintenance reads a row once per put, not once per indexed column.** A three-index table paid
  three full deserializes — each re-allocating that row's string and byte columns — on every put and
  every remove.
- **The in-memory store publishes one version per record per table, not one per op.** Every intermediate
  version was structurally shared but never observable, and each cost a path copy of the row map plus one
  of every secondary index.
- **`WriteSet.OpsFor` is indexed by table.** It scanned every staged op to find one table's, on every
  overlay read.
- **Refilled rate-limiter buckets are evicted.** The map held one bucket per (identity, reducer) forever.
  Eviction changes no decision: buckets are created full, so a refilled bucket is indistinguishable from
  one that never existed, and a caller mid-burst is never evicted.
- **The FASTER store composes keys into a reused buffer** instead of allocating a `byte[]` per upsert,
  delete, and read.

- **`1003 SlowReducer` now thresholds on the locked portion, not the total**, at every isolation level,
  and carries `LockedMs` and `Isolation` alongside the existing split (`melange.locked_ms` and
  `melange.isolation` on the span event; `melange.isolation` is also a tag on the `melange.reducer` span
  itself). **For the default `Isolation.Serialized` this changes nothing** — the clock already started
  inside the write lock, so the locked portion and the total are the same interval. It matters for
  snapshot-isolated reducers, whose body blocks nobody: thresholding on the total would page an operator
  about write latency that did not happen. The message text changed to lead with the lock hold; the
  EventId is unchanged, which is the guarantee alerts key on.

- **A scheduled table declaring a `Placement` or `ShardBy` is now compile error `MELANGE0022`**
  instead of having the declaration silently discarded. `[Table(Scheduled = "...", Placement =
  Placement.Partitioned, ShardBy = ...)]` — the natural thing to write after reading
  [docs/CLUSTERING.md](docs/CLUSTERING.md) — compiled clean and meant something else. Scheduled
  tables are always `Local`, which on a per-shard engine already *is* per-shard. The runtime schema
  registration mirrors the check for placements it can distinguish from the default.

## [0.1.0] — 2026-08-03

The first release: the work that landed before the repository was made public.

**Alpha.** The reference-workload port ([phase 11](docs/road-to-0.1/plan-phase-11.md)) is the one
outstanding phase, so no application has yet run on MelangeDB end to end. Everything below is
implemented and tested, but "tested" and "proven in production" are different claims and only the
first one is being made.

### Added

- **Core engine.** Schema model, ordered write sets, transactions with a read overlay
  (read-your-writes inside a reducer with no I/O), durable per-table `[AutoInc]` sequences, and an
  append-only commit log with a configurable fsync policy, per-record CRC, and torn-tail recovery.
  Appliers checkpoint their own LSN independently.
- **Source generator and host integration.** `AddMelangeDb(...)` in any .NET host; tables and
  reducers discovered at compile time; reducers resolved per-call from a DI scope, so
  `IOptionsMonitor<T>` and `ILogger<T>` are constructor-injected. Compile-time diagnostics
  `MELANGE0001`–`MELANGE0019`, including ambient time/randomness, `async` reducers, known I/O types
  in reducer bodies, and unindexed scans over paged tables. Reducer arguments are validated during
  decode — non-finite floats, over-long strings and collections — before a transaction opens.
- **Transport, subscriptions, and the C# client.** Framed MessagePack protocol over WebSockets with
  a versioned handshake and a per-frame channel tag; four query shapes (whole table, equality,
  range, column projection) with live deltas; `Resume` from a last-acked LSN against a named log
  epoch instead of refetching; subscription cost limits enforced before execution; bounded
  per-connection buffering with a `DropAndResync` default. HTTP endpoints for one-shot reducer
  calls, bulk ingestion, ad-hoc SQL, and connect tickets. HTTP/2 WebSockets via `CONNECT`.
- **Identity, auth, and policies.** JWT identity hashed from issuer *and* subject; connect tickets
  for browsers that cannot set headers; mid-session re-authentication that may refresh a token but
  never change identity; row policies composing as a union and column policies composing as an
  intersection, both enforced on the initial set and every delta; `[ServerOnly]` columns that never
  reach the wire; reducer authorization with a startup report of every unpoliced client-callable
  reducer; per-identity rate limits and connection caps.
- **Scheduled and lifecycle reducers.** Timers stored as rows, so scheduling is transactional and
  survives restart; repeating timers derive their next fire and write nothing per fire; explicit
  overrun and catch-up policies; `ClientConnected`/`ClientDisconnected` firing on real sessions
  only, including heartbeat-detected drops.
- **Event bus.** `ctx.Publish` lands events in the write set and delivers them only after the log
  append commits — the transactional outbox, with the log as the outbox. At-least-once, replayable
  from per-subscriber checkpoints, depth-limited against cycles, with retry, backoff, and a
  dead-letter path that never wedges the applier.
- **Durable paged hot store.** `FasterHotStore` over `Microsoft.FASTER.Core`, with declarative
  residency tiers (`Paged` by default, `Resident` opt-in, `Auto` on request), out-of-line blob
  storage, a startup residency report, bulk ingestion, and full snapshots with log truncation that
  respects every applier, live event subscriber, and the resume retention window.
- **Postgres tier and ad-hoc SQL.** `[Table(Tier = StorageTier.Relational)]` tables applied from
  the log with their own checkpoint, so Postgres may lag without blocking anything; additive
  automatic migration with destructive changes refused loudly; aggregates (`COUNT`, `SUM`,
  `GROUP BY`, `DATE_TRUNC`) in owner mode.
- **Clustering.** Four table placements, hub and shard node roles, instancing, spatial sharding, and
  seamless handoff; per-shard commit logs and schedulers; cross-shard interaction as co-location,
  ownership transfer, or saga — never two-phase commit.
- **Typed client bindings.** A language-neutral `melange-schema.json` manifest exported by the
  `melange` CLI from a module assembly or a running dev server, consumed by the same analyzer to
  generate typed rows, refcounted merged caches, index accessors, subscription helpers, and reducer
  stubs — one tree per consuming project. Includes a `Manual` dispatch mode that applies whole
  frames on the host's own thread, for game engines that allow scene mutation only from theirs.
- **Observability.** An `ActivitySource` and `Meter` named `MelangeDB` with no OpenTelemetry package
  dependency in core, plus an optional `MelangeDB.OpenTelemetry` package that registers the signal
  names. The full register is in [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).
