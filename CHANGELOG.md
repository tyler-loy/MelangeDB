# Changelog

All notable changes to MelangeDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the pre-1.0 caveat that **the
public API may break in any release** until 1.0.

All packages ship together at one version; there is no per-package versioning. See
[docs/RELEASING.md](docs/RELEASING.md).

## [Unreleased]

### Fixed

- **A clustered client is no longer dropped about seven minutes after it connects.** The session a
  hub-minted assertion opened on a node expired with the assertion: `Cluster:AssertionTtlSeconds`
  (300 s) plus `Auth:ReauthGraceSeconds` (120 s) after attaching, whatever the client's token said.
  Clients that never re-authenticated were closed as `token_expired` and resynced; the overnight
  soaks showed it as one `token_expired` and one resync per client.
  - **The fix:** the assertion now carries the client token's expiry, and the session lives by it.
    The TTL still bounds how long the assertion itself can be redeemed.
  - **Mixed versions:** an assertion from an older hub carries no token expiry, and the session ends
    with the assertion as before, so a rolling upgrade is never more permissive.
- **A node that commits rarely but in bulk now snapshots, and so truncates its log.** Automatic
  snapshots counted only transactions (`Snapshots:IntervalTransactions`, 100,000). A shard taking
  few, large transactions never reached the count and its log grew without bound; in the overnight
  soak, the shards just outside the world, which take only border-band mirrors, grew their logs by
  several GiB an hour while the busy shards held flat. The new **`Snapshots:IntervalBytes`** (default
  256 MiB, `0` off) also snapshots once that much log has been appended since the last snapshot.
### Changed

- **Node links carry row data as binary, not base64 JSON.** Border batches, replica batches, handoff
  rows, event payloads and bulk rows now travel after a frame's JSON in a binary attachment, encoded
  in one pass into a bounded buffer pool.
  - **Why:** each row went over as base64, serialized twice and staged in the shared `ArrayPool`, whose
    per-thread caches held over 300 MiB under a paging soak, outside every memory budget.
  - **Measured** (`link-bench`, a 500-row batch of 1 KiB rows): 4.5 ms to 0.49 ms a round trip, 4.5 MB
    to 1.1 MB allocated, 763 KB to 520 KB on the wire, and nothing held after a full collection.
  - **Mixed versions:** a node advertises binary frames at authentication and each end switches only
    once the other has said it reads them, so an older hub or node keeps exchanging JSON.
- **A primary key too long for MelangeLog no longer makes the database unstartable (#307).**
  - **What happened:** MelangeLog holds keys of at most 1 KiB. A longer one reached the commit log,
    failed when the store applied it (closing the database to writes), and failed again on every
    replay, so the database could not start.
  - **Fix:** the engine now refuses such a write before anything is appended, as an ordinary
    reducer error. The store declares its limit through the new `IHotStore.MaxKeyBytes`, which is
    unlimited by default.
  - **Recovering a log that already holds such a record:** start once on
    `HotStore:PagedEngine = Faster`, which has no key limit.
- **MelangeLog is the default paged engine** (`HotStore:PagedEngine`). It now compacts its log while
  running, flushes in the background, grows its index incrementally, enforces disk limits, and
  reports itself through statistics, metrics and health checks. On the overnight paging soak its
  log held flat at about 2.55 GiB for 2.7 hours, where FASTER's grew from 1.4 to 22 GiB. `Faster`
  (unmaintained upstream) and `Tsavorite` remain selectable. Switching is a restart: the store is
  rebuilt from the log on every start, so there is nothing to migrate. MelangeLog holds primary
  keys of up to about 1 KiB; the engine refuses longer ones before they reach the log (#307).

### Added

- **`HotStore:NativeDirectory` keeps the paged store's key directory and secondary indexes off
  the managed heap**, in native copy-on-write B+trees (1 KiB pages). On by default; `false` keeps
  the managed trees.
  - **Why:** a paged row kept about 300 bytes and five GC objects resident in persistent managed
    trees, so memory and full-collection marking grew with row count whatever the budget.
  - **Measured** (bake-off, 1M rows + 200k blobs, 1 GiB cap):
    - bookkeeping memory falls from 279 MiB managed to 50 MiB managed plus 39.5 MiB native;
    - rebuild is 40% faster;
    - point reads are 40–70% faster and index window scans 44% faster;
    - updates, deletes and churn are at or above the managed trees (hot updates 234–242k/s
      against 239k/s).
  - **Reclamation:** a superseded version's pages go back as soon as its last reader is done; read
    views and lazy key walks hold their version until they finish.
  - **Reporting:** `HotStoreStatistics.NativeDirectoryBytes` and the
    `melange.store.directory.native_bytes` gauge.
- **The load tool can soak a paged store that really pages.** `--ballast-rows` seeds paged rows
  larger than the memory budget and `--ballast-churn` keeps rewriting them, so a soak exercises
  appends, flushing and compaction; the serve console reports the hot store's log, live and
  reclaimed bytes. `all` now passes `--data` to its serve process. Results:
  [docs/LOAD-TESTING.md](docs/LOAD-TESTING.md#paging-soak--melangelog-against-faster-under-constant-rewrites-2026-11-24).
- **`MelangeDB.LoadTest link-bench`** measures the node link alone: round trip, allocation, wire bytes
  and retained heap per message shape, in both frame formats.
- **`HotStore:PagedEngine` chooses the engine under the paged store.** `Faster` stays the default,
  and two preview engines join it.
  - **Why:** FASTER has been unmaintained upstream since August 2026.
  - **The bake-off:** FASTER, Tsavorite, LMDB, RocksDB, ZoneTree and MelangeLog were compared under a
    1 GiB memory cap ([docs/design/hot-store-bakeoff.md](docs/design/hot-store-bakeoff.md)).
  - **Switching is a restart.** The store is rebuilt from the commit log on every start, so there is
    nothing to migrate in either direction.
  - **Logging:** EventId 1512 names the engine in use.

  `Tsavorite` is microsoft/garnet's maintained fork of FASTER, with the same design.
  - **Packaging:** it is not on NuGet, so its source is vendored, and `MelangeDB.Storage.Faster`
    carries the assembly and Garnet's Linux native devices. That grows the package to about 5.6 MB.
  - **Linux needs libaio** (`libaio1t64` on Debian and Ubuntu). Without it, or in a RID-specific
    publish, the store falls back to a managed device and logs EventId 1513.

  `MelangeLog` is MelangeDB's own append-only log.
  - **Writes:** a record still in memory is updated in place; otherwise the new version is appended
    without reading the old one.
  - **Reads:** records that have left memory are read through memory-mapped segment files, and each
    record carries a checksum.
  - **Measured:** under the cap it matched LMDB on reads (5× FASTER's cold reads) and Tsavorite on
    writes.
  - **Reclaims log space as it runs** (#287): `HotStore:Compaction:Enabled` (default on),
    `TriggerRatio` (default 2: compact while the log is over twice its live bytes) and
    `MaxBytesPerSecond`. A background thread copies the live records out of the segment with the
    least live data, in steps of about 1 MiB under the store lock, and deletes the segment. A store
    rewriting its 1M-row table three times over ends with its log at about twice its live data.
    EventIds 1514–1516 log it; `melange.store.compaction.reclaimed_bytes` and `relocated_bytes`
    count it. FASTER and Tsavorite still do not reclaim.
  - **Flushes in the background:** a flusher thread per log space writes pages to disk as they
    leave the writable part of memory, so a write never waits on disk unless the disk has fallen a
    sixteenth of the memory budget behind. `melange.store.flush.stalls` counts those waits.
  - **Disk limits:** `HotStore:Compaction:MaxLogBytes` (a soft cap on the logs) and
    `MinFreeDiskBytes` (default 1 GiB) make compaction run flat out when crossed, with EventIds 1519
    and 1518. `HotStore:MelangeLog:ReadMode` chooses `Mmap` (default) or `Pread`, under which a disk
    read error is an exception rather than a signal that ends the process.
  - **The index grows incrementally:** a full index no longer rehashes inside one write.
  - **Likely the future default.**
  - **Exact without reads:** its index keys each record by a 128-bit hash and records its size, so a
    delete reads nothing, and the log knows which bytes are live without reading them back.

- **Paged log statistics and instruments.** `HotStoreStatistics.Logs` reports each paged space
  (`main`, `blob`): log against live bytes, in-place against appended writes, memory held, flush
  lag and stalls, segment files, index size and resizes, integrity failures, and compaction's
  steps, lock time and bytes moved. Each is a `melange.store.*` instrument with a `space`
  dimension ([docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)). The gap between `log_bytes` and
  `live_bytes` is the backlog of #287. FASTER and Tsavorite report the log's extent only;
  MelangeLog reports them all.

### Fixed

- **Promoting a paged table to resident left its paged records in the log** until the next restart.
  They are now deleted as the table moves.
- **A shard node now reopens a shard whose engine closed itself to writes** (its hot store failed to
  apply a record, or its commit log was poisoned). The node kept the lease and the hub kept routing
  to a shard that refused every write; now the node closes it and reopens it on the next heartbeat,
  rebuilding it from its log, as a restart would, for that shard alone (EventId 1765).
- **A scan failed whole on one unreadable paged row under MelangeLog.** Its batched read throws on
  a record that fails its checksum; the store now reads that batch again a row at a time and skips
  only the bad row (EventId 1511), as it does for FASTER.
- **A hot store that failed to apply a committed record left the database taking writes.** The
  record was already in the commit log, the caller got a generic error, and the next commit
  replayed into the part-applied store. Now the database closes to writes, as it does when the log
  is poisoned. The caller gets `CommitOutcomeUnknownException` (the write may or may not be
  durable), later writes get `TransientRejectionException`, EventId 1517 logs it at Critical, and
  the `melange-log` health check reports unhealthy so the host is rotated. A restart rebuilds the
  store from the log. This applies to every hot-store engine; MelangeLog reaches it when a disk
  write fails, and it now preallocates segment files so a full disk fails there rather than
  partway through a write.

## [0.4.1] — 2026-11-23

### Fixed

- **A newly opened shard served calls before the hub's `Replicated` rows reached it**
  ([#283](https://github.com/tyler-loy/MelangeDB/issues/283)). A node published a shard as soon as
  recovery finished, and the `Replicated` stream reached it only from the next heartbeat on, from
  the shard's cursor: for a new shard, the whole history. Until then a reducer reading a `Replicated`
  reference table found nothing and refused as if the row did not exist, on the first calls into a new
  shard and on the first handoff into one. The window was a heartbeat plus the catch-up, so slow hosts
  with a large seed hit it most. A shard now pulls its `Replicated` tables up to the hub's head before it
  is published, so being in the load view means it is ready; EventId 1763 logs each catch-up. A hub
  older than its nodes cannot answer the pull, and the node logs EventId 1764 and opens shards as
  before. The marker-row workaround is no longer needed.

## [0.4.0] — 2026-11-22

### Breaking

- **`Public = true` on a scheduled table is a build error** (`MELANGE0032`). A scheduled table is
  always private, so the flag was silently dropped: a client subscribed to it saw no rows, and no
  frame ever carried its `ScheduleAt` column (found writing the client contract, finding 3 in
  [docs/CLIENT-CONFORMANCE.md](docs/CLIENT-CONFORMANCE.md#findings)). Modules ported from
  SpacetimeDB, which allows public timer tables, are the likely case. Drop `Public`, and put the
  state clients need in a public table of its own beside the timer table, created and deleted with
  the timer row.
- **`/melange/sql` writes 64-bit integers and timestamps as JSON strings.** `Int64` and `UInt64`
  values, `Timestamp` microseconds, `COUNT`s, and `MIN`/`MAX`/`DATE_TRUNC` over them are decimal
  strings (`"9007199254740993"`), not numbers; integers of 32 bits and less, floats, and a `SUM`'s
  `numeric` stay numbers. A JavaScript consumer's `JSON.parse` rounded any integer past 2^53 with
  no error, so an id, a counter or a timestamp could come back wrong (finding 5 in
  [docs/CLIENT-CONFORMANCE.md](docs/CLIENT-CONFORMANCE.md#findings)). The MCP `query` tool returns
  the same form. `@melangedb/client`'s `queryTable` reads both forms; a consumer of the raw JSON
  that called `GetInt64()` or treated these as numbers must parse the string.

### Deprecated

- **Subscription parameters outside the client contract's value shapes** (C15.2 in
  [docs/CLIENT-CONFORMANCE.md](docs/CLIENT-CONFORMANCE.md)). The server still coerces a float into
  an integer column, a decimal string into an integer or timestamp, a base64 string into bytes, and
  a hex string into an identity, so these subscriptions work in 0.4. It now logs a warning naming
  the table, column and shape (EventId `1207 ParameterOutsideContract`, once per combination).
  **0.5 refuses them.** The C# and TypeScript clients already send the contract's shapes.

### Added

- **MCP reducer calls on a cluster** (road-to-0.4 phase 29's last deliverable). On a cluster hub,
  `call_reducer` takes a `shard` and the hub sends the call to that shard's node, where it
  commits as the client call it is: the agent's identity, token claims and guest flag travel with
  it, so the reducer's policy and `RateLimit:*` apply there. Node-link error replies now carry the
  call failure's code, so a refusal on the node reaches the agent as `denied`, `rate_limited`,
  `rejected` and so on; a lost link is `outcome_unknown`. A shard that does not exist is refused
  and never created. A call without `shard` on a hub is refused, so hub-site reducers are not
  callable from the agent endpoint on a cluster. On a single node `shard` is ignored.

- **The client conformance contract** (road-to-0.4 phase 23, step 1).
  [docs/CLIENT-CONFORMANCE.md](docs/CLIENT-CONFORMANCE.md) states every rule a MelangeDB client
  must keep as a numbered obligation with the failure it prevents: framing, the handshake, rows and
  masks, keys, the anchor boundary, the merged cache, calls, resume, `overflow_resync`, and the
  subscription parameter map's closed set of value shapes (pinned, not changed). Beside it,
  `conformance/corpus.json` is a decode corpus recorded from a live server by the new
  `tools/MelangeDB.Conformance`, with key, column, and argument vectors; a test re-records it on
  every build and fails when it drifts. Writing it down found four things, listed in the
  document's Findings section.
- **`@melangedb/client`, the TypeScript client** (phase 23, step 4), in `clients/typescript`.
  Browser-first and dependency-free. It provides subscriptions into a merged, refcounted cache;
  reducer and producer calls; resume and resync by diff; recovery from `overflow_resync`; manual
  dispatch with `frameTick()`; automatic reconnect; and a `/melange/sql` reader that keeps
  64-bit integers exact. `melange generate --lang ts` writes its typed bindings, mirroring the
  C# API. It passes the live conformance suite, every scenario the C# client passes, in CI. It
  publishes to npm at the same version as the NuGet packages, from the same release job, by
  trusted publishing ([docs/RELEASING.md](docs/RELEASING.md#npm)).
- **`melange generate --lang cs`** (phase 23, step 3). Client bindings from a manifest without
  a compiler, for engines and builds that cannot run the Roslyn generator. It runs the
  generator's own parser and emitters as a library, so its files are the analyzer's byte for
  byte; a test compares the two for three real manifests. TypeScript (`--lang ts`) arrives with
  the TypeScript client.
- **The live conformance suite** (phase 23, step 2). `MelangeDB.Conformance run -- <adapter>`
  drives any client through a small newline-delimited JSON adapter against a real server. It
  covers the corpus, the encoding vectors, the handshake, calls, the merged cache, re-scoping,
  column masks, the anchor boundary under concurrent writes, resume against the same and a new
  log, backpressure, reauthentication, and the heartbeat. Each scenario names the obligations it
  checks. The C# client's adapter passes it on every build.
- **The cluster metrics the docs listed but nothing emitted** (road-to-0.4 phase 31).
  `melange.handoff.duration` (a completed transfer, start to release), `melange.handoff.failed`
  (`reason`: `aborted` or `unresolved`), `melange.shard.owned` (per node) and
  `melange.shard.span_violations` (by reducer) had been in [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)
  since phase 09 without being implemented; they are now. New beside them,
  `melange.shard.band_overruns` (by table) counts writes refused because an entity outran its handoff.
- **`melange.subscription.rescope.duration`** (road-to-0.4 phase 31): a histogram of how long
  each re-scope held the write lock, by table. A client that moves its area of interest every step
  pays this per step; the load test now reports its share of the lock.
- **A startup warning for workstation GC** (road-to-0.4 phase 31): EventId `1108 WorkstationGc`
  when the host runs workstation GC on four or more logical processors. At 400 players in the load
  test it spent 55% of the time paused for GC (2% under Server GC) and p99 call-to-delta went from
  0.27 s to 15 s. A Web SDK host already runs Server GC; any other host should
  set `<ServerGarbageCollection>true</ServerGarbageCollection>` (now also in the README).

- **`MelangeDB.Mcp`, the agent endpoint** (road-to-0.4 phase 29). A new, optional package:
  `AddMelangeMcp()` and `app.MapMelangeMcp()` put a Model Context Protocol endpoint (stateless
  streamable HTTP, `/melange/mcp`) in the host's own pipeline. Tools: `describe_schema`, `query`
  (ad-hoc SQL under exactly the `/melange/sql` rules for the same caller), `health` and `signals`.
  `call_reducer` exists only for reducers named in code (`AllowReducers`, `AllowReducer(name,
  runAs)`); each call is an ordinary client call, policed and rate-limited, and outside
  Development it also needs `Mcp:AllowReducersInProduction`. The endpoint is off until
  `Mcp:Enabled` (`403 mcp_disabled`) and needs its own role claim, `Mcp:OwnerRole`
  (default `melange-agent`). `melange mcp <url>` in the CLI bridges a stdio-only agent to a
  running host's endpoint. A `melange.mcp.tool` span per call. See [docs/MCP.md](docs/MCP.md)
  and the new THREAT-MODEL section.

- **`MelangeDB.Testing`, the reducer test kit** (road-to-0.4 phase 22). A new package:
  `MelangeWorld.Create(gameAssembly).Build()` runs the game's own tables and reducers in a real
  engine over a throwaway directory, on a `ManualClock` whose due timers fire synchronously before
  `AdvanceBy` returns, so scheduled-reducer tests are deterministic with no sleeps. `Call` goes
  through the production dispatch path and returns the commit; `Of<T>()` decodes its write set
  into the game's row types; `Read` uses the generated accessors; `Rejects` and `Faults` keep a
  deliberate rejection apart from a bug; `Restart` and `RestartAndVerify` rebuild from disk;
  `WithServices` substitutes dependencies; `Engine` is the documented escape hatch. Fsync is on an
  interval by default and `WithDurableCommits()` opts up. No test framework dependency. See
  [docs/TESTING.md](docs/TESTING.md). This repo's own suites now use its clock; the two internal
  copies are gone.

- **Warm replicas** (road-to-0.4 phase 30, steps 2c to 2e). With two or more shard nodes, the hub
  places each shard's replica on a node other than its owner (`Cluster:ShardReplicas`, default 1;
  0 opts out). The replica's node follows the shard's log read-only and keeps its hot store current
  up to the owner's durable LSN, which the owner reports in its heartbeat and the hub relays.
  `melange.cluster.replica.lag` shows how far behind each one is. When an owner dies, the hub
  hands its shard to the replica's node, which **promotes** the replica: the new owner's engine
  starts from the replica's store and replays only the records it had not applied (EventIds 1014,
  1762), where it used to recover the whole snapshot. A replica that cannot stand for the log is
  refused (1015) and the shard recovers cold. `Cluster:ReplicaResidency` (`Owner` by default,
  or `Paged`) sets how a replica holds its tables. Shard hot stores now live in node-local scratch
  (`Cluster:LocalScratchPath`), no longer under the shared shard directory. `IMembershipStore`
  gains `SetReplica`, and the Postgres store a `replica_node` column, added in place.
- **A log segment per ownership term: the storage fence** (road-to-0.4 phase 30, step 2b). A
  cluster shard's log now opens under the shard's fencing token. Opening under a higher term seals
  the previous owner's segment at its last valid record (EventId 1011) and appends to a new file,
  `melange.log.{term}`; opening under a lower one is refused (`StaleTermException`). Nothing past a
  seal is read again, so a previous owner that was only paused can no longer write into the new
  owner's log, whether or not the shared filesystem honours `melange.lock`. A manifest,
  `melange.segments`, lists the segments. Existing logs migrate in place on the first change of
  owner, with no rename; non-cluster engines keep their single `melange.log`. Truncation deletes
  sealed segments whole; backup capture and the restore check read across segments, and archives
  are unchanged.
- **`outcome_unknown`**, a new call error code (road-to-0.4 phase 30, step 2a). A cluster shard
  node now re-checks its lease after a commit is durable and before acknowledging it. If the lease
  ended in between, the shard may already have a new owner that cut the log before the record, so
  the caller is told the outcome is unknown instead of success. It is not `transient`: the write may
  have happened. `MelangeCallException.IsOutcomeUnknown` names it; over HTTP it is a 503 with that
  code. `ICommitGuard` gains `ValidateAcknowledgement`, a default no-op, which the engine calls
  after durability on every commit path.
- **A liveness ping that a starved thread pool cannot delay** (road-to-0.4 phase 30). Shard nodes
  ping the hub every `Cluster:HeartbeatIntervalMs` from a dedicated thread, and the hub answers on
  the node link's reader thread, which is now a dedicated thread too; each acknowledged ping renews
  the node's lease and the hub's deadline for it. Under load the thread-pool heartbeat arrived up to
  4 s late from healthy nodes, which ruled out short failure timeouts. A ping cannot revive a node
  the hub has declared dead; its heartbeat still does.
- **Liveness instruments** (road-to-0.4 phase 30): `melange.cluster.heartbeat.gap` on the hub and
  `melange.cluster.lease.renewal_gap` on shard nodes time how close a healthy cluster comes to its
  failure timeout, and `melange.cluster.nodes.declared_dead` and `melange.cluster.lease.lapses`
  count the outcomes. `ClusterMetrics` carries the same, and the load tool reports them. They are
  what the heartbeat and failure-timeout settings should be chosen from; see OBSERVABILITY.md.
- **`Cluster:FastFailoverOnLinkLoss`** (off by default; road-to-0.4 phase 30, step 1). The hub
  declares a node dead as soon as its node link closes and stays closed for a heartbeat interval
  (EventId 1759), instead of at the failure timeout. A killed process is detected in 1.0–1.2 s, and
  its shards are writable again in 1.9–3.2 s at 100k rows, down from 10–12 s. **Requires shared
  storage that enforces exclusive file locks across hosts**; see CONFIGURATION.md.
- **Clients built against an older schema survive additive server changes** (road-to-0.4 phase 28).
  A typed subscription used to refuse any descriptor that was not identical to its bindings, so
  every added column broke every client not yet rebuilt, which a shipped game client cannot avoid.
  Bindings now map columns by name and skip the ones they predate: an added column at any position,
  or a reorder. An update that only changes such a column fires no `OnUpdate`. A removed, renamed,
  or re-kinded column still refuses, naming it. Producer result records tolerate *appended* fields.
- **Reducer parameters may declare defaults**, as producer parameters already could. A trailing
  defaulted parameter may be left off a call, so a client whose bindings predate it keeps calling.
  A required parameter after a defaulted one is refused at compile time (MELANGE0009).
- **Automatic reconnect**, opt-in through `MelangeClientOptions.Reconnect`. A dropped connection
  is re-dialed with exponential backoff and jitter and resumed through the existing
  `ReconnectAsync` path, so caches, subscriptions, and handlers survive the drop.
  `OnReconnecting`, `OnReconnected`, and `OnReconnectFailed` report it, and `IsReconnecting` says
  it is under way. It stops on dispose, on `MaxAttempts`, or on a refusal retrying cannot fix, and
  it never comes back as a different identity.
- **`MelangeClientOptions.TokenProvider`** supplies the token for every dial, for expiring
  credentials.
- **The load-test tool measures failover and fan-out** (road-to-0.3 phase 20). A `failover`
  subcommand kills a crowded shard's owner the way a crash would, and times detection, reopen,
  first write, and each client's restore; `--hot-store faster` runs it on the FASTER store.
  `--lock-split` and `--movers` report the post-commit (fan-out) share of write-lock time at a fixed
  commit rate. Results are in docs/LOAD-TESTING.md.

### Changed

- **The gateway reaches each shard node over one shared link** (road-to-0.4 phase 31;
  `Cluster:GatewayUpstream`, default `Shared`). Every player's session to a node rides one
  websocket (`{Transport:Path}/mux`) as a stream with its own flow control, instead of a websocket
  per player per node. A handoff now opens a session rather than a connection, so it costs no local
  port; per-player sockets churned one connection per handoff, enough at 1,200 players to hold most
  of a gateway host's ephemeral ports in TIME_WAIT. Sessions authenticate as before, and a slow
  client backs up only its own session. `PerSession` restores the old shape; nodes serve both
  endpoints, so either setting works against any node of this release.
  The link packs whatever is queued into one websocket message (up to 64 KiB) rather than one
  message per envelope, and the gateway keeps `Cluster:GatewayLinksPerNode` (default 4) links to
  each node, sessions taking them in turn: at 1,600 players these took delivery from 398k to about
  740k rows a second and rejected calls from 222,710 to about 200.

- **Shared subscription fan-out** (road-to-0.4 phase 31). Subscriptions that are one query (same
  table, projection, predicate and operands, on a table with no row or column policies) are
  evaluated once per committed row instead of once each, and when two or more share it their row
  ops are encoded once and written into every member's frame. Each connection's send loop is now
  woken once per commit from the thread pool rather than from inside the write lock. No protocol
  change: the bytes on the wire are identical. Under the write lock a subscriber now costs about
  250 ns and 293 bytes per commit in the benchmark, from about 575 ns and 865 bytes.

- **Cluster defaults: `Cluster:HeartbeatIntervalMs` 250 (was 1000), `Cluster:FailureTimeoutMs`
  5000 (was 10000)** (road-to-0.4 phase 30). Chosen from a soak under load: with the liveness ping,
  no healthy node went silent longer than 3.2 s even with the cluster collapsing. A killed shard
  owner's shards are writable again in 5.9–6.2 s (were 10.1–11.8 s), and 1.5–1.6 s with
  `Cluster:FastFailoverOnLinkLoss`. Deployments whose nodes see longer pauses (multi-second
  collections of very large heaps) should set a longer timeout; `melange.cluster.heartbeat.gap`
  shows how close they come.
- **A subscription's initial set is collected off the engine's write lock**
  ([#213](https://github.com/tyler-loy/MelangeDB/issues/213)), for tables without row or column
  policies on a store that can pin a read view (both shipped stores can). The row and byte
  ceilings bound what a subscription returns, not what it reads, so a filtered subscription over a
  large paged table used to hold every commit and the node's heartbeat for its whole scan.
  - The set is now read from a view pinned at head *h1*, while the subscription engine records the
    commits that land after it.
  - Under one short lock hold at head *h2*, those commits are folded into the set, the ceilings are
    re-checked, and the subscription registers.
  - The client receives a set anchored at *h2*, exactly what the locked path would have sent.
  - Tables with policies keep the locked path, because a policy may read other tables, which off
    the lock would be live state rather than the pin.
- **Paged FASTER scans read a block of rows at a time, about 8× faster on a large shard**
  ([#212](https://github.com/tyler-loy/MelangeDB/issues/212)). A paged scan used to read one row
  at a time, each row a synchronous random read of its main record and then its out-of-line
  payloads. Scans now issue the reads for 128 rows, wait once, then do the same for their
  payloads. A snapshot of a 1M-row shard (256 B rows) went from 4–5.5 min to 29–41 s, and a
  300k-row one from 19.4 s to 4.5 s. This covers snapshots, border resets, subscription initial
  sets, and index scans: everything that materializes paged rows. Row order, the directory as the
  source of truth, unreadable-row skipping (EventId 1511), and a read view's pinned values are
  unchanged. Each block holds the store lock for about 2 ms, where a single row used to hold it for
  about 250 µs. A scan whose store is disposed mid-scan can still return the rest of the block it
  had already read before it reports `ObjectDisposedException`.
- **A reducer call with the wrong number of arguments names the problem.** Instead of
  `Expected 3 argument(s); the call carries 2.`, a short call is told which parameters it is
  missing and that only a defaulted parameter can be left off, and a long one is told the
  parameter list. Generated dispatch now calls `ReducerArgsReader.ExpectCount(required, declared,
  names)`, so regenerating a module changes its generated source.
- **`ClientRowShape.Verify` checks readability, not identity.** It used to throw on any difference,
  including a reorder or an added column. It now throws only when the bindings could not read the
  rows.

### Fixed

- **A client resumed across a deploy that changed a table's columns misread its rows.** An
  additive migration keeps the log's epoch, so the resume was accepted, and the client went on
  decoding with the descriptors it held while the server encoded with the new shape: a column
  added mid-table shifted every column after it, with no error (finding 2 in
  [docs/CLIENT-CONFORMANCE.md](docs/CLIENT-CONFORMANCE.md#findings)). The server now refuses a
  resume whose subscriptions read a table whose columns changed after the cursor, and the client
  resyncs with fresh descriptors, as it does for any refused resume. Both clients already handle
  the refusal. A tier or `[Unique]` change alone changes no row and does not refuse.
- **The rebalance loop could move a shard with almost no load.** A move only had to lower the
  pair of nodes' maximum load by any amount, so once two nodes were level, a shard nobody wrote to
  qualified as soon as it read any load at all. Every 10 s a shard's own load sampling holds its
  write lock for a statistics pass, and that reads as a few microseconds of utilization: the loop
  then drained the shard, pausing its players, for a gain of 0.004%. A move must now lower the
  maximum by at least `Cluster:RebalanceMinGain` of the hot node's load (default `0.1`). This
  was the intermittent `RebalanceLoopTests` failure on CI, which only slow runners reached; the
  test now watches past a sampling pass and failed 5 runs in 6 without the fix.
- **A log compaction whose final rename failed left the commit log unusable until restart.**
  The compaction closes the log's file, renames the compacted file over it, and reopens it. If
  the rename threw, the log kept the closed file: appends failed with `ObjectDisposedException`,
  and so did closing the log. On Windows this is a race with any read of the log, not a disk
  fault: a replace is refused while another handle has the file open, and each read opens one. It
  showed up as an intermittent teardown failure in the cluster tests. Now a refused rename keeps
  the old file and the log stays writable. The truncation still stands, since the base sidecar
  already records it, and the next compaction removes the bytes. Warning `1016
  CompactionSwapFailed` reports it. A rename that happened but may not be durable, or a file that
  cannot be reopened, poisons the log as a failed fsync does (Critical `1017
  CompactionSwapPoisoned`). Reads no longer return records at or below the base that are still
  on disk, which a crash between a compaction's two steps could also leave behind.
- **On Windows, a read of the commit log no longer blocks its compaction.** The compaction's
  rename (and every other durable file replace) now uses POSIX semantics, as Linux always had: the
  replaced file's name goes at once, and a reader already holding it open finishes on the old
  file. The rename is still made durable by a write-through flush. Filesystems without POSIX
  renames (FAT, Windows before 10 1709) fall back to the previous replace, where a concurrent
  read makes the rename fail and the log keeps its old file until the next compaction.
- **The gateway could stall a player for tens of seconds around a handoff** (found measuring
  road-to-0.4 phase 31's game shape). Four things, fixed together:
  - Routing a call or a subscription to a shard held the connection's attachment lock while it
    connected to a new owner, and a swap held it while it waited for the destination, so either
    stalled the other for as long as a connect took (29 s seen). Both now connect before taking the
    lock; swaps stay in order through a lock of their own.
  - Mid-transfer, the hub's map names the destination before the swap lands, and a subscription
    routed in that window attached the destination itself, without the subscription replay. The
    swap then found the session "already there" and returned without flushing the calls it held,
    so the player's calls queued until the cap refused them. Routing now waits for the swap, and a
    swap that finds itself already attached still flushes.
  - Sessions pre-opened to neighbouring shards on approach were never closed unless a swap used
    them. At 1,200 players that was ten upstream sockets a player, enough to run the host out of
    ephemeral ports, so connects failed and retried for seconds. One nothing has asked for in 10 s
    is now closed, and a failed one is retried on the next approach.
  - The held-call overflow warning (EventId 1720) is logged once per transfer, not per refused call.

  At 1,200 players rejected calls fell from 44,721 to 11,487 in two minutes and swaps that took
  over 5 s from 236 to 62; at 1,600, rejections fell from 560,585 to 56,300.

- **Handoff refusals that meant "retry" said `internal`** (found measuring road-to-0.4 phase 31's
  game shape). A player who walked past the border band before its handoff landed had the write
  refused as a `ShardSpanException`, logged as a reducer failure and answered `internal`; it is now
  a transient rejection, counted in `ShardRuntime.BandOverruns`. Other writes past the band are
  still span violations. The gateway's held-call cap (256 calls during one transfer) also answered
  `internal`, though nothing had run; it now answers `transient`. At 1,200 players these were
  2,164 `internal` refusals in two minutes.

- **`MelangeClient` recovers from `overflow_resync`** (found writing the phase 23 conformance
  contract). After the server dropped a slow connection's subscriptions, the client raised
  `OnError` and nothing else, so every subscription looked live while its cache stopped changing.
  It now re-subscribes each one at once and reconciles the new initial sets by diff.
- **The cluster gateway decoded, and freshly buffered, every frame it relayed** (found in road-to-0.4
  phase 31). Its upstream session allocated a 64 KiB receive buffer per message a node sent,
  copied it, and decoded every row, although it only forwards a delta's bytes. Under load that was
  most of a cluster's allocation: at 400 subscribers on one shard the serve process spent a quarter
  of its time paused for GC and call-to-delta reached 15 s at p50. It now reuses one buffer and
  relays deltas without decoding them; the same run is at 9.6 ms, delivering every row asked of
  it. The capacity knee in the load test's 4-shard shape moved from about 150 players to about 600.
- **The MCP `signals` tool failed whenever any MelangeDB observable instrument in the process threw**
  when polled, such as a gauge over an engine already disposed whose meter outlived it. It now
  returns every other instrument's reading. Seen as a random `signals` failure in CI.

- **A large border reset took down the node link, and with it a healthy node.** A band reset was
  sent as one node-link frame, and a link drops any frame over 64 MiB. A million-row shard's band
  against a side neighbour came to 152-170 MiB, so the hub's reader closed the owner's link; the
  node's lease lapsed, and with `Cluster:FastFailoverOnLinkLoss` on the hub declared it dead at
  once and moved its shards. Resets now travel in parts of about 16 MiB (`border-reset-part`),
  which the observer stages and applies whole on the last one, so its deletions and cursor move
  only with a complete band. A link also refuses to send a frame over the limit, failing that one
  request rather than writing what its peer would drop the link for. Found by the phase 30
  failover runs at 1M rows.
- **Shards for blocks with a negative x coordinate could not be reached.** Their keys have the top
  bit set, and the shard node's websocket route only matched keys that fit in a signed 64-bit
  integer, so every connection to them got a 404: no player could be served west of x = 0. Under
  load the gateway's pre-opens to such neighbours, retried every 250 ms, exhausted the ephemeral
  port range, and ordinary handoffs then failed to connect.
- **A handoff swap that failed left the client unable to call shard reducers.** The gateway kept
  holding the client's calls for a transfer nothing would end, then refused them at the 256-call
  cap for as long as it stayed connected. The held calls now go to the shard's current owner and
  the client is told to re-establish its subscriptions. A swap whose pre-opened session had failed
  now connects afresh instead of failing.
- **A node is declared dead at its deadline, not up to half a timeout later**
  ([#201](https://github.com/tyler-loy/MelangeDB/issues/201)). The hub checked every `FailureTimeoutMs/2`. It now checks each node's deadline every
  `FailureTimeoutMs/20` (25–250 ms), from memory, and reads the membership store only to record a
  death. Measured at defaults: detection 9.0–9.9 s after a kill, from 10.6–13.4 s.
- **A shard node dying under a forwarded call no longer disconnects the client from the
  gateway.** When a node was killed while the gateway was sending it a client's call, the
  websocket reported the aborted send as `OperationCanceledException`. The gateway read that as
  the client leaving and tore down the client's whole connection, so a client without automatic
  reconnect stayed disconnected. Found by the failover measurement: about one kill in twelve,
  with 4 clients at 15 Hz. The gateway now treats a cancellation as the client leaving only when
  the client connection's own token was cancelled. The call fails on its own and the next one
  re-attaches. An unsubscribe forwarded to an upstream that dies mid-send is answered as for an
  upstream already gone.
- **The shard fence holds without timing luck** (road-to-0.4 phase 30, step 0;
  [design/shard-ha.md](docs/design/shard-ha.md)). Five gaps, each masked today by how slow a
  reassignment is, and each one a warm replica's fast promotion would have exposed:
  - **A node's lease could outlive the hub's death declaration** ([#214](https://github.com/tyler-loy/MelangeDB/issues/214)). It ran from when the
    heartbeat reply arrived, against the node's own timeout. It now runs from when the heartbeat
    was sent, for 90% of the hub's timeout (taken from registration; a mismatch logs 1757), on a
    monotonic clock. The hub measures silence on its own monotonic clock too, so a wall-clock jump
    can't declare a node dead early.
  - **Internal commits ignored the lease** ([#215](https://github.com/tyler-loy/MelangeDB/issues/215)). Replica batches, border copies, and handoff
    markers appended to a shard's log after the lease lapsed. Every origin is fenced now.
  - **An expired lease released nothing** ([#216](https://github.com/tyler-loy/MelangeDB/issues/216)). A partitioned but live owner kept its engines
    open and their log locks held, which blocked the new owner indefinitely on storage that honours
    the lock. The node now parks its shards when the lease lapses (1756), and reopens the ones still
    assigned to it when it registers again.
  - **A failure in the dead-node sweep crashed the hub** ([#217](https://github.com/tyler-loy/MelangeDB/issues/217)). An exception from the
    membership store (a Postgres outage) was unhandled on a timer thread. It's logged (1755) and the
    next tick retries.
  - **The hub routed border batches, border resets, and forwarded events without checking the
    sender** ([#218](https://github.com/tyler-loy/MelangeDB/issues/218)). It now refuses them from a node that doesn't own the source shard (1758).
- **A call to a shard whose engine was just released is refused as retryable, not reported as an
  internal error** ([#202](https://github.com/tyler-loy/MelangeDB/issues/202)). A shard node
  disposes a shard's engine when the shard is released, drained, or the node stops, while client
  calls are still arriving. Each one used to hit the engine's disposed nested-call flag and come back
  as `ObjectDisposedException`: an internal error to the client and an error-level log line (EventId
  1204), 547 of them in one failover trial. Disposal now closes the engine first, and every write
  entry point checks that before anything else, so the caller gets the transient refusal a drained
  engine already gave. A call already past its append when the engine is disposed still reports an
  internal error, because whether it committed is genuinely unknown.
- **The rebalance loop no longer moves a shard onto a node only because it couldn't see that
  node's load** ([#204](https://github.com/tyler-loy/MelangeDB/issues/204)). A shard with no
  heartbeat sample inside `Cluster:RebalanceWindowSeconds` (a late heartbeat, or a shard that had
  just moved in) was left out of its node's total, so the node read as idle and the move rule
  chose it as the target. That moved a hot shard onto an equally hot node. Nodes with a shard of
  unknown load are now skipped as targets until their window fills; a freshly provisioned node with
  no shards is still a target. Found as the flaky scale-in curve test.
- **A border reset no longer freezes its shard, or gets a live node fenced**
  ([#208](https://github.com/tyler-loy/MelangeDB/issues/208)). To rebuild a neighbour's border
  copy (EventId 1715), the owner read every row of the shard under the engine write lock and kept
  the band's. On a paged FASTER shard that is random disk reads per row: about 250 µs a row, so
  minutes at a million rows, with every commit and the node's heartbeat load sample queued behind
  it until the hub declared the node dead. The scan now runs against a read view pinned at the
  reset LSN, with the lock held only to take the pin. The hub's Replicated bootstrap, the same
  whole-table scan under the lock, moved the same way.
- **The load-test tool's call-to-delta latency collected no samples** from protocol v2 (0.2.0) on,
  because the driver matched its own row's `Identity` column as `byte[]`. The smoke test now
  asserts the metric has samples. Results recorded before 2026-08-06 are unaffected.
- **A FASTER store that's been disposed no longer reports its rows as corrupt**
  ([#199](https://github.com/tyler-loy/MelangeDB/issues/199)). A read view that outlived its
  store, for example a snapshot still running when its shard was released, read paged rows after
  the store's `deleteOnClose` files were gone, and was told "out-of-line payload ... is missing" or
  "main record is missing". Scans quietly skipped those rows as unreadable (EventId 1511). Reads on
  a disposed store now throw `ObjectDisposedException`.
- **FASTER `Statistics()` no longer takes the store lock.** The lock is held across each paged
  read, including the disk I/O, so a heartbeat's load sample queued behind a long scan. The
  counters it reports change only while a commit applies, which happens under the engine write
  lock that consistent callers already hold.
- **A client on a shard whose machine died no longer hangs**
  ([#200](https://github.com/tyler-loy/MelangeDB/issues/200)). The gateway's upstream connect had
  no timeout. A dead process refuses connections, but a dead machine answers nothing, so the
  connect waited out the OS TCP timeout (about 21 s on Windows, about two minutes on stock Linux)
  while holding the client's attachment lock, and every shard call that client made waited
  behind it. Each upstream handshake is now bounded (`Cluster:FailureTimeoutMs / 4`, 1–5 s). The
  shard attachment's retry, previously a fixed 20 × 250 ms that on a fast-refusing OS ran out
  before the hub had even declared the node dead, now lasts `2 × Cluster:FailureTimeoutMs`, so a
  call waits out a reassignment instead of failing inside it. Failed handshakes also dispose
  their socket now.
- **A node that takes over a dead node's shard no longer stops heartbeating while it recovers it**
  ([#198](https://github.com/tyler-loy/MelangeDB/issues/198)). The open, a full replay of the
  shard's snapshot and log tail, ran inside the heartbeat loop. So a recovery longer than
  `Cluster:FailureTimeoutMs`, measured at 3–6 s for a 1M-row shard, got the *adopting* node
  declared dead and self-fenced on every shard it owned, turning one node death into a cascade.
  Opens now run on their own task, and the node keeps its heartbeat and lease through them. A failed
  open is retried on the next heartbeat and no longer drops the hub link. `1707 ShardOpened` now
  carries the recovery time; `1753 ShardOpenFailed` and `1754 ShardOpenAbandoned` are new.
- **A shard opened while its node's Replicated stream was running could permanently miss
  Replicated rows.** The stream runs from the node's low-water mark, so a slice in flight when a
  new shard was published started past that shard's cursor. The shard applied it anyway and moved
  its cursor over every earlier record. Each slice now carries the last Replicated LSN before it,
  and a shard that's behind skips the slice until the re-subscribe its own publication triggers.
  (The window existed before; asynchronous opens widened it enough for a test to catch.)
- **`FsyncPolicy = Interval` no longer puts the disk on the commit path.** The interval flusher
  fsynced while holding the log's append lock, so every commit that landed during a flush waited
  for it under the engine's write lock. A consumer game saw one-row commits of 63–206 ms with a
  0.1 ms reducer body, and a flush slower than the interval let timer ticks queue behind each
  other, keeping the lock held almost continuously. The flusher now does what the group flusher has
  done since 0.2: it takes the head under the lock, then fsyncs through its own file handle with the
  lock free. With an 80 ms fsync on a 50 ms interval, append latency went from p50 78 ms / p99
  254 ms to p50 0.05 ms / p99 0.5 ms. Two smaller changes in the same path: a tick with nothing new
  to flush no longer fsyncs (an idle log used to fsync 20 times a second at 50 ms), and a tick that
  finds the previous flush still running skips instead of parking a pool thread. Durability is
  unchanged: the loss window is still the interval plus one flush.
- **Under Manual dispatch, a full resync no longer swallows a queued `OnDisconnected`** that no
  `FrameTick` had delivered yet.

- **A `[Unique]` declared over existing rows is checked against them**
  ([#192](https://github.com/tyler-loy/MelangeDB/issues/192), road-to-0.4 phase 27). Adding
  `[Unique]` was classified as an index change, and indexes are rebuilt projections, so a boot over
  duplicate rows succeeded, the unique index held several rows for one value, and only later
  writes were refused. The boot that adds `[Unique]` now scans the table once and refuses, naming
  each duplicated value and the keys holding it. Nothing is recorded, and the previous declaration
  still boots. The shape sidecar now records each table's unique columns. **On upgrade**, a
  directory whose sidecar predates this checks every `[Unique]` column once, so duplicates an
  earlier build let in refuse that boot rather than staying hidden. See
  [MIGRATION.md](docs/MIGRATION.md#adding-or-removing-unique).
- **Removing `[Unique]` from a table with a relational backplane drops its Postgres unique index.**
  Index DDL was create-if-not-exists by name, so `ux_<table>_<column>` stayed behind and refused
  the applier's first duplicate. It is now dropped on the tier's start, or printed in the refusal's
  DDL when `Postgres:AutoMigrate` is off. A plain index superseded by a new unique one is dropped too.

- **The engine's file swaps survive power loss, not only a crash.** The commit log's compaction
  swap, its base and epoch sidecars, the epoch lineage, the shape sidecar, snapshots, and backup
  archives were written temp-then-rename, which is atomic against a process crash; but on Linux
  and macOS a rename is not durable until its directory is fsynced, and the sidecars' temp files
  were never flushed before the rename. Power loss could revert the compaction swap — orphaning
  appends already acknowledged as durable in the new file — or leave a zero-length shape sidecar
  that refuses the next boot. Renames now fsync the containing directory (Windows renames with
  `MOVEFILE_WRITE_THROUGH`), and every sidecar's contents reach disk before its rename.

## [0.3.1] — 2026-11-16

### Fixed

- **An additive schema change no longer crashes an engine that has never committed** ([#184](https://github.com/tyler-loy/MelangeDB/issues/184)).
  A cluster node holding no partitions has an empty log, so the shape it adopted and the
  migration's marker record both started at LSN 1, and the boot threw `Shape entry from LSN 1
  does not follow the current entry's 1` — every schema change forced a wipe of the nodes'
  stores. A reign that governs no record now has its sidecar entry rewritten in place, with no
  marker and no snapshot, so the engine stays fresh and its init reducers still run.

## [0.3.0] — 2026-10-23

### Breaking

- **`StorageTier.Relational` is renamed `StorageTier.HotWithRelationalBackplane`.** The old name
  described where people assumed the rows lived, not where they do: a relational table's rows live
  in the hot store, reducers read them there, and Postgres holds a copy the applier projects from
  the log. Phase 25's Postgres-only archive tier would have made the old name actively misleading,
  so it goes first. **The break is source-only.** The value stays `1`, so nothing on disk changes —
  no hot-store rebuild, no Postgres DDL, no shape-sidecar change, and the applier's checkpoint is
  untouched. `StorageTier.Relational` remains as an `[Obsolete(error: true)]` alias whose message
  names the replacement, so an upgrade fails at compile time with the fix in the error. Also renamed:
  `ServerSubscription.Compile`'s `allowPrivateRelational` parameter (now `allowPrivateBackplane`),
  and "relational tier" in the docs, XML docs and log and error *messages*. Deliberately **not**
  renamed: the types named for Postgres (`PostgresRelationalTier`, `IRelationalQueryExecutor`,
  `RelationalAggregateQuery`), the wire error codes (`not_relational`, `no_relational_tier`,
  `relational_unavailable`), the `Postgres:*` configuration keys, and EventIds and their names —
  each is a contract something outside this repo matches on. See
  [docs/MIGRATION.md](docs/MIGRATION.md#upgrading-to-03-the-relational-backplane-rename) and
  [plan-phase-25.md](docs/road-to-0.3/plan-phase-25.md).

### Added

- **The shape sidecar records each table's tier.** Where a table's rows live is part of what a data
  directory holds, and phase 25's Postgres-only archive tier will make some tier changes lose data,
  so a changed tier is now detected at boot like any other shape change. Between `Hot` and
  `HotWithRelationalBackplane` the change is additive: a migration boot, EventId 1006 naming it. A
  recorded tier this build doesn't know (written by a newer one) refuses boot. A 0.2 sidecar has its
  tiers filled in place on the first boot (EventId 1009), without a migration. Tiers are persisted
  by explicit name, never by the enum's own formatting, because `Relational` and
  `HotWithRelationalBackplane` share a value and which aliased name `ToString()` returns is
  unspecified.

- **The archive tier: `StorageTier.Archive`, tables that live only in Postgres.** For unbounded
  history, such as a wallet ledger whose recent rows stay in a hot table and whose old ones move to
  an archive with a reducer (`Insert` into the archive, `Delete` from the hot table, one atomic write
  set). Writes ride the commit log and the Postgres applier projects them; the engine strips them
  from every record before the hot store sees it, through commit and recovery alike, so they are in
  no hot store, snapshot or `.mbak`, and that holds for any `IHotStore`. Reducers can `Insert` and
  blind-`Delete` by key; nothing that reads is generated, and the reflection path refuses reads with
  the reason. AutoInc ids stay unique across restarts. MELANGE0024–0026 refuse a Public archive
  table, `[Unique]` on one, and Scheduled, Replicated or a declared Residency. An archive table
  without `Postgres:ConnectionString` refuses to start, every tier change into or out of `Archive`
  refuses boot with the supported path named, shard and node-local engines refuse archive writes
  (declare them `Placement.Global`), and an applier that would have to rebuild over a truncated log
  refuses rather than begin an archive with a hole (EventId 1611). Ad-hoc aggregates run over
  archive tables. Each archive row carries the `_melange_lsn` and `_melange_epoch` it was committed
  at. An insert whose primary key Postgres already holds is never an overwrite and never a stall: it
  is dead-lettered into `__melange_archive_dead_letter` in the batch's own transaction (values kept
  as `row_values` jsonb), logged at Error by key and reducer but never by value (EventId 1612),
  counted (`melange.archive.dead_lettered`), and reported as a Degraded `melange-applier` until the
  table is cleared — through the new `IApplierCondition`, which any decoupled applier can implement.
  Back up Postgres with point-in-time recovery; see [docs/BACKUP.md](docs/BACKUP.md).

- **`KeepKeys`: an archive table's keys kept in the hot store.** Opt-in per archive table
  (`[Table(Tier = StorageTier.Archive, KeepKeys = true)]`, off by default). The engine maintains a
  key-only hot table, `__keys_<Table>`, from the archive's ops; the generated accessor gains
  `Contains(key)`, so a reducer can check an idempotency key that has left the hot window, and an
  insert refuses an already-archived key before commit instead of it being dead-lettered after.
  `IDbView.ContainsKey<TRow>` is the untyped form, a default interface method. Must be declared when
  the table is created; turning it on later refuses boot, turning it off doesn't. MELANGE0027 refuses
  it on any other tier.

- **A wallet sample, `samples/MelangeDB.Sample.Wallet`: the archive tier's worked example.** A hot
  balance, a hot recent ledger keyed by the caller's idempotency key, a `KeepKeys` archive, a
  scheduled sweep that moves entries past the hot window (an index range on `At`, one atomic write set
  per batch), a credit/debit reducer that stays idempotent across both, and a `History` producer that
  pages the caller's own history — hot ledger and archive, the archive read as of the producer's
  pinned LSN so an entry the sweep moves mid-call is listed once — behind a players-only policy.
  Needs Postgres.

- **`IArchiveReader`: reading archive history outside a reducer.** Registered by `AddPostgres`.
  Keyset-paginated pages of an archive table, ordered by primary key (ascending or newest first),
  filtered on the key or one indexed column by value or inclusive range, values in the columns' own
  CLR types. Each page reads Postgres and the applier's checkpoint in one repeatable-read transaction,
  then overlays the unapplied log tail for that table, so a row that just left the hot store is never
  briefly invisible; past 50,000 unapplied records the overlay is skipped and the page says `Stale`.

- **`Postgres:ArchiveLogRetentionHours`, the archive retention floor.** Off by default. While archive
  tables are declared, the log keeps the records committed within the window — a named truncation
  floor, `archive-retention`, searched by commit timestamp like the resume window — so a Postgres
  restored from a plain dump taken inside it catches its archive up from the log. Outside it, the
  refusal names the lost LSN range and the setting (EventId 1611). For deployments without
  point-in-time recovery; set it to at least the dump interval.

- **Restores record their lineage, and the Postgres projection is reconciled after one.** A restored
  directory's `melange.lineage` names the source epoch and the LSN it forked at, plus that epoch's own
  ancestors (carried in the archive's per-engine identity as an optional field, so the archive format
  is unchanged and older archives still restore). On first boot, a Postgres checkpoint from an
  ancestor epoch is reconciled to the restored history instead of refused: archive rows outside it
  move to `__melange_archive_rewound` (not deleted), backplane tables that ran past the fork are
  re-copied from the hot store, and the applier resumes from the fork (EventId 1613). A checkpoint
  from an epoch the lineage doesn't name — another branch — is still refused (1605). A reconcile
  whose resume point is below the restored log's base refuses for archive tables (1611), and a
  clone starts its archive without the source's history (1614). **Behavior change:** a restore of
  a backplane-only deployment used to stop at EventId 1605 until the schema was cleared; it now
  reconciles on its own. Clearing the schema still works.

- **Producers: `[Producer]` methods, typed read-only calls a client makes for data** (road-to-0.3
  phase 26, [design/producers.md](docs/design/producers.md)). A producer is
  `T Name(ProducerContext ctx, ...)`, `Task<T>`, or `ValueTask<T>`: it writes nothing, so it may be
  async and await I/O, and it returns a typed value — `List<WalletLine> History(int page = 1)`. The
  generator discovers producers beside reducers (register them with `AddProducersFrom`), decodes
  their parameters as reducer arguments with trailing defaults a caller may leave off, and emits an
  encoder for the result: one tagged value in the reducer-argument encoding, a record as an array of
  its public fields and properties in declaration order. Results may be scalars, strings, `byte[]`,
  `Identity`, `Timestamp`, enums, nullable values, arrays and lists, and records, classes, or
  structs of those, recursively. MELANGE0028 refuses a signature the dispatcher cannot call,
  MELANGE0029 a result it cannot encode — a `[Table]` struct included, so a table's shape reaches a
  client only through a type its author wrote — naming the member path, and MELANGE0030 a producer
  sharing its name with a reducer or another producer.

  `MelangeProducerHost.CallAsync` runs a call: arguments validated, a client call rate limited
  (`RateLimit:*`) and authorized (the producer's `IReducerPolicy`, or `Policies:DefaultReducerPosture`)
  before anything is pinned; then one read view is pinned for the whole call (`ctx.Db`, `ctx.Lsn`),
  the body runs under `Producers:TimeoutSeconds`, the result is encoded under
  `Producers:MaxResponseBytes` while the pin is still held, and the answer waits for the log to be
  durable through the pinned LSN, as a reducer's acknowledgement does. A producer that invokes a
  reducer is refused. The unpoliced report lists producers (EventId 1107), and a hot store without
  pinned reads refuses to start once any producer is registered. Telemetry: the `melange.producer`
  span and `melange.producer.calls`, `.duration`, and `.pin.duration`.

  On the wire, producers are **protocol version 3**: `CallProducer` (17) and `ProducerResult` (18)
  on the calls channel, with error codes `unknown_producer` and `timeout` beside the reducer ones.
  The server negotiates 2–3 and serves a version-2 client exactly as before, refusing it only if it
  sends a producer frame; `MelangeClient` offers 2–3 and gains `CallProducerAsync`, whose result
  `ProducerResultReader` decodes. The socket runs calls off the read loop, cancels them when the
  connection closes, and holds at most `Producers:MaxConcurrentPerConnection` at once (EventId 1206
  for an unexpected failure). Over HTTP, `POST {path}/producers/{name}` takes a JSON array of
  arguments and answers `{"ok", "lsn", "result"}`, the result rendered as JSON under the response
  cap while the read view is still pinned. In a cluster, producers run where the reducer rules put
  them — the hub for `Global`/`Replicated` reads, the caller's shard otherwise, `[Producer(Site = ...)]`
  to override — and the gateway, now on version 3, routes and handoff-holds them as it does reducer
  calls; every shard engine gets its own producer host.

  The schema manifest carries producers — **format 2**, written only by a module that has them, so
  every other module's manifest and schema hash are unchanged — with each producer's parameters
  (defaults included) and result as a type reference, and each result type's fields in wire order.
  The client generator re-declares result types as records and emits `conn.Producers.<Name>Async`,
  whose defaults are the server method's and whose decoders read the result positionally.
  MELANGE0031 refuses a result type whose client name another result type, a public table's row
  type, an enum, or a scalar kind already has.

- **`MelangeEngine.OpenReadView()` — committed state pinned at one LSN, readable off the write
  lock.** A `PinnedReadView` is a read-only `IDbView` with an `Lsn`, safe to read from several
  threads (each read and each step of a lazy query runs under its own lock), that throws on a read
  after disposal rather than reading a released pin. `SupportsReadViews` says whether the store can
  pin, and `ReadCommitted(db => ...)` runs a synchronous read through a pin or, without one, under
  the write lock. It is the read scope phase 26's producers run in.

- **`ArchiveQuery.AsOfLsn` — read the archive as of a pinned view's LSN.** Postgres rows committed
  after it and the overlaid log tail beyond it are left out, so a reader that merges hot and archive
  rows sees a row the sweep moved in between exactly once, rather than twice (archive read later) or
  not at all (archive read first). The read waits for the log to be durable through that LSN, since
  the overlay reads only durable records. Archive deletes carry no LSN, so a delete committed after
  it and already applied shows early.

### Fixed

- **A table switched from `Hot` to the relational backplane now gets its existing rows in
  Postgres.** The applier skipped the table's ops while it was hot, so its checkpoint was already
  past the table's history: the Postgres table was created empty, and only rows written after the
  change ever reached it. The hot store still held everything, so nothing was lost, but the copy
  was silently incomplete. The applier now keeps a projected-tables registry
  (`__melange_projected`) beside its checkpoint and, on start, fills any backplane table missing
  from it from the hot store at one consistent LSN before resuming (EventId 1609). A table that
  leaves the backplane leaves the registry (EventId 1610), so moving it back copies it afresh
  instead of resuming a stale copy. A 0.2 projection seeds the registry from the tables Postgres
  already holds. See [docs/MIGRATION.md](docs/MIGRATION.md#changing-a-tables-tier).

- **A snapshot-isolated reducer on the in-memory store could read a commit half applied.**
  `IReadViewSource` requires the pinned view to be opened while no apply runs, and the engine opened
  it without the write lock. A commit writing to two tables could land between the two captures, so
  the body saw the first table after the commit and the second before it. The FASTER store takes its
  own lock and was not affected. The view is now opened under the write lock for the instant the
  capture takes (references only).

- **Reducer policies, and the cluster gateway's shard strategy, read the live store off the write
  lock.** `MayCall` runs before its transaction opens, and `IShardStrategy.ShardForSession` runs on
  the gateway, both against `CommittedView`, which the `IHotStore` contract allows only on paths the
  engine serializes. Both now read through `MelangeEngine.ReadCommitted`: a pinned view, or the write
  lock on a store without one.

## [0.2.3] — 2026-10-19

### Added

- **`ReducerContext.Claims` — reducers can read allow-listed claims from the caller's token**
  ([#152](https://github.com/tyler-loy/MelangeDB/issues/152)). Validation built a full `ClaimsIdentity`, read
  the four roles whose *names* are configuration off it, and dropped the rest: anything else the IdP put on the
  token was invisible to the application. Since MelangeDB deliberately owns no issuer or audience settings —
  the IdP is the gate — an external OIDC provider is precisely the thing most likely to carry
  application-specific claims, which is what made the gap bite.

  `Auth:CaptureClaims` names the claim types to keep. They reach a reducer as `ReducerContext.Claims`,
  available from `ClientConnected` onward — where a workload first learns a session exists — and identically
  however the token was presented: as an `Authorization` header, in the `Hello` frame, or behind a connect
  ticket. The ticket path matters most, because a browser's WebSocket API cannot set headers, and host
  middleware reading claims off the upgrade request covers only the header case.

  **Clustered deployments carry them too.** A gateway vouches for a client to a shard node with an internal
  identity assertion rather than the client's JWT, so captured claims now ride inside that signed payload —
  additive and fail-closed like the capability flags before them, so an assertion minted by an older node
  deserializes to no claims. Without this, capture would have worked on a single node and silently produced
  nothing the moment a deployment clustered.

  An allow-list rather than the whole principal: claims are connection-scoped state held for a session's life,
  stored against a ticket, and carried on every gateway hop, so an unbounded `ClaimsPrincipal` would make a
  large token a per-connection cost nobody declared. A repeated claim — `role` and `groups` routinely repeat —
  keeps every value through `GetValues`, while the indexer answers the first, which is what a single-valued
  claim like an account id wants. Nothing is captured until a host names it.

### Fixed

- **`ctx.Random` draws the same numbers in every process, and different numbers for every transaction**
  ([#156](https://github.com/tyler-loy/MelangeDB/issues/156)). It was seeded from
  `(int)micros ^ caller.GetHashCode()`, and `Identity.GetHashCode` is `HashCode.Combine`, which .NET randomizes
  per process. So the same call against a frozen test clock rolled different dice in every test run — an
  RNG-dependent balance test failed about one run in sixteen — while two calls by one caller in one microsecond
  rolled *identical* dice, which cost a store sale whose idempotency key came from `ctx.Random`. The `int` cast
  also discarded the timestamp's high half.

  The default is now `SeededReducerRandomProvider`: SplitMix64 behind the `Random` surface, seeded from a
  stable 64-bit hash of the caller's bytes, the full timestamp, and the engine's **attempt sequence** — a
  per-engine counter that aborted attempts also consume, so a retry draws anew. A 64-bit generator rather than
  `new Random(int)`, because 32 bits of seed collide every ~77,000 transactions. The LSN was the obvious
  discriminator and is not available: it is assigned at append, after the body runs, and a snapshot-isolated
  body holds no lock to reserve one under.

  **`IReducerRandomProvider` is new, and resolved from DI.** Registering one replaces the default for every
  reducer, and it is handed the reducer name, caller, timestamp, and sequence — so a test can force a crit
  through real dispatch rather than hunting for a seed. `ctx.Random` stays a `Random`; no module code changes.

  **Behaviour change:** every reducer's random outcomes differ from before. Tests that pinned specific rolls
  under the old seeding — which could only have passed by luck of the process's hash seed — will see new
  numbers. The generator is not cryptographic; a key handed to another system should not come from it.

- **A 60 Hz scheduled reducer runs at 60 Hz** ([#154](https://github.com/tyler-loy/MelangeDB/issues/154)). A
  repeating timer at `ScheduleAt.Interval(TimeSpan.FromSeconds(1.0 / 60.0))` overran on every other fire even
  when its body and commit took a millisecond. The overrun was invented by the scheduler after the tick returned,
  and charged to the reducer.

  The re-arm floored any positive delay at 16 ms, "the platform timer's practical resolution", so a 1 ms tick
  out of a 16.67 ms interval left a 15.67 ms residual that was rounded up to 16 — a wake already past the next
  due time. And 16 ms sits one tick *above* the 15.625 ms Windows quantum, so every floored re-arm waited two:
  measured on Windows 11, requesting 16 ms returns a median 30.68 ms. The loop ran at 32.6 Hz with 81 overruns
  in 163 fires.

  The floor existed to stop a real spin — an early wake finding nothing due and re-arming on an ever-smaller
  residual — and a **fire tolerance** now ends that loop instead: an entry due within 2 ms counts as due and
  fires, and a residual that small is armed as zero. The same replay runs at 59.9 Hz with no overruns.

  **Behaviour change:** a fire, one-shot `ScheduleAt.Instant` included, may now land up to 2 ms early.
  Rescheduling stays anchored to the entry's due time, so nothing drifts. The practical minimum interval is one
  platform quantum (15.625 ms on Windows), which `docs/CONFIGURATION.md` now states.

- **`1003 SlowReducer` no longer reports a number that did not cross the threshold**
  ([#150](https://github.com/tyler-loy/MelangeDB/issues/150)). The line read *"held the write lock 48.9ms,
  over the Telemetry:SlowReducerMs threshold of 50ms"* — 48.9 is not over 50, and a warning that contradicts
  itself gets the measurement distrusted rather than the code.

  Two call sites reached one message. The serialized path fires on the **total** and the snapshot path on the
  **locked portion**, and the shared message asserted the locked portion had crossed — true on the snapshot
  path, false on the serialized one. It rested on an invariant stated in the code, that the two are "equal by
  construction" under `Isolation.Serialized`; they are not, and have not been since phase 17 moved the
  durability wait outside the write lock precisely so the next transaction's body can run while this one
  waits. The gap between the two numbers *is* that wait.

  The asymmetry is kept, because each level fires on the only number that makes sense for it: a serialized
  body ran under the lock, so a stalled disk behind a trivial body still warns — telling a wide body from a
  slow disk is what this warning is for — while a snapshot body ran outside it, and thresholding a 500 ms
  snapshot body would defeat the isolation level. What changes is that the line now names which measure
  crossed, as a structured `FiredMeasure` field (`melange.fired_measure` on the span event) alongside both
  numbers, and the label travels from the comparison that produced it rather than being inferred. **No
  transaction that warned before stops warning, and none starts.** The docs carried the same stale invariant
  in three places and are corrected with it.


## [0.2.2] — 2026-08-26

### Added

- **Every HTTP error carries an id the caller can quote** ([#136](https://github.com/tyler-loy/MelangeDB/issues/136)).
  A player whose connect failed could report "couldn't connect around nine" and nothing in that report found the
  server's side of the request — the server was tracing the whole time, so the id existed and simply never
  reached the caller. Error bodies now carry `traceId` (`Activity.Current`, where the host configured tracing)
  and `requestId` (`HttpContext.TraceIdentifier`, always), and stamp the same values on `X-Trace-Id` /
  `X-Request-Id` for a caller that never reads the body. A host whose own middleware already stamps those names
  keeps its values; MelangeDB fills the gap rather than overwriting.

  On the client, the connect ticket — the step ahead of the socket, and the one a player hits first — no longer
  swallows the response. `MelangeCallException` gains `Status`, `ResponseHeaders`, and `Reference`, the id worth
  putting on a boot screen; `Reference` is also interpolated into `Message` ahead of the response body, so a
  client that only logs the message keeps the id through a truncated log line. A ticket step that gets a success
  status with a body that is not the ticket JSON — a captive portal, a proxy's own page — now fails as the ticket
  step failing, with the same id, instead of surfacing a raw `JsonException` from inside the SDK. All three
  members are additive and null on socket-frame failures. See
  [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md#correlating-a-failure-a-user-reports).

- **`IHotStore.ContainsKey(table, key)`** — a key-directory existence probe that reads no row. The
  follow-up to [#137](https://github.com/tyler-loy/MelangeDB/issues/137): an existence check used
  `TryGetRow`, which decodes the whole row, so a row that is present but whose stored projection
  cannot be read (a corrupt out-of-line blob) made the check throw — and every write path probes
  existence first (`Insert`/`Update`/`Delete`, bulk staging), so a single poisoned row wedged the
  whole table until a restart. `TransactionDb.Exists` and bulk staging now use `ContainsKey`, and
  the FASTER store answers it from the directory (paged) or the resident map without touching the
  out-of-line payload.

### Fixed

- **Shutdown's drain is now a one-way close, so the LSN the host announces is the last one**
  ([#145](https://github.com/tyler-loy/MelangeDB/issues/145)). `MelangeEngine.Drain()` took the write
  lock and released it — a barrier, which establishes that nothing is being written *at that instant*
  and says nothing about a caller already past every check and merely queued on the lock. A scheduled
  fire clears its own stopping check, then creates a DI scope and resolves its reducer before it ever
  reaches the lock; a shutdown that runs `Drain` → `Checkpoint` → `1102 MelangeStopped` inside that
  window leaves the fire to commit *after* the announced LSN. That is the two unaccounted LSNs in the
  report — a repeating fire commits its own writes plus the timer-row reschedule.

  Three changes. The drain now closes the engine to writes, and a write arriving after it is refused
  with `TransientRejectionException` — reported to callers as `transient`, whose contract is already
  "retry unchanged", which is exactly right for a node on its way down. `MelangeScheduler.Stop()`
  waits out a fire already running on the timer's thread instead of only setting a flag and disposing
  the timer, so that fire finishes and is *counted* in the announced LSN rather than refused. And
  `MelangeEventBus.Stop()` no longer discards the result of its ten-second `Task.WaitAll`, which had
  made a timeout silent while dispatch loops stayed alive with handlers free to call reducers. Both
  waits are bounded at ten seconds and say so when they expire
  (`1313 SchedulerStopAbandonedFire`, `1405 EventDispatchLoopsStillRunning`); neither is configurable,
  because the numbers only matter on a path already going down. Reads are untouched — checkpointing
  runs after the drain and has to read.

  This is filed against the SIGSEGV report because it is the mechanism behind that issue's clearest
  lead, and because the engine is a DI singleton disposed *after* every hosted service stops: without
  the gate a writer can still be inside the hot store when the store's teardown begins, which for a
  native store is a crash rather than an exception. **It is not confirmed as the cause of the
  crash** — that still wants a backtrace — but a commit landing after "flushed at LSN N" makes that
  line false on its own terms. See [docs/DESIGN.md](docs/DESIGN.md#shutdown-the-announced-lsn-is-the-last-one).

- **The scheduler's dispatch closes a re-entrancy race that could drop a re-arm.** Dispatch runs one
  drain at a time behind a guard; a call that arrived while a drain was already running returned
  having armed nothing. Whether the in-flight drain's own re-arm then covered that dropped request
  depended on the interleaving — a world committing regularly re-armed on the next commit's
  timer-observer anyway — but the race is real. The guard now records the request and the in-flight
  drain honours it (a rescan flag re-checked after each drain), so a re-entrant request is never
  lost. The drain also logs a start summary (EventId 1303) and its registration and re-arm decisions
  at Debug (1304–1309). (Found while investigating a reported scheduled-reducer stall on the
  reference workload that turned out to be a measurement artifact — the scheduler was dispatching at
  rate the whole time — so this is latent-correctness hardening, not an observed outage.)

- **A scheduler tick's telemetry can no longer break dispatch, and a faulting drain can no longer
  crash the process.** Every `StartSchedulerTick` / `RecordSchedulerTick` / `RecordSchedulerOverrun`
  / span dispose in the fire path is contained (EventId 1312 on fault): the reschedule is downstream
  of `RecordSchedulerTick`, so a metric or span export that threw would skip the timer's reschedule
  and re-arm — telemetry must never affect dispatch. And a drain that throws for any reason is now
  caught, logged (1311), and the timer re-armed regardless, which **prevents a process crash**: the
  timer callback is a bare delegate with no handler of its own, so an unhandled exception in a drain
  would otherwise terminate the process (EventId list: 1310 re-entry, 1311 drain fault, 1312
  telemetry fault).

- **The scheduler no longer spins re-arming its timer.** A platform timer routinely wakes a
  fraction of a millisecond before the delay it was armed with; the dispatch drain then found
  nothing due (the entry's time had not quite arrived) and re-armed on the tiny remainder — waking
  early again on a smaller one, tens of times a second, until the due time finally passed. `Rearm`
  now floors a positive re-arm delay at the timer's resolution (16ms), so a sub-resolution remainder
  is never armed and the next wake lands at or after the due time and fires. A fire is at most one
  resolution late, which is nothing against any real interval. Found by instrumenting a real
  migrated store; the scheduler now also logs a start summary (EventId 1303) and, at Debug, its
  per-table registration and per-fire/drain/re-arm decisions (1304–1308).

- `MelangeScheduler.ReadScheduleAt`'s column-kind walk now throws on an unhandled kind rather than
  silently failing to advance the reader — which would mis-time every timer on that table with no
  error. Safe today (all kinds handled); a guard against a future one.

- **A poisoned row no longer wedges the reducers and subscriptions on its table.** Beyond the
  existence probe above, the two remaining places that read a row's bytes on behalf of *others*
  now tolerate one that cannot be read: a subscription's initial-set materialization for a
  primary-key equality or range predicate skips an unreadable row (the gap the read-side #137 fix
  left — it covered full and index scans but not the point-read-per-key path), and the commit
  fan-out treats an unreadable pre-image as "no prior row" so a write that repairs or removes the
  poisoned row commits (reaching subscribers as an insert of the corrected row, or a delete). With
  these, a scheduled sweep that rewrites the row heals it live rather than failing on it — the
  world-wide wedge the reference workload hit off one `GrowFlora` row. Point reads (`Find`, HTTP)
  still surface the fault by design; a caller asking for one row by key is owed the truth about it.

- **A shrinking overwrite of an out-of-line blob no longer leaves a stale, longer tail** — the root
  cause of [#137](https://github.com/tyler-loy/MelangeDB/issues/137). The FASTER store's in-place
  writer (`StoreFunctions.ConcurrentWriter`) copied the new value over the old but did not shrink
  the record's serialized length, so a `byte[]` column written large and then rewritten smaller —
  the reference workload's per-chunk blob, folded to fewer records by its periodic sweep — read back
  at the *old* length with a stale tail. The out-of-line payload then no longer matched the length
  the main record declared (a blob at 339 bytes rewritten to 315 read back as 339), and every scan
  of that row threw. The writer now shrinks the slot to the new length in place, or sends an
  oversized value to a fresh record; a value's stored length always equals what was written. Because
  the hot store is rebuilt from the commit log on every start, the corrected write path also
  **heals an already-corrupt row on the next restart** — replay re-runs the shrinking overwrite
  correctly.

  This corrects the record on the read-side fix shipped alongside it (below): the disagreement was
  *not* unproducible through the public API, and it *did* survive a restart, because replay re-ran
  the same faulty in-place shrink. A peer's `Insert(339)`→`Update(315)` reproduction against that
  build is what found it. With the write path fixed, the state is genuinely no longer reachable
  through the store — which is why `UnreadableBlobRowTests` must force it through a test seam.

- **One unreadable row no longer takes down every client that scans it** (the read-side half of
  [#137](https://github.com/tyler-loy/MelangeDB/issues/137)). Even with the write path fixed, a row
  that is unreadable for any reason must not be a server-wide outage. A paged row whose out-of-line
  payload disagrees with its main record threw `InvalidDataException` out of a subscription's
  initial-set scan (`FasterHotStore.Scan` / `ScanIndexRange`); the exception escaped the subscribe
  handler, Kestrel logged an unhandled error, and the socket died — so every client whose first
  subscription scanned that row failed to connect. A scan now skips such a row and logs it with its
  key (EventId 1511 `UnreadableRowSkipped`) instead of failing; a point read of the key still
  surfaces the fault, now carrying the key and both lengths, so one row can be found and repaired
  without bisecting the table.

## [0.2.1] — 2026-08-22

### Breaking

- **`/melange/bulk` answers a per-engine results array instead of a single `lsn`.** The body is now
  `{"ok", "rows", "results": [{"shard", "lsn", "rows"}]}`. An LSN is meaningful only within one
  log, so a batch that spans shards has no single LSN to report; the array is the shape that
  survives that. On a single node — and for the `Global`, `Replicated` and `Local` share of any
  batch — it is one element with `shard` null, because a node-local engine is not a shard's; a
  batch fanned out by a hub ([#115](https://github.com/tyler-loy/MelangeDB/issues/115), below)
  reports one element per receiving shard. The array shape landed first so that the fan-out added
  elements rather than breaking every caller a second time. The atomicity guarantee is stated as
  **per engine** rather than per batch — a single-node deployment has one engine, so nothing
  weaker is promised there than before.

### Added

- **Bulk ingestion has a cluster path: a hub fans a batch out to the shards that own its rows**,
  closing [#115](https://github.com/tyler-loy/MelangeDB/issues/115). One batch, one endpoint, no
  topology in the loader — the hub groups by `IShardStrategy.ShardForRow`, keeps `Global`,
  `Replicated`, and `Local` rows on its own engine, and forwards each group to its owner. Unlike
  ad-hoc SQL on a `Partitioned` table (#114), this could not be answered by refusing: refusing
  leaves a clustered deployment seeding worlds through routed reducer calls, forfeiting bulk's
  measured 44× advantage on every bake.

  `[AutoInc]` is allocated by the **owning shard**, never the hub, so ids keep their originator
  prefix and two shards allocating "the first row" cannot collide. Each receiving shard
  **re-resolves every row and refuses the group on a mismatch** — the shard-span check looks like
  it already covers this, but `Cluster:ShardSpanCheck` defaults to `DebugOnly` and a spatial
  strategy's `MayCommit` admits the seam by design, so the guard is absent exactly where a
  misrouted bake would need it. New `Bulk:CreateShards`, off by default: a batch routing to a shard
  that does not exist is refused whole before any engine writes, rather than turning one POST into
  thousands of durable directories. See [CLUSTERING.md](docs/CLUSTERING.md).

- **A fourth subscription predicate, `WHERE col <> <default>` — the rows where a column has been set
  at all**, closing [#122](https://github.com/tyler-loy/MelangeDB/issues/122). Both SQL spellings
  (`<>` and `!=`) parse. The operand must *be* the column's own default — written exactly, so
  neither a null parameter nor a `0.5` that would round to zero on the write path compiles as
  not-default — and the shape is served on `bool` and unsigned integer columns, whose default is
  also their minimum so "not the default" is a single index range; a signed column would need two
  and is refused by name, as `unsupported_predicate`, along with any operand that is not the
  default. `bool` is named by its own default (`<> false`), never by `0`.

  It exists because the range shape could not express it: a counter has no bounded span, so
  `BETWEEN 1 AND :hi` needs a clamp the client invents purely to get past `MaxRangeSpan`, and
  callers who hit this denormalised an indexed boolean beside the counter instead. **That
  workaround is the counterexample to its own necessity** — equality on an indexed boolean already
  has unbounded cardinality, so it selects the same rows under the same row ceiling at the same
  cost, and the only thing it bought was a column the schema did not need. Accordingly this shape
  is *not* span-checked (there is no span) and *is* bounded by the row and byte ceilings (there is
  nothing weaker about it). Restricted to the column's own default rather than generalised to
  arbitrary inequality, because skipping the default's bucket is an index walk while "everything
  except 7" is a table scan wearing a predicate. Aggregates do not serve it: they run on the
  relational tier, and they refuse it by name rather than mis-report it. No typed client helper,
  matching the projected shape.

- **`MelangeClusterCoordinator.ReapShardAsync` removes a shard that holds nothing of its own** — the
  counterpart `EnsureShard` never had, closing [#112](https://github.com/tyler-loy/MelangeDB/issues/112).
  A host API rather than an endpoint, since this is the only cluster operation that destroys durable
  state, and it sits beside `EnsureShard` on the same coordinator: the verb that creates shards and
  the verb that removes them are reachable from the same place.

  Emptiness is decided and the engine sealed against further writes in **one hold of the engine
  write lock** — nothing weaker separates "holds nothing" from "will still hold nothing when the
  directory goes", since a reducer call resolves its shard under the node's shard-set lock and then
  commits with it released. The destruction goes **last**: the owner seals and closes, the hub
  retires the membership row, and only then is the directory deleted, so an interruption anywhere
  leaves a shard that is still owned and still openable rather than a retired originator a stale
  assignment could re-mint under. Refused while the shard owns rows, while a truncation floor beyond
  the routine ones is present, while its cluster events are unshipped, or mid-drain. The shard key
  is not reserved: visiting it again creates a new shard with a new originator, because a reaped
  prefix retires with its shard.

- **Two per-shard row gauges, `melange.cluster.shard.authoritative_rows` and
  `melange.cluster.shard.borrowed_rows`.** The first counts only rows in `Partitioned` tables minus
  the border-band copies a shard holds of its neighbours' — what would be *permanently* lost if the
  shard were removed, as against what comes back on its own (`Local` timer rows are rebuilt by the
  shard's init reducer, `Replicated` rows are the hub's, borrowed rows are rebuilt by a band reset).
  Both ride the existing load sample at no extra sampling cost, though not at the same freshness:
  the authoritative count is computed in the throttled pass that samples resident bytes, because
  `SampleLoad` sits inside the interval its own utilization divides by and per-heartbeat work there
  would make shards report as less loaded than they are. Advisory by construction — see
  [OBSERVABILITY.md](docs/OBSERVABILITY.md). Groundwork for
  [#112](https://github.com/tyler-loy/MelangeDB/issues/112).
- **`ClusterLoadView.ShardsHoldingNothing`** narrows on those gauges: shards reporting no
  authoritative rows, not drain-quiesced, and heard from recently enough that a silent node cannot
  make its shards look empty. A narrowing rather than a verdict — nothing pinning the log and
  unoccupied are deliberately absent, because neither can be answered from a sample — so a caller
  re-checks both on the owning node, under the engine write lock that also stops further writes.

### Fixed

- **The fan-out decodes a row once per op, not once per subscriber.** A subscription predicate on
  an indexed column — `WHERE RoomId = 7`, `WHERE OwnerId = …` — encoded that column by deserializing
  the whole row, and did so for every subscriber on the table, for the pre-image and the new row
  both, under the engine's write lock: N subscribers, 2N full decodes per op. Row and column
  policies did the same to hand each policy a typed row — up to 4N. The row is identical for all of
  them; only the verdict is per caller. A primary-key predicate was free (a key compare), which is
  why the reference workload's rings never showed it, and the existing fan-out benchmark had no
  predicated or policied subscribers, so it could not either.

  One `DecodedRow` per op now carries the bytes, materializes the typed row on first demand and
  hands the same instance to every predicate and policy, and memoizes each predicate column's
  encoding. The test registers sixty subscribers and counts decodes through the table's codec: two
  per op (the pre-image and the new row), where it was 120 and 240. On `FanoutBenchmarks`' new
  `Predicated` axis — 500 subscribers on one indexed-column predicate, one shared projection — the
  op went from **108 µs to 86 µs**, and now sits within 8% of the unpredicated row (79 µs) where it
  sat 25% above it; the bench row is seven narrow columns, so the per-decode cost there is small,
  and a real row with strings and blobs pays proportionally more per subscriber.

- **A reducer's pending rows are index-overlaid, not rescanned.** `Filter`, `FilterRange` and the
  `[Unique]` check had to consider the rows the same transaction had already staged, and did so by
  decoding every pending row of the table per call — a reducer inserting N rows into a table with a
  unique column decoded N²/2 of them on the way, and one that filters an indexed column once per
  row it stages was quadratic in its own write set, all under the write lock. The transaction now
  keeps a per-(table, column) overlay of pending values, maintained from the typed row the reducer
  handed in (no decode) and read by seek. Four hundred inserts into a unique-column table decode
  nothing; a `Filter` decodes exactly the rows it returns.

- `TableSchema.Column(name)` is a dictionary lookup; it was a linear scan with a string compare per
  step, on every per-row path that reads a column by name (predicate encoding, the shard
  strategies' `RowRef`, the reflection-path index maintenance). `RowRef` also decodes through the
  generated codec now when the schema has one, rather than reflection — it runs per committed row,
  and in a cluster under the write lock.

- **The commit log seeks.** `FileCommitLog.ReadFrom(lsn)` started at the file header and read,
  CRC-checked and decoded every record below the one it was asked for, then discarded them. The
  answer was right; the cost was the size of the retained log, paid per call — and every incremental
  consumer of the log calls it per batch with a cursor near the head: a client's resume replay (twice
  per resume), the Postgres applier, the event bus's catch-up, an applier catching up one record
  *under the engine's write lock*, and in a cluster the event forwarder, the border publisher (once
  per observer) and the hub's replica pump (once per node). Each of them re-read the whole retained
  log from byte zero on every batch. The same defect the primary-key range walk was — correct answers,
  linear cost where it should be logarithmic, invisible until the thing it scales with got big —
  found by going looking for its siblings rather than waiting for production to.

  The log now keeps a sparse in-memory LSN→offset index (one entry per 32 KB of file, built by the
  recovery walk it already does, extended by every append, rebased by every compaction), and a read
  seeks to the entry below its LSN, verifies the record there, and hops frame headers — no CRC, no
  decode, no allocation — to the one it wants. A compaction that moves the file under a reader fails
  that verification and falls back to the old scan, so the race costs time, never correctness. The
  cost of a read is now the size of its batch plus one stride. Measured on a 100,000-record log,
  ten records read from the start, the middle and the end: **195 µs / 12.0 ms / 25.6 ms before,
  219 µs / 262 µs / 218 µs after** — flat where it was linear, and the linear figure grows with the
  retained log, which a pinned floor can make gigabytes (`LogSeekBenchmarks`).

- **Log compaction no longer freezes the engine for the size of the log.** When a snapshot truncated
  the log it decoded every record — removed and surviving alike — to learn its LSN, rewrote the
  survivors one by one, and fsynced, all while holding the engine's write lock (so no commit could
  start), the append lock and the fsync lock (so no parked commit could finish). Every snapshot was a
  stall proportional to the *retained* log, which is by design at least the resume window and whatever
  any floor pins — and it landed on the next commits' lock wait, where the slow-reducer diagnostics
  deliberately bill no one. The retention floor's own scan decoded every removable record under the
  same lock to find the first one inside the window.

  The decision — every floor, and which governs — still runs under the write lock, because floors
  are engine state and one of them writes a file when read; the retention floor is now a binary
  search over LSNs (one seek and one record per probe) rather than a walk. The compaction then runs
  with the lock released: it seeks to the first survivor, copies the survivors as bytes, and takes the
  log's locks only to carry across whatever was appended meanwhile, fsync, and swap. A truncation pin
  taken while a compaction is in flight pins at that compaction's floor, which is where the base is
  about to stand, so the backup's guarantee is unchanged. The test for it commits from another thread
  from inside the compaction and asserts the commit lands — which against the old code is a deadlock.

- **A primary-key range subscription walked the key directory from row zero to reach its window.**
  The cost was O(keys before the window), so it grew with the table *and* with where in key order
  the client happened to be looking — a moving-window subscription got steadily more expensive the
  further it travelled, which is the one shape terrain streaming guarantees. Reported against the
  reference workload's 1.44M-row chunk table, where each client holds a nineteen-window ring
  re-scoped on every chunk crossing: **12 clients fell from 120 position-reducer calls/s to 7, and
  joins took 17–114s instead of 3.** Disabling only that ring restored both.

  Two things made it that bad rather than merely slow. The walk runs inside
  `MelangeEngine.ReadConsistent`, so it holds the engine's write lock — nineteen of them per client
  per crossing stalled every writer on the node, which is why host CPU sat at one core while
  throughput collapsed. And `FasterHotStore.ScanKeys` **copied every key of the table into an
  array** before returning, under the store lock, on every call: a multi-megabyte large-object-heap
  allocation per subscription evaluation. It never needed to — the version it reads is immutable, so
  capturing the reference is enough.

  Stores now answer `IHotStoreReader.ScanKeyRange(table, low, high)` by **seeking** to the lower
  bound, so the cost is the size of the window rather than the distance to it. Measured on a
  200,000-row table, one twenty-key window at the far end: **~19ms to ~0.01ms in the in-memory
  store and ~21ms to ~0.01ms in the FASTER store**, or roughly 1,800–2,500x, and flat in table size
  where it was linear. Both callers take the new route — subscriptions and the `FilterRange`
  a reducer runs against a primary key.

  **Confirmed end to end on the workload that reported it**, which is the number that actually
  matters — the two above are a microbenchmark of the seek, and a microbenchmark of the thing you
  changed is the easiest measurement to be wrong about. Same 12-client acceptance run, ring
  enabled, clean machine, fresh world, on `0.2.1-ci.189`: **120 position-reducer calls/s (from 7),
  flat across every 10s sample; all twelve clients up in 4s (from 17s); and 0 shed sends, from
  14,044.** The ring now costs nothing measurable — the run matches this world's reference numbers
  with the ring disabled. Host working set moved 1.17GB idle to 1.34–1.37GB during the run and
  returned to 1.16GB, at ~0.35 core.

  The key directory itself changed shape to make that possible: an `ImmutableSortedDictionary`
  cannot begin enumerating at a lower bound, so it is now one sorted set of entries, which can
  (`RowDirectory`). This is the same change `SecondaryIndex` made for the same reason — indexed
  *column* ranges have sought since then, and only the primary key was left walking. The trade is
  one extra tree operation when a write replaces an existing key, paid in short-lived garbage
  rather than in resident memory, which is the right currency for a store whose defining constraint
  is the RAM ceiling.


- **A shard node's own engine accepted bulk rows that belonged to a shard, and reported success.**
  `PlacementGuards.NodeLocalAccess` has always said that engine holds only `Local` tables, but it is
  a *table-access* guard and bulk ingestion never touches a table handle — so a batch of
  `Partitioned` rows committed into an engine nothing reads those rows from: authoritative nowhere,
  invisible to the shard owning the key, answered as an ok. The hub has had the commit-point
  counterpart since clustering landed, and its comment names bulk as the path it exists for; the
  node-local engine simply never got one. Found while building the fan-out
  ([#115](https://github.com/tyler-loy/MelangeDB/issues/115)).

- **Postgres membership no longer derives a new shard's originator from the shards that still
  exist.** It allocated `MAX(originator) + 1`, so deleting a shard row would hand its originator to
  the next shard created. An originator prefixes every AutoInc id its shard mints and those ids
  outlive the shard — an entity that crosses a border carries its id into the neighbour — so the
  reused prefix would re-mint ids still in use, breaking AutoInc's unique-not-dense contract.
  Allocation is now a high-water mark that only moves forward, seeded once from any existing rows.
  Latent today because nothing removes a shard; fixed ahead of
  [#112](https://github.com/tyler-loy/MelangeDB/issues/112), which would have made it live. The
  in-memory store was already monotonic and is unchanged.

- **Ad-hoc SQL against a `Partitioned` table is now refused in a cluster instead of returning an
  empty result.** `/melange/sql` reads the node-local engine, and in a cluster a partitioned
  table's rows live in the shard engines — on a hub, and on a shard node, whose own engine is not
  any shard's. The endpoint answered 200 with zero rows, which an operator console cannot
  distinguish from "this table is empty"; it now answers 400 `partitioned_elsewhere`, naming the
  placement and the node's `Cluster:Role`. Nothing changes for single-node deployments, where
  `Cluster:Role` is `None` and placement is inert. Reported against the reference workload
  ([#111](https://github.com/tyler-loy/MelangeDB/issues/111)).

  **Aggregates are refused on the same grounds**, which the first cut missed: placement and tier are
  independent axes, so a table can be both `Relational` and `Partitioned`, and the aggregate path
  returned before the row path's placement check. A `COUNT(*)` over a fraction of the rows is worse
  than the empty result — an empty set at least looks like nothing, while a number looks like an
  answer.

## [0.2.0] — 2026-08-17

### Breaking

- **Protocol version 2: rows travel as schema-ordered bytes, not as named column maps.** Version 1 sent
  every row and every delta op as a MessagePack map of column name to boxed value, which re-sent the
  schema with each row, built a dictionary per subscriber per row on the fan-out path under the engine's
  write lock, and rebuilt one per op on the client. Measured against the map shape: **1.18–1.40× the
  bytes, 4.6–12.4× the encode time, 2.4–2.9× the decode time, and 2.4–3.6× the allocation.** The
  performance sweep called this "likely the #1 bandwidth and client CPU issue" — the bandwidth half of
  that is wrong, and the CPU half is the whole case, because it is spent on the fan-out path while the
  write lock is held.

  A subscription's shape now travels once, as a **wire descriptor** on the first initial-set chunk: the
  table and its ordered, kinded columns. Every row after it is values. The unprojected case — no
  projection, no `[ServerOnly]` column — costs the server nothing at all, because the store already holds
  the row in the format the wire wants; a full row is the committed bytes handed to every subscriber
  without a decode, a dictionary, or a copy. A projection copies the kept columns' raw slices in schema
  order. A row narrowed by a **column policy** carries a mask bitset over the descriptor, which is empty
  — one byte — for every row on a table that has no column policies.

  **There is no version-1 encoder left, and no negotiation.** A version-1 peer is refused at the handshake
  with `unsupported_version` rather than accepted and failed later on a row it cannot read. Clients must
  be rebuilt against 0.1.2 bindings; a stale client is a handshake error, not a decode error.

  For consumers of the public API:

  - `WireRow` and `WireRowOp` carry `ReadOnlyMemory<byte> Row` and `ReadOnlyMemory<byte> ColumnMask`
    where they carried `IReadOnlyDictionary<string, object?> Columns`. A delete's `Row` is empty.
  - `MelangeRow` is a class rather than a record and exposes `Row`, `ColumnMask`, and `Descriptor`.
    **`Columns` still works** and returns the same name→value map as before — decoded on first read and
    then cached, so a typed client that never asks for it never builds it.
  - `IClientRowCodec<TRow>.DecodeRow` takes a `ReadOnlySpan<byte>` and the interface gains `Columns`, the
    shape the bindings were generated from. Both are emitted by the generator; hand-written codecs must
    be updated.
  - `ClientWireValues` is **removed**. It existed to undo MessagePack's deliberate lossiness — every
    integer arriving as `long`, an `Identity` as raw bytes — and row bytes have no lossiness to undo.
    `ClientRowShape.Verify` replaces it, and catches more: a renamed column, a **reordered** one, and one
    whose kind changed all fail the same structural comparison, once per subscription, before any row
    decodes. Ordered bytes have no names in them, so drift that the map wire reported as a missing column
    would otherwise decode into plausible garbage.
  - A row narrowed by a column policy reaching a **typed** cache now throws
    `MelangeSchemaMismatchException` naming the untyped API, rather than filling the missing field with a
    default. Partially visible rows were always the untyped API's business; this is the first release in
    which the typed path cannot silently pretend otherwise.
  - `RowWriter`, `RowReader`, and `ColumnKind` moved from `MelangeDB.Core` to `MelangeDB.Abstractions`,
    namespace `MelangeDB`, so a client can read row bytes without referencing the engine. Generated code
    is requalified automatically; hand-written references need the namespace change.

### Added

- **Backup, second pass: `--check`, `clone`, and `--at-lsn`**
  ([road-to-0.2 phase 19](docs/road-to-0.2/plan-phase-19.md)) — the three verbs phase 15 recorded
  as next, now with the shipped archive underneath them.

  **`melange restore … --check`** proves a restore boots. Verify walks frames; the check runs the
  *real* recovery machinery — the actual `FileCommitLog` constructor (epoch, torn tail, CRC, base
  sidecar), the snapshot opened under the restored epoch, every sidecar parsed — so a refusal is
  the refusal a boot would have given, on a day you chose rather than one the outage did. It runs
  against a scratch copy, so a checked restore is byte-identical to an unchecked one. The
  schema-dependent half — the shape guard, index builds, residency, the projection rebuild — needs
  the application's registry, so it is a second rung where that lives: **`host.CheckRestore(dir)`**
  boots the restored directory through the ordinary engine constructor and reports per-table row
  counts without serving. Each rung states in its own output what it proves and what it does not.

  **`melange clone <archive> -o <dir>`** materializes an explicitly *different* world from a
  production archive. Subscriber checkpoints are **dropped, not clamped** — a clone has no
  subscribers, and inherited delivery state would skip events this world never emitted — and a
  provenance sidecar records the source epoch, captured head, and archive, which the server
  announces at every boot (EventId `1804 CloneProvenance`). A separate verb rather than a restore
  flag, because the semantics differ in kind. Originators are untouched.

  **`melange restore … --at-lsn <n>`** cuts the tail at a named LSN: the moment just before the
  mistake, within the window one archive holds. Refused below the archive's snapshot floor (with
  the earlier archive in the series named as the remediation) and above its captured head, and
  refused on cluster archives, whose engines were captured at different fences. The rewind is
  total, ids included: AutoInc values allocated above the cut are free again, and the fresh epoch
  is what forces every consumer outside the world to rebuild rather than carry a stale reference
  across the boundary.

- **Truncation-floor observability: "why is the log not truncating" is now one look**
  ([road-to-0.2 phase 18](docs/road-to-0.2/plan-phase-18.md)). Everything that still needs old
  records pins the commit log through a *truncation floor*, and until now those registrations were
  anonymous `Func<ulong?>`s whose minimum truncation took in silence — a crashed subscriber, a
  stalled applier, or an orphaned handoff marker filled a disk with no path to the cause short of a
  debugger. Floors now have names (`AddTruncationFloor(string, Func<ulong?>)`; the unnamed overload
  survives, reporting as `unnamed`), and every truncation decision says which one **governed**:
  EventId `1503 LogTruncated` gained the floor's name, its LSN, the pinned record count, and the log's
  size in bytes, and the new `1510 LogTruncationPinned` covers the decision that removes *nothing*
  because a floor pinned it — the interesting case, and the one that was perfectly silent.

  The signals: **`melange.log.pinned_records`** is the headline gauge to alert on, and
  **`melange.log.truncation_floor`** (tagged `floor`) is the drill-down that names the holder. The
  floors are the reading taken at the last truncation decision — providers run under the engine write
  lock and one of them writes a file on evaluation, so a per-scrape evaluation was never on the table —
  paired with the *live* head, which is exactly the number that grows while a stuck holder stands
  still. Neither series publishes at all before the first decision: an absent series says "never
  evaluated", where a zero would say "healthy". The **`melange-retention`** health check is the same
  finding as an alert, unhealthy past `HealthChecks:RetentionPinnedThreshold` (records, default
  1,000,000) with the governing floor named in its description. A runbook —
  [OBSERVABILITY.md](docs/OBSERVABILITY.md), "the log is growing — who is holding it" — walks the
  sequence of looks from the gauge to the named holder's own signals.

- **Group commit: concurrent commits share fsyncs at unchanged `OnCommit` semantics**
  ([road-to-0.2 phase 17](docs/road-to-0.2/plan-phase-17.md)). The commit path splits in two:
  the append buffers under the engine's write lock, the committing caller waits for durability
  after releasing it, and whoever finds the flusher idle fsyncs everything buffered so far —
  batches form from contention itself, with no timer and no batch-size knob, and the fsync runs
  through its own file handle so appends never stall behind the in-flight flush (measured: on
  Windows a flush serializes against writes on the same handle, and batches simply never formed
  through one). A lone caller fsyncs immediately at the old inline latency; a reducer that
  returns has committed durably, exactly as before. Re-measured on the crowded-shard hotspot:
  **~500 commits/s sequential → ~4,000 commits/s at 16 concurrent callers, a mean batch of 8
  commits per fsync** ([CLUSTERING.md](docs/CLUSTERING.md)). `CommitLog:GroupCommit`, shipped
  accepted-and-reserved in phase 07 for exactly this, is now honored — `false` restores the
  inline fsync.

  What moved with it, because durability is now a watermark rather than a property of every
  appended byte: `ICommitLog` gained `DurableLsn` and `WaitDurable`, and **everything that
  leaves the process is fenced at the watermark** — `ReadFrom` serves nothing beyond it (which
  gated the Postgres dispatch loop, resume replay, replica and border streams, and the event
  bus wholesale), live subscription deltas, initial-set anchors, resume heads, and the replica
  bootstrap gate explicitly, and the online backup fences its capture at it. A failed group
  fsync fails every commit it covered, rolls the file back to the last durable length, and
  poisons the log until restart (EventId 1007, Critical). Crash recovery accepts multi-record
  torn tails — buffered appends mean a crash's un-fsynced region can span records, with
  intact-looking ones beyond the tear — and stays fatal for damage the durable floor proves was
  fsynced: the newest snapshot's LSN, which every snapshot now forces the log durable through
  before its file is written, under every fsync policy (closing a pre-existing
  `Interval`/`OsBuffered` window where a crash could boot a snapshot ahead of its own log).
  Telemetry: `melange.fsync_ms` is the durability wait the caller actually experienced,
  `melange.commit_ms` the append alone, and the new `melange.log.group_commit.batch_size`
  histogram's shape is the feature working.

- **Hot-tier schema migration: add a column, redeploy, boot**
  ([road-to-0.2 phase 16](docs/road-to-0.2/plan-phase-16.md)). Row format v1 is
  positional — a row is its columns' bytes in declaration order — so until now the engine could
  not even *detect* a schema change against an existing log; it would replay old bytes under new
  expectations. Now a `melange.shape` sidecar persists each table's shape as an LSN-fenced
  history, and boot compares it against the code: **additive changes migrate automatically and
  loudly** (EventId 1006) — recovery re-encodes every old-shape row to the new shape *by column
  name*, so added and reordered columns are both fine wherever they land in the struct; new
  columns fill with their kind's zero value, the phase 08 backfill rule one tier down — while
  **destructive changes refuse boot** with every reason named (`SchemaShapeException`: removed
  table or column, changed kind, moved `[PrimaryKey]`, and therefore also renames). An empty
  marker record opens the new shape's reign in the log's own timeline and the migration seals
  itself with an immediate snapshot; correctness never depends on the seal, because every reader
  decodes records under the shape governing their LSN (lagging appliers included — pipeline
  appliers automatically, decoupled readers via `MelangeEngine.TransformToCurrentShape`). The
  sidecar rides into `.mbak` archives, so newer code boots a restored directory through an
  ordinary migration boot. Every reader that can reach records below the current reign routes
  through the same transform: pipeline applier catch-up, the Postgres applier's own dispatch
  loop, resume replay to reconnecting clients, and the cluster's replica and border streams —
  each a pass-through LSN compare once the reign is current. One upgrade rule: the first boot
  that creates the sidecar adopts the booting schema, so never combine the MelangeDB upgrade
  itself with a schema change. See [docs/MIGRATION.md](docs/MIGRATION.md).

- **The cluster archive**
  ([road-to-0.2 phase 15](docs/road-to-0.2/plan-phase-15.md), final slice — the phase is
  complete). On a hub, `/melange/backup` fans out: the hub's own engine plus every shard engine
  under `Cluster:ShardDataPath` over shared storage, one fenced LSN per engine, under one
  manifest keyed by shard — **per-shard consistent, not globally consistent**, because there is
  no global total order to capture and the archive does not pretend otherwise. Shard engines
  stream handle-consistently while their owners keep serving them (ordered handle opens, a
  dense-chain check, bounded retry — no remote pin, no quiesce, no player-visible pause); border
  registries ride along and restore under each shard's fresh epoch. `melange restore`
  materializes the deployment layout (`hub/` + `shards/shard-k/`), and the round trip is a test:
  hub plus two shards out through the live endpoint, verified, restored, booted, serving — with
  the per-shard skew asserted rather than assumed away. Decisions settled in the
  [phase plan](docs/road-to-0.2/plan-phase-15.md): replacement-not-cloning, no quiesced mode,
  structural verify, and where the endpoint lives. See [docs/BACKUP.md](docs/BACKUP.md).

- **The online backup: `/melange/backup` and `melange backup <url>`**
  ([road-to-0.2 phase 15](docs/road-to-0.2/plan-phase-15.md), second slice). A running server
  streams the archive at a **fenced LSN** while commits continue, holding a truncation pin for
  exactly the stream's duration — and the pin is bounded like every truncation pin: a client that
  stalls past `Backup:StreamStallTimeoutMs` is cut off with the pin released (EventId 1803),
  because a wedged backup client must not become a full disk. Gated per the `Sql:*`/`Bulk:*`
  posture: off by default (`Backup:Enabled`), owner-role-gated when on (`Backup:OwnerRole`, its
  own key — read-everything-as-archive is its own capability, and internal identity assertions
  carry it additively and fail-closed). The CLI's URL form takes `--token` (or `MELANGE_TOKEN`);
  in-process schedulers get `MelangeBackup.CreateOnline`. On the restore side the Postgres tier
  now names both refusals: the fresh-epoch mismatch a real restore produces (EventId 1605, its
  message now stating the restore remediation) and the same-epoch checkpoint-ahead swap
  (EventId 1608, new) — both tested to recover when the printed remediation is followed
  literally. New metrics `melange.backup.bytes` and `melange.backup.duration` (also the pin's
  hold time). See [docs/BACKUP.md](docs/BACKUP.md).

- **Backup, restore, and verify: the `.mbak` archive and the offline verbs**
  ([road-to-0.2 phase 15](docs/road-to-0.2/plan-phase-15.md), first slice). The commit log is the
  source of truth and every store is a projection of it, so a backup is the truth, not the
  projections: one versioned, CRC-framed archive carrying the snapshot rows, the log tail above
  the snapshot LSN, and the sidecars — no FASTER files, no Postgres dump, and therefore
  store-engine agnostic (a FASTER-written archive restores under the in-memory engine and vice
  versa; both directions are tests). `melange backup <data-dir>` captures a stopped server's
  directory and refuses a live one; `melange restore <archive> -o <dir>` materializes a directory
  ordinary recovery boots — under a fresh epoch, always, so pre-restore resume cursors full-resync
  instead of resuming into history that no longer happened — and removes everything it wrote on
  any failure; `melange backup verify <archive>` CRC-walks every frame and dry-replays the archive
  into an in-memory projection (every single-bit flip fails it, exhaustively tested). The same API
  is public as `MelangeBackup` in `MelangeDB.Core` for operators' own tooling. The online form
  (`/melange/backup`) and the cluster archive land in the following slices. See
  [docs/BACKUP.md](docs/BACKUP.md).

- **`melange.lock`: the data directory's liveness lock.** A live server holds this empty sidecar
  exclusively for its lifetime; the offline backup probes it to refuse capturing a live directory,
  and a second server pointed at an already-open directory now refuses at boot instead of
  corrupting it. The lock exists because share modes on the log file itself are only enforced on
  Windows — Unix maps only `FileShare.None` onto a real (advisory) lock, so the liveness signal
  needs a file whose sole job is to be held that way.

- **The 2 a.m. bill: scale-in**
  ([road-to-0.2 phase 14](docs/road-to-0.2/plan-phase-14.md), final slice — the phase is
  complete, and with it the whole elastic-capacity design record is built). Behind its own switch
  (`Cluster:ScaleInEnabled`, off by default — giving nodes back is the half with sharp edges):
  when the fleet's aggregate sustained load fits under `Cluster:RebalanceColdUtilization` on one
  node fewer, the hub drains the emptiest node's shards onto the rest — phase 13 drains, one at a
  time, re-checking the cold condition before each — and hands the node to `DecommissionAsync`
  only after membership confirms it owns nothing *and* a last-moment re-check still says cold,
  because decommissioning a node the loop now needs is the one mistake players would see.
  Floored by `Cluster:MinNodes`, paced by `Cluster:ScaleInCooldownMs` (which also exempts freshly
  provisioned nodes — the newest node is the emptiest by definition, and the two fleet moves must
  never take turns), refused on partial load-view coverage and in a fleet with unreachable nodes,
  and free to abort at every step (EventIds 1744–1746). The whole curve — two hot nodes grow to
  three at the ceiling, cool, and consolidate back to the floor with the surplus process exiting —
  runs as one test.
- **The fleet follows load: provision-then-reassign**
  ([road-to-0.2 phase 14](docs/road-to-0.2/plan-phase-14.md), second slice). The rebalance loop's
  second move, taken only when the first is unavailable: every live node sustained-hot, no shard
  move that helps. The hub asks its registered `INodeProvisioner` for one node, records the
  ticket, and moves on — the new instance announces itself by joining membership, the loop
  spreads a shard onto it by the ordinary phase 13 rule, and nothing special stays behind to keep
  correct. Bounded the way money must be: never past `Cluster:MaxNodes`, one outstanding ticket at
  a time, one re-request on expiry (`Cluster:ProvisionTicketTimeoutMs`), and on the second failure
  an operator alert (EventId 1738, the new `melange-capacity` health check) — the loop stops
  asking, because the posture on repeated failure is *tell a human*, never *keep trying*. A node
  arriving after its ticket expired is the at-least-once contract's surplus: decommissioned
  without ever owning a shard, unless shards were genuinely waiting for an owner, in which case
  capacity arrived late but arrived. New observability: `melange.cluster.nodes`,
  `melange.cluster.provision.outstanding`, `melange.cluster.provision.latency`,
  `melange.cluster.decommissions`, EventIds 1735–1743.
- **The capacity seam: `INodeProvisioner`**
  ([road-to-0.2 phase 14](docs/road-to-0.2/plan-phase-14.md), first slice). The public interface
  through which the hub will obtain one more shard node when every node it has is sustained-hot,
  and give the emptiest one back when the fleet is cold — which cloud, rack, or stack of warm
  processes supplies the node is the deployment's business, not MelangeDB's. A DI registration,
  not a configuration string (the membership-store precedent); no registration means the fleet is
  fixed and phase 13 behaviour is unchanged. The contract's safety clauses are documented on the
  interface: fire-and-track (a provisioned node announces itself by joining membership like any
  other node), at-least-once made safe by fencing, and shared-storage access as part of the deal.
  With it, `Cluster:MaxNodes` — the hard fleet ceiling, deliberately without a default: a
  registered provisioner with the ceiling unset is refused at startup, because a loop that can
  spend money must have its bound set by a human, never by a default.
- **The cluster follows load: the rebalance loop**
  ([road-to-0.2 phase 13](docs/road-to-0.2/plan-phase-13.md), final slice — the phase is complete).
  With `Cluster:RebalanceEnabled` (off by default: a loop that relocates the world should be a
  decision, not a surprise), the hub watches the load view and drains a sustained-hot node's
  largest-load shard to the least-loaded live node — but only when the pair's maximum utilization
  strictly improves, so relocating a whole hotspot is refused rather than churned. Hysteresis at
  every layer: the sustained window (`Cluster:RebalanceWindowSeconds`, and no action until the
  history covers it), the per-shard move floor (`Cluster:ShardMoveMinIntervalMs`, started by
  operator drains too), and one automatic move in flight at a time. A hot node the rule cannot
  help is reported, rate-limited, with EventId **1732 as the granularity guardrail**: a node whose
  whole load lives in one shard is the ceiling no cluster size changes, and the signal the
  strategy's split lines were drawn too coarse. This is the five-islands scenario end to end: all
  shards on one node at 2 a.m., the hot island peeled off to another node at 2 p.m., by itself.

- **A live shard can move between nodes: the planned drain**
  ([road-to-0.2 phase 13](docs/road-to-0.2/plan-phase-13.md), second slice).
  `MelangeClusterCoordinator.DrainShardAsync(shard, destination?)` is the node-death reassignment
  path made polite: the origin takes a fresh snapshot and closes the shard (so the destination's
  recovery tail is short), membership moves it under a bumped fencing token, the destination
  recovers it from the shard's own log on shared storage — an ownership transfer, not a data copy —
  and the gateways swap attached clients invisibly: calls issued during the window queue (bounded
  by the new `Cluster:DrainQueueTimeoutMs`) and flush in order on the destination, and
  subscriptions re-scope so each client's cache is atomically replaced. The drained shard's writes
  pause for the handover window; every other shard is untouched. A failed drain hands the shard
  back to its origin, and a hub death mid-drain self-heals: the origin's draining mark expires
  after `2 × Cluster:FailureTimeoutMs` and the shard reopens where it was. Null destination picks
  the live node owning the fewest shards. EventIds 1724–1730 narrate every ending.

- **The hub knows which shard is hot** ([road-to-0.2 phase 13](docs/road-to-0.2/plan-phase-13.md),
  first slice). Every shard node's heartbeat now carries one load sample per owned shard — the busy
  fraction of that shard engine's write lock since the previous beat (the resource the published
  hotspot ceilings are ceilings on, via the new `MelangeEngine.WriteLockBusyTicks` counter), the
  shard log's head, the resident footprint, and the border-band row count. The hub aggregates the
  feed into `MelangeClusterCoordinator.LoadView()` and exports it as
  `melange.cluster.shard.utilization` / `melange.cluster.shard.resident_bytes` gauges, tagged by
  shard and node — the operator's "which island is busy" answer, and the feed the coming rebalance
  loop decides from. No new clock, no new message: the samples ride the heartbeat that already
  exists.

- **The client knows its own identity.** The Welcome frame now carries the identity the server
  derived during the handshake, surfaced as `MelangeClient.Identity` and `conn.Identity` on the
  generated `MelangeConnection` — the value that distinguishes "my rows" from everyone else's in a
  subscription-fed cache. Clients previously had to re-derive it from their own token (a second
  implementation of the one derivation that must never disagree, and no option at all once the IdP
  is a third party) or be told it by an issuance endpoint that isn't the party computing it.
  Alongside it, [docs/CLIENT-BINDINGS.md](docs/CLIENT-BINDINGS.md) now records what a client knows
  the moment `ConnectAsync` returns — including that a row created by a `ClientConnected` reducer
  may arrive as a delta just after the initial set rather than in it. (#30)

- **`MELANGE0023`: a warning on read-modify-write inside a snapshot-isolated reducer.** The
  detectable common shape of getting `Isolation.Snapshot` wrong — a row obtained from a single-row
  `Find` and written back through the table handle's `Update` — is now flagged at compile time,
  seen through the wrappers the shape is written with (`?? throw`, `.Value`, `GetValueOrDefault()`,
  `with`, and local copies). The write-back carries every column of a row read from a view pinned
  at one LSN, so a concurrent commit to any other column of the same row is silently reverted —
  a move reducer that looks like a blind position write is a read-modify-write at row granularity,
  and the failure mode is lost writes with no error anywhere. A warning, never an error: a body
  that recomputes a row it also read is legitimate, so silence is not proof of eligibility. Rows
  from `Iter`/`Filter`/`First` are deliberately not tracked — updating rows mid-sweep is what the
  isolation level's legitimate customers do every tick.

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

- **A schema migration broke every decoupled reader in the process that performed it**
  ([#92](https://github.com/tyler-loy/MelangeDB/issues/92)). The boot-time shape resolution sized
  its per-reign mapper table once, and the migration then appended a reign to the same history — so
  transforming any record at or above the marker threw. Pipeline-driven appliers skip the transform
  for a record they just watched commit, which is why the suite stayed green; the Postgres applier,
  the replica and border pumps, resume replay, and a lagging applier's catch-up all transform
  unconditionally, and any of them was the first crash after an additive deploy with no restart in
  between. The reigns and their mappers are now one immutable snapshot replaced wholesale, which
  also removes the unsynchronized lazy cache those same readers were racing.

- **A live `CommitLog:FsyncPolicy` / `CommitLog:GroupCommit` change could publish LSNs a crash
  untells** ([#91](https://github.com/tyler-loy/MelangeDB/issues/91)). Group commit was the first
  time the log head could sit above the fsynced watermark under `OnCommit`, and both keys are
  documented live — so leaving `OnCommit` made `DurableLsn` jump to the head and `WaitDurable`
  return immediately over records that were still only OS-buffered. A policy change is now a
  **durability boundary**: everything appended under the outgoing policy is fsynced before the new
  one takes effect, published under both locks so no append lands in between.

- **A reducer whose body threw `ArgumentException` was reported as `unknown_reducer`**
  ([#98](https://github.com/tyler-loy/MelangeDB/issues/98)). The reducer existed, had been
  resolved, and had run — the arity check passing proves it — but any argument-shaped exception
  escaping the body (an `ArgumentOutOfRangeException` from a row decode, say) answered "No reducer
  named 'X' is registered", sending debugging to registration for a fault two layers down. The new
  `UnknownReducerException` names the one condition that may answer `unknown_reducer`, and a body's
  failure now takes the general arm. **Client-visible:** such a call answers `internal` / HTTP 500
  where it previously answered `unknown_reducer` / HTTP 404. The HTTP path also now logs EventId
  `1204` for it, which it never did — "see the server logs" was not true on that transport.

- **FASTER recovery stopped paying for read views nobody holds.** Making the store's managed state
  persistent containers (the pinned-reads work) made recovery measurably slower on `FasterHotStore`
  — a consumer measured **+16.5%** (3.76 s → 4.37 s) replaying a 269 MB log ([#51](https://github.com/tyler-loy/MelangeDB/issues/51))
  — because replay applied one op at a time, and each op paid a path copy of its table's containers
  to publish a version no reader could observe: no read view can exist before the engine finishes
  constructing. The engine now brackets recovery (snapshot load included) with the new optional
  `IBulkRecovery` capability, and the FASTER store takes the whole replay through the containers'
  builders, publishing one version per table at the end — the same trade the in-memory store's
  snapshot load already made. Measured on a 34 MB log of sweep-shaped records (a 200k-row resident
  indexed table seeded in 100-row commits, then 8-row update sweeps): recovery **3.12 s → 2.00 s,
  0.64×** — the regression erased, with room to spare. Mid-replay Auto demotion runs inside the
  builders and is pinned by a test. `1003` fired only on the commit path, so a reducer
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

- **Adopting a schema over an existing directory is no longer silent**
  ([#99](https://github.com/tyler-loy/MelangeDB/issues/99)). The first boot that creates
  `melange.shape` adopts the running code's schema as the meaning of every record already on disk —
  the only possible reading, and correct exactly when the upgrade rule was followed (boot the old
  schema once, then change it). Combining the two in one deploy produced a clean boot, a sidecar
  appearing on disk, and a per-table mis-decode an arbitrary amount of time later. That moment now
  logs **EventId 1008 `ShapeAdopted`** at warning level over a non-empty directory, naming the LSN
  it adopted over and the recovery; a new world naming its first shape stays silent. The new
  **`Schema:AllowAdoption`** decides whether it happens at all, and **defaults to `false`** — the
  `Postgres:AutoMigrate` posture, because the two errors are not symmetric: refusing costs one boot
  and one setting, assuming wrongly costs silently mis-read rows found arbitrarily later. The
  refusal names both branches. It is a once-per-directory gate by construction, and a fresh install
  never meets it. A row that no longer decodes now names the table, its byte count, and
  where to look, instead of one word about a parameter name — on the reducer read path, the store's
  index-maintenance path, and the transport's projection paths. [MIGRATION.md](docs/MIGRATION.md)
  gains the recovery section it was missing.

- **Mid-handoff write refusals are a typed transient rejection, not an internal error.** The
  conditions the cluster itself designed — a row frozen mid-handoff, a write routed to a border
  copy just after the shard map flips, a fenced node awaiting re-registration — reached the client
  as `internal` ("The reducer failed; see the server logs") with a full server error log per
  unlucky crossing, which at seam-walking scale is log noise for the product working as designed.
  They now surface as the new wire code **`transient`** carrying the precise reason, and the
  server logs nothing; the HTTP call endpoint answers 409 rather than 500. The retry contract is
  named on the client as `MelangeCallException.IsTransient`: retry the call unchanged on the next
  tick. For module and host code, `ShardFencedException` and `BorderReadOnlyException` now derive
  from the new `TransientRejectionException` (itself an `InvalidOperationException`, so existing
  catches still hold), and the frozen-row guard throws it directly. `rejected` stays reserved for
  what reducer code itself decided. (#22)

- **CI can be triggered by hand.** `workflow_dispatch` on `ci.yml`, and the `pack` job now gates on the
  ref being `main` rather than on the event being a push — equivalent for every trigger that existed
  before, and it means a dispatch from `main` produces the `-ci.<run-number>` prerelease rather than
  only re-running the tests. Recovering a run lost to an Actions outage no longer requires an empty
  commit on `main`. Nothing about a dispatch can reach nuget.org.

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
