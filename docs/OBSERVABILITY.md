# Observability

MelangeDB is instrumented with OpenTelemetry from the first commit, not retrofitted.

> **The rule, as with [CONFIGURATION.md](CONFIGURATION.md):** when a phase adds a span or a metric, it is
> recorded in this document in the same change. **Signal names are public API.** The moment someone builds a
> dashboard or an alert on `melange.applier.lag`, renaming it is a breaking change — so names get the same care
> as method signatures.

Each row lands with its phase. The phase 01 spans and metrics are implemented and asserted by tests over a
collecting `ActivityListener` / `MeterListener`. The phase 03 rows — `melange.subscription.initial`, the
sampled `melange.subscription.delta` (ratio: `Telemetry:DeltaSpanSampleRatio`), `melange.subscriptions.active`,
`melange.subscription.delta_rows`, `melange.subscription.rejected` (dimension: the wire error code), and
`melange.connections.active` — shipped with the transport, emitted on the same `MelangeDB` source and meter.
`CallReducer`'s `traceparent` shipped as specified below: the server parses it into an `ActivityContext` that
parents the `melange.reducer` span directly. The phase 04 rows — `melange.ratelimit.rejected` (dimension:
reducer) and `melange.policy.rows_filtered` (dimension: table) — shipped with identity and policies, on the
same meter. Neither carries an identity dimension, per the cardinality rule below: *whose* call was limited
belongs on the span and in the log, never on a time series. The phase 05 rows — the `melange.scheduler.tick`
span (a new trace root, with the fire's `melange.reducer` span as its child), `melange.scheduler.overruns`
(dimension: reducer; one increment per overrun event), and `melange.scheduler.tick.duration` — shipped with
the scheduler. `melange.shard` stays off the tick span until phase 09 gives timer tables a placement. The
phase 06 rows — the `melange.event.handle` span (a new trace **linked** to the emitting transaction's span,
never parented under it, exactly as specified below; a subscriber catching up from the log gets no link, since
the emitting trace is gone), `melange.events.queue_depth` (the bounded in-memory delivery window), and
`melange.events.deadlettered` (dimension: `event_type` — bounded, being a code-declared set) — shipped with the
event bus on the same source and meter. The phase 07 rows — `melange.store.resident_bytes`,
`melange.store.page_faults`, and `melange.store.scan_rows` (dimension: `table` on all three — bounded, being
the schema) — shipped with the paged store: the hot store self-reports per-table statistics through
`IHotStore.Statistics()`, and the engine's telemetry exposes them as observable instruments, so both store
engines feed the same signals. `page_faults` counts rows served from disk instead of the buffer pool (always
zero for the in-memory store); `scan_rows` counts rows returned by full scans, the runtime shadow of analyzer
MELANGE0017. Phase 08 shipped no new metric — `melange.applier.lag` has carried the postgres applier's story
since phase 01 by design; the tier registers as a decoupled applier, so its checkpoint appears there under
`applier="postgres"` and pins log truncation like any other. The `melange.apply` span is emitted per
**batch** by the Postgres applier as specified below, with one honest narrowing: it starts a new trace with
no links, because a log record persists no trace context — a catch-up batch may cover a hundred committed
transactions whose traces are long gone. (The event bus can link because it carries the emitter's context in
memory; a durable log cannot.) The `melange-applier` health check shipped this phase; its threshold is
`HealthChecks:ApplierLagThreshold`.

## The dependency decision

**`MelangeDB.Core` takes no telemetry package reference at all** — not OpenTelemetry, and not even
`System.Diagnostics.DiagnosticSource`, since `ActivitySource` and `Meter` are in the `net10.0` framework. (The
SDK confirms this: adding the package errors with NU1510, "will not be pruned… likely unnecessary.")

So MelangeDB emits through built-in primitives — `ActivitySource` for traces, `Meter` for metrics, `ILogger` for
logs — and the *host application* chooses exporters. This is the same pattern ASP.NET Core, `HttpClient`, and
EF Core use, and it matters for three reasons:

1. A library that forces an exporter choice on its consumer is a library people fight.
2. Any OpenTelemetry setup, any vendor SDK, or no telemetry at all all work unchanged.
3. It costs nothing when unused — an `ActivitySource` with no listener is a null check.

`MelangeDB.OpenTelemetry` exists purely as convenience, registering the source and meter names:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddMelangeDbInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddMelangeDbInstrumentation().AddOtlpExporter());
```

Equivalent without the package — which must keep working:

```csharp
.WithTracing(t => t.AddSource("MelangeDB").AddOtlpExporter())
.WithMetrics(m => m.AddMeter("MelangeDB").AddOtlpExporter())
```

## The cardinality and privacy trap

Getting this wrong is expensive in a real observability bill, and it is the single easiest mistake to make here.

- **Reducer name, table name, applier name, and outcome are bounded** — safe as metric dimensions.
- **Caller identity, row keys, and shard keys are unbounded.** They belong on **spans** (where they're
  per-trace and invaluable for chasing one player's problem) and must **never** be metric dimensions — one
  identity dimension on a counter is one time series per player, forever.
- **Reducer arguments are never auto-attached to spans.** They can contain anything, including secrets, and the
  commit log already records them. Opt-in per reducer at most.
- `Telemetry:IncludeCallerIdentity` exists so identity can be dropped entirely where that's a privacy
  requirement.

## Traces and the log do different jobs

The commit log already records caller, reducer name, arguments, and timestamp for every transaction — a
complete ordered audit trail (see [THREAT-MODEL.md](THREAT-MODEL.md)). So tracing is **not** for *what happened*.

- **The log answers "what happened, and in what order."** It's the truth.
- **Traces answer "how long did it take, and what caused what."** Latency and causality.

Don't duplicate the log into span attributes.

## Spans

Source name: `MelangeDB`.

| Span | Phase | Attributes | Notes |
| --- | --- | --- | --- |
| `melange.reducer` | 01 | `melange.reducer.name`, `melange.outcome` (`commit`/`abort`/`rejected`), `melange.writeset.rows`; `melange.caller` unless `Telemetry:IncludeCallerIdentity` is off; `melange.reducer.args` only when `Telemetry:IncludeReducerArguments` is opted in (formatted values for in-process calls; the hex-encoded argument payload, capped at 256 bytes, for encoded dispatch) | The root span for client-initiated work. |
| `melange.commit` | 01 | `melange.lsn`, `melange.writeset.bytes` | The locked critical section — since phase 17 the append alone, because the durability wait runs after the lock releases. The `melange.fsync` span belongs to whichever transaction performed the shared flush (a lone caller's own, under group commit possibly another commit's trace), so durability cost stays isolated from serialization cost without pretending a batch's flush was private. |
| `melange.apply` | 01 | `melange.applier` | One per applier, per batch. |
| `melange.subscription.initial` | 03 | `melange.table`, `melange.rows`, `melange.bytes` | The expensive half of a subscription; worth its own span. |
| `melange.subscription.delta` | 03 | `melange.table`, `melange.subscribers` | Sampled — this is the highest-frequency operation in the system and must not be traced per row op at full rate. |
| `melange.event.handle` | 06 | `melange.event.type`, `melange.handler` | **Linked, not parented** — see below. |
| `melange.scheduler.tick` | 05 | `melange.reducer.name`, `melange.shard` (attribute from 09) | A tick has no client parent, so it starts a new trace. |
| `melange.handoff` | 09 | `melange.shard.from`, `melange.shard.to` | Spans two processes. This is where distributed tracing earns its keep. |

A `melange.reducer` span whose **locked portion** exceeds `Telemetry:SlowReducerMs` additionally carries a
`melange.slow_reducer` span event and produces a warning log entry (`1003`) — shipped with phase 02,
threshold live-reloadable. Both carry the same split, because a slow transaction has more than one cause
and they call for opposite responses:

| Field | Span event tag | Log field | What a large value means |
| --- | --- | --- | --- |
| Locked | `melange.locked_ms` | `LockedMs` | How long the write lock was held — the threshold fires on this, and it is global write latency. |
| Total | `melange.duration_ms` | `DurationMs` | The whole transaction, durability wait included. Since phase 17 it exceeds `LockedMs` by that wait even under `Isolation.Serialized`. |
| Body | `melange.body_ms` | `BodyMs` | The module does too much per transaction — narrow the window. |
| Commit | `melange.commit_ms` | `CommitMs` | The log append — buffered since phase 17, so the disk no longer appears here. |
| Fsync | `melange.fsync_ms` | `FsyncMs` | The durability wait this caller experienced — under group commit the shared flush's cost from this transaction's seat. Disk contention on this host — infrastructure, not application. |
| Post-commit | `melange.post_commit_ms` | `PostCommitMs` | A commit observer, an applier handoff, or an automatic snapshot. |
| Rows | `melange.writeset.rows` | `Rows` | Sizes the transaction; a wide body with one row op is a read-side problem. |
| Isolation | `melange.isolation` | `Isolation` | `serialized` or `snapshot` — which of the two numbers above to believe. |

**`LockedMs` is the stall; `DurationMs` is the experience.** Under the default `Isolation.Serialized` the
two differ only by the durability wait (phase 17 moved the fsync outside the lock), so a large gap between
them on a serialized transaction reads as disk, not contention. Under `Isolation.Snapshot` the body also
ran outside the lock, so the gap is the body plus the wait — and a 500 ms snapshot body does not warn at
all, because it froze nothing. That is the point of thresholding on the locked half: an alert built to catch
write stalls should not fire on a reducer that caused none. `melange.isolation` is also a tag on the
`melange.reducer` span itself, so the two populations can be separated before any warning is involved.

`76.9ms body / 2.3ms commit` and `0.5ms body / 141.7ms commit (141.6ms fsync)` are the two failures that
used to produce identical warnings — one fixed in the module, one on the host. Body time is measured
directly rather than derived as *total − commit*: commit observers, applier notification, and any automatic
snapshot run after the append but inside the same span, so subtracting would bill all of them to the
reducer body.

**The fsync field is absent, not zero, when the flush was deferred.** Under `CommitLog:FsyncPolicy` of
`Interval` or `OsBuffered` the flush happens on a timer thread or not at all, so no durability cost belongs
to the appending transaction; a zero would read as "the disk was instant". Those warnings keep event id
`1003` — alerts key on the id — under the event name `SlowReducerDeferredFsync` rather than `SlowReducer`.
Under `OnCommit` the field is always present and can legitimately be near zero: a commit whose record an
in-flight flush already covered waited almost nothing, which is group commit doing its job — the
`melange.log.group_commit.batch_size` histogram beside it is how the amortized view is derived without
either metric lying.

**A transaction that aborts warns too.** For a serialized transaction, rolling back costs nothing and buys
nothing: the write lock was held for the full duration either way, so a reducer that walks five thousand rows
and then rejects the move stalls every other writer exactly as long as one that commits. Those entries carry
`melange.outcome` / `Outcome` — `abort` for a bug, `rejected` for an ordinary refusal that happened to be
expensive — under the event name `SlowReducerAborted`, again on id `1003`. They report `LockedMs`,
`DurationMs`, and `BodyMs` only: nothing was appended, so there is no commit, fsync, or post-commit to
attribute, and zeroes would invite a dashboard to average them into the committed ones. A rejection is a
normal outcome and warning on it is deliberate, because "rejections are cheap" is exactly the assumption that
makes a validating reducer expensive; an alert that disagrees can filter on `Outcome`. A *snapshot*
transaction that threw in its body held no lock and so does not reach this warning at all; one rejected by a
commit guard reports the commit attempt it did hold.

**`1004 SnapshotIsolationUnavailable`** fires once per engine when a reducer declares
`Isolation.Snapshot` but the configured hot store does not implement `IReadViewSource`. Such reducers run
serialized — correct, just not faster — and both shipped stores offer pinned reads, so this is a signal about
a custom or future store rather than about a MelangeDB deployment. Degrading rather than refusing to start is
deliberate: isolation is a latency property, not a semantic one. Degrading *silently* is what would be wrong.

**Read reducer duration as global write latency — unless the span says `snapshot`.** For the default
`Isolation.Serialized`, the engine's write lock is held across the entire transaction: body, commit guards,
append, fsync, commit observers, and any automatic snapshot the commit triggers (see
[DESIGN.md §4](DESIGN.md)). Every millisecond on such a `melange.reducer` span is a millisecond in which no
other transaction on that engine could start. So `Telemetry:SlowReducerMs` is not "how slow is too slow for
this caller" but **"how long is it acceptable to freeze the world"**, and a `melange.reducer` histogram is
the closest thing the system has to a write-stall metric. Reads are exempt: subscription fan-out and
committed reads take no lock and keep serving throughout.

A span tagged `melange.isolation=snapshot` breaks that reading, on purpose — its body ran outside the lock,
so its duration is what the reducer cost and **not** what it cost everyone else. Use `melange.locked_ms` for
the write-stall view and filter the histogram by isolation, or a sweep deliberately moved off the lock will
show up as the worst write-latency offender on the dashboard precisely because it stopped being one.

### Context propagation

Two places, both of which need explicit protocol support:

- **Client → server.** The `CallReducer` frame carries a `traceparent`, so a click in a Godot client links to
  the server-side reducer span. Without this, client and server traces are two disconnected stories.
- **Node → node.** Handoff and cross-shard sagas propagate context, so a player transfer is one trace across
  both nodes rather than two unrelated spans. A handoff bug is close to undebuggable otherwise.

### Parent vs. link, and why it matters

An event handler runs **after** the emitting transaction commits, possibly much later and possibly on another
node. Making its span a *child* of the reducer span is wrong: it distorts the reducer's duration and produces
traces that never close. The handler gets its own trace with a **span link** back to the emitter.

Same reasoning for appliers: a Postgres applier batch may cover transactions from a hundred different traces, so
it links rather than parents.

## Metrics

Meter name: `MelangeDB`.

| Metric | Type | Unit | Dimensions | Phase |
| --- | --- | --- | --- | --- |
| `melange.transactions` | counter | `{tx}` | `reducer`, `outcome` | 01 |
| `melange.reducer.duration` | histogram | `ms` | `reducer` | 01 |
| `melange.commit.duration` | histogram | `ms` | — | 01 |
| `melange.fsync.duration` | histogram | `ms` | — | 01 |
| `melange.log.group_commit.batch_size` | histogram | `{record}` | — | 17 |
| `melange.writeset.rows` | histogram | `{row}` | `reducer` | 01 |
| `melange.log.head_lsn` | gauge | `{lsn}` | — | 01 |
| `melange.log.truncation_floor` | gauge | `{lsn}` | `floor` | 18 |
| **`melange.log.pinned_records`** | gauge | `{record}` | — | 18 |
| **`melange.applier.lag`** | gauge | `{tx}` | `applier` | 01 |
| `melange.store.resident_bytes` | gauge | `By` | `table` | 07 |
| `melange.store.page_faults` | counter | `{fault}` | `table` | 07 |
| `melange.store.scan_rows` | counter | `{row}` | `table` | 07 |
| `melange.subscriptions.active` | gauge | `{sub}` | `table` | 03 |
| `melange.subscription.delta_rows` | counter | `{row}` | `table` | 03 |
| `melange.subscription.rejected` | counter | `{sub}` | `reason` | 03 |
| `melange.connections.active` | gauge | `{conn}` | — | 03 |
| `melange.ratelimit.rejected` | counter | `{call}` | `reducer` | 04 |
| `melange.policy.rows_filtered` | counter | `{row}` | `table` | 04 |
| `melange.scheduler.overruns` | counter | `{tick}` | `reducer` | 05 |
| `melange.scheduler.tick.duration` | histogram | `ms` | `reducer` | 05 |
| `melange.events.queue_depth` | gauge | `{event}` | — | 06 |
| `melange.events.deadlettered` | counter | `{event}` | `event_type` | 06 |
| `melange.handoff.duration` | histogram | `ms` | — | 09 |
| `melange.handoff.failed` | counter | `{handoff}` | `reason` | 09 |
| `melange.shard.owned` | gauge | `{shard}` | — | 09 |
| `melange.shard.span_violations` | counter | `{tx}` | `reducer` | 09 |
| `melange.cluster.shard.utilization` | gauge | `{ratio}` | `shard`, `node` | 13 |
| `melange.cluster.shard.resident_bytes` | gauge | `By` | `shard`, `node` | 13 |
| `melange.cluster.nodes` | gauge | `{node}` | — | 14 |
| `melange.cluster.provision.outstanding` | gauge | `{ticket}` | — | 14 |
| `melange.cluster.provision.latency` | histogram | `ms` | — | 14 |
| `melange.cluster.decommissions` | counter | `{node}` | — | 14 |
| `melange.backup.bytes` | counter | `By` | `outcome` | 15 |
| `melange.backup.duration` | histogram | `ms` | `outcome` | 15 |

### The four that actually matter

Most of the table is routine. These four are the ones this architecture specifically demands, because each
corresponds to a documented silent failure mode:

- **`melange.applier.lag`** — the whole two-tier design rests on appliers being allowed to lag. The dangerous
  failure is a *silently stalled* Postgres applier: writes keep succeeding while the relational tier falls hours
  behind and nobody notices until an admin query returns stale data. This metric is that alarm.
- **`melange.store.resident_bytes`** — turns the residency budget from a startup log line into something
  continuously observable. "Does a 20km world fit in 8GB" becomes a dashboard rather than an argument.
- **`melange.scheduler.overruns`** — a tick overrunning its interval is how a simulation death-spirals under
  load. It needs to be visible before players feel it.
- **`melange.shard.span_violations`** — the one contract MelangeDB cannot verify statically. Non-zero in
  production means transactions are silently going distributed.
- **`melange.log.pinned_records`** — the log is the system of record, so anything that still needs old records
  keeps them, and a holder that stops checkpointing fills a disk with a log that will not compact. Alert on
  this one number; drill into `melange.log.truncation_floor`'s `floor` tag for the name.

**Reading the two truncation gauges.** `melange.log.truncation_floor` reports, per named holder, the highest LSN
it permits compaction to remove — as of the last truncation *decision*, not as of the scrape. Floor providers
run under the engine write lock and one of them writes a file on evaluation (the cluster's borrowed-sidecar
refresh is registered as a floor precisely so it runs at that moment), so evaluating them per scrape would race
engine state and rewrite that sidecar continuously. `melange.log.pinned_records` pairs that cached floor with
the **live** head, which is exactly the number that grows while a stuck holder stands still. Neither series is
published at all until a decision has been made — snapshots off, `Snapshots:TruncateLog` off, or no snapshot
yet: an absent series says "never evaluated", where a zero would say "healthy". The floor names are a small
static set by construction (they name mechanisms, not instances), which is what keeps the `floor` tag on the
right side of the cardinality rule: `snapshot`, `resume-window`, `event-bus`, `backup-pin`, `shard-freeze`,
`shard-import`, `shard-sidecar`, `cluster-events`, one per applier name, and `unnamed` for a third-party floor
registered through the overload that takes no name.

## Logs

Structured through `ILogger` with stable `EventId`s so log-based alerts don't break on message rewording.
No parallel logging abstraction — the host's configured providers are the whole story.

Stable ids so far: `1001 TornRecordTruncated`, `1002 AppendRollbackFailed` (01);
`1007 GroupFlushFailed` — a group fsync failed, every commit it covered is failed and rolled back,
and the log rejects appends until restart; Critical, and the `melange-log` health check goes
unhealthy with it (road-to-0.2 phase 17); `1003 SlowReducer` —
also emitted as `SlowReducerDeferredFsync` when the fsync policy defers the flush, and
`SlowReducerAborted` when the transaction did not commit, both under the same id —
`1004 SnapshotIsolationUnavailable`, once per engine when a store offers no pinned reads,
`1101 MelangeStarted`, `1102 MelangeStopped` (02); `1005 CommitObserverFailed`,
`1006 ShapeMigrated` — an additive schema migration at boot: the changes, the rebuild, and the
marker LSN the new shape governs from; Warning-level because automatic must never mean silent
(road-to-0.2 phase 16, [MIGRATION.md](MIGRATION.md)) — `1203 HeartbeatTimeout`,
`1204 ReducerCallFailed` (03); `1104 UnpolicedReducers` (04); `1205 LifecycleReducerFailed`,
`1301 SchedulerOverrun`, `1302 SchedulerTickFailed` (05); `1401 EventHandlerRetry`, `1402 EventDeadLettered`,
`1403 SubscriberCheckpointEvicted` — the loud eviction the expiry design promises — and
`1404 SubscriberLostPlace`, how a returning subscriber is told it starts from current state (06);
`1501 ResidencyReport` — the startup residency report: per resident table row count and measured bytes, the
buffer-pool cap, and the total they sum to — `1502 SnapshotWritten`, `1503 LogTruncated` — since phase 18 it
names the *governing* floor rather than the anonymous LSN it respected: floor name, floor LSN, records still
pinned behind the head, and the log file's size in bytes — `1504 SnapshotFailed` (an automatic snapshot failing must not fail the committed transaction),
`1505 AutoResidencyDemoted` — an `Auto` table crossing its threshold is the cliff arriving, and it announces
itself — `1506 StaleSnapshotIgnored`, `1507 ResidencyChangeFailed`, `1508 ResidencyChanged` — the careful
per-table override being applied at runtime (07) — `1509 SnapshotAlreadyRunning`, Debug-level, the
only signal that snapshots now write outside the write lock: an interval short enough for two to
overlap is a configuration to raise, not an error to chase; and `1510 LogTruncationPinned` — a truncation that
removed **nothing** because a floor pinned it, naming the holder, its LSN, the head, the pinned record count,
and the bytes they occupy. The same shape as 1503 on purpose: from silence an operator cannot tell a log that
is already compacted from one that has stopped compacting, and the second is the one that fills the disk
(road-to-0.2 phase 18); `1601 PostgresApplierStalled` — the loud stall the phase-08
risk register demands: first failure always, then every 30 seconds under `Diagnostics:ReportApplierLag` with
the growing lag — `1602 PostgresApplierRecovered`, `1603 PostgresSchemaMigrated` (the DDL AutoMigrate
applied — automatic must not mean silent), `1604 PostgresMigrationRefused` (carrying the exact pending DDL,
so the manual migration is a copy-paste), `1605 PostgresEpochMismatch` (LSNs are meaningless across log
epochs; the applier stalls rather than guesses), `1606 PostgresTierBootstrapped` (a tier attached after log
truncation is rebuilt from the hot store at one consistent LSN), and `1607 RelationalTablesWithoutPostgres` —
tables declared `Tier = Relational` in a deployment that configured no Postgres run fine in the hot store,
and are told what they are missing (08); `1700 ClusterHubStarted` (naming the node-link listener's bound
port), `1701 ClusterNodeRegistered`, `1702 ClusterNodeSuspectedDead` — the failure-detection decision made
loud: how long the silence was and how many shards moved under bumped fencing tokens —
`1703 ClusterLinkAuthFailed` (a link that could not prove the cluster secret; a rogue dialer or a
misconfigured node, either way worth an alert), `1704 ForeignEventHandlerFailed` — a hub handler exhausted
its retries for a shard-forwarded event; foreign events have no dead-letter file, and the log says so
plainly — `1705 HandoffCompleted` and `1706 HandoffAborted` (the two ends every transfer saga reaches),
`1707 ShardOpened` (which node, recovered to which LSN — reassignment is recovery, and this is its receipt),
`1708 ShardReleased`, `1709 HubLinkLost` — the node's own view of a partition, ending in self-fencing if
`Cluster:FailureTimeoutMs` passes first — `1710 HandoffUnresolved` — an import request that timed out or lost
its link: the destination may or may not hold the import, so the player deliberately stays frozen until the
origin's reconciler learns the truth from the destination's log; unavailable beats duplicated — and
`1711 ReplicaStreamBootstrapped`: a node subscribed replication from below the hub log's truncation base, so
the gap could not be served from the log and the full Replicated state was sent as a reset instead of the
stream silently resuming past it (09); `1712 HandoffRequested` — an origin node's boundary monitor saw an
anchored entity cross past the margin and asked for a transfer — `1713 HandoffRateLimited` (Debug: a trigger
suppressed by `Cluster:HandoffMinIntervalMs`; hysteresis working as intended), `1714 HandoffResolvedRemotely`
— a node's reconciler resolved a saga the coordinator lost mid-flight, and the hub ran its session-map and
gateway notifications late but correctly — `1715 BorderStreamReset` — an owner shard could not serve an
observer's border cursor from its log (truncated past, another epoch, or a changed band depth), so the full
band went as a reset instead of the stream silently resuming past the gap — `1716 BorrowedRegistryRebuilt`
— a shard's borrowed-row sidecar was missing or unusable while its log is truncated, so the read-only
registry was rebuilt from row content: correct but a full scan, expected once when upgrading a pre-phase-10
shard directory — `1717 TransferListenerFailed`, an `IShardTransferListener` that threw: the transfer is
durable regardless, but the application's session map may lag until the idempotent listener runs again —
`1718 GatewaySwapCompleted` (Debug: a client's shard attachment swapped seamlessly — how many subscriptions
re-scoped and held calls flushed; the client observed nothing), `1719 GatewaySwapFailed` (the swap could not
complete; the client converges through the ordinary resync path), `1720 GatewayHandoffQueueOverflow` (a
client queued more than the cap during one transfer; further calls get a retryable error),
`1721 GatewayPreopened` (Debug: a destination session opened on approach, so the swap is instant), and
`1722 HandoffRequestStale` (Debug: a transfer request dropped because the sender is not the entity's current
owner — the fencing rule applied to triggers, and the guard that stops a stale origin from re-importing the
past over the present) (10).

Since 0.1.0: `1105 InitReducersFired` and `1106 InitReducerFailed` — a fresh engine's seeding, and a seed
that threw and therefore created nothing (if that was a scheduled table's timer rows, that engine will never
tick) — and `1723 ShardHasNoTimerRows`, a shard that opened holding no rows in **any** scheduled table. The
last one is worth an alert in a world whose shards are created by players arriving: the state it names is
otherwise completely silent, because a shard with no timers serves reads and writes perfectly and simply
never ticks.

The planned drain (road-to-0.2 phase 13): `1724 ShardDrainCompleted` — the receipt, with the two step
durations that matter: quiesce (snapshot + close) and destination open (recovery) — `1725 ShardDrainFailed`
(the shard stayed with, or returned to, its origin; queued gateway calls flushed to the current owner),
`1726 ShardQuiesced` (node-side: the origin's half done, closed at which LSN), `1727 ShardDrainAborted`
(the hub abandoned a drain after quiesce; the origin reopens on its next heartbeat),
`1728 ShardDrainMarkExpired` — the self-healing bound for a hub that died between quiesce and reassign:
the origin's draining mark outlived `2 × Cluster:FailureTimeoutMs` and the shard reopened —
`1729 ShardDrainApplyPushFailed` (the fast-path assignments push to the destination failed; membership
already records the move, so the destination opens on its own next heartbeat), and
`1730 GatewayDrainQueueTimedOut`, a wedged drain's queue flushed after `Cluster:DrainQueueTimeoutMs` —
non-zero occurrences of this one mean drains are exceeding the deployment's stated patience and the cap or
the shard size needs a look.

The rebalance loop: `1731 RebalanceMoving` — the decision with its arithmetic: origin's sustained
utilization, the moving shard's own, the target's — `1732 RebalanceSingleShardHot` and
`1733 RebalanceNoFit`, the two ways a hot node the loop refuses to churn is reported (rate-limited to
`Cluster:ShardMoveMinIntervalMs`). **1732 is the granularity guardrail** and worth an alert: it names a
node whose whole load lives in one shard — the ceiling no cluster size changes, and the signal that the
strategy's split lines were drawn too coarse (docs/design/elastic-rebalancing.md). `1734
RebalanceEvaluationFailed` is a tick that threw and was skipped; recurring, it means the loop is
effectively off and nobody turned it off.

The capacity seam (phase 14): `1735 ProvisionTicketIssued` and `1736 ProvisionFulfilled` bracket a
normal scale-out, the latter carrying the provision latency the histogram records. `1737
ProvisionTicketExpired` is one strike; **`1738 ProvisionGaveUp` is the operator alert** — two
attempts failed or expired, the loop has deliberately stopped asking (money is involved), and the
`melange-capacity` health check goes unhealthy until a ticket-named node joins. `1739
ProvisionLateArrivalDecommissioned` / `1743 ProvisionLateArrivalKept` are the at-least-once
contract's two endings for a node that arrives after its ticket expired. `1740
ProvisionerCallFailed` is the seam's user code throwing (isolated — the fleet degrades to fixed,
never the hub to dead). `1741 ProvisionSkippedGranularity` is 1732's capacity-shaped sibling —
every node hot but shards no longer outnumber nodes, so a new node could receive nothing — and
`1742 ProvisionAtCeiling` is `Cluster:MaxNodes` doing its job while every node is hot: the two
rate-limited "provisioning will not help / is not allowed" reports, both worth an operator's eyes.

Scale-in: `1744 ScaleInStarting` carries the whole decision's arithmetic — the fleet's aggregate
sustained load, what it comes to per node on one node fewer, the cold threshold, and the victim
with its shard count. `1745 ScaleInDecommissioning` is the last-moment re-check passing — the node
owns nothing and the fleet is still cold — and `1746 ScaleInAborted` is any consolidation stopping
short (the fleet warmed, a drain failed, the re-check refused): the node stays and nothing was
lost, so an occasional 1746 is the hysteresis working, while a *recurring* one means the fleet
hovers at the cold boundary and the thresholds deserve a look.

The online backup (phase 15): `1801 BackupStreamStarted` and `1802 BackupStreamCompleted` (bytes,
duration, and the fenced LSN) bracket a normal stream; the duration doubles as the truncation
pin's hold time — `melange.backup.duration` is the same number as a histogram, and a growing one
means archives are outgrowing the window the snapshot interval leaves them. **`1803
BackupStreamAborted`** is the bound doing its job: a client stalled past
`Backup:StreamStallTimeoutMs` (or disconnected) and the connection was cut with the pin released —
a wedged backup client must not become a full disk. On the restore side, **`1608
PostgresCheckpointAhead`** joins 1605: the applier's checkpoint sits past the log's head within
one epoch — a data directory swapped for an older copy that kept its epoch — refused loudly with
the remediation in the message, exactly as the epoch-mismatch refusal (which is what an actual
`melange restore` produces, since restore always mints a fresh epoch). Both are the events an
operator greps for at 3 a.m. after a restore; both messages print the way out.

**`1804 CloneProvenance`** (phase 19) is the boot banner of a cloned world: the source epoch, the
captured head LSN, the archive it came from, and when it was captured and cloned — read back from
the `melange.provenance.json` sidecar `melange clone` leaves behind. Information rather than a
warning, because a clone is deliberate; unconditional, because "which world is this, and how
stale?" gets asked at the worst possible moment, and a server that answers in its own startup log
answers faster than any runbook. Its absence is equally informative: a world with no 1804 is not a
clone. The restore check's two rungs raise no EventId of their own — they are tools that return a
report or throw, and their verdict is the exit code a CI job alerts on.

The hub's `ClusterMetrics` also carries the handoff counters the phase-10 acceptance tests read:
`HandoffsStarted`, `HandoffsCompleted`, `HandoffsAborted`, `HandoffsUnresolved` (import fate unknowable when
the coordinator gave up; a reconciler resolves each later), `HandoffsRateLimited`, and the `HandoffsInFlight`
gauge. Handoff *rate* is the first counter over time; a growing `HandoffsUnresolved` with a flat
`HandoffsCompleted` means reconcilers cannot reach the truth — look at the node links.

Cluster link traffic is additionally counted per message type in `ClusterMetrics`, as messages **and payload
bytes** in each direction (phase 10 added the bytes). Border-band bandwidth is read straight off it: sum the
`border-apply` / `border-reset-apply` types received on a node to see what holding its neighbours' edges
costs, and the `border-batch` / `border-reset` types sent to see what serving its own edges costs.

The **shard ownership map** is a first-class query: `MelangeClusterCoordinator.OwnershipMap()` returns every
shard, its owning node, and its fencing term, straight from the membership store — the same truth the hub
routes by, so debugging "who owns this?" is one call, not a log archaeology session. **Per-shard transaction
rate and lag** come from the fact that a shard *is* an engine: each per-shard engine emits the standard
engine telemetry (transaction counters, applier lag, log head) under `Telemetry:*` exactly as a single node
does, and a shard's log head LSN over time is its transaction rate.

## Health checks

Standard `IHealthCheck` registrations, since they're nearly free once the metrics exist. These require a DI
host to register into, so the first one landed with phase 02's host integration rather than phase 01.
`AddMelangeDb` registers each check automatically; the host only opts into health checks at all
(`AddHealthChecks()`).

| Check | Unhealthy when | Phase |
| --- | --- | --- |
| `melange-log` | The commit log is unwritable or out of disk — concretely, before startup opens it, or once a failed append has poisoned it (**shipped, 02**) | 02 |
| `melange-applier` | Any applier's lag exceeds `HealthChecks:ApplierLagThreshold` (**shipped, 08**) — the silent-stall alarm in health-endpoint form; `melange.applier.lag` is its metric form | 08 |
| `melange-shard` | This node's shard ownership is contested — its hub lease expired and it has self-fenced (**shipped, 09**). Degraded while registered but owning nothing; healthy on hubs and single-node deployments, where ownership is not a question. | 09 |
| `melange-capacity` | Two provision attempts failed or expired and the loop has stopped asking (**shipped, 14**) — EventId 1738 in health-endpoint form, cleared when a ticket-named node joins. Degraded while a ticket is outstanding; healthy off the hub or with no provisioner registered. | 14 |
| `melange-retention` | More records are pinned above the log's truncation floor than `HealthChecks:RetentionPinnedThreshold` allows (**shipped, 18**) — the disk-filling alarm, with the governing floor's name and LSN in the description. `melange.log.pinned_records` is its metric form. Healthy trivially when snapshots or truncation are not configured, and while no truncation has been decided yet. The applier check overlaps by one holder deliberately: an applier is one of seven, and it fires far earlier at its own threshold. | 18 |

## Runbook: the log is growing — who is holding it

The commit log is the system of record, so everything that still needs old records keeps them, and each of
those holders is a **truncation floor**. Every one of them is *supposed* to pin the log — briefly. The failure
is one of them pinning it for days: a crashed event subscriber that never checkpoints again, a stalled
applier, a handoff marker orphaned by a bug, a backup pin whose stream died. The symptom is always the same
disk. This is the sequence of looks that names the cause.

1. **Is anything holding it at all?** `melange.log.pinned_records`. Flat and small is a healthy log; a line
   climbing without ever falling back is a floor that has stopped moving. Note that it reaches one
   `Snapshots:IntervalTransactions` in normal operation — the head advances between snapshots — so the shape
   to react to is *never returning to the floor*, not the height alone.
2. **Which holder?** `melange.log.truncation_floor`, broken out by the `floor` tag. The lowest series is the
   one governing. `snapshot` being lowest means nothing is holding the log back at all; anything else names a
   mechanism: `event-bus` (the slowest live subscriber's checkpoint), an applier name such as `postgres`,
   `resume-window`, `backup-pin`, `shard-freeze` / `shard-import` (a handoff saga that never resolved),
   `cluster-events`, or `unnamed` for a floor some third-party code registered without naming.
3. **Confirm against the log line.** Every truncation decision says the same thing at Information level:
   `1503 LogTruncated` when records went, `1510 LogTruncationPinned` when the decision removed nothing because
   a floor pinned it. Both carry `FloorName`, `FloorLsn`, `PinnedRecords`, and `LogBytes`. A stream of 1510
   with one unchanging `FloorLsn` is the diagnosis; the byte figure is what to compare against the disk.
4. **Then work the named holder, not the log.** `event-bus` → a subscriber that never checkpoints, which the
   bus's own checkpoint expiry (EventId `1403 SubscriberCheckpointEvicted`) resolves on its own timescale; an
   applier name → `melange.applier.lag` and the `melange-applier` check, which say why that projection
   stopped; `backup-pin` → a backup stream that never finished (`Backup:StreamStallTimeoutMs`);
   `shard-freeze` / `shard-import` → an unresolved handoff saga, visible in the cluster events;
   `resume-window` → `Resume:RetentionWindowSeconds` is simply larger than the truncation interval, which is
   configuration rather than a fault.

`melange-retention` is the same finding as an alert: unhealthy once the pinned distance passes
`HealthChecks:RetentionPinnedThreshold`, with the governing floor named in its description. Truncation
floors are never evicted automatically — expiring an abandoned holder is policy the event bus owns for its
own checkpoints, and generalizing it would let observability grow teeth it deliberately does not have.

## Standing requirement

Every phase instruments what it adds. A phase is not done if its failure modes are invisible — which is why
several phases' done-criteria name specific metrics rather than saying "add telemetry."
