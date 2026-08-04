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
| `melange.commit` | 01 | `melange.lsn`, `melange.writeset.bytes` | The critical section; a child `melange.fsync` span isolates durability cost from serialization cost. |
| `melange.apply` | 01 | `melange.applier` | One per applier, per batch. |
| `melange.subscription.initial` | 03 | `melange.table`, `melange.rows`, `melange.bytes` | The expensive half of a subscription; worth its own span. |
| `melange.subscription.delta` | 03 | `melange.table`, `melange.subscribers` | Sampled — this is the highest-frequency operation in the system and must not be traced per row op at full rate. |
| `melange.event.handle` | 06 | `melange.event.type`, `melange.handler` | **Linked, not parented** — see below. |
| `melange.scheduler.tick` | 05 | `melange.reducer.name`, `melange.shard` (attribute from 09) | A tick has no client parent, so it starts a new trace. |
| `melange.handoff` | 09 | `melange.shard.from`, `melange.shard.to` | Spans two processes. This is where distributed tracing earns its keep. |

A `melange.reducer` span whose duration exceeds `Telemetry:SlowReducerMs` additionally carries a
`melange.slow_reducer` span event (with `melange.duration_ms`) and produces a warning log entry — shipped
with phase 02, threshold live-reloadable.

**Read reducer duration as global write latency.** The engine's write lock is held across the entire
transaction — body, commit guards, append, fsync, commit observers, and any automatic snapshot the commit
triggers (see [DESIGN.md §4](DESIGN.md)). Every millisecond on a `melange.reducer` span is a millisecond in
which no other transaction on that engine could start. So `Telemetry:SlowReducerMs` is not "how slow is too
slow for this caller" but **"how long is it acceptable to freeze the world"**, and a `melange.reducer`
histogram is the closest thing the system has to a write-stall metric. Reads are exempt: subscription fan-out
and committed reads take no lock and keep serving throughout.

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
| `melange.writeset.rows` | histogram | `{row}` | `reducer` | 01 |
| `melange.log.head_lsn` | gauge | `{lsn}` | — | 01 |
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

## Logs

Structured through `ILogger` with stable `EventId`s so log-based alerts don't break on message rewording.
No parallel logging abstraction — the host's configured providers are the whole story.

Stable ids so far: `1001 TornRecordTruncated`, `1002 AppendRollbackFailed` (01); `1003 SlowReducer`,
`1101 MelangeStarted`, `1102 MelangeStopped` (02); `1005 CommitObserverFailed`, `1203 HeartbeatTimeout`,
`1204 ReducerCallFailed` (03); `1104 UnpolicedReducers` (04); `1205 LifecycleReducerFailed`,
`1301 SchedulerOverrun`, `1302 SchedulerTickFailed` (05); `1401 EventHandlerRetry`, `1402 EventDeadLettered`,
`1403 SubscriberCheckpointEvicted` — the loud eviction the expiry design promises — and
`1404 SubscriberLostPlace`, how a returning subscriber is told it starts from current state (06);
`1501 ResidencyReport` — the startup residency report: per resident table row count and measured bytes, the
buffer-pool cap, and the total they sum to — `1502 SnapshotWritten`, `1503 LogTruncated` (naming the floor it
respected), `1504 SnapshotFailed` (an automatic snapshot failing must not fail the committed transaction),
`1505 AutoResidencyDemoted` — an `Auto` table crossing its threshold is the cliff arriving, and it announces
itself — `1506 StaleSnapshotIgnored`, `1507 ResidencyChangeFailed`, and `1508 ResidencyChanged`, the careful
per-table override being applied at runtime (07); `1601 PostgresApplierStalled` — the loud stall the phase-08
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

## Standing requirement

Every phase instruments what it adds. A phase is not done if its failure modes are invisible — which is why
several phases' done-criteria name specific metrics rather than saying "add telemetry."
