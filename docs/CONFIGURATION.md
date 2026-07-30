# Configuration reference

Every configurable setting MelangeDB exposes, in one place.

> **The rule:** when a phase introduces a configuration item, it is added to this document **in the same
> change** that introduces it. A setting that isn't listed here doesn't exist as far as users are concerned —
> undocumented knobs are how a library becomes folklore.

This applies to every phase, not just the one that created this file.

## Status

Every row is `planned` until the phase that owns it lands, at which point its status becomes `shipped` and its
default is verified against the code rather than the plan. Treat this as a design register that becomes a
reference.

**Shipped as of phase 01** (defaults verified against `MelangeDbOptions` and friends in `MelangeDB.Core`):
`HotStore:Path`, `CommitLog:Path`, `CommitLog:FsyncPolicy`, `CommitLog:FsyncIntervalMs`, `Telemetry:Enabled`,
`Telemetry:IncludeCallerIdentity`, `Telemetry:IncludeReducerArguments`. `HotStore:Path` is created and
reserved by the engine; the in-memory store persists nothing in it until the paging engine lands in phase 07.

**Shipped as of phase 02**: `Validation:RejectNonFiniteFloats`, `Validation:MaxStringLength`,
`Validation:MaxCollectionLength`, and `Telemetry:SlowReducerMs` (defaults verified against
`ValidationOptions`/`TelemetryOptions`). Phase 02's `AddMelangeDb` also delivered the binding itself: the
whole `MelangeDb:` section binds through `IOptions<T>`/`IOptionsMonitor<T>`, so `appsettings.json`,
environment variables, and Azure App Configuration all work with no MelangeDB-specific code, and `live` keys
reach the running engine through the hosted service's reload bridge — a configuration change takes effect on
the next operation with no restart. One doc correction made when it shipped: `Diagnostics:EmitGeneratedFiles`
is a build-time switch and lives in the project file, not in `appsettings.json` — see its row.

**Shipped as of phase 03** (defaults verified against `TransportOptions`, `SubscriptionsOptions`, and
`ResumeOptions`): every `Transport:*` key, every `Subscriptions:*` key, `Resume:RetentionWindowSeconds`, and
`Telemetry:DeltaSpanSampleRatio`. One doc correction made when it shipped: the default of
`Subscriptions:BackpressurePolicy` is **`DropAndResync`**, not the `Buffer` this register originally planned —
`MaxBufferedBytes` exists to bound per-connection memory, and a default that buffers without bound past its own
trigger would make the trigger meaningless. `Buffer` remains as an explicit opt-in for trusted links.

**Shipped as of phase 04** (defaults verified against `AuthOptions`, `PoliciesOptions`, `RateLimitOptions`,
and `SqlOptions`): every `Auth:*`, `Policies:*`, and `RateLimit:*` key, plus `Sql:AdHocMode` — shipped ahead
of its phase-08 row because `/melange/sql` already returns rows, so the policy contract could not wait. Three
doc corrections made when it shipped: (1) the planned `Auth:Authority` and `Auth:Audience` keys were
**removed** rather than implemented — token validation reads the host's own JWT bearer scheme registration
(`Auth:Scheme`, a new key), and duplicating the host's authority/audience settings here would eventually
disagree with them, the same reasoning as the deliberate absence of TLS knobs; (2)
`Policies:DefaultReducerPosture` is `live`, not the `restart` this register planned — it is read per call
through the options monitor, and a cheaper-than-planned semantic is recorded, not rounded down; (3)
`Policies:UnpolicedReducerReport` stays `restart` as planned (it acts at startup only).

## Conventions

- **Everything lives under the `MelangeDb:` configuration section**, so a host can bind it from
  `appsettings.json`, environment variables (`MelangeDb__HotStore__Path`), Azure App Configuration, or any
  other `IConfiguration` provider with no MelangeDB-specific code. That is the point of the DI-first design —
  configuration is not a special case.
- **Bound through `IOptions<T>` / `IOptionsMonitor<T>`.** Anything marked hot-reloadable is read through
  `IOptionsMonitor<T>` and takes effect without a restart; everything else is read once at startup.
- **The builder API is sugar over configuration, never a parallel path.** `melange.UseHotStore(o => ...)` sets
  the same options object the configuration section binds, so there is exactly one source of truth and code
  wins over file only because it runs last.
- **Reload semantics:**
  - `live` — takes effect on the next operation.
  - `restart` — read at startup; changing it later requires a restart.
  - `careful` — can change at runtime, but changing it has a cost (rebuilding an index, faulting a table into
    memory) and is rate-limited.

## Core — storage and log

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `HotStore:Path` | string | `./data/hot` | restart | 01 | Directory for the hot store's files. |
| `HotStore:Engine` | enum | `Auto` | restart | 07 | `InMemory` \| `Faster` \| `Auto`. `Auto` picks `Faster` when the FASTER storage package is registered, else `InMemory` — selection by registration, not by path, since `HotStore:Path` always has a default. `InMemory` is a legitimate choice, not just a test double. |
| `HotStore:MemoryBudgetBytes` | long | — | restart | 07 | Cap on the paging buffer pool. **Excludes** resident tables, which are accounted separately — total footprint is this plus the residency report. |
| `CommitLog:Path` | string | `./data/log` | restart | 01 | |
| `CommitLog:FsyncPolicy` | enum | `OnCommit` | live | 01 | `OnCommit` \| `Interval` \| `OsBuffered`. `OnCommit` is the only durable choice; the others trade a bounded window of committed-but-lost transactions for throughput and must say so in their XML docs. |
| `CommitLog:FsyncIntervalMs` | int | `100` | live | 01 | Only read when `FsyncPolicy = Interval`. This is the size of the data-loss window. |
| `CommitLog:GroupCommit` | bool | `true` | live | 07 | Batch concurrent commits into one fsync. Improves throughput without weakening durability. Deliberately **not** phase 01 — group commit is an optimization phase 01 explicitly defers; 01 only makes the fsync policy configurable. |
| `Snapshots:Enabled` | bool | `true` | live | 07 | |
| `Snapshots:IntervalTransactions` | long | `100000` | live | 07 | |
| `Snapshots:TruncateLog` | bool | `true` | live | 07 | Truncation never passes the slowest applier or event-subscriber checkpoint regardless of this setting. |

## Residency

Per-table residency, overriding the `[Table(Residency = ...)]` attribute in code.

```json
{
  "MelangeDb": {
    "Residency": {
      "ItemDefinition": "Resident",
      "PlacedBuilding": "Resident",
      "TerrainChunkData": "Paged"
    }
  }
}
```

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Residency:<TableName>` | enum | attribute value | careful | 07 | `Resident` \| `Paged` \| `Auto`. Overrides the attribute. |
| `Residency:Default` | enum | `Paged` | restart | 07 | The global default. **Leaving this at `Paged` is deliberate** — a resident-by-default store reproduces the RAM ceiling MelangeDB exists to remove, and does it as a cliff that arrives under production load. |
| `Residency:AutoThresholdBytes` | long | `8388608` | restart | 07 | Only read for tables set to `Auto`. |
| `Residency:ReportOnStartup` | bool | `true` | restart | 07 | Logs each resident table's row count and measured bytes, plus the total. The memory budget has to be observable, not theoretical. |

**Why this is configurable and not code-only:** the right residency set depends on deployment size — a 2km test
world and a 20km production world want different answers — and an operator hitting a slow scan should be able
to fix it without a code change and a redeploy.

## Relational tier

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Postgres:ConnectionString` | string | — | restart | 08 | Absent means no relational tier. A deployment with no relational tables needs no Postgres at all. |
| `Postgres:Schema` | string | `melange` | restart | 08 | |
| `Postgres:ApplyBatchSize` | int | `100` | live | 08 | Log records per Postgres transaction. The applier checkpoint advances only with the batch, so batching stays correct. |
| `Postgres:AutoMigrate` | bool | `false` | restart | 08 | Off by default: schema changes against a production database should be deliberate. |
| `Sql:AdHocEnabled` | bool | `false` | live | 08 | |
| `Sql:AdHocMode` | enum | `PolicyEnforced` | live | 04 | `PolicyEnforced` \| `Owner`. There is no third mode and no default-to-owner — ambiguity here is a security hole. Shipped with 04 because `/melange/sql` already returns rows; `PolicyEnforced` applies row and column policies exactly as a subscription would, `Owner` deliberately bypasses them, and `[ServerOnly]` columns are excluded in **both** modes. Per-caller owner authorization lands with 08's full contract. |

## Transport and subscriptions

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Transport:Path` | string | `/melange` | restart | 03 | |
| — | — | — | — | 03 | **There is deliberately no MelangeDB setting for HTTP version or TLS.** Those come from the host's Kestrel listener (`HttpProtocols.Http1AndHttp2AndHttp3`, default-on since .NET 8). MelangeDB maps an endpoint; it doesn't own a listener. Adding a knob here would duplicate the host's configuration and eventually disagree with it. |
| `Transport:MaxMessageBytes` | int | `4194304` | live | 03 | |
| `Transport:Serializer` | enum | `MessagePack` | restart | 03 | Behind `IMelangeSerializer`. |
| `Validation:RejectNonFiniteFloats` | bool | `true` | live | 02 | Rejects `NaN` / `±Infinity` reducer arguments during decode. A `NaN` position propagates through terrain and chunk math and poisons rows that then replicate to every client. Turning this off should feel alarming. |
| `Validation:MaxStringLength` | int | `4096` | live | 02 | |
| `Validation:MaxCollectionLength` | int | `4096` | live | 02 | |
| `Transport:CompressionEnabled` | bool | `true` | restart | 03 | `permessage-deflate`. Terrain blobs are already RLE-compressed; delta frames of many small rows are what benefit. |
| `Transport:HeartbeatIntervalMs` | int | `15000` | live | 03 | |
| `Transport:HeartbeatTimeoutMs` | int | `45000` | live | 03 | A closed socket is not the only way a client goes away; this is what makes `ClientDisconnected` fire on ungraceful drops. |
| `Transport:HttpEndpointsEnabled` | bool | `true` | restart | 03 | One-shot reducer calls, bulk ingestion, tickets. WebSocket is the wrong shape for CLI tools and admin consoles. |
| `Transport:MaxInitialSetChunkBytes` | int | `262144` | live | 03 | Large initial sets are chunked and interleaved so a 30MB terrain subscription can't block a movement reducer response. |
| `Resume:RetentionWindowSeconds` | int | `300` | live | 03 | How far back a reconnecting client can resume. Too small and every blip becomes a full resync; too large and it fights log compaction. |
| `Subscriptions:MaxPerConnection` | int | `64` | live | 03 | |
| `Subscriptions:BackpressurePolicy` | enum | `DropAndResync` | live | 03 | `DropAndResync` \| `Buffer` \| `Disconnect`. Applied when buffered deltas exceed `MaxBufferedBytes`: drop the delta stream and tell the client to re-establish (default, bounded memory), keep buffering (explicit opt-in, unbounded), or close the socket. Matters most during bulk terrain streaming to a slow client. |
| `Subscriptions:MaxBufferedBytes` | long | `16777216` | live | 03 | Per connection; the trigger for the policy above. |
| `Subscriptions:MaxRowsPerSubscription` | long | `100000` | live | 03 | Ceiling on an initial result set. Rejected before execution, not mid-stream. |
| `Subscriptions:MaxBytesPerSubscription` | long | `67108864` | live | 03 | The one that actually matters for blob tables. |
| `Subscriptions:MaxRangeSpan` | long | `1024` | live | 03 | Maximum width of a `BETWEEN` predicate. Lets a client stream a ring around itself but not the whole map. |
| `Subscriptions:RequirePredicateOn` | string[] | — | live | 03 | Tables where an unbounded subscription is rejected. An entry is a table name (any predicate satisfies) or `Table.Column` (the predicate must constrain that column). Terrain and blob tables belong here. |

## Identity and policies

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Auth:Scheme` | string | `Bearer` | live | 04 | The host authentication scheme whose `JwtBearerOptions` connection tokens are validated against — `MapMelangeSocket` fails fast if it is not registered. |
| — | — | — | — | 04 | **There are deliberately no MelangeDB settings for authority, audience, or signing keys.** Those live on the host's own `AddAuthentication().AddJwtBearer(...)` registration — the IdP is the gate, and duplicating its settings here would eventually disagree with them (the same reasoning as the absent TLS knob above). The originally planned `Auth:Authority`/`Auth:Audience` rows were removed when phase 04 shipped; this row is the record. |
| `Auth:GuestRole` | string | `guest` | live | 04 | Role claim value marking IdP-issued guest tokens. **The IdP is the gate**: every connection presents a valid token, MelangeDB mints no identities, and guest-issuance throttling is the IdP's job. This setting only lets policies and caps treat guests differently; empty disables guest-specific treatment. |
| `Auth:TicketTtlSeconds` | int | `30` | live | 04 | Connect tickets are single-use and short-lived, so a leaked one is near-worthless. Exists because browsers cannot set WebSocket headers. |
| `Auth:ReauthGraceSeconds` | int | `120` | live | 04 | How long past token expiry a connection survives while awaiting `Reauthenticate`. Zero means expiry drops the socket — correct for a bank, wrong for a game. |
| `Auth:MaxConnectionsPerIdentity` | int | `4` | live | 04 | Without this, a valid token holds unlimited sockets, subscriptions, and rate-limit buckets. |
| `Policies:UnpolicedReducerReport` | enum | `Warn` | restart | 04 | `Off` \| `Warn` \| `Fail`. Lists client-callable reducers with no authorization policy at startup (EventId 1104). Turns "did we forget one?" into a build artifact; `Fail` refuses to start. |
| `Policies:DefaultReducerPosture` | enum | `Allow` | live | 04 | `Allow` \| `Deny`. `Deny` is safer but annotates every ordinary gameplay reducer; pair `Allow` with the report above. Governs client-originated calls only — in-process dispatch is the host's own code. |
| `RateLimit:Enabled` | bool | `true` | live | 04 | |
| `RateLimit:ReducerCallsPerSecond` | int | `20` | live | 04 | Default sustained rate; the bucket is per identity **per reducer**, so one spammed reducer cannot starve the rest of a player's actions. Rejected before a transaction opens, so it costs no log volume. Client-originated calls only. |
| `RateLimit:BurstCapacity` | int | `60` | live | 04 | Bursts pass at human click speed; sustained rates are what actually stop macros. |
| `RateLimit:PerReducer:<ReducerName>` | int | — | live | 04 | Per-reducer override of the global rate. |

## Scheduling

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Scheduler:Enabled` | bool | `true` | live | 05 | Off is useful for tooling processes that must not tick the world. |
| `Scheduler:OverrunPolicy` | enum | `Skip` | live | 05 | `Skip` \| `RunImmediately` \| `Coalesce`. `Skip` and log is the default because silent pile-up is how a simulation death-spirals under load. |
| `Scheduler:CatchUpAfterDowntime` | enum | `FireOnce` | live | 05 | `FireOnce` \| `CatchUpAll`. `FireOnce` is right for a simulation; `CatchUpAll` is right for billing. |
| `Scheduler:MaxConcurrentTicks` | int | `1` | live | 05 | Default 1 keeps transactions serialized. Raising it needs care. |

## Event bus

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Events:MaxQueueDepth` | int | `10000` | live | 06 | Bounded on purpose: a slow handler must not be able to grow memory without limit. |
| `Events:HandlerRetries` | int | `3` | live | 06 | |
| `Events:RetryBackoffMs` | int | `500` | live | 06 | |
| `Events:DeadLetterPath` | string | `./data/deadletter` | restart | 06 | |
| `Events:MaxPublishDepth` | int | `4` | live | 06 | Cycle guard for handlers that call reducers that publish. |
| `Events:SubscriberExpirySeconds` | int | `604800` | live | 06 | A checkpoint whose subscriber no longer exists (handler deleted, service retired) would pin log truncation forever — a full disk on a timer. Idle past this window, it is evicted with a loud log; a returning subscriber has lost its place and starts from current state. Seven days default. |

## Cluster

Ignored entirely by single-node deployments.

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Cluster:Enabled` | bool | `false` | restart | 09 | |
| `Cluster:Role` | enum | `Standalone` | restart | 09 | `Standalone` \| `Hub` \| `Shard`. |
| `Cluster:HubAddress` | string | — | restart | 09 | |
| `Cluster:Shards` | string | — | restart | 09 | **Seed** assignment only — ownership lives in the membership store, which failover updates when a dead node's shards are reassigned. "Static" means no load-based rebalancing, not immutable. |
| `Cluster:MembershipStore` | string | — | restart | 09 | Ownership registry. Likely the hub's Postgres rather than a new consensus dependency. |
| `Cluster:ShardSpanCheck` | enum | `ThrowInDevelopment` | live | 09 | `Off` \| `Warn` \| `ThrowInDevelopment` \| `Throw`. Catches the one contract MelangeDB cannot verify statically: rows mutated in one transaction must resolve to one shard. |
| `Cluster:FencingTokenTimeoutMs` | int | `5000` | live | 09 | Stops a wrongly-suspected-dead node from continuing to write a player it no longer owns. |
| `Cluster:BorderBandChunks` | int | `2` | careful | 10 | Deeper is smoother and costs bandwidth plus memory on every node. Default should be derived from movement speed and tick rate, not guessed. |
| `Cluster:HandoffHysteresisMeters` | float | `16` | live | 10 | Stops a player pacing across a boundary from triggering a handoff per step. |

## Diagnostics

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Diagnostics:ReportApplierLag` | bool | `true` | live | 08 | A silently stalled Postgres applier — writes succeeding while the tier falls hours behind — is the dangerous failure mode. Not optional in practice. |
| `Diagnostics:EmitGeneratedFiles` | bool | `false` | restart | 02 | Writes source-generator output to disk. Incremental generators fail obscurely; this pays for itself. **Realized as the standard MSBuild property `<EmitCompilerGeneratedFiles>` in the consuming project file** — it acts at compile time, so it cannot be an `appsettings.json` key; the tests and sample set it, and output lands under `obj/.../generated`. |

## Telemetry

MelangeDB emits `ActivitySource` and `Meter` signals named `MelangeDB`; the **host** configures exporters and
sampling through OpenTelemetry's own configuration. These settings only control what MelangeDB emits, never
where it goes. See [OBSERVABILITY.md](OBSERVABILITY.md).

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Telemetry:Enabled` | bool | `true` | restart | 01 | Off short-circuits instrumentation entirely. With no listener attached the cost is already negligible, so this is for the paranoid rather than the pragmatic. |
| `Telemetry:IncludeCallerIdentity` | bool | `true` | live | 01 | Adds caller identity to **spans only** — never a metric dimension, which would be one time series per player. Turn off where identity is a privacy requirement. |
| `Telemetry:IncludeReducerArguments` | bool | `false` | live | 01 | Off by default: arguments can contain anything, including secrets, and the commit log already records them. |
| `Telemetry:DeltaSpanSampleRatio` | double | `0.01` | live | 03 | `melange.subscription.delta` is the highest-frequency operation in the system; tracing every one at full rate would cost more than the work. |
| `Telemetry:SlowReducerMs` | int | `50` | live | 02 | Reducers over this threshold get a span event and a log entry. |
| `HealthChecks:ApplierLagThreshold` | long | `10000` | live | 08 | Transactions behind before the `melange-applier` check reports unhealthy. |
