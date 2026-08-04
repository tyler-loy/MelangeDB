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

**Shipped as of phase 05** (defaults verified against `SchedulerOptions`): every `Scheduler:*` key. Two doc
corrections made when it shipped: (1) `Scheduler:CatchUpAfterDowntime` is **`restart`**, not the `live` this
register planned — it is read exactly once, at scheduler start after recovery, which is the only moment
downtime catch-up can mean anything, and a "live" label on a startup-only key would be an empty promise;
(2) `Scheduler:MaxConcurrentTicks` shipped **accepted-and-reserved at its default of 1**: the scheduler is
a single-threaded dispatch loop on purpose, because reducer transactions serialize end to end on the
engine's single-writer lock — which covers each body, not merely each commit — and a tick worker pool
would parallelize nothing that matters (see
docs/road-to-0.1/plan-phase-05.md, scheduler fairness). Values above 1 bind and validate but do not change dispatch.

**Shipped as of phase 06** (defaults verified against `EventsOptions`): every `Events:*` key. Two notes made
when it shipped: (1) `Events:RetryBackoffMs` is the **base of an exponential backoff** — each retry doubles it,
capped at 30 seconds — not a fixed delay, and the doc now says so; (2) `Events:MaxQueueDepth` bounds the
in-memory delivery *window*, not delivery itself: overflow evicts the oldest window entries and a lagging
subscriber replays from the commit log, which is the buffer that actually holds the events. Nothing is dropped;
the subscriber's checkpoint lag is the honest measure of how far behind it is.

**Shipped as of phase 07** (defaults verified against `HotStoreOptions`, `ResidencyOptions`, and
`SnapshotsOptions`): `HotStore:Engine`, `HotStore:MemoryBudgetBytes`, every `Residency:*` key, every
`Snapshots:*` key, and `CommitLog:GroupCommit`. Three notes made when it shipped: (1)
`HotStore:MemoryBudgetBytes` planned no default and shipped with a real one — `134217728` (128 MiB) —
because a paging store with an unset cap is unbounded, which is the failure mode this phase exists to
remove. (2) `CommitLog:GroupCommit` shipped **accepted-and-reserved** at its default of `true` (the
`Scheduler:MaxConcurrentTicks` precedent): the engine's single-writer lock is held across each whole
transaction, body included, so no two appends are ever in flight for one fsync to cover — the
bulk-ingestion path is the batching that actually exists; the knob binds and validates so a future
concurrent commit path can honor it without a config break. (3) `Residency:Default` is consulted only for tables whose attribute leaves residency
unspecified — and because `Paged` is the attribute's default value, an attribute explicitly declaring
`Paged` is indistinguishable from silence; under a non-`Paged` configured default, the per-table
override is how a table is pinned back down. `Residency:<TableName>` is `careful` as planned: a
changed override applies to the running store per table (pinning faults the table wholly in,
unpinning migrates it to the buffer pool) when the store supports runtime residency control; the
in-memory store, which does not page, takes the label at restart.

**Shipped as of phase 08** (defaults verified against `PostgresOptions`, `SqlOptions`, `DiagnosticsOptions`,
and `HealthChecksOptions`): every `Postgres:*` key, `Sql:AdHocEnabled`, `Sql:OwnerRole` (a new key — see below),
`Diagnostics:ReportApplierLag`, and `HealthChecks:ApplierLagThreshold`. Three notes made when it shipped:
(1) `Sql:AdHocEnabled` gates the whole `/melange/sql` endpoint — off answers `403 sql_disabled` — and shipped
at its planned default of `false`, which is a behavior change from phases 04–07, where the endpoint answered
unconditionally; the register's row was always the contract, the endpoint simply predated it. (2) The
owner-mode per-caller authorization deferred from phase 04 landed as **a role claim**, `Sql:OwnerRole`,
following the `Auth:GuestRole` precedent — the IdP is the gate, owner capability is a claim it issues, and in
`Owner` mode a caller without the claim is refused (`403 owner_required`), never silently downgraded to
policy-enforced. (3) `Diagnostics:ReportApplierLag` gates only the *periodic re-logging* of a continuing
stall; the first stall (EventId 1601) and the recovery (1602) always log, because a silent stall is the
failure mode the phase exists to prevent.

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

**Shipped as of phase 09** (defaults verified against `ClusterOptions`): every `Cluster:*` key in the table
below. The register's planned rows were reshaped when they met the implementation, and the corrections are the
design record: (1) `Cluster:Enabled` was **removed** — `Cluster:Role = None` (the default) *is* the off
switch, and two switches that can disagree are one too many; the planned `Standalone` value is spelled `None`.
(2) `Cluster:Shards` (a seed assignment list) was **removed**: shards are created at runtime
(`MelangeClusterCoordinator.EnsureShard`, or implicitly by the gateway routing to a new instance) and
ownership lives only in the membership store — a config-file copy of it would be a second source of truth.
(3) `Cluster:MembershipStore` is **not a configuration string**: the store is a DI registration —
`AddMelangeCluster()` defaults to in-memory, `AddPostgresClusterMembership()` opts into the hub's Postgres —
because a store is a component with a connection, not a name. (4) `Cluster:ShardSpanCheck` shipped as
`DebugOnly | Always | Off` rather than the planned four values: `Warn` was dropped (a warning about a
distributed commit on the hot path is a page nobody reads until it is an outage), and `ThrowInDevelopment`
is spelled `DebugOnly`, probing the entry assembly's build configuration. (5) The planned
`Cluster:FencingTokenTimeoutMs` is spelled `Cluster:FailureTimeoutMs`, because one number serves both sides
by design: the hub suspects a node dead after this much silence, and the node self-fences its writes on the
same clock — which is exactly what stops a wrongly-suspected-dead node from writing players it no longer
owns. It is `restart`, not the planned `live`: the two sides must agree on the value, and nodes learn it at
registration.

**Shipped as of phase 10** (defaults verified against `ClusterOptions`): `Cluster:BorderBandChunks`,
`Cluster:HandoffMarginChunks`, and `Cluster:HandoffMinIntervalMs`. The register's planned rows were reshaped
when they met the implementation: (1) the planned `Cluster:HandoffHysteresisMeters` is spelled in **chunks**,
not meters — MelangeDB never learns the world's metric scale (chunk decoding is the developer's
`SpatialGeometry`), so a meters knob would have been a unit the library cannot interpret — and it split into
the margin (`HandoffMarginChunks`, the crossing depth that triggers) and the rate limit
(`HandoffMinIntervalMs`, the floor between an entity's transfers), because hysteresis needs both a distance
and a time to bound pacing. (2) `Cluster:BorderBandChunks` shipped at its planned default of 2 with the
derivation documented rather than guessed (margin + one handoff window of travel; docs/road-to-0.1/plan-phase-10.md
shows the arithmetic), validated loudly at strategy construction (`≥ 1`, `> HandoffMarginChunks`, `≤` the
block dimension) and clamped on live reads — `careful` because deepening it only fully materializes on the
next border re-subscribe, when the owner sends a full band reset.

**Shipped with issue #31** (defaults verified against `BulkOptions`): `Bulk:Enabled` and `Bulk:OwnerRole` —
the bulk ingestion gate. A behavior change from phases 03–12, where `/melange/bulk` answered any valid
bearer token: bulk writes bypass every reducer and its policies, so the endpoint now follows the `Sql:*`
posture — off unless opted into, and owner-role-gated when on. See the Bulk ingestion section below.

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
| `HotStore:MemoryBudgetBytes` | long | `134217728` | restart | 07 | Cap on the paging buffer pool. **Excludes** resident tables, which are accounted separately — total footprint is this plus the residency report. Planned with no default; shipped with 128 MiB, because an unset cap is unbounded. Ignored by the in-memory engine, which does not page. |
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
| `Postgres:ConnectionString` | string | — | restart | 08 | Absent means no relational tier. A deployment with no relational tables needs no Postgres at all; one that declares relational tables without configuring this runs anyway (rows stay in the hot store) and is told so loudly at startup (EventId 1607). |
| `Postgres:Schema` | string | `melange` | restart | 08 | The Postgres schema (namespace) relational tables and the applier checkpoint live in. The schema and the checkpoint table are always created — they are the tier's own plumbing; `AutoMigrate` governs user tables only. |
| `Postgres:ApplyBatchSize` | int | `100` | live | 08 | Log records per Postgres transaction. The applier checkpoint advances only with the batch — batch and checkpoint commit atomically — so batching stays correct. |
| `Postgres:AutoMigrate` | bool | `false` | restart | 08 | Off by default: schema changes against a production database should be deliberate. Governs only **additive** DDL (create table, add column); destructive disagreement is refused loudly (EventId 1604) in both settings. Off, the applier validates and stalls with the exact pending DDL in the log; running it manually recovers without a restart. |
| `Sql:AdHocEnabled` | bool | `false` | live | 08 | Gates the whole `/melange/sql` endpoint; off answers `403 sql_disabled`. Ad-hoc SQL is a tooling surface, and a deployment that never opted in should not be exposing one. |
| `Sql:AdHocMode` | enum | `PolicyEnforced` | live | 04 | `PolicyEnforced` \| `Owner`. There is no third mode and no default-to-owner — ambiguity here is a security hole. Shipped with 04 because `/melange/sql` already returns rows; `PolicyEnforced` applies row and column policies exactly as a subscription would, `Owner` deliberately bypasses them, and `[ServerOnly]` columns are excluded in **both** modes. Since 08, `Owner` additionally requires the caller's `Sql:OwnerRole` claim, may name private *relational-tier* tables, and is the only mode that runs aggregates. |
| `Sql:OwnerRole` | string | `melange-owner` | live | 08 | The role claim that authorizes a caller when `AdHocMode` is `Owner` — the per-caller half of the two-mode contract, per the `Auth:GuestRole` precedent (the IdP is the gate). A caller without it is refused (`403 owner_required`), never silently downgraded to policy-enforced. Empty makes owner mode unusable by everyone. |

## Bulk ingestion

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Bulk:Enabled` | bool | `false` | live | #31 | Gates the whole `/melange/bulk` endpoint; off answers `403 bulk_disabled`. Off by default because bulk ingestion is a trusted-pipeline surface: rows land with **no reducer policy, no argument validation, and no reducer-body invariants** — a deployment that never opted in should not be exposing one. |
| `Bulk:OwnerRole` | string | `melange-bulk-owner` | live | #31 | The role claim that authorizes a caller on `/melange/bulk`, per the `Sql:OwnerRole` precedent (the IdP is the gate). Deliberately a **distinct key from `Sql:OwnerRole`** — read-everything and write-anything are different capabilities; an operator who wants one god-role sets both keys to the same value. A caller without it is refused (`403 owner_required`), never silently downgraded. Empty makes bulk ingestion unusable by everyone. |

**Why this exists** (issue #31): `/melange/bulk` is the one write path where "the reducer is the
authorization boundary" does not hold — it bypasses all reducers at once, so a syntactically valid bearer
token (which, in a game, every player holds) must not be enough. `Transport:HttpEndpointsEnabled` still
turns off all plain-HTTP endpoints together; these keys gate bulk independently, so a host can serve `/sql`
to its admin console without also serving unauthenticated-in-effect bulk writes to its players.

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
| `Transport:HttpEndpointsEnabled` | bool | `true` | restart | 03 | One-shot reducer calls, bulk ingestion, tickets. WebSocket is the wrong shape for CLI tools and admin consoles. Bulk ingestion additionally requires its own opt-in — see the Bulk ingestion section. |
| `Transport:SchemaEndpointEnabled` | bool? | *(unset)* | live | 12 | Serves the module's [schema manifest](CLIENT-BINDINGS.md) at `{path}/schema` — the Swagger pattern. Unset follows the host environment (on in Development, off elsewhere); `true`/`false` overrides in either direction. Anonymous while on, by design: the manifest carries only what every client already receives. Off means a plain 404. |
| `Transport:MaxInitialSetChunkBytes` | int | `262144` | live | 03 | Large initial sets are chunked and interleaved so a 30MB terrain subscription can't block a movement reducer response. |
| `Resume:RetentionWindowSeconds` | int | `300` | live | 03 | How far back a reconnecting client can resume. Too small and every blip becomes a full resync; too large and it fights log compaction. |
| `Subscriptions:MaxPerConnection` | int | `64` | live | 03 | |
| `Subscriptions:BackpressurePolicy` | enum | `DropAndResync` | live | 03 | `DropAndResync` \| `Buffer` \| `Disconnect`. Applied when buffered deltas exceed `MaxBufferedBytes`: drop the delta stream and tell the client to re-establish (default, bounded memory), keep buffering (explicit opt-in, unbounded), or close the socket. Matters most during bulk terrain streaming to a slow client. |
| `Subscriptions:MaxBufferedBytes` | long | `16777216` | live | 03 | Per connection; the trigger for the policy above. |
| `Subscriptions:MaxRowsPerSubscription` | long | `100000` | live | 03 | Ceiling on an initial result set. Rejected before execution, not mid-stream. |
| `Subscriptions:MaxBytesPerSubscription` | long | `67108864` | live | 03 | The one that actually matters for blob tables. |
| `Subscriptions:MaxRangeSpan` | long | `1024` | live | 03 | Maximum width of a `BETWEEN` predicate. Lets a client stream a ring around itself but not the whole map. |
| `Subscriptions:RequirePredicateOn` | string[] | — | live | 03 | Tables where an unbounded subscription is rejected. An entry is a table name (any predicate satisfies) or `Table.Column` (the predicate must constrain that column). Terrain and blob tables belong here. |

## Client dispatch (`MelangeClientOptions`)

The client's knobs live on `MelangeClientOptions` — a client is constructed in code, not bound from the
`MelangeDb:` configuration section — but they follow the same rule as every server key: named here in the
change that adds them. Added by the frame-tick pump, issue #26.

| Option | Type | Default | Notes |
| --- | --- | --- | --- |
| `Dispatch` | `DispatchMode` | `Immediate` | `Immediate` \| `Manual`. `Immediate` applies data frames and raises events on the receive loop as they arrive — the pre-pump behaviour, unchanged for every existing consumer. `Manual` queues whole frames and applies them only inside `MelangeClient.FrameTick()`, on the caller's thread — the game-loop mode, for hosts (Godot, Unity) whose scene graph may only be touched from their own thread. A `Manual` client that is never ticked applies nothing and completes no subscribe/reconnect; `FrameTick` on an `Immediate` client throws, so the misconfiguration is loud either way. See [CLIENT-BINDINGS.md](CLIENT-BINDINGS.md), threading. |
| `DispatchQueueLimit` | int | `65536` | Manual dispatch only: the ceiling on entries (whole frames) queued between ticks. On overflow the client synthesizes a `dispatch_overflow` error at the **head** of the queue and aborts its own socket — never dropping a delta silently (the cache would diverge without a trace) and never blocking the receive loop (a blocked loop stops answering pings, and the server convicts the client as dead illegibly). The default is deliberately in the tens of thousands: at a sustained 1,000 commits/second it is over a minute of not ticking — far past any loading screen or alt-tab — while still bounding worst-case memory. Recovery is the ordinary `ReconnectAsync` resume path once the app ticks again: the cursor never advanced past the dropped frame, so the replay picks up exactly there. |

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
| `Scheduler:Enabled` | bool | `true` | live | 05 | Off is useful for tooling processes that must not tick the world. Timer rows are untouched while off; re-enabling fires whatever is due. |
| `Scheduler:OverrunPolicy` | enum | `Skip` | live | 05 | `Skip` \| `RunImmediately` \| `Coalesce`. Applied when a tick ran past its own interval: skip the missed fires and resume one interval after the slow tick (default — silent pile-up is how a simulation death-spirals under load), replay every missed fire back to back, or collapse them into one immediate fire. All three log (EventId 1301) and count `melange.scheduler.overruns`. |
| `Scheduler:CatchUpAfterDowntime` | enum | `FireOnce` | restart | 05 | `FireOnce` \| `CatchUpAll`, applied to repeating timers overdue at recovery. `FireOnce` is right for a simulation (the world was paused); `CatchUpAll` fires once per missed interval and is right for billing. Downtime is measured from the recovered log's tail record, since repeating timers persist no per-fire bookkeeping. Was planned `live`; corrected — it acts at scheduler start only. |
| `Scheduler:MaxConcurrentTicks` | int | `1` | live | 05 | Default 1 keeps transactions serialized. Shipped accepted-and-reserved: dispatch is a single-threaded loop because the engine's single-writer lock serializes tick transactions anyway; see the phase 05 note above. |

## Event bus

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Events:MaxQueueDepth` | int | `10000` | live | 06 | Bounded on purpose: a slow handler must not be able to grow memory without limit. Bounds the in-memory delivery window; overflow evicts the oldest entries and a lagging subscriber replays from the log — the log is the buffer, so nothing is lost. |
| `Events:HandlerRetries` | int | `3` | live | 06 | Retries after the first failed attempt; exhaustion dead-letters the event and delivery moves on. |
| `Events:RetryBackoffMs` | int | `500` | live | 06 | The **base** of an exponential backoff: each retry doubles it, capped at 30 seconds. |
| `Events:DeadLetterPath` | string | `./data/deadletter` | restart | 06 | One JSON line per poisoned event in `melange.deadletter.ndjson`: subscriber, event type, LSN, attempts, error, payload. |
| `Events:MaxPublishDepth` | int | `4` | live | 06 | Cycle guard for handlers that call reducers that publish. Each event carries its publish depth durably; a publish at the limit throws, aborting the reducer, and the handler failure dead-letters — the cycle ends loudly. |
| `Events:SubscriberExpirySeconds` | int | `604800` | live | 06 | A checkpoint whose subscriber no longer exists (handler deleted, service retired) would pin log truncation forever — a full disk on a timer. Idle past this window, it is evicted with a loud log; a returning subscriber has lost its place and starts from current state. Seven days default. |

## Cluster

Ignored entirely by single-node deployments.

Everything here is restart-only by design: a node's role, name, and addresses are its identity in the
cluster, and changing them live would be a different node. See the phase 09 status note for the planned keys
that were reshaped or removed when this shipped.

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Cluster:Role` | enum | `None` | restart | 09 | `None` \| `Hub` \| `Shard`. `None` (the default) is the whole off switch: a single-node deployment ignores placement entirely and behaves exactly as in M1. |
| `Cluster:NodeName` | string | `""` | restart | 09 | This node's stable name — the membership store's key for assignments and fencing. Required for shard nodes; the hub is `hub`. |
| `Cluster:Secret` | string | `""` | restart | 09 | The cluster secret: HMAC key behind node-link mutual authentication and hub-minted identity assertions. Required whenever `Role != None`; treat like a database password — see docs/THREAT-MODEL.md for the trust boundary it draws. |
| `Cluster:NodeListenPort` | int | `0` | restart | 09 | Hub only: the TCP port the node-link listener binds. `0` binds an ephemeral port (tests); production names one. |
| `Cluster:NodeListenAddress` | string | `127.0.0.1` | restart | 09 | Hub only: the interface the node-link listener binds. The default admits only same-machine nodes — safe by construction; a multi-machine cluster sets `0.0.0.0` or a specific internal interface. Every connection still proves the cluster secret, but widening the bind should be paired with network-level controls — see docs/THREAT-MODEL.md. |
| `Cluster:HubAddress` | string | `""` | restart | 09 | Shard only: the hub's node-link address as `host:port`. |
| `Cluster:PublicAddress` | string | `""` | restart | 09 | Shard only: the base HTTP address where this node's per-shard websocket endpoints are reachable **by the gateway**. Internal infrastructure — never handed to clients. |
| `Cluster:AssertionTtlSeconds` | int | `300` | restart | 09 | Cap on an internal identity assertion's lifetime (an assertion never outlives the client token it vouches for). Bounds how long a captured assertion stays redeemable. |
| `Cluster:HeartbeatIntervalMs` | int | `1000` | restart | 09 | How often a shard node heartbeats the hub. Heartbeats renew the node's lease and piggyback assignment changes. |
| `Cluster:FailureTimeoutMs` | int | `10000` | restart | 09 | One number, both sides: silence after which the hub suspects a node dead and reassigns its shards (bumping fencing tokens), and after which the node itself considers its lease expired and fences its own writes. The self-fencing half is what stops a wrongly-suspected-dead node from writing players it no longer owns. |
| `Cluster:ShardSpanCheck` | enum | `DebugOnly` | live | 09 | `DebugOnly` \| `Always` \| `Off`. Catches the one contract MelangeDB cannot verify statically: rows mutated in one transaction must resolve to one shard. `DebugOnly` probes whether the entry assembly is a Debug build; a violation throws `ShardSpanException` and aborts with zero trace. Also armed on single-node deployments that register an `IShardStrategy` and call `AddMelangeCluster()`. |
| `Cluster:ShardDataPath` | string | `./data/shards` | restart | 09 | Shard only: the root under which per-shard engines keep their commit logs and hot stores (`{ShardDataPath}/shard-{key}`). Must be storage every shard node can reach — reassignment means the new owner opens the shard's directory and recovers it from the shard's own log (phase 09 assumes shared or re-attachable volumes; log shipping is a later phase). |
| `Cluster:BorderBandChunks` | int | `2` | careful | 10 | Spatial strategy only: how deep each shard's read-only border band reaches into its neighbours, in chunks. Deeper is smoother and costs bandwidth plus memory on every node. The default is derived (docs/CLUSTERING.md shows the derivation): margin + the distance an entity covers during one handoff window — for the reference workload `1 + ceil(8 m/s x ~1 s / 64 m) = 2`. Must be ≥ 1 and > `HandoffMarginChunks` and ≤ the block dimension — validated loudly at strategy construction; live reads clamp instead of crashing a running node. `careful` because deepening it live only fully materializes on the next border re-subscribe (the owner then sends a full band reset). |
| `Cluster:HandoffMarginChunks` | int | `1` | live | 10 | Spatial strategy only: the hysteresis margin. An automatic handoff triggers only once an entity is *strictly more* than this many chunks past a block boundary, so after a transfer the entity must walk back through the whole margin before the reverse transfer can fire — pacing on the line triggers nothing. `0` disables the margin (creatures transfer on first crossing regardless of this setting). Must be ≥ 0 and < `BorderBandChunks`. |
| `Cluster:HandoffMinIntervalMs` | int | `2000` | live | 10 | Rate limit on automatic (boundary-triggered) handoffs: the hub will not start a new transfer for the same entity within this window. The second half of hysteresis — even an entity oscillating deeper than the margin triggers a bounded number of transfers per unit time. Must be ≥ 0. |

## Diagnostics

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Diagnostics:ReportApplierLag` | bool | `true` | live | 08 | A silently stalled Postgres applier — writes succeeding while the tier falls hours behind — is the dangerous failure mode. Not optional in practice. Gates only the periodic (30s) re-logging of a continuing stall's growing lag; the stall itself (EventId 1601) and the recovery (1602) always log. |
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
| `Telemetry:SlowReducerMs` | int | `50` | live | 02 | Reducers over this threshold get a span event and a log entry. Pick the number as **"how long may one transaction freeze every other writer"** — the write lock covers the whole reducer body, so this is a global write-latency budget, not a per-caller one. |
| `HealthChecks:ApplierLagThreshold` | long | `10000` | live | 08 | Transactions behind before the `melange-applier` check reports unhealthy. Applies to every applier — the hot store's included — though a decoupled applier (Postgres) is the one that realistically lags. |
