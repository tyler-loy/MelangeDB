# Configuration reference

Every configurable setting MelangeDB exposes, in one place.

> **The rule:** when a phase introduces a configuration item, it is added to this document **in the same
> change** that introduces it. A setting that isn't listed here doesn't exist as far as users are concerned —
> undocumented knobs are how a library becomes folklore.

This applies to every phase, not just the one that created this file.

## Status

Nothing here is implemented yet. Every row is `planned` until the phase that owns it lands, at which point its
status becomes `shipped` and its default is verified against the code rather than the plan. Treat this as a
design register that becomes a reference.

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
| `HotStore:Engine` | enum | `Auto` | restart | 07 | `InMemory` \| `Faster` \| `Auto`. `Auto` picks `Faster` when a path is set. `InMemory` is a legitimate choice, not just a test double. |
| `HotStore:MemoryBudgetBytes` | long | — | restart | 07 | Cap on the paging buffer pool. **Excludes** resident tables, which are accounted separately — total footprint is this plus the residency report. |
| `CommitLog:Path` | string | `./data/log` | restart | 01 | |
| `CommitLog:FsyncPolicy` | enum | `OnCommit` | live | 01 | `OnCommit` \| `Interval` \| `OsBuffered`. `OnCommit` is the only durable choice; the others trade a bounded window of committed-but-lost transactions for throughput and must say so in their XML docs. |
| `CommitLog:FsyncIntervalMs` | int | `100` | live | 01 | Only read when `FsyncPolicy = Interval`. This is the size of the data-loss window. |
| `CommitLog:GroupCommit` | bool | `true` | live | 01 | Batch concurrent commits into one fsync. Improves throughput without weakening durability. |
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
| `Sql:AdHocMode` | enum | `PolicyEnforced` | live | 08 | `PolicyEnforced` \| `Owner`. There is no third mode and no default-to-owner — ambiguity here is a security hole. |

## Transport and subscriptions

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Transport:Path` | string | `/melange` | restart | 03 | |
| `Transport:MaxMessageBytes` | int | `4194304` | live | 03 | |
| `Transport:Serializer` | enum | `MessagePack` | restart | 03 | Behind `IMelangeSerializer`. |
| `Subscriptions:MaxPerConnection` | int | `64` | live | 03 | |
| `Subscriptions:BackpressurePolicy` | enum | `Buffer` | live | 03 | `Buffer` \| `DropAndResync` \| `Disconnect`. Matters most during bulk terrain streaming to a slow client. |
| `Subscriptions:MaxBufferedBytes` | long | `16777216` | live | 03 | Per connection; the trigger for the policy above. |

## Identity and policies

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Auth:Authority` | string | — | restart | 04 | Uses the host's existing ASP.NET Core authentication. |
| `Auth:Audience` | string | — | restart | 04 | |
| `Auth:AllowGuests` | bool | `true` | live | 04 | |
| `Auth:GuestSigningKey` | string | — | restart | 04 | **Secret.** Belongs in a secret store, never `appsettings.json`. Guest identities are forgeable without it being secret. |

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

## Cluster

Ignored entirely by single-node deployments.

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Cluster:Enabled` | bool | `false` | restart | 09 | |
| `Cluster:Role` | enum | `Standalone` | restart | 09 | `Standalone` \| `Hub` \| `Shard`. |
| `Cluster:HubAddress` | string | — | restart | 09 | |
| `Cluster:Shards` | string | — | restart | 09 | Which shard keys this node owns. Static assignment; dynamic rebalancing is out of scope for 09–10. |
| `Cluster:MembershipStore` | string | — | restart | 09 | Ownership registry. Likely the hub's Postgres rather than a new consensus dependency. |
| `Cluster:ShardSpanCheck` | enum | `ThrowInDevelopment` | live | 09 | `Off` \| `Warn` \| `ThrowInDevelopment` \| `Throw`. Catches the one contract MelangeDB cannot verify statically: rows mutated in one transaction must resolve to one shard. |
| `Cluster:FencingTokenTimeoutMs` | int | `5000` | live | 09 | Stops a wrongly-suspected-dead node from continuing to write a player it no longer owns. |
| `Cluster:BorderBandChunks` | int | `2` | careful | 10 | Deeper is smoother and costs bandwidth plus memory on every node. Default should be derived from movement speed and tick rate, not guessed. |
| `Cluster:HandoffHysteresisMeters` | float | `16` | live | 10 | Stops a player pacing across a boundary from triggering a handoff per step. |

## Diagnostics

| Key | Type | Default | Reload | Phase | Notes |
| --- | --- | --- | --- | --- | --- |
| `Diagnostics:ReportApplierLag` | bool | `true` | live | 08 | A silently stalled Postgres applier — writes succeeding while the tier falls hours behind — is the dangerous failure mode. Not optional in practice. |
| `Diagnostics:SlowReducerMs` | int | `50` | live | 02 | Logs reducer invocations over this threshold. |
| `Diagnostics:EmitGeneratedFiles` | bool | `false` | restart | 02 | Writes source-generator output to disk. Incremental generators fail obscurely; this pays for itself. |
