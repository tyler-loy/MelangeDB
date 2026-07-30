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
belongs on the span and in the log, never on a time series.

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
complete ordered audit trail (see [SECURITY.md](SECURITY.md)). So tracing is **not** for *what happened*.

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
`1204 ReducerCallFailed` (03); `1104 UnpolicedReducers` (04).

## Health checks

Standard `IHealthCheck` registrations, since they're nearly free once the metrics exist. These require a DI
host to register into, so the first one landed with phase 02's host integration rather than phase 01.
`AddMelangeDb` registers each check automatically; the host only opts into health checks at all
(`AddHealthChecks()`).

| Check | Unhealthy when | Phase |
| --- | --- | --- |
| `melange-log` | The commit log is unwritable or out of disk — concretely, before startup opens it, or once a failed append has poisoned it (**shipped, 02**) | 02 |
| `melange-applier` | Any applier's lag exceeds its threshold | 08 |
| `melange-shard` | This node's shard assignment is unknown or contested | 09 |

## Standing requirement

Every phase instruments what it adds. A phase is not done if its failure modes are invisible — which is why
several phases' done-criteria name specific metrics rather than saying "add telemetry."
