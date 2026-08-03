# Changelog

All notable changes to MelangeDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the pre-1.0 caveat that **the
public API may break in any release** until 1.0.

All packages ship together at one version; there is no per-package versioning. See
[docs/RELEASING.md](docs/RELEASING.md).

## [Unreleased]

Nothing has been released yet. `0.1.0` will be the first tagged version. Everything below is what
that release will contain — the work that landed before the repository was made public.

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
