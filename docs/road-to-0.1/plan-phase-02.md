# Phase 02 — Source generator and host integration

**Goal:** `builder.Services.AddMelangeDb(...)` in an ordinary .NET host, with tables and reducers discovered
at compile time and reducers resolved from DI with real injected dependencies.

**Depends on:** [01](plan-phase-01.md).

## Why here

This is the phase that answers the original complaint. Everything before it is plumbing; this is where a
developer's `IOptionsMonitor<CombatSettings>` reaches a reducer body and Azure App Configuration works
because there was never anything special about configuration in the first place.

It also removes the reflection path from phase 01, which matters for startup time and for NativeAOT later.

## Deliverables

**`MelangeDB.CodeGen`** (Roslyn incremental generator, netstandard2.0)
- Discover `[Table]` types; emit `TableSchema` registration, typed accessors, and index accessors — so
  `ctx.Db.Player.Id.Find(id)` and `ctx.Db.Creature.ChunkId.Filter(range)` are generated, not reflected.
- Emit serializers per table (no reflection, no boxing) behind the versioned format from phase 01.
- Discover `[Reducer]` methods; emit an argument-decoding dispatcher keyed by reducer name.
- **Diagnostics are a first-class deliverable, not polish.** Report at compile time: a table with no
  `[PrimaryKey]`; `[AutoInc]` on a non-integer column; `[Unique]` on a `Partitioned` table (a unique
  index is a single-writer guarantee — see [CLUSTERING.md](../CLUSTERING.md)); a reducer whose parameters
  aren't serializable; `DateTime.Now` / `new Random()` in a reducer body (use `ctx.Timestamp` /
  `ctx.Random`); a subscription-visible table that is not `Public` — shipped as MELANGE0007, with
  `[ServerOnly]` as the compile-time marker of subscription visibility: a column mask only means anything
  on a table clients can see, so declaring one on a private table is the detectable mismatch. Each with a
  stable `MELANGE####` id (the full register is MELANGE0001–0013; see `MelangeDB.CodeGen/Diagnostics.cs`).

**`MelangeDB.Core` — host integration**
- `AddMelangeDb(Action<MelangeDbBuilder>)` with `UseHotStore`, `AddTablesFrom`, `AddReducersFrom`.
- Reducer invocation creates a **DI scope per call**; reducer classes are resolved from it, so constructor
  injection of scoped and singleton services both work.
- `IHostedService` owning startup (log recovery, projection rebuild) and graceful shutdown (drain, flush,
  checkpoint).
- **Argument validation on the generated decode path.** Reducer arguments come from clients and are otherwise
  trusted. The framework can't check semantics, but it must reject the inputs that corrupt state regardless of
  game rules — above all **`NaN` and `±Infinity` floats**, which propagate through position and chunk math and
  poison rows that then replicate to every client. Plus string length caps, collection length caps, and
  declared integer ranges. Rejection happens during decode, before a transaction opens.
- Options bound through `IOptions`/`IOptionsMonitor` so `appsettings.json`, environment variables, and
  Azure App Configuration all work with no MelangeDB-specific code.

**`samples/MelangeDB.Sample.Worker`** — the first project that compiles against the real API. Its existence
is the proof the package composes; keep it building from here on.

## Out of scope

Networking (03) — reducers are still invoked directly. No client SDK generation yet (that needs the wire
format from 03).

## Decisions to settle

- ~~**One generator or two?** Server-side registration and client-side typed bindings have different
  audiences and output trees. Splitting them early is cheaper than splitting them later.~~
  **Settled: two — split now.** `MelangeDB.CodeGen` ships only `MelangeServerGenerator` (registration,
  codecs, accessors, dispatcher, diagnostics); client bindings get their own generator project when the
  client SDK lands with the wire format (03+). The audiences never share output, so nothing is gained by
  one pipeline, and separating later would mean re-partitioning a shipped package.
- ~~**How does a scoped service interact with the no-I/O rule?** A reducer can be injected a `DbContext` or
  `HttpClient` and misuse it mid-transaction. Options: document it, or ship an analyzer that flags awaits
  and known I/O types inside reducer bodies. Prefer the analyzer — the rule is invisible otherwise.~~
  **Settled: the analyzer, scoped to what is statically knowable this phase.** `async`/`await` is rejected
  outright (MELANGE0008 — a non-async body cannot await, so async detection *is* the await detection);
  ambient time and randomness are flagged (MELANGE0005/0006); and MELANGE0010 flags a fixed list of known
  I/O types used directly in a body (`HttpClient`, `File`, `Directory`, `Console`, `Thread.Sleep`,
  `Task.Delay/Wait/Result/GetAwaiter`). Tracing I/O reached *through an injected service* (a `DbContext`
  method call) is dataflow analysis and is deliberately deferred — the fixed list catches the accidents,
  and the rule is now visible instead of folklore.
- ~~**Async reducers.**~~ **Settled: reducers are synchronous.** The transaction is a synchronous critical
  section, and permitting `await` invites exactly the I/O the design forbids. Relaxing this later is
  backwards-compatible; tightening it later would not be. The generator should reject an `async` reducer with
  a diagnostic rather than letting it compile and misbehave.
- ~~**Struct tables and generated mutation.** Tables are `partial struct` mutated with `with` expressions;
  confirm the generated accessors don't defensively copy on the hot path.~~
  **Settled: verified — no defensive copies.** Generated codecs take rows by `in` (serialize, key encode)
  and `ref` (AutoInc assignment), reading fields directly off the reference; the generated handle and
  column accessors are `readonly struct`s holding only the `IDbView` reference, so `ctx.Db.Player.Id`
  copies one reference, never a row. The single by-value row pass left is the public
  `IDbView.Insert/Update(TRow)` boundary itself — one deliberate copy per write op, not a hidden one per
  member access. Asserted by test (readonly accessors, single `IDbView` field).

## Done when

- The sample worker starts from `Host.CreateApplicationBuilder`, registers MelangeDB, and runs a reducer
  that reads `IOptionsMonitor<T>` and logs through `ILogger<T>`.
- Changing a value in `appsettings.json` (or a mounted config source) changes reducer behaviour on the next
  invocation with no restart — the feature-flag scenario, demonstrated end to end.
- Zero reflection on the reducer invocation path, asserted by a test that fails if
  `Type.GetType`/`MethodInfo.Invoke` appear in the hot path.
- Every diagnostic above has a test proving it fires, and a test proving valid code compiles clean.
- Phase 01's tests still pass against generated registration instead of reflection.
- A table added to the sample requires no manual registration anywhere.
- A reducer call carrying `NaN`, `Infinity`, an over-long string, or an over-long array is rejected during
  decode, with no transaction opened and no log record appended.

## Risks

- **Generator debuggability.** Incremental generators fail obscurely. Emit generated sources to disk
  (`EmitCompilerGeneratedFiles`) from the start and snapshot-test the output — otherwise every later phase
  pays the debugging cost.
- **Scope lifetime bugs.** A reducer capturing a scoped service beyond its call is a use-after-dispose that
  will look like data corruption. Consider failing fast on captured `ctx` or scoped services.
