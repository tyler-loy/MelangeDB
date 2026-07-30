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
  `[PrimaryKey]`; `[AutoInc]` on a non-integer column; a reducer whose parameters aren't serializable;
  `DateTime.Now` / `new Random()` in a reducer body (use `ctx.Timestamp` / `ctx.Random`); a subscription-
  visible table that is not `Public`. Each with a stable `MELANGE####` id.

**`MelangeDB.Core` — host integration**
- `AddMelangeDb(Action<MelangeDbBuilder>)` with `UseHotStore`, `AddTablesFrom`, `AddReducersFrom`.
- Reducer invocation creates a **DI scope per call**; reducer classes are resolved from it, so constructor
  injection of scoped and singleton services both work.
- `IHostedService` owning startup (log recovery, projection rebuild) and graceful shutdown (drain, flush,
  checkpoint).
- Options bound through `IOptions`/`IOptionsMonitor` so `appsettings.json`, environment variables, and
  Azure App Configuration all work with no MelangeDB-specific code.

**`samples/MelangeDB.Sample.Worker`** — the first project that compiles against the real API. Its existence
is the proof the package composes; keep it building from here on.

## Out of scope

Networking (03) — reducers are still invoked directly. No client SDK generation yet (that needs the wire
format from 03).

## Decisions to settle

- **One generator or two?** Server-side registration and client-side typed bindings have different
  audiences and output trees. Splitting them early is cheaper than splitting them later.
- **How does a scoped service interact with the no-I/O rule?** A reducer can be injected a `DbContext` or
  `HttpClient` and misuse it mid-transaction. Options: document it, or ship an analyzer that flags awaits
  and known I/O types inside reducer bodies. Prefer the analyzer — the rule is invisible otherwise.
- ~~**Async reducers.**~~ **Settled: reducers are synchronous.** The transaction is a synchronous critical
  section, and permitting `await` invites exactly the I/O the design forbids. Relaxing this later is
  backwards-compatible; tightening it later would not be. The generator should reject an `async` reducer with
  a diagnostic rather than letting it compile and misbehave.
- **Struct tables and generated mutation.** Tables are `partial struct` mutated with `with` expressions;
  confirm the generated accessors don't defensively copy on the hot path.

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

## Risks

- **Generator debuggability.** Incremental generators fail obscurely. Emit generated sources to disk
  (`EmitCompilerGeneratedFiles`) from the start and snapshot-test the output — otherwise every later phase
  pays the debugging cost.
- **Scope lifetime bugs.** A reducer capturing a scoped service beyond its call is a use-after-dispose that
  will look like data corruption. Consider failing fast on captured `ctx` or scoped services.
