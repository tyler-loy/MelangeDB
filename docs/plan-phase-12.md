# Phase 12 — Typed client bindings

**Goal:** generate typed rows, per-table client caches, reducer stubs, and subscription helpers from one
schema, so the client edge is compile-time safe and a SpacetimeDB client port is a rename pass instead of
a rewrite.

**Depends on:** [02](plan-phase-02.md), [03](plan-phase-03.md). **Gates:** [11](plan-phase-11.md) — this is
issue [#20](https://github.com/tyler-loy/MelangeDB/issues/20), filed from the phase 11 scoping pass.

## Why here

Phase 11's scoping measured the gap precisely: across Vibe Shaft's three binding trees there are 459 call
sites (214 table accessors, 156 row handlers, 89 reducer calls) that are mechanical against a typed client
and a safety-destroying rewrite against `row.Columns["chunk_id"]` and stringly-named reducers. Phase 03
deliberately deferred client codegen "once the wire format has settled" — protocol v1 has now shipped and
survived a load test, so the deferral is spent.

The wire itself is the constraint that shapes everything below: frames carry per-row `name → value` maps
and **no schema** — MessagePack decoding is lossy (integers surface as `long`, `Identity` as `byte[]`,
`Timestamp` as `long`), so a typed client must own the name→CLR mapping entirely on its side.

## Deliverables

- **A schema manifest.** `MelangeServerGenerator` grows a client-visible schema export: public tables,
  their client-visible columns (kinds, PK/unique/index flags), enum definitions (name, underlying kind,
  members), and reducer signatures (parameter names, kinds, arrays). A small exporter
  (`tools/MelangeDB.SchemaExport`) writes it from a built module assembly as `melange-schema.json`.
- **`MelangeClientGenerator`** in the existing `MelangeDB.CodeGen` package, triggered by a
  `melange-schema.json` AdditionalFile. Emits: row structs and enums, a typed connection wrapper over
  `MelangeClient` with `.Db.<Table>` cache handles and `.Reducers.<Name>(…)` stubs, and subscription
  helpers for the supported query shapes. Adding the manifest to N projects is the multiple-output-trees
  story — nothing to configure.
- **Typed cache runtime in `MelangeDB.Client`** — the generic machinery (per-table merge across
  subscriptions, refcounting, rescope diffing, typed `OnInsert(T)` / `OnUpdate(T, T)` / `OnDelete(T)`)
  lives in the library behind a small codec interface the generator implements per table; emitted code
  stays thin.
- **Typed reducer stubs** encoding through the existing `ReducerArgs` tags, respecting its documented
  asymmetries (unsigned integers ride as `UInt64`; enums narrow to their underlying kind).
- **The sample client ported** to the generated bindings — zero `row.Columns[` string lookups left — as
  the in-repo proof that one schema serves a server tree and a client tree.
- Glossary nouns, configuration keys (if any), and these decisions recorded in the same change.

## Out of scope

- **Typed projection rows.** Settled below — projected subscriptions stay on the untyped API in v1.
- **A frame-tick event pump.** The Godot client wants handlers raised on its own thread; that is its own
  issue, but the typed cache dispatches all events through one seam so the pump can slot in without
  another rewrite.
- **Non-C# clients.** The manifest is deliberately language-neutral JSON so they become possible, not to
  build them now.

## Decisions to settle

- ~~**Where does the client learn the schema?**~~ **Settled: a generated manifest, not a
  ProjectReference.** Reducers are methods with server-only bodies in a server-only compilation — no
  client build can ever see them, so sharing types can never produce reducer stubs; a schema-only
  assembly still drags struct identity across a trust boundary the wire doesn't share. The manifest is
  also the `spacetime generate` workflow, which is the migration-path argument made in #20.
- ~~**One generator project or two?**~~ **Settled: one package, two generators — superseding phase 02's
  "own project" note.** The triggers are orthogonal (`[Table]` source vs. a manifest AdditionalFile), so
  neither generator fires in the other's world, and one analyzer package is one less thing for a consumer
  to version-match. This reconciles DESIGN.md, which already placed typed clients in `MelangeDB.CodeGen`.
- ~~**Column projection.**~~ **Settled: untyped in v1.** A projected row bound to the full struct would
  read as zeros in the missing columns — the exact trap #20 names. SpacetimeDB has no projection at all,
  so no ported call site needs it typed; new code that opts into projection keeps the dictionary API,
  and a typed representation waits for a real consumer to shape it.
- ~~**Cache ownership.**~~ **Settled: one cache per table, merged across subscriptions, refcounted per
  key.** `Conn.Db.<Table>` is the shape 214 call sites expect; overlapping and re-scoped subscriptions
  over the same table are Vibe Shaft's normal case, and a row leaves the cache only when its last
  covering subscription drops it.
- ~~**Row identity across a rescope.**~~ **Settled: encoded primary-key bytes, reconciled by diff.**
  When a rescope's new initial set completes, the typed cache diffs it against the merged state — deletes
  for rows that left scope, inserts for arrivals, updates for survivors whose bytes changed. No flush,
  no event storm, and server-driven rescopes (a gateway shard swap) take the same path.
- **`KeyCodec` lives in Core but the client needs primary-key encoding.** Lean: move it to Abstractions
  (it is dependency-free and pre-1.0); the implementer verifies nothing in Core's packaging story objects.

## Done when

- A client project referencing only `MelangeDB.Client` + the analyzer + a manifest compiles typed
  bindings: row structs for every public table, cache handles with typed events and index lookups, one
  stub per reducer.
- Round-trip tests cover every client-visible `ColumnKind` through the msgpack coercion path, and reducer
  stubs are verified against `ReducerArgsReader` — the server decodes what the stubs encode.
- Cache semantics are tested: overlapping subscriptions refcount correctly, unsubscribe removes only
  orphaned rows, a rescope fires the diff (not a flush), and a delete op (key only) resolves against the
  cached row.
- Subscription helpers emit exactly the SQL shapes `SqlSubsetParser` accepts, proven by parsing them.
- The sample client uses generated bindings end-to-end against the sample worker's exported manifest, and
  a second consumer of the same manifest exists in tests — multiple trees, one schema.
- Generated output is snapshot-tested in the house style, and the full suite passes Debug and Release
  with zero skips.

## Risks

- **The manifest can go stale against the module.** Mitigation: the manifest records the generator
  version and a schema hash; the client generator emits that hash into the bindings, and the connection
  wrapper can surface a mismatch loudly at connect time rather than as silently-wrong columns.
- **Msgpack type coercion is the soft underbelly.** Every kind needs an explicit, tested coercion —
  `Convert.ToInt64` sprinkled per-site is exactly what this phase deletes. One coercion table, tested
  against values produced by the real serializer, not hand-built dictionaries.
- **Cache merge semantics can drift from server row identity.** Whether the server dedupes a row that
  matches two subscriptions on one connection — in initial sets and in deltas — decides the refcount
  design, and it must be read out of `SubscriptionEngine` before building, not assumed. The
  overlapping-subscription tests are the guard, and they must run against a real server, not a mock.
