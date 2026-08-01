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
- **The manifest served from the running server** — added during the phase: `GET {path}/schema`, gated the
  way Swagger gates its document (on in Development, `Transport:SchemaEndpointEnabled` overrides), and the
  exporter accepts a URL as well as a DLL path, so the workflow is "generate from the running local dev
  server". One writer, two sources, byte-identical output; the Roslyn generator itself stays file-only —
  no network in a compiler.
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
- ~~**`KeyCodec` lives in Core but the client needs primary-key encoding.**~~ **Settled: the typed half
  moved to Abstractions** — see Shipped notes; the schema-boxed overloads stayed in Core as
  `SchemaKeyCodec` because `ColumnSchema` could not follow.

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

## Shipped notes

The decisions the plan left to the implementer, as they were actually taken.

- **KeyCodec split, not a whole-class move.** The typed, allocation-honest encoders
  (`EncodeBool` … `EncodeTimestamp`) moved to Abstractions as `MelangeDB.KeyCodec` — they are
  dependency-free and both generators emit calls to them. The boxed overloads could not follow:
  `Encode(ColumnSchema, object)` and `Decode(ColumnSchema, in RowKey)` interpret a column schema,
  and `ColumnSchema`/`ColumnKind` are Core types with no business in Abstractions. They stay in
  Core as `SchemaKeyCodec`, delegating every byte decision to the Abstractions class so the two
  cannot drift. Call sites were fixed directly — no type-forward, no re-export, pre-1.0.
- **Manifest discovery is a well-known generated type, not an attribute.** The generator embeds the JSON
  as `MelangeDB.Generated.MelangeSchemaManifest` (`Json` + `Hash` constants); the exporter and the host
  builder read it by type name. An assembly-level attribute would have added public API surface to
  Abstractions for no gain — the type name is as much a contract as an attribute name, and reading a
  constant runs no module code.
- **What the manifest excludes, and why.** Lifecycle reducers (the transport fires them; a client call is
  meaningless) and timer-row reducers (their argument is the server codec's own row bytes for a private
  table — a stub would invite constructing it). Enums referenced only by `[ServerOnly]` columns or private
  tables stay home. Enum member values ride as invariant decimal *strings* so `ulong`-backed enums survive
  JSON readers that parse numbers as doubles.
- **Enum identity is the simple name.** The manifest keys enums by the name the client bindings will
  declare; two client-visible enums sharing a simple name across namespaces is refused at the server build
  with MELANGE0019 rather than shipped as an ambiguous contract.
- **The schema endpoint rides `MapMelangeSocket`, anonymous while on.** It is part of the same client
  surface the socket serves, so it maps with it rather than as a separate opt-in call. The transport
  authenticates per endpoint (there is no blanket auth middleware to fight), so anonymity is a local
  choice: the manifest carries only what every authenticated client already receives, and the dev-default
  gate is posture, not secrecy. Off means a plain 404 — a probe learns nothing. Multi-module hosts get a
  404 with an explanation rather than one-of-several: serving a single module's manifest as "the schema"
  would misstate it; per-module export via the tool covers that case.
- **The endpoint gate is evaluated per request.** `Transport:SchemaEndpointEnabled` is live-reloadable, so
  the route maps unconditionally and answers 404 while disabled, rather than existing only when enabled at
  startup.
- **The server dedup question, answered by reading `SubscriptionEngine.Fanout`:** there is none. Deltas are
  computed per registered subscription and grouped per sink as one `SubscriptionUpdate` per subscription id
  — a row matching two subscriptions on one connection arrives twice, in initial sets and in deltas alike.
  The client merge therefore refcounts covering subscriptions per key and derives typed events from
  transitions, with value-equality on the wire column maps as the duplicate detector (an overlapping
  subscription's identical copy of an update compares equal and stays silent). Wire op kinds are input, not
  truth — the same posture as `MelangeSubscription`'s insert↔update self-healing one layer down.
- **The typed cache rides an internal sink, attached before the Subscribe frame leaves.** Public
  subscription events carry no LSN and never fire for initial sets, so the typed layer hooks
  `MelangeSubscription` internals instead: `OnSnapshot` (completed initial set, before buffered deltas
  replay), `OnRowOp` (each applied op, resolved kind), `OnReset`. Attaching at construction closes the
  window where a row could slip past between subscribe and attach. The sink hears each op before the
  public events do — the cache's consistency must not depend on what a user's untyped handler throws.
- **Unsubscribe fires `OnDelete` for orphaned rows.** A consumer watching the cache must see every row
  leave it; rows another subscription still covers stay, silently. Detach happens after the server's
  unsubscribe acknowledgement.
- **All typed events dispatch through one seam** — `ClientCacheRegistry.DispatchTypedEvent`, invoked on the
  receive loop. The frame-tick pump the Godot client wants (issue #20 "Adjacent") replaces that method and
  nothing else. *Superseded when the pump landed (issue #26)* — the settled seam moved one level up, and the
  decision record is: the pump defers **both** cache mutation and events, at `MelangeClient.HandleFrame`'s
  two data-channel cases (`SubscriptionApplied`, `TransactionUpdate`), not at `DispatchTypedEvent` — deferring
  only events would let a handler's cross-table lookup see a newer world than the event it is handling, the
  skew game clients hit hardest, so "what the cache says" and "what handlers have been told" advance on one
  clock and `DispatchTypedEvent` stays a plain synchronous call. `FrameTick` drains **whole frames** — one
  `TransactionUpdate` frame is one whole commit, so transaction atomicity falls out of the drain unit and a
  budget counted in frames can never tear a commit (a completed rescope's reconcile is one indivisible,
  possibly large, frame — accepted). Backpressure is a **bounded queue that fails loud**
  (`DispatchQueueLimit`): overflow synthesizes a client-side `dispatch_overflow` error at the head of the
  queue and aborts the socket, because dropping deltas silently diverges the cache and blocking the receive
  loop stops the pings that keep the server from convicting the client. The resume cursor **keeps advancing
  at receive time** — queued frames are retained in-process, the same "applied or retained" precedent as the
  rescope `_pending` buffer — so an overflow's dropped frame (never acked) is exactly where the reconnect's
  resume replay picks up.
- **Schema drift fails loud, typed.** The coercion table (`ClientWireValues`) throws
  `MelangeSchemaMismatchException` on a missing column, a wrong wire kind, or an out-of-range integer —
  never a default. A mismatch surfacing in an initial set fails that `SubscribeAsync` with the message;
  one surfacing mid-stream (a schema change under a live connection) is allowed to kill the receive loop
  loudly rather than be swallowed.
- **Bindings emit into `MelangeDB.Types`** — the `SpacetimeDB.Types` shape, because renames are the
  porting budget. Divergences from the SpacetimeDB C# SDK, chosen for house style and noted in
  CLIENT-BINDINGS.md: stubs are `Task<ulong>`-returning `<Name>Async` rather than fire-and-forget `void`;
  the wrapper is `MelangeConnection`, not `DbConnection`; non-PK lookups scan the local cache rather than
  maintaining client-side index dictionaries (caches are subscription-sized; an index should earn its
  place in a profile first). Range and equality filters compare by encoded `RowKey`, so client-side
  comparison semantics are byte-for-byte the server's — including UTF-8 string ordering.
- **Typed subscription helpers cover three of the four shapes** — full table, equality, range, on
  exactly the predicate-legal columns (PK, `[Unique]`, `[Index]`). The column-list shape stays untyped by
  the projection decision above. Typed rescope lives on the column accessor
  (`RescopeAsync`/`RescopeRangeAsync`), because the parameter names (`p` / `lo`,`hi`) are part of the
  emitted query and only the accessor knows them; `TypedSubscription.RescopeAsync(dictionary)` remains
  for hand-built parameter maps.
- **Client generator diagnostics:** MELANGE0020 (manifest missing fields, malformed JSON, or an unknown
  format version — one id, the message says which) and MELANGE0021 (two manifests in one compilation:
  one project binds one module, split consumers into separate projects).
- **The client generator's tests eat the server generator's output.** `GeneratorTestHost.ExportManifest`
  runs the server generator and pulls the JSON back out of the generated constant, so the reader is
  always tested against the writer of record, never a hand-maintained fixture.
- **Manifest bytes are LF-only and git never touches them.** The first Windows build surfaced it: the
  JSON writer used `AppendLine`, so the manifest — and the schema hash covering it — depended on which
  operating system ran the compiler. The writer now emits hard `\n`, and `.gitattributes` marks
  `melange-schema.json -text` so autocrlf checkouts cannot rewrite the committed artifact the staleness
  tests hold byte-identical to the build.
- **Two staleness guards, two consumers under test.** `MelangeDB.Transport.Tests` is both the server
  module and a typed consumer of its own exported manifest — the end-to-end proof runs real generated
  bindings against the real transport, and a test holds the committed manifest byte-identical to the
  build. The sample pair gets the same treatment in `MelangeDB.Host.Tests`, which drives the sample
  client's actual generated bindings against the real worker in-process. Drift in either module breaks
  the build with a message naming the re-export command, never a client at runtime.

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
