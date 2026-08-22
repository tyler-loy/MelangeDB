# Client bindings

How a client project gets typed rows, caches, reducer stubs, and subscription helpers from one
schema — phase 12, issue [#20](https://github.com/tyler-loy/MelangeDB/issues/20).

The wire is the constraint everything here answers to: rows arrive as **schema-ordered bytes**, and
what they mean is described once per subscription by a [wire descriptor](GLOSSARY.md#definitions)
rather than repeated per row. A typed client therefore decodes positionally, and it learns the shape
to decode into from a **manifest** — a JSON file the server module exports — never from referencing
server code. Sharing the module assembly would drag server types across a boundary the wire itself
doesn't share, and could never describe reducers anyway, whose bodies exist only in the server
compilation.

That makes the manifest load-bearing in a way it was not under protocol v1, and the bindings say so
loudly. Ordered bytes carry no names, so a client generated from a stale manifest would read a
renamed, reordered, or re-kinded column into *plausible garbage* rather than failing. The generated
codec therefore carries the column shape it was built from, and it is compared against the server's
descriptor once per subscription, before any row decodes — a mismatch throws
`MelangeSchemaMismatchException` naming the column and both kinds. Re-export the manifest when the
module's tables change; the mismatch is the reminder, and it arrives at subscribe time rather than as
wrong data.

## The workflow

```
server module (.dll, built with MelangeDB.CodeGen)
        │
        │  melange schema  (from the DLL, or from the running dev server)
        ▼
melange-schema.json          ← committed next to the consumer(s)
        │
        │  AdditionalFile + the same MelangeDB.CodeGen analyzer
        ▼
typed client bindings        ← one tree per consuming project; N consumers, one schema
```

The exporter ships as the `melange` CLI (the `MelangeDB.Cli` dotnet tool). Install it once, then
export the manifest either way — the two paths produce byte-identical files:

```
dotnet tool install --global MelangeDB.Cli
melange schema path/to/Module.dll -o melange-schema.json
melange schema http://localhost:5310 -o melange-schema.json
```

(Working from a checkout of this repo, `dotnet run --project src/MelangeDB.Cli -- schema …`
does the same without installing anything.)

The URL form fetches `GET {path}/schema` from a running server (a bare base URL gets
`/melange/schema` appended). That endpoint follows the Swagger pattern: **on in Development, off
everywhere else**, with `Transport:SchemaEndpointEnabled` overriding in either direction — see
[CONFIGURATION.md](CONFIGURATION.md). It is anonymous while on, deliberately: the manifest
carries only what every connected client already receives, so gating it to Development is
posture, not secrecy. While off it answers a plain 404.

A client project then needs three things — the client library, the analyzer, and the manifest:

```xml
<ProjectReference Include="src/MelangeDB.Client/MelangeDB.Client.csproj" />
<ProjectReference Include="src/MelangeDB.CodeGen/MelangeDB.CodeGen.csproj"
                  OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
<AdditionalFiles Include="melange-schema.json" />
```

(Or the `MelangeDB.Client` and `MelangeDB.CodeGen` NuGet packages.) Adding the same
manifest to several projects is the whole multiple-output-trees story — a game client, an admin
tool, and a CLI each generate their own tree from the one file, nothing to configure.

## The manifest format

`melange-schema.json` is public API, format `1`. It is deliberately language-neutral: a non-C#
client generator becomes possible by reading this file, not by linking anything.

| Field | Meaning |
| --- | --- |
| `format` | Manifest format version. Readers must reject versions they don't know. |
| `generator` | Version of the `MelangeDB.CodeGen` that wrote it. Informational, and deliberately **not** part of `schemaHash`. |
| `schemaHash` | SHA-256 (lowercase hex) over this JSON rendered with `schemaHash` **and** `generator` empty. Stamped into the generated bindings; a connection wrapper surfaces it so schema drift is a visible property, not a silent wrong column. |
| `module` | The exporting assembly's name. Informational. |
| `enums` | Every enum the client-visible surface references: `name`, `underlying` (a `ColumnKind` integer name), and `members` (`name` + `value`, the value as invariant decimal text so `ulong`-backed enums survive JSON). Ordered by name. |
| `tables` | Public tables only, ordered by name: wire `name`, CLR `type` name, and `columns` in declaration order — which is wire order. `[ServerOnly]` columns are absent, exactly mirroring what the server ever puts in a frame. Each column: `name`, `kind`, optional `enum` (a name in `enums`), and `primaryKey` / `autoInc` / `unique` / `indexed` flags. |
| `reducers` | Client-callable reducers, ordered by name: `Standard` kind only — lifecycle reducers are fired by the transport, and a scheduled reducer's timer-row argument is server codec bytes no client can legitimately construct. Each `params` entry: `name`, `kind`, `isArray` (arrays carry the element kind), optional `enum`. |

Excluding `generator` from the hash is what makes the hash mean anything: it identifies the
**schema**, not the build that emitted it. Upgrading MelangeDB does not change `schemaHash`, so
bindings generated by one version stay hash-identical against a server built with another as long
as the schema itself is unchanged — and a hash that differs always means a schema that differs,
which is the only reading that makes it a drift detector rather than a version stamp.

Enums ride by **simple name** — that is the type name the client bindings declare — so two
client-visible enums cannot share one; the generator refuses with `MELANGE0019` rather than
shipping an ambiguous contract.

The manifest is embedded in the module assembly as the generated
`MelangeDB.Generated.MelangeSchemaManifest` class (`Json` and `Hash` constants). The exporter,
the schema endpoint, and the committed file are all that constant verbatim — one writer, several
transports, byte-identical everywhere.

## The generated API

Bindings land in the `MelangeDB.Types` namespace — deliberately the shape of `spacetime generate`'s
`SpacetimeDB.Types`, because issue #20's consumer is porting 459 call sites and renames are the
budget. Per public table `Creature`:

```csharp
var conn = new MelangeConnection(client);          // wraps a connected MelangeClient

conn.Db.Creature.OnInsert += c => ...;             // typed events off the merged cache
conn.Db.Creature.OnUpdate += (old, now) => ...;
conn.Db.Creature.OnDelete += c => ...;
conn.Db.Creature.Count;                            // locally cached rows
conn.Db.Creature.Iter();                           // snapshot of the cache
conn.Db.Creature.Id.Find(creatureId);              // PK lookup, O(1) by encoded key
conn.Db.Creature.ChunkId.Filter(5, 15);            // index lookup, scans the local cache

await conn.Db.Creature.SubscribeAllAsync();                        // SELECT * FROM Creature
var sub = await conn.Db.Creature.ChunkId.SubscribeRangeAsync(0, 10);  // ... WHERE ChunkId BETWEEN :lo AND :hi
await conn.Db.Creature.ChunkId.RescopeRangeAsync(sub, 5, 15);      // the terrain pattern: a diff, not a flush
await sub.UnsubscribeAsync();                                      // removes only rows nothing else covers

var lsn = await conn.Reducers.SpawnAsync(chunkId, "wolf", stats);  // typed stub per reducer
conn.Identity;                                                     // who this connection authenticated as
conn.SchemaHash;                                                   // the drift detector
```

Rows are `partial struct`s with the server's field names; enums are re-declared from the manifest.
The caches are **merged per table and refcounted per key** across every subscription — a row
matching two subscriptions is one cached row and one event, and the server deliberately sends it
once per subscription (the engine deduplicates nothing across subscriptions on a connection; the
client merge is where that collapses). Subscription helpers cover three of the SQL shapes — full
table, equality, range — on primary-key, `[Unique]`, and `[Index]` columns, which is exactly the
set the server accepts predicates on. Two shapes stay on the untyped `MelangeClient` API. An
explicit column list, because a projected row bound to a full struct would read as zeros — the
precise trap typed bindings exist to close. And `WHERE col <> <default>` (the not-default shape, issue
#122), because a helper for it would be a nullary method per indexed column on every table, and the
shape is new enough that the demand for it should show up before the surface does.

Deliberate divergences from the SpacetimeDB C# SDK, for porting hands: reducer stubs are
`Task<ulong>`-returning `<Name>Async(...)` (the LSN, house async style) rather than fire-and-forget
`void`; the connection wrapper is `MelangeConnection`, not `DbConnection`; non-PK lookups scan the
local cache instead of maintaining client-side index dictionaries — client caches are
subscription-sized, and an index that earns its keep should show up in a profile first.

## What a client knows the moment ConnectAsync returns

Two connect-time facts trip porting hands, one present and one absent:

**Your identity is on the connection — read it, never re-derive it.** `conn.Identity` (and
`MelangeClient.Identity` underneath) is the identity the server derived during the handshake, and it
is what distinguishes "my rows" from everyone else's in a subscription-fed cache:

```csharp
conn.Db.PlayerState.Identity.Find(conn.Identity);   // am I spawned yet?
if (creature.OwnerId == conn.Identity) ...          // is that tamed wolf mine?
```

The derivation (`SHA256(issuer|subject)`) belongs to the server alone. Parsing your own JWT and
hashing its claims makes every client a second implementation of the one piece of the contract that
must never disagree — and tells you nothing anyway once the IdP is a third party. Re-auth can never
change the value: a token that maps to a different identity closes the connection instead.

**`ConnectAsync` returning does not mean `ClientConnected` has been applied.** The lifecycle reducer
is its own transaction. It commits before the server processes your next frame, but a subscription's
initial set is computed from the hot store, which the applier updates behind the log — so under load,
a row your `ClientConnected` reducer creates may arrive as a **delta moments after the initial set**
rather than in it. The natural port shape — connect, subscribe to `PlayerState`, assert your row is
there — passes on an idle machine and fails under load. Wait for the row (an `OnInsert` handler, or
poll the cache) instead of assuming the initial set contains it.

## Threading

Which thread events fire on is a mode, `MelangeClientOptions.Dispatch` (issue #26 — see
[CONFIGURATION.md](CONFIGURATION.md)):

- **`DispatchMode.Immediate`** (the default): every event — typed cache events, untyped subscription
  events, `OnError`, `OnDisconnected` — fires on the client's **receive loop** as frames arrive. Right for
  servers, tools, and tests that want data the moment it exists; wrong for a game client whose engine
  allows scene mutation only from its own main thread.
- **`DispatchMode.Manual`**: whole data frames queue in arrival order and apply only inside
  `client.FrameTick(maxFrames)` — so every one of those events fires on **the thread that calls
  `FrameTick`**. Call it from Godot's `_Process` (or any host loop) and handlers may touch the scene tree
  directly, no `CallDeferred` anywhere. The pump defers cache mutation *and* events together: an
  `OnInsert` handler that looks up a related row sees a world consistent with its event, never a newer
  one, because "what the cache says" and "what handlers have been told" advance on the same clock. A tick
  drains whole frames — one `TransactionUpdate` frame is one whole commit, so a budget never tears a
  transaction; note a completed rescope's reconcile is one indivisible (possibly large) frame.

**The Manual-mode rule: await, never block.** In Manual mode, `SubscribeAsync`, `ReconnectAsync`, and
`UnsubscribeAsync` complete only as ticks apply their frames. `await` them from the ticking thread and
they finish across later ticks; **block** the ticking thread on one (`.Result`, `.Wait()`) and the
application deadlocks — the completion is waiting for the very tick the block is preventing. The same
applies inside handlers: a handler that synchronously waits on anything frame-driven wedges the pump.

**Reducer continuations.** `await conn.Reducers.<Name>Async(…)` resumes via the caller's
`SynchronizationContext`. Godot installs a main-thread one, so awaiting a reducer in `_Process`-adjacent
code resumes on the main thread and may touch nodes safely. The footgun is user code that discards that
context: inside `Task.Run(...)` or after `ConfigureAwait(false)` there is no context to return to, the
continuation runs on a thread-pool thread, and node access from it is the intermittent renderer-thread
crash the pump exists to prevent. The same applies to code after `await sub.UnsubscribeAsync()`: the
cache-eviction `OnDelete` events it triggers fire on the resuming thread.

## Staleness

The manifest can go stale against the module it was exported from. The defenses, in order: the
schema hash embedded in the bindings and surfaced by the connection wrapper; the sample worker's
committed manifest is asserted byte-identical to its assembly's embedded manifest in
`MelangeDB.Host.Tests`, so drift fails the build rather than a player.
