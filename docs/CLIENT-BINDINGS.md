# Client bindings

How a client project gets typed rows, caches, reducer stubs, and subscription helpers from one
schema — phase 12, issue [#20](https://github.com/tyler-loy/MelangeDB/issues/20).

The wire is the constraint everything here answers to: frames carry per-row `name → value` maps
and **no schema**, and MessagePack decoding is lossy (integers surface as `long`, `Identity` as
32 raw bytes, `Timestamp` as its microsecond count). A typed client therefore owns the name→CLR
mapping entirely on its side, and it learns the schema from a **manifest** — a JSON file the
server module exports — never from referencing server code. Sharing the module assembly would
drag server types across a boundary the wire itself doesn't share, and could never describe
reducers anyway, whose bodies exist only in the server compilation.

## The workflow

```
server module (.dll, built with MelangeDB.CodeGen)
        │
        │  tools/MelangeDB.SchemaExport  (from the DLL, or from the running dev server)
        ▼
melange-schema.json          ← committed next to the consumer(s)
        │
        │  AdditionalFile + the same MelangeDB.CodeGen analyzer
        ▼
typed client bindings        ← one tree per consuming project; N consumers, one schema
```

Export the manifest either way — the two paths produce byte-identical files:

```
dotnet run --project tools/MelangeDB.SchemaExport -- path/to/Module.dll -o melange-schema.json
dotnet run --project tools/MelangeDB.SchemaExport -- http://localhost:5310 -o melange-schema.json
```

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

(Or the `MelangeDB.Client` and `MelangeDB.CodeGen` packages, off the repo feed.) Adding the same
manifest to several projects is the whole multiple-output-trees story — a game client, an admin
tool, and a CLI each generate their own tree from the one file, nothing to configure.

## The manifest format

`melange-schema.json` is public API, format `1`. It is deliberately language-neutral: a non-C#
client generator becomes possible by reading this file, not by linking anything.

| Field | Meaning |
| --- | --- |
| `format` | Manifest format version. Readers must reject versions they don't know. |
| `generator` | Version of the `MelangeDB.CodeGen` that wrote it. Informational. |
| `schemaHash` | SHA-256 (lowercase hex) over this JSON rendered with `schemaHash` empty. Stamped into the generated bindings; a connection wrapper surfaces it so schema drift is a visible property, not a silent wrong column. |
| `module` | The exporting assembly's name. Informational. |
| `enums` | Every enum the client-visible surface references: `name`, `underlying` (a `ColumnKind` integer name), and `members` (`name` + `value`, the value as invariant decimal text so `ulong`-backed enums survive JSON). Ordered by name. |
| `tables` | Public tables only, ordered by name: wire `name`, CLR `type` name, and `columns` in declaration order — which is wire order. `[ServerOnly]` columns are absent, exactly mirroring what the server ever puts in a frame. Each column: `name`, `kind`, optional `enum` (a name in `enums`), and `primaryKey` / `autoInc` / `unique` / `indexed` flags. |
| `reducers` | Client-callable reducers, ordered by name: `Standard` kind only — lifecycle reducers are fired by the transport, and a scheduled reducer's timer-row argument is server codec bytes no client can legitimately construct. Each `params` entry: `name`, `kind`, `isArray` (arrays carry the element kind), optional `enum`. |

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
conn.SchemaHash;                                                   // the drift detector
```

Rows are `partial struct`s with the server's field names; enums are re-declared from the manifest.
The caches are **merged per table and refcounted per key** across every subscription — a row
matching two subscriptions is one cached row and one event, and the server deliberately sends it
once per subscription (the engine deduplicates nothing across subscriptions on a connection; the
client merge is where that collapses). Subscription helpers cover three of the four SQL shapes —
full table, equality, range — on primary-key, `[Unique]`, and `[Index]` columns, which is exactly
the set the server accepts predicates on. The fourth shape (an explicit column list) stays on the
untyped `MelangeClient` API: a projected row bound to a full struct would read as zeros, the
precise trap typed bindings exist to close.

Deliberate divergences from the SpacetimeDB C# SDK, for porting hands: reducer stubs are
`Task<ulong>`-returning `<Name>Async(...)` (the LSN, house async style) rather than fire-and-forget
`void`; the connection wrapper is `MelangeConnection`, not `DbConnection`; non-PK lookups scan the
local cache instead of maintaining client-side index dictionaries — client caches are
subscription-sized, and an index that earns its keep should show up in a profile first.

## Staleness

The manifest can go stale against the module it was exported from. The defenses, in order: the
schema hash embedded in the bindings and surfaced by the connection wrapper; the sample worker's
committed manifest is asserted byte-identical to its assembly's embedded manifest in
`MelangeDB.Host.Tests`, so drift fails the build rather than a player.
