# @melangedb/client

The MelangeDB client for browsers and Node. It has no dependencies; the core needs only
`WebSocket`, `fetch`, and `TextEncoder`. It is held to the same written contract and live test
suite as the C# client
([CLIENT-CONFORMANCE.md](https://github.com/tyler-loy/MelangeDB/blob/main/docs/CLIENT-CONFORMANCE.md)).

```
npm install @melangedb/client
```

## Typed bindings

Generate bindings from the server's schema manifest with the `melange` CLI
(`dotnet tool install -g MelangeDB.Cli`):

```
melange schema http://localhost:5310 -o melange-schema.json
melange generate --lang ts melange-schema.json -o src/generated
```

That writes one module, `melange-bindings.ts`, with a row type, a codec, and a table handle per
public table, plus stubs for each reducer and producer and the schema hash. Regenerate it whenever
the manifest changes. Bindings built against an older schema keep working after an additive
server change: columns are matched by name.

```ts
import { MelangeClient } from "@melangedb/client";
import { MelangeConnection } from "./generated/melange-bindings.js";

const client = new MelangeClient({ url: "wss://game.example.com/melange", token });
await client.connect();
const conn = new MelangeConnection(client);

conn.db.Creature.onInsert((c) => spawn(c));
conn.db.Creature.onUpdate((old, now) => move(old, now));
conn.db.Creature.onDelete((c) => despawn(c));

const area = await conn.db.Creature.ChunkId.subscribeRange(0, 10); // ... WHERE ChunkId BETWEEN :lo AND :hi
await conn.db.Creature.ChunkId.rescopeRange(area, 5, 15);          // a diff, not a flush
conn.db.Creature.Id.find(42n);                                      // by encoded primary key
conn.db.Creature.ChunkId.filter(5, 15);                             // scans the local cache

const lsn = await conn.reducers.spawn(3, "wolf", [1, 2, 3]);
const census = await conn.producers.count(3);
conn.identity;                                                      // read it; never derive it
```

The cache is **merged per table and refcounted per key**. The server sends a row once per
subscription, so a row two subscriptions cover is still one cached row and one event, and
unsubscribing one subscription removes only the rows nothing else covers.

## Values

| Column kind | TypeScript |
| --- | --- |
| `Bool` | `boolean` |
| `Int8`–`Int32`, `UInt8`–`UInt32`, `Float32`, `Float64` | `number` |
| `Int64`, `UInt64` | `bigint` |
| `Timestamp` | `bigint` microseconds since the Unix epoch (`timestampToDate`, `dateToTimestamp`) |
| `String` | `string \| null` |
| `Bytes` | `Uint8Array \| null` |
| `Identity` | `Identity` (`.toString()` is 64 hex characters, `.equals()`) |
| an enum | its generated constant object's member type |

LSNs are `bigint` throughout: a resume cursor must never round.

## Without bindings

`client.subscribe(query, params)` returns a `Subscription` whose rows expose `.columns` by name.
A column a policy masked out is absent, never defaulted. `client.callReducer(name,
encodeArguments([arg.int64(5), arg.string("x")]))` calls with explicitly tagged arguments,
because a JavaScript `number` does not say which integer or float kind it is.

## Dispatch, reconnect, and tokens

- `dispatch: "manual"` queues data frames until you call `client.frameTick()` (from
  `requestAnimationFrame`, say), so the cache and your handlers advance together on your clock.
  The default applies frames as they arrive.
- `reconnect: {}` turns on automatic reconnection with backoff. `client.reconnect()` does it by
  hand and resolves `true` when the server resumed the gap rather than resyncing.
- In a browser, pass the token in `token` (it travels in the first frame) or set `useTicket: true`
  to exchange it for a single-use connect ticket. A browser cannot set headers on a WebSocket,
  and the JWT should never sit in a URL.

## Ad-hoc SQL

`new MelangeSql({ url: "https://game.example.com/melange", token }).query("SELECT COUNT(*) FROM
Creature")` runs one-shot queries, aggregates included, against `/melange/sql` (owner mode needs
the server's `Sql:OwnerRole` claim). `queryTable(CreatureCodec, "SELECT * FROM Creature WHERE
...")` returns typed rows. Integers past 2^53 come back as exact `bigint`.

## Developing this package

```
npm ci
npm test        # builds, then runs the unit tests and the decode corpus (conformance/corpus.json)
```

`conformance/melange-bindings.ts` and `test/generated/transport/melange-bindings.ts` are
generated, and a .NET test fails when they go stale. Regenerate them from the repo root:

```
dotnet run --project src/MelangeDB.Cli -- generate --lang ts conformance/melange-schema.json -o clients/typescript/conformance
dotnet run --project src/MelangeDB.Cli -- generate --lang ts tests/MelangeDB.Transport.Tests/melange-schema.json -o clients/typescript/test/generated/transport
```
