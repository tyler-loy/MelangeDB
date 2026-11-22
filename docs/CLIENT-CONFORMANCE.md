# Client conformance

What a MelangeDB client must do to be a MelangeDB client, written down so that "a correct client"
is a document and a test suite rather than whatever the C# client happens to do (road-to-0.3
phase 23, carried into 0.4).

This is for someone writing a client in a language that cannot reuse `MelangeDB.Client`: the
TypeScript client this phase ships, and any client after it. Every rule below is an **obligation**,
numbered so the suite and issues can cite it, with the failure it prevents. A rule without a stated
failure is a rule nobody will keep.

Two things hold a client to these rules:

- **The decode corpus**, [`conformance/corpus.json`](../conformance/corpus.json): real frames a
  live server sent, byte for byte, each beside its exact expected decode, plus pure-function vectors
  for key, column, and argument encoding. It is recorded by `tools/MelangeDB.Conformance` and a test
  fails the build when it drifts from what the server sends. See [The corpus](#the-corpus).
- **The live suite** (step 2 of phase 23, not yet built): a harness that drives a client under
  test against a real server through the behaviours a frame corpus cannot show — the anchor
  boundary, resume, backpressure, the merged cache.

The C# client in `src/MelangeDB.Client` defines conformance, so where it and this document disagree
the disagreement is a [finding](#findings), not a licence.

## Contents

1. [Transport](#1-transport)
2. [Framing](#2-framing)
3. [The handshake](#3-the-handshake)
4. [Rows](#4-rows)
5. [Primary keys](#5-primary-keys)
6. [Subscriptions and the anchor boundary](#6-subscriptions-and-the-anchor-boundary)
7. [Deltas](#7-deltas)
8. [The merged cache](#8-the-merged-cache)
9. [Reducer and producer calls](#9-reducer-and-producer-calls)
10. [Resume](#10-resume)
11. [Backpressure: `overflow_resync`](#11-backpressure-overflow_resync)
12. [Heartbeat](#12-heartbeat)
13. [Reauthentication](#13-reauthentication)
14. [Errors](#14-errors)
15. [The subscription parameter map](#15-the-subscription-parameter-map)
16. [Identity](#16-identity)

## 1. Transport

**C1.1 — One WebSocket at `{Transport:Path}`** (default `/melange`), carrying **binary** messages,
**one frame per message**. HTTP/1.1 upgrade and HTTP/2 extended CONNECT both work;
`permessage-deflate` is accepted when the server has `Transport:CompressionEnabled` (default on).

**C1.2 — Authenticate one of three ways.** An `Authorization: Bearer` header on the upgrade; a
single-use connect ticket (`POST {path}/ticket` with the bearer header answers
`{"ticket": "..."}`, then connect to `{path}?ticket=...`); or the token in `Hello`. A browser cannot
set headers on a WebSocket, so it uses the ticket or `Hello`. *Fails if:* a browser client puts the
JWT itself in the URL, where proxies and logs keep it. The ticket exists so that only a
near-worthless single-use string ever appears there.

**C1.3 — Keep each frame under `Transport:MaxMessageBytes`** (default 4 MiB). A larger frame is
answered with `message_too_large`.

## 2. Framing

Every frame is one MessagePack array: `[type, channel, ...fields]`. Field order is the contract;
there are no names on the wire.

| Type | Frame | Direction | Fields after `[type, channel]` |
| --- | --- | --- | --- |
| 1 | `Hello` | client → server | `minVersion` int, `maxVersion` int, `token` str \| nil |
| 2 | `Welcome` | server → client | `version` int, `connectionId` bin(16), `epochId` bin(16), `headLsn` uint64, `httpProtocol` str, `identity` bin(32) |
| 3 | `CallReducer` | client → server | `requestId` uint32, `reducer` str, `arguments` bin, `traceParent` str \| nil |
| 4 | `ReducerResult` | server → client | `requestId` uint32, `ok` bool, `lsn` uint64, `errorCode` str \| nil, `message` str \| nil |
| 5 | `Subscribe` | client → server | `subscriptionId` uint32, `query` str, `parameters` map \| nil |
| 6 | `Unsubscribe` | client → server | `subscriptionId` uint32 |
| 7 | `Unsubscribed` | server → client | `subscriptionId` uint32 |
| 8 | `SubscriptionApplied` | server → client | `subscriptionId` uint32, `anchorLsn` uint64, `chunkIndex` uint32, `isLast` bool, `descriptor` (`[table str, [[name str, kind int], ...]]`) \| nil, `rows` (`[[key bin, row bin, mask bin], ...]`) |
| 9 | `TransactionUpdate` | server → client | `lsn` uint64, `updates` (`[[subscriptionId uint32, [[kind int, key bin, row bin, mask bin], ...]], ...]`) |
| 10 | `Error` | server → client | `code` str, `message` str, `requestId` uint32, `subscriptionId` uint32 |
| 11 | `Ping` | both | `id` uint32 |
| 12 | `Pong` | both | `id` uint32 |
| 13 | `Resume` | client → server | `epochId` bin(16), `lastAckedLsn` uint64, `subscriptions` (`[[subscriptionId uint32, query str, parameters map \| nil], ...]`) |
| 14 | `ResumeResult` | server → client | `accepted` bool, `reason` str \| nil |
| 15 | `Reauthenticate` | client → server | `token` str |
| 16 | `ReauthenticateResult` | server → client | `accepted` bool, `message` str \| nil |
| 17 | `CallProducer` | client → server | `requestId` uint32, `producer` str, `arguments` bin, `traceParent` str \| nil — protocol 3 only |
| 18 | `ProducerResult` | server → client | `requestId` uint32, `ok` bool, `lsn` uint64, `payload` bin, `errorCode` str \| nil, `message` str \| nil — protocol 3 only |

**C2.1 — Decode any valid MessagePack width.** The server writes the shortest encoding of every
integer, string, binary, array, and map header today, but a decoder that only handles the widths
it has seen breaks the first time a value crosses a boundary (a 256-byte row is `bin 16`, a
256-entry update is `array 16`). The corpus has both.

**C2.2 — Never lose precision on a 64-bit field.** LSNs, `Int64`/`UInt64` columns, and timestamps
are full 64-bit values. *Fails if:* a JavaScript client reads an LSN into a `number`; past 2^53 the
resume cursor silently rounds and a resume either skips or replays commits. Use `BigInt` (or the
language's equivalent) for every `uint64`/`int64` field.

**C2.3 — Encode with standard MessagePack.** The server's reader accepts any integer width for an
integer field (signed or unsigned, while the value fits), `str` or `nil` for a nullable string, and
only `bin 8/16/32` for a binary field — **never `str`** for bytes. *Fails if:* a MessagePack library
is left in a "strings for binary" compatibility mode. The frame is refused as a protocol error and
the connection closes.

**C2.4 — Ignore trailing fields.** Read the fields this table lists, in order, and ignore any
further elements of a frame array. The reader the server and the C# client share already does this.
Ignoring trailing fields is what lets a later version add a field without breaking a reader that
predates it.

**C2.5 — Treat `connectionId` and `epochId` as 16 opaque bytes.** They are .NET `Guid` byte
layouts, whose first three groups are little-endian. *Fails if:* a client parses them into a UUID
string and back. The standard UUID string form is big-endian, so the bytes such a library writes
back are not the bytes the server sent, and a `Resume` naming that epoch is rejected as unknown. Keep the bytes and
echo them exactly.

**C2.6 — Channels order frames only within a channel.** Every frame carries a channel; frames on
different channels may arrive in any relative order.

| Channel | Carries |
| --- | --- |
| 0 — control | `Hello`, `Welcome`, `Error`, `Ping`, `Pong`, `Resume`, `ResumeResult`, `Reauthenticate`, `ReauthenticateResult`, `Unsubscribed` |
| 1 — calls | `CallReducer`, `CallProducer` and their results |
| 2 — data | `Subscribe`, `Unsubscribe` from the client; every `TransactionUpdate`, in LSN order |
| 16 + id — bulk | one subscription's `SubscriptionApplied` chunks, in chunk order |

Send on the channel this table names. The server does not depend on a client's channel today, but
a transport that maps channels to streams will. *Fails if:* a client assumes a reducer's
`ReducerResult` arrives before or after that commit's `TransactionUpdate`. They ride different
channels, and the corpus records both orders as one step for exactly that reason.

## 3. The handshake

**C3.1 — `Hello` first, then wait for `Welcome`.** The server refuses any other first frame and
closes. Offer `minVersion 2, maxVersion 3`; the server settles on the highest version both speak.
With no overlap it answers `Error unsupported_version` and closes.

**C3.2 — Producer frames need version 3.** On a version-2 connection, refuse a producer call
locally (`unsupported_version`) rather than sending a frame the server would reject.

**C3.3 — Keep what `Welcome` says.** `version` is what this connection speaks. Keep `epochId` for
resume ([§10](#10-resume)) and `identity` as the caller ([§16](#16-identity)). `httpProtocol` is
what was actually negotiated; never assume HTTP/2 because you asked for it.

**C3.4 — Handshake refusals are connection-scoped `Error` frames followed by a close:**
`unauthorized` (no valid token, or a revoked identity), `connection_cap`
(`Auth:MaxConnectionsPerIdentity`), `unsupported_version`.

## 4. Rows

Rows travel as **schema-ordered row bytes** described once per subscription by a **wire
descriptor**: the table name and the ordered `[name, kind]` columns every row carries. No names
travel per row.

**C4.1 — Hold the descriptor for the subscription's life.** It arrives on chunk 0 of the initial
set and on no other chunk and no delta. *Fails if:* a client keys descriptors by table, or drops
them after the initial set. Every later delta for that subscription is undecodable, because a row
is only values in an order. A new initial set for the same subscription (a re-establishment, or a
server-driven re-issue) carries a fresh descriptor that replaces the old one.

**C4.2 — Decode positionally by kind.** The row format is little-endian with no padding and no
alignment:

| Kind | Name | Row bytes |
| --- | --- | --- |
| 1 | `Bool` | 1 byte, 0 or 1 |
| 2 / 3 | `Int8` / `UInt8` | 1 byte |
| 4 / 5 | `Int16` / `UInt16` | 2 bytes |
| 6 / 7 | `Int32` / `UInt32` | 4 bytes |
| 8 / 9 | `Int64` / `UInt64` | 8 bytes |
| 10 | `Float32` | 4 bytes, IEEE-754 |
| 11 | `Float64` | 8 bytes, IEEE-754 |
| 12 | `String` | 1 flag byte (0 = null); when 1, an `Int32` byte length and that many bytes of UTF-8 |
| 13 | `Bytes` | 1 flag byte (0 = null); when 1, an `Int32` length and the bytes |
| 14 | `Identity` | 32 bytes |
| 15 | `Timestamp` | `Int64` microseconds since the Unix epoch (may be negative) |
| 16 | `ScheduleAt` | 1 byte (0 = an instant, 1 = an interval), then `Int64` microseconds |

An enum column is its underlying integer kind; the manifest names its members. `ScheduleAt` is
only legal on scheduled tables, and scheduled tables are never public, so no frame carries it
today. Implement it anyway: it is in the descriptor's vocabulary, and the corpus's column vectors
cover it.

**C4.3 — Honour the column mask.** Each row carries a mask. **Empty means every descriptor column
is present**, the common case and a one-byte cost. Otherwise descriptor column `i` is present when
`mask[i >> 3] & (1 << (i & 7))` is set, and the mask is `(columnCount + 7) / 8` bytes. A masked-out
column is **absent from the row bytes**, not zeroed: skip it in the positional walk. *Fails if:* a
client ignores the mask. It reads the next column's bytes as the hidden one, and every column after
it is garbage.

**C4.4 — Never present a masked column as a value.** A client that maps rows to fixed records
must not fill a masked column with a default; that would show a column policy's "you may not see
this" as "this is zero". Refuse the row in the typed path or expose it as partial. The C# typed
bindings refuse with `MelangeSchemaMismatchException` and point at the untyped API.

**C4.5 — Map generated bindings to the descriptor by name.** A client generated from an older
manifest must tolerate a server that changed the schema additively (road-to-0.4 phase 28). Find
each column the bindings read by name in the descriptor: added or reordered columns are skipped,
and a missing or re-kinded column fails the subscription naming the column. Plan the mapping once
per descriptor, never per row. *Fails if:* bindings compare by position. The first server deploy
that adds a column breaks every client not yet updated, and a player's game client cannot be
updated in lockstep with a server deploy.

**C4.6 — A projection is a narrower descriptor.** `SELECT a, b FROM t` carries a descriptor naming
only `a` and `b`. Typed bindings must not bind a projected subscription to the full row type;
columns the projection dropped would read as defaults. C# keeps projections on the untyped API.

## 5. Primary keys

Every row and every delta op carries its **encoded primary key**: the uniform, order-preserving
byte form the server indexes by. Keys are the row's identity in every cache.

**C5.1 — Compare keys as bytes.** Two rows are the same row exactly when their key bytes are
equal. Never decode a key to identify a row.

**C5.2 — Encode a key exactly, to look a row up by value.** A `Find` by value is a lookup by
encoded key, so the bytes must match the server's:

| Kind | Key bytes |
| --- | --- |
| `Bool` | 1 byte, 0 or 1 |
| unsigned integers | big-endian, at the kind's width |
| signed integers, `Timestamp` | big-endian at the kind's width, with the sign bit flipped (so `-1` sorts below `0`) |
| `String` | UTF-8, no length prefix |
| `Bytes` | the bytes |
| `Identity` | the 32 bytes |

Floats and `ScheduleAt` are not key-encodable. An enum key is its underlying integer kind. The
corpus's `keyVectors` give the exact bytes for every kind's edges.

## 6. Subscriptions and the anchor boundary

**C6.1 — Subscription ids are non-zero and unique on the connection.** Zero means "no
subscription" in an `Error` frame. Re-sending `Subscribe` with a **live** id re-scopes that
subscription instead of creating one ([§7](#7-deltas), LSN 0).

**C6.2 — An initial set is chunks, then a boundary.** Chunks arrive on channel `16 + id` with
`chunkIndex` 0, 1, 2, … and `isLast` on the final one. Every chunk names the same `anchorLsn`, and
an empty set is one chunk with no rows, which still carries the descriptor and the anchor.
Accumulate rows until `isLast`, then replace the subscription's rows with the set in one step.

**C6.3 — The anchor boundary.** The set is consistent at `anchorLsn`, and the server sends the
subscription only deltas with LSNs **greater** than the anchor. Deltas for the subscription can
reach the client **before** its last chunk, because they ride the data channel and the set rides a
bulk channel. Buffer them, and when the set completes apply the buffered deltas whose LSN exceeds
the anchor, in order. Drop any at or below it. *Fails if:* a client applies early deltas straight
away (the set then overwrites them, losing commits) or applies them after the set without the
anchor test (commits already in the set apply twice). Both failures are intermittent and depend on
timing, which makes this the rule most worth getting right. The live suite forces both races.

**C6.4 — Subscription refusals are `Error` frames carrying the subscription id**, on channel 0.
Codes include `unknown_table` (also for private tables: the server does not reveal which),
`unknown_column`, `unindexed_column`, `parse`, `predicate_required`, `range_too_wide`,
`unsupported_predicate`, `too_many_rows`, `too_many_bytes`, `too_many_subscriptions`, and
`server_only_column`. A refused subscription holds nothing on the server.

**C6.5 — Unsubscribe.** `Unsubscribe` is answered by `Unsubscribed`. Deltas already in flight may
still name the id; ignore deltas for an id the client no longer holds.

## 7. Deltas

**C7.1 — One `TransactionUpdate` is one commit.** It carries the commit's LSN and one entry per
matching subscription, each a list of ops: kind `1` insert, `2` update, `3` delete. A delete's row
bytes are empty. Apply the whole frame, every subscription in it, before the next. *Fails if:* a
client applies a frame partially (one subscription's ops, say) and raises events in between.
Handlers then observe a half-applied transaction.

**C7.2 — LSN 0 is a synthetic diff.** Re-scoping a live subscription answers with a
`TransactionUpdate` at LSN `0`: deletes for rows that left the new scope and inserts for rows that
entered it, with no initial set. Apply it **without** the anchor test, and do not let it move the
resume cursor. *Fails if:* a client judges LSN 0 against its anchor, so every re-scope is dropped
and the moving-range pattern (terrain streaming) silently stops moving.

**C7.3 — Tolerate redundant ops.** Treat an insert for a key already held as an update, an update
for a key not held as an insert, and a delete for a key not held as nothing. A resume's replay
emits the last kind legitimately. The C# client counts the first two as `Inconsistencies`, a
diagnostic that is zero unless the anchor boundary leaked.

## 8. The merged cache

The server sends a row **once per subscription** and deduplicates nothing across subscriptions on a
connection. Collapsing that is the client's job, and it is **semantics, not an optimization**.

**C8.1 — Merge per table, refcount per key.** Keep one row per (table, key) and count the
subscriptions covering it. Derive events from **transitions**, not from wire op kinds:

- the **first** covering subscription raises **insert**;
- a **value change** raises **update** once. Compare row bytes: two subscriptions over one table
  share a descriptor, so an overlapping subscription's identical copy compares equal and stays
  silent;
- the **last** uncovering (a delete, a scope exit, or an unsubscribe) raises **delete**.

*Fails if:* a client mirrors wire ops directly. A row in two overlapping subscriptions raises two
inserts, and unsubscribing either one deletes a row the other still covers.

**C8.2 — Reconcile a new initial set by diff.** When a subscription's set is replaced (a
re-establishment after resync or backpressure, or a server-driven re-issue), diff it against the
keys that subscription covered: uncover departed keys, cover arrivals, and raise update only for
survivors whose bytes changed. *Fails if:* a client flushes and reloads. A full resync then raises
a delete and an insert for every row the player can see.

**C8.3 — Cache state and events advance together.** Whatever the dispatch model (on arrival, or
queued and applied in a host's frame tick), a handler for an event must see the cache as of that
event, never a newer one.

## 9. Reducer and producer calls

**C9.1 — Request ids are non-zero and unique among in-flight calls**, and reducer and producer
calls share one id space.

**C9.2 — The argument encoding.** A payload is a little-endian `UInt16` count followed by that many
**tagged values**. **No arguments is the empty payload, not a zero count.**

| Tag | Value |
| --- | --- |
| 0 `Null` | nothing |
| 1 `Bool` | 1 byte |
| 2 `Int64` | 8 bytes LE: every signed integer width, and an enum whose underlying type is not `ulong` |
| 3 `UInt64` | 8 bytes LE: every unsigned integer width, and a `ulong`-backed enum |
| 4 `Float64` | 8 bytes LE: `Float32` widens |
| 5 `String` | `Int32` LE byte length, then UTF-8 |
| 6 `Bytes` | `Int32` LE length, then the bytes |
| 7 `Identity` | 32 bytes |
| 8 `Timestamp` | `Int64` LE microseconds |
| 9 `Array` | `Int32` LE count, then that many tagged values |

Note the enum rule: a `byte`-backed enum travels as `Int64`, not `UInt64`. The corpus's
`argumentVectors` pin every case. A trailing parameter the manifest gives a `default` may be left
off; the server fills it.

**C9.3 — `ReducerResult`.** `ok` with `lsn` the commit's LSN (`0` for a call that committed
nothing), or `ok = false` with a code. The commit's own delta may arrive before or after the result
(C2.6). Codes: `rejected` (the reducer refused), `invalid_args`, `unknown_reducer`, `denied` (a
reducer policy), `rate_limited`, `transient` (retry next tick, unchanged), `outcome_unknown` (it
may have committed; read state before retrying anything not idempotent), `internal`.

**C9.4 — Producers (protocol 3).** `CallProducer` uses the same argument encoding. A successful
`ProducerResult.payload` is **one** tagged value with no count, decoded positionally against the
producer's `returns` type reference in the manifest: a record is an `Array` of its fields in
declaration order, a list an `Array` of its elements, and a nullable value `Null` or the value.
Decoding must tolerate fields appended to a record after the bindings were generated. On failure
the payload is empty and `errorCode` says why (`unknown_producer`, `timeout`, `invalid_args`, …).

**C9.5 — A call can also fail with an `Error` frame carrying its request id.** Route it to the
pending call.

## 10. Resume

A dropped connection resumes rather than refetches: the server replays exactly the commits the
client missed.

**C10.1 — The resume cursor** is the highest LSN the client has fully applied, or durably retained
to apply: the `anchorLsn` of each completed initial set and the `lsn` of each whole
`TransactionUpdate`. LSN 0 diffs never move it. *Fails if:* the cursor advances on receipt of a
frame that is then dropped. That commit is never replayed, and the cache silently misses it.

**C10.2 — On reconnect, resume before anything else.** After `Welcome`, if `epochId` equals the
epoch the cursor counts against and the client holds live subscriptions, send `Resume(epochId,
cursor, [each live subscription's id, query, current parameters])` **before any `Subscribe`**. The
server refuses a resume after a subscribe on the same connection. Subscriptions whose initial set
had not completed are re-subscribed normally afterwards. If `epochId` differs, skip `Resume` and
resync ([C10.4](#c104)).

**C10.3 — Accepted.** The client keeps its caches **and its descriptors**: no initial set and no
descriptor arrive. The gap arrives as ordinary `TransactionUpdate` frames above the cursor, then
the subscriptions carry on live.

<a id="c104"></a>**C10.4 — Rejected means a full resync.** The server rejects a resume against
another epoch, a cursor beyond its head, a gap older than `Resume:RetentionWindowSeconds`
(default 300), a subscription that no longer validates, or a resume after a subscribe. A rejected
resume holds nothing on the server. Reset the cursor, re-establish every subscription with
`Subscribe`, and reconcile each new initial set by diff (C8.2). Drop anything still queued from the
old connection: a frame from the dead era must never apply to the rebuilt caches.

**C10.5 — Never switch identity on reconnect.** If the new `Welcome.identity` differs from the
identity the caches were filled under, close before resuming or subscribing. Not one row of
another identity's view may reach the caches.

## 11. Backpressure: `overflow_resync`

A client that falls behind its delta stream past `Subscriptions:MaxBufferedBytes` is not
disconnected by default (`DropAndResync`). The server discards the deltas it had queued, **forgets
every subscription on the connection**, and sends one connection-scoped `Error overflow_resync`.

**C11.1 — On `overflow_resync`, re-establish every subscription.** Re-send `Subscribe` for each one
(the same ids are fine: the server no longer holds them), and reconcile each new initial set by
diff (C8.2). *Fails if:* a client only reports the error. Its subscriptions look live, and its
caches stop changing, with no further error.

**C11.2 — Never block the receive path.** A client that stops reading the socket stops answering
pings and is disconnected (§12), and a full receive buffer is what triggers backpressure in the
first place. A client that queues frames for a host's frame tick must bound the queue and, when it
fills, fail the connection loudly without advancing the cursor past what it kept, so the next
resume replays the rest. The C# client aborts with `dispatch_overflow`.

## 12. Heartbeat

**C12.1 — Answer every server `Ping` with a `Pong` carrying the same id, promptly.** The server
pings every `Transport:HeartbeatIntervalMs` (default 15 s) and aborts a connection it has not heard
from for `Transport:HeartbeatTimeoutMs` (default 45 s) after a ping. A client may ping the server,
which answers in kind.

## 13. Reauthentication

**C13.1 — Refresh before the token expires.** Send `Reauthenticate(token)` before the current
token's expiry plus `Auth:ReauthGraceSeconds` (default 120). `ReauthenticateResult` reports the
outcome; a refused refresh is not fatal while the grace window lasts. Past it, the server sends
`Error token_expired` and closes.

**C13.2 — A refresh may not change identity.** A token for another identity is answered with
`Error identity_changed` and a close: every delta already sent was filtered under the current
identity's policies.

## 14. Errors

**C14.1 — Route an `Error` by scope.** A non-zero `subscriptionId` fails that subscription; a
non-zero `requestId` fails that call; both zero is connection-scoped.

**C14.2 — Branch on `code`, never on `message`.** Codes are stable API (`MelangeErrorCodes`);
messages are for humans and change.

## 15. The subscription parameter map

`Subscribe.parameters` and each `Resume` subscription's parameters bind the query's `:name`
placeholders. This is the one place the wire still carries MessagePack **values** rather than a
MelangeDB encoding.

**Frozen decision** (owner, 2026-11-21): **pinned as it is, with a closed set of value shapes.**
Changing it would break the wire to save one map per subscribe, not per row. Protocol v2's break
was justified by a per-row cost, and this has none. It is written down here so the next person to
notice the inconsistency finds an answer rather than an omission.

**C15.1 — The map is `nil` or a MessagePack map of `str` keys** (the placeholder names, without
the colon) to values. Every placeholder the query names must be present.

**C15.2 — A value is exactly one of these shapes, chosen by the column it is compared against:**

| Column kind | Parameter value |
| --- | --- |
| `Bool` | MessagePack `bool` |
| signed integers (`Int8`–`Int64`), and enums over them | a MessagePack integer, any width, in the kind's range |
| unsigned integers (`UInt8`–`UInt64`), and enums over them | a non-negative MessagePack integer, any width, in range |
| `String` | MessagePack `str` |
| `Bytes` | MessagePack `bin` |
| `Identity` | MessagePack `bin` of exactly 32 bytes |
| `Timestamp` | a MessagePack integer: microseconds since the Unix epoch |

An enum is sent as its member's underlying number. Floats, `ScheduleAt`, `nil`, arrays, maps, and
extension types are outside the set. Predicates only apply to indexed columns, and float and
`ScheduleAt` columns cannot be indexed. `nil` is refused (`parse`). An array, map, or extension
value is a protocol error that closes the connection.

**C15.3 — The server's acceptance is wider than the set, and that is not contract.** Today the
server's coercion also turns a float into an integer column's value (by rounding), a decimal string
into an integer, a base64 string into bytes, and a hex string into an identity. A client that relies
on any of those is relying on an accident a later server may refuse.

## 16. Identity

**C16.1 — Read identity from `Welcome`; never derive it.** The identity is
`SHA256(issuer | subject)` computed by the server, and that derivation is the one piece of the
contract that must never disagree. *Fails if:* a client hashes its own JWT. Once the IdP is a third
party, or the server's derivation changes, "my rows" quietly become nobody's. Show it as 64
lowercase hex characters.

## The corpus

[`conformance/corpus.json`](../conformance/corpus.json), recorded by `tools/MelangeDB.Conformance`
against the conformance module (its manifest is
[`conformance/melange-schema.json`](../conformance/melange-schema.json)). `CorpusTests` re-records
it on every build and fails if one byte differs, so the file is always what the current server
sends. Regenerate it after a deliberate wire change with:

```
dotnet run --project tools/MelangeDB.Conformance -c Release -- record
```

Review the diff before committing a regenerated file: every changed frame is a change every client
must follow.

**Shape.** `sessions` are ordered connections. Each has `steps`, and each step has a `send`
describing what the client did and the frames the server answered with. A frame has `hex` (the
exact message) and `expect` (its decode). Frames within a step are ordered by channel, because only
within-channel order is promised. A session with `descriptorsFrom` resumed another session's
subscriptions: replay that session first to hold its descriptors. `normalized` lists the only bytes
not exactly as sent: each `Welcome`'s per-run `connectionId` and `epochId` are rewritten to fixed
values so the file is deterministic.

**To use it**, replay each session's frames through your decoder, carrying descriptors per
subscription id (C4.1), and compare your decode to `expect` exactly. Then check `columnVectors`
(one column value's row bytes per kind, including `ScheduleAt`), `keyVectors` (C5.2), and
`argumentVectors` (C9.2) byte for byte.

**Canonical values in `expect`:**

| Value | JSON form |
| --- | --- |
| `Int8`–`Int32`, `UInt8`–`UInt32`, request and subscription ids, channels, kinds | number |
| `Int64`, `UInt64`, `Timestamp`, LSNs, microseconds | decimal string |
| `Float32`, `Float64` | lowercase hex of the IEEE-754 bits, big-endian (8 or 16 digits), so NaN, `-0`, and infinities compare exactly |
| `String` | string, or `null` |
| `Bytes`, `Identity`, keys, masks, 16-byte ids, payloads | lowercase hex; `Bytes` may be `null` |
| `ScheduleAt` | `{"interval": bool, "micros": "<decimal>"}` |
| a masked-out column | absent from `columns` |
| a tagged value | `{"tag": "Int64", "value": ...}`; `Null` has no value; `Array` has `items` |

## Findings

Writing the contract down found these, as the phase plan said it would. None is fixed in this
step; each becomes an issue or a fix in a later step, at the owner's call.

1. **The C# client does not recover from `overflow_resync`** (C11.1). `MelangeClient` raises
   `OnError` and keeps treating its subscriptions as live; nothing re-subscribes, so the caches
   stop changing with no further signal until the application reconnects. `BackpressureTests`
   re-subscribe by hand over a raw socket, which is why no test noticed. The live suite's
   backpressure scenario will fail the C# adapter until this is fixed.
2. **Resume across an additive migration may decode with a stale descriptor.** Found by reading;
   not yet reproduced. An additive migration keeps the log's epoch
   (`SchemaShapeResumeTests` relies on that), so a client that reconnects after a deploy which
   added a column is resumed. It keeps the descriptor it held (C10.3), while the server encodes the
   gap and every later delta with the new one. A trailing column would be read past harmlessly; a
   column added mid-table would shift every column after it. `SchemaShapeResumeTests` reorders
   stored history without changing the live descriptor, so it does not cover this. The fix is
   probably to refuse a resume whose subscriptions' shapes changed since the cursor, which falls
   back to a full resync with fresh descriptors.
3. **`Public = true` on a scheduled table is silently dropped.** Scheduled tables are forced
   private, which is why no frame can carry `ScheduleAt`. Ignoring the attribute without a
   diagnostic is surprising; the corpus covers the kind with vectors instead.
4. **The server accepts parameter shapes wider than the closed set** (C15.3). That is harmless while
   there is one client. It becomes a compatibility promise nobody made the moment a second client
   leans on it.
