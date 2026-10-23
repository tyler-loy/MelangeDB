# Phase 26 — Producers: typed, read-only calls a client makes for data

**Status: Built.** Ships in the same release as [phase 25](plan-phase-25.md), whose breaking
changes already make that release a version boundary, and ahead of [phase 23](plan-phase-23.md),
which pins the wire contract for a second client runtime.

**Goal:** a client calls a server method and gets a typed answer back —
`List<WalletLine> History(int page)` — without the answer having to be a table it may subscribe to.
The design, including the transaction scope, is [design/producers.md](../design/producers.md); this
plan is the order it gets built in and the decisions the build needs that the design left open.

**Depends on:** phase 25's `IArchiveReader` and its `_melange_lsn` column (for the consistent
hot-plus-archive read), snapshot isolation's `IReadViewSource` (for the pinned view), and the
reducer dispatch, policy, rate-limit, and manifest machinery, all of which producers reuse.

## Why here

Phase 25 settled that clients never read archive tables directly, so archived history currently has
no way to reach a client at all. The wallet sample works around it with a hand-written HTTP endpoint
that any caller can hit for any owner. Producers are the replacement, and they are a protocol
change: landing them before phase 23 means the TypeScript client is written against frames that
already exist rather than reopening the contract a release later.

## The shape

```csharp
public sealed class WalletProducers(IArchiveReader archive)
{
    [Producer(Policy = typeof(SignedInPolicy))]
    public async Task<List<WalletLine>> History(ProducerContext ctx, string? after = null)
    {
        var recent = ctx.Db.WalletEntry.Owner.Filter(ctx.Caller).Select(WalletLine.From).ToList();
        var older = await archive.ReadAsync<WalletEntryArchive>(new ArchiveQuery
        {
            Column = nameof(WalletEntryArchive.Owner), Equal = ctx.Caller,
            Descending = true, After = after, AsOfLsn = ctx.Lsn,
        }, ctx.CancellationToken);
        return [.. recent, .. older.Rows.Select(WalletLine.From)];
    }
}

public sealed record WalletLine(string Key, long Delta, string Reason, Timestamp At);
```

```csharp
var lines = await conn.Producers.HistoryAsync(after: null);   // List<WalletLine>
```

## Deliverables

One stacked PR each, in this order.

### 1. Pinned committed views

- `MelangeEngine.OpenReadView()` returns a disposable, read-only `IDbView` pinned at one LSN
  (`Lsn` property), built on `IReadViewSource`, with access serialized by its own lock so concurrent
  reads from one async body are safe. Throws, naming the capability, on a store without pinned reads.
- `ArchiveQuery.AsOfLsn`: Postgres rows with a greater `_melange_lsn` are excluded and the
  unapplied-tail overlay stops at that LSN.
- **Fixes a latent race:** a reducer's `MayCall` policy reads `CommittedView` *before* the
  transaction opens, off the write lock, which the `IHotStore` contract does not allow. It now
  evaluates against a pinned view, or inside `ReadConsistent` on a store without one.

**As built:** two more findings went into the same PR. The gateway's `IShardStrategy` had the same
unlocked read and now goes through the same `MelangeEngine.ReadCommitted`. And snapshot-isolated
reducers opened their pinned view *without* the write lock, which `IReadViewSource` requires. On the
in-memory store a two-table commit could land mid-capture, giving a view that was half one commit;
a test reproduces it within about 100 ms. Views are now opened under the lock. `AsOfLsn` also waits
for the log to be durable through that LSN, because a pinned view can include a commit whose fsync
is still in flight, and the overlay reads only durable records.

### 2. `[Producer]` and the generator

- `ProducerAttribute` (`Name`, `Policy`, `Site`) and `ProducerContext` (`Caller`, `Claims`,
  `Timestamp`, `Lsn`, `Db`, `CancellationToken`) in Abstractions.
- Discovery and dispatch emission: parameters exactly as reducers (`ReducerArgsReader`, defaults
  allowed); return type `T`, `Task<T>`, or `ValueTask<T>`.
- **Return-type codecs.** A result is one value in the reducer-argument encoding; a record is an
  `Array` of its fields in declaration order, so no new tag is needed and the manifest supplies the
  names. Supported: every reducer parameter kind, `T[]`, `List<T>`, `IReadOnlyList<T>`, nullable
  values, and records, classes, or structs of those (public fields and properties, positional).
- Diagnostics: MELANGE0028 (producer signature: first parameter `ProducerContext`, a non-void
  return, not a lifecycle kind), MELANGE0029 (unserializable return type, naming the member path —
  `[Table]` structs included, see below), and producer names sharing the reducer namespace (one
  name, one callable).
- Registration: `ProducerDescriptor` and `ProducerRegistry`, emitted into the generated model beside
  the reducers.

### 3. The producer host

- `MelangeProducerHost.CallAsync`: decode and validate, rate limit, authorize, resolve from a fresh
  DI scope, pin a view, invoke, encode, unpin, in that order — validation and policy before any pin.
- Limits, each in [CONFIGURATION.md](../CONFIGURATION.md): `Producers:TimeoutSeconds` (the
  cancellation token fires; the call answers `timeout`), `Producers:MaxResponseBytes` (encoded
  result over the cap answers `too_many_bytes`), `Producers:MaxConcurrentPerConnection` (bounds the
  pins one client can hold). Rate limiting reuses `RateLimit:*` with the producer's name as the key.
- The unpoliced report lists producers beside reducers, under the same `Policies:*` posture.
- A reducer invoked from inside a producer call is refused.
- Startup refuses a store without `IReadViewSource` when any producer is registered.
- Telemetry: a `melange.producer` span, call count and duration by producer and outcome, pin age.

### 4. The wire

- Protocol version 3: `CallProducer` (17) and `ProducerResult` (18) on the calls channel. The server
  negotiates 2–3 and refuses producer frames on a version-2 connection; the client offers 3.
- The socket runs producer calls off the read loop, and a disconnect cancels the calls in flight.
- `POST {path}/producers/{name}`: a JSON array of arguments in, the result as JSON out, the same
  error codes as the reducer call endpoint.

### 5. Manifest and client bindings

- The manifest records each producer (name, parameters, return type) and each result type (name,
  fields), both inside the schema hash.
- `ClientEmitter` emits the result records, their decoders, and `conn.Producers.NameAsync(...)`;
  `MelangeClient.CallProducerAsync` returns the decoded value.
- Result type names share the client namespace with tables and enums; a collision aborts the
  manifest the way an enum collision does (MELANGE0019).

### 6. Cluster

- Execution site resolved by the reducer rules (hub when the body touches only `Global` and
  `Replicated` tables, the shard otherwise, the shard when the body is opaque), `[Producer(Site =
  ...)]` to override.
- The gateway routes `CallProducer` frames as it routes reducer calls, and speaks version 3 on both
  sides.

**As built:** PR 4 shipped the gateway settling on version 2, so clients through it never sent a
producer frame it could not route; this PR lifts it to 3. Shard-bound producer calls join the
handoff hold queue with reducer calls, and a held call that cannot be delivered is refused in the
frame its kind answers with.

### 7. The wallet sample and the docs

- The wallet's anonymous `/wallet/{owner}/archive` endpoint becomes a `History` producer, scoped to
  the caller.
- DESIGN, CLIENT-BINDINGS, OBSERVABILITY, CONFIGURATION, GLOSSARY, THREAT-MODEL, CHANGELOG, ROADMAP.

## Out of scope

- **Server-side caching and push.** A producer is a read at one moment. A client that needs live
  data subscribes.
- **Streaming results.** One call, one result, bounded by `MaxResponseBytes`. A large history is
  paged by the producer's own arguments, as the archive reader's keyset `After` already is.
- **The TypeScript client.** Phase 23 implements these frames from the start.

## Decisions to settle

### How a result is encoded

**Settled: the reducer-argument encoding, records as positional arrays.** The alternatives were the
v1 row format (schema-ordered, but it has no nesting or lists) and MessagePack maps (self-naming,
but a second value codec for every client to implement). The tagged-value format already exists on
both sides, already nests arrays, and the manifest carries field names, so decoding is positional
and cheap.

### Whether a `[Table]` struct can be returned

**Settled: no, MELANGE0029.** A producer's return type is its contract with the client. Returning
a table row would publish the table's shape — including a private or archive table's — through the
back door the archive decision closed, and a client would need that table's row type even when it
is not public. Mapping to a record is one line, and it is the line where the author decides what
the client sees.

### Which policy interface a producer uses

**Settled: `IReducerPolicy`, unchanged.** `MayCall(name, context)` already answers "may this
caller invoke this named thing"; a second interface with the same shape would only split the
startup report.

### Where producers run in a cluster

**Settled: the reducer execution-site rules**, as the design leaned. A producer reading `Global`
tables runs on the hub; one reading a player's `Partitioned` rows runs on the owning shard; an
opaque body goes to the shard, where a `Global` read fails loudly with the fix in the message.

### Error codes

**Settled:** two new codes, `unknown_producer` and `timeout`. A shared `unknown_callable` was
considered and dropped, because renaming `unknown_reducer` would break version-2 clients.
`too_many_bytes`, `rate_limited`, `denied`, `invalid_args`, and `internal` carry over unchanged.
