# Producers: typed, read-only calls a client makes for data

**Goal:** a client asks the server a question and gets a typed answer — "my last 50 wallet lines,
page 2" — without the answer having to be a table the client may subscribe to. The motivating case
is archive history: [phase 25](../road-to-0.3/plan-phase-25.md) settled that **clients never read
archive tables directly**, so the history a player is entitled to see has to be shaped and authorized
by server code. A producer is that code.

**Status:** **scheduled as [phase 26](../road-to-0.3/plan-phase-26.md)**, in the same release as
phase 25 and ahead of [phase 23](../road-to-0.3/plan-phase-23.md). Written when phase 25's
client-facing read was replaced by this idea; the decisions below are settled unless marked as
leanings.

## The shape

```csharp
public sealed class WalletProducers(IArchiveReader archive)
{
    [Producer]
    public async Task<List<WalletLine>> History(ProducerContext ctx, int page = 1)
    {
        var recent = ctx.Db.WalletEntry.Owner.Filter(ctx.Caller).Select(WalletLine.From);
        var older = await archive.ReadAsync<WalletEntryArchive>(new ArchiveQuery
        {
            Column = nameof(WalletEntryArchive.Owner), Equal = ctx.Caller, Descending = true, /* … */
        }, ctx.CancellationToken);
        return [.. recent, .. older.Rows.Select(WalletLine.From)];
    }
}

public sealed record WalletLine(string Key, long Delta, string Reason, Timestamp At);
```

```ts
const lines = await client.producers.history({ page: 2 }); // WalletLine[]
```

A producer is everything a reducer is not, on purpose:

| | Reducer | Producer |
| --- | --- | --- |
| Writes | Yes — the write set is the commit | **No.** It has no write set and commits nothing |
| I/O | None; synchronous, under the write lock | **Allowed; async**, off the write lock |
| Returns | Nothing (success or rejection) | **A typed value** |
| In the log | Yes, as a record | **No** — a read leaves no history |
| Reads | The transaction's overlay | A **committed read view** of hot tables, plus any service it injects (`IArchiveReader`, an HTTP client) |
| Determinism | Required (replay) | Not required — nothing replays it |

The name is the user-facing contract: *a producer produces data*. Because it never writes, it can
do what a reducer must not — await Postgres, call another service — without threatening the
commit path, replay, or determinism.

## What it needs

- **`[Producer]` methods**, DI-resolved like reducers (scoped services, `IOptions`, `ILogger`),
  discovered by the source generator. Async (`Task<T>`/`ValueTask<T>`) or sync. Parameters use the
  reducer argument encoding (`ReducerArgs`, self-describing tagged values), defaults allowed.
- **`ProducerContext`**: `Caller`, `Claims`, `Timestamp`, a `CancellationToken` that fires when the
  client disconnects or the call times out, `Lsn`, and `Db` — a **read-only view of hot tables
  pinned at `Lsn` for the whole call** (see [transaction scope](#transaction-scope)). Archive tables
  stay unreadable through it, as in a reducer; the producer injects `IArchiveReader` explicitly,
  which makes the Postgres read a visible choice.
- **Return-type serialization.** The one genuinely new piece. Reducers return nothing and tables
  ship as schema-ordered row bytes; a producer returns arbitrary DTOs (`List<WalletLine>`). The
  generator emits a codec for each return type it reaches, from the same kinds the row format
  supports (scalars, `string`, `byte[]`, `Identity`, `Timestamp`, enums, arrays/lists, nested
  records of those), and records the shapes in the schema manifest so client generators emit the
  matching types. Anything outside that set is a compile error, as an unserializable reducer
  parameter is today (MELANGE0004's sibling).
- **Wire frames**: a call (`id`, producer name, args) and a result (`id`, payload) or error, on the
  existing socket's calls channel, plus an HTTP form (`POST {path}/producers/{name}`) for clients
  without one. The frames arrive with protocol version 3; the server still accepts version 2, which
  is version 3 without producers.
- **Authorization** on the existing ladder: the reducer policy shape (`MayCall`) applies unchanged,
  and the unpoliced-reducer startup report covers producers too — an unpoliced producer is an open
  read of whatever it returns. Row policies do not apply: the producer's own code decides what the
  caller may see, which is the point.
- **Limits**: the reducer rate limiter, a per-call timeout, and a response size cap, each a
  documented setting.

## Transaction scope

A producer has a read scope and no write scope. Both halves need saying, because a producer is
async and runs off the write lock, which is the one thing every other `IDbView` consumer avoids.

### Hot reads: one pinned view for the whole call

`CommittedReadView` — the view policies use — is **not** what a producer gets. It reads the hot
store with no lock, which the `IHotStore` contract allows only on paths the engine serializes
against `Apply`; an async body is not one of them, and a raw read racing an apply is undefined
("collection was modified", a half-applied batch, or a plausible stale answer).

The three ways to scope it, and why the first wins:

| | Consistency | Cost |
| --- | --- | --- |
| **One read view pinned for the call** (`IReadViewSource.OpenReadView`) | Every read sees one committed state, named by its LSN | The store retains superseded versions while the pin is held, including across awaits |
| `ReadConsistent` around each read | Each read is clean; two reads can disagree | Every read takes the write lock — producers become write latency |
| The write lock for the whole call | Same as a pin | Impossible across an await |

So a call opens one pinned view before the body runs and exposes it as `ctx.Db`, with its LSN as
`ctx.Lsn`. A producer is the read-only half of a snapshot-isolated reducer, which already works
this way. The rules that follow:

- **The pin outlives the body until the result is serialized.** A return value may hold a lazy
  `ctx.Db` enumeration; disposing the pin first would read it after release. Return types are
  lists, arrays, and records (below), so the codec materializes everything before the pin goes.
- **The per-call timeout bounds the pin.** A producer stuck on Postgres holds old versions only as
  long as the timeout allows; pin age is a metric.
- **One reader at a time.** A pinned view is not thread-safe for concurrent use, and a producer
  can `Task.WhenAll` two `ctx.Db` reads. The view handed to the body serializes access with its own
  lock, so that is safe rather than undefined.
- **A store without pinned reads cannot host producers.** Snapshot-isolated reducers fall back to
  running serialized; a producer cannot, because it cannot hold the write lock across an await. Both
  shipped stores pin, so a third-party store without `IReadViewSource` is refused at startup when
  any producer is registered, naming the capability.

### Hot and archive read the same moment

A producer that merges hot and archive rows — the wallet's whole point — reads two stores at two
moments unless told otherwise. The archive sweep moves rows from hot to archive in between:

- archive read **after** the pin: an entry moved in between appears in both — a duplicate;
- archive read **before** the pin: an entry moved in between appears in neither — silently missing.

So `ArchiveQuery` gains `AsOfLsn`. Given `ctx.Lsn`, the reader excludes Postgres rows whose
`_melange_lsn` is greater, and overlays only the unapplied tail up to that LSN. Both tiers then
answer for the same commit, and which is read first stops mattering. The `_melange_lsn` column
phase 25 already writes is what makes this possible.

**The known gap:** archive deletes carry no LSN, so a blind delete committed after `ctx.Lsn` and
already applied is visible early — the row is simply absent. Deletes from an archive are rare and
the effect is a row missing that was deleted anyway; documented, not engineered around.

### No write scope

- **A producer commits nothing.** It cannot invoke a reducer: the nested-call guard covers only
  reducer bodies, so the producer host refuses a reducer invocation from inside a producer call
  explicitly. Code that needs to write is a reducer.
- **Nothing spans calls.** A client reading a balance through a producer and then debiting it
  through a reducer runs two transactions; the reducer re-checks. This is the same contract a
  subscription already has.
- **Read-your-writes on one node.** A reducer call is acknowledged only after its write set is in
  the hot store, so a producer the same client starts afterwards pins a view that includes it, and
  the archive overlay covers anything Postgres has not applied. In a cluster this holds when the
  producer runs where the reducer did — see the execution site below.

## Decisions to settle

### Before or after phase 23 pins the protocol

**Settled:** before. Phase 23 exists to pin the wire contract while there is still only one client
runtime; adding two frames after it means reopening that contract and implementing them twice.
Producers ship in the same release as phase 25, whose breaking changes already make it a version
boundary, with both the socket frames and the HTTP form.

### Whether producers may read archive tables through `ctx.Db`

**Settled:** no. Keeping `ctx.Db` the same read-only hot view in reducers, policies, and producers
means one rule — "`Db` is the hot store" — and an archive read is always an injected, awaited, visible
call. The cost is one constructor parameter.

### Where a producer runs in a cluster

A producer reading `Global` tables (the archive's placement) must run on the hub; one reading a
player's `Partitioned` rows wants the owning shard. **Leaning:** the reducer execution-site rules,
resolved at compile time from the tables the body touches, and the same conservative-plus-loud
fallback.

### Caching and staleness

A producer's answer is a read at one moment and is not kept live, unlike a subscription. **Leaning:**
say so, and nothing more in the first version — no server-side cache, no push. A client that needs
live data subscribes; a client that needs history calls a producer. The `Stale` flag an
`IArchiveReader` page carries is the producer author's to pass on.

## What this replaces

Phase 25's plan leaned toward a generic, policy-checked HTTP read of archive tables. That is
withdrawn: a generic read exposes a table's shape and needs a visibility knob on a tier that is
private by construction, while a producer exposes exactly the answer its author wrote and nothing
else. Server-side code that is not a client — an admin tool, a report — keeps using `IArchiveReader`
directly, as the wallet sample's endpoint does.
