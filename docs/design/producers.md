# Producers: typed, read-only calls a client makes for data

**Goal:** a client asks the server a question and gets a typed answer — "my last 50 wallet lines,
page 2" — without the answer having to be a table the client may subscribe to. The motivating case
is archive history: [phase 25](../road-to-0.3/plan-phase-25.md) settled that **clients never read
archive tables directly**, so the history a player is entitled to see has to be shaped and authorized
by server code. A producer is that code.

**Status:** **design record, not scheduled.** Written when phase 25's client-facing read was
replaced by this idea. It is a protocol change, so its sequencing against
[phase 23](../road-to-0.3/plan-phase-23.md) — which pins the wire contract for a second client
runtime — is the first decision below.

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
  client disconnects or the call times out, and `Db` — a **read-only committed view** of hot tables
  (`CommittedReadView` already exists for policies). Archive tables stay unreadable through it, as
  in a reducer; the producer injects `IArchiveReader` explicitly, which makes the Postgres read a
  visible choice.
- **Return-type serialization.** The one genuinely new piece. Reducers return nothing and tables
  ship as schema-ordered row bytes; a producer returns arbitrary DTOs (`List<WalletLine>`). The
  generator emits a codec for each return type it reaches, from the same kinds the row format
  supports (scalars, `string`, `byte[]`, `Identity`, `Timestamp`, enums, arrays/lists, nested
  records of those), and records the shapes in the schema manifest so client generators emit the
  matching types. Anything outside that set is a compile error, as an unserializable reducer
  parameter is today (MELANGE0004's sibling).
- **Wire frames**: a call (`id`, producer name, args) and a result (`id`, payload) or error, on the
  existing socket, plus an HTTP form (`POST {path}/producers/{name}`) for clients without one.
- **Authorization** on the existing ladder: the reducer policy shape (`MayCall`) applies unchanged,
  and the unpoliced-reducer startup report covers producers too — an unpoliced producer is an open
  read of whatever it returns. Row policies do not apply: the producer's own code decides what the
  caller may see, which is the point.
- **Limits**: the reducer rate limiter, a per-call timeout, and a response size cap, each a
  documented setting.

## Decisions to settle

### Before or after phase 23 pins the protocol

**Leaning:** before, or folded into it. Phase 23 exists to pin the wire contract while there is still
only one client runtime; adding two frames after it means reopening that contract and implementing
them twice. If 23 is already underway, producers ship HTTP-only first, and the socket frames wait for
the next deliberate protocol revision.

### Whether producers may read archive tables through `ctx.Db`

**Leaning:** no. Keeping `ctx.Db` the same read-only hot view in reducers, policies, and producers
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
