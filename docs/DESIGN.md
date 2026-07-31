# MelangeDB — Design

MelangeDB is a C# alternative to SpacetimeDB: a relational store where your application logic runs
*inside* the transaction boundary, and connected clients subscribe to live query results instead of
polling an API.

It exists to fix three specific things about SpacetimeDB.

| Problem | Fix |
| --- | --- |
| No innate clustering | Tables declare a `Placement`; one commit log per shard, one writer per shard. The *sharding function itself is yours to define* — spatial partitioning and instancing are both first-class. See [CLUSTERING.md](CLUSTERING.md). |
| Whole dataset pinned in RAM | Stores are *projections* of the log with their own paging and spill-to-disk. Working set, not total set, bounds memory. |
| Statics everywhere, no DI | MelangeDB is a NuGet library inside **your** host process. Reducers are DI-resolved classes. `IConfiguration`, `ILogger<T>`, and `IOptionsMonitor<T>` are constructor-injected like any other service, so Azure App Configuration and feature flags just work. |

## 1. Shape of the thing

MelangeDB is **not a server you deploy.** It is a package you add to a .NET Worker Service or ASP.NET
Core app. Your reducers compile into your executable, which you fully control.

```csharp
var builder = Host.CreateApplicationBuilder(args);

// Azure App Configuration, feature flags, whatever — this is just the normal host.
builder.Configuration.AddAzureAppConfiguration(/* ... */);

builder.Services.AddMelangeDb(melange =>
{
    melange.UseHotStore(o => o.Path = "./data/hot");            // world state machine
    melange.AddPostgres(builder.Configuration                    // optional "servicey" tier
        .GetConnectionString("Melange"));
    melange.AddTablesFrom(typeof(Player).Assembly);
    melange.AddReducersFrom(typeof(Program).Assembly);
});

var app = builder.Build();
app.MapMelangeSocket("/melange");
app.Run();
```

Consequences of this choice, stated plainly:

- **No module ABI, no WASM, no `AssemblyLoadContext`.** The entire class of problems that produced
  SpacetimeDB's statics disappears, because there is no host/guest boundary to marshal across.
- **No sandbox.** Reducer code runs with the host process's full authority. That is a deliberate
  trade: this is *your* code in *your* exe, not untrusted third-party modules.
- **Deployment is `dotnet publish`.** No `melange-cli publish`, no separate database process to
  operate.

## 2. Data model

Tables are `partial struct` mutated with `with` expressions — value types keep allocation off the
reducer hot path. Attributes describe schema; a source generator turns them into registrations and
serializers.

```csharp
[Table(Public = true)]                        // hot tier, syncable to clients
public partial struct Player
{
    [PrimaryKey] public Identity Id;
    [Index]      public int RoomId;
    public float X;
    public float Y;
}

[Table(Tier = StorageTier.Relational)]        // opts into Postgres; private by default
public partial struct Registration
{
    [PrimaryKey, AutoInc] public long Id;
    [Unique] public string Email;
    public DateTimeOffset CreatedAt;
}
```

Four orthogonal knobs, and conflating any two of them is a design error:

- **`Tier`** — *where the row is stored.* Hot (default) or Relational. Postgres is opt-in, so a table
  only lands there if it asks to, and a deployment only needs Postgres if some table asks to.
- **`Public`** — *may this table sync to clients at all?* **Private by default.** A private table is
  server-internal and no subscription may name it.
- **`Residency`** — *must this table stay wholly in memory?* See §8.
- **`Placement`** — *which node holds it in a cluster?* Partitioned, Replicated, Global, or Local.
  Single-node deployments ignore it. See [CLUSTERING.md](CLUSTERING.md).

`[AutoInc]` sequence values are assigned into the write set **before** the log append, from a durable
per-table sequence recovered on startup — otherwise replay would reassign different ids. The contract is
**unique, not dense**: gaps are normal, which is what lets a clustered deployment allocate from
originator-prefixed ranges without coordination (see [CLUSTERING.md](CLUSTERING.md)).

## 3. Scheduled and lifecycle reducers

Not all reducers are called by clients, and in a simulation most of the interesting ones aren't.

**Scheduled reducers** drive world simulation: AI ticks, resource respawn, growth, decay, expiry,
compaction. Timers are stored **as rows** in a private table, which makes scheduling transactional and
recoverable — a schedule survives a crash because it lives in the log like any other data.

```csharp
[Table(Scheduled = nameof(TickCreatures))]
public partial struct CreatureAiTick
{
    [PrimaryKey, AutoInc] public ulong Id;
    public ScheduleAt ScheduledAt;            // a one-shot instant, or a repeating interval
}
```

**Lifecycle reducers** fire on session transitions:

```csharp
[Reducer(ReducerKind.ClientConnected)]    public void OnConnected(ReducerContext ctx) { }
[Reducer(ReducerKind.ClientDisconnected)] public void OnDisconnected(ReducerContext ctx) { }
```

"A client session began" must stay distinct from "someone ran an admin query." Conflating the two
forces every module to special-case tooling identities to avoid spawning ghost player rows.

Timers must be **data, not code**. An inline `[Cron("*/5 * * * *")]` attribute would be simpler to read,
but static code cannot be partitioned, transactionally scheduled, or rescheduled at runtime. Because a
timer is a row, it inherits everything rows get: it commits atomically with the work that scheduled it,
it survives a crash, and — per [CLUSTERING.md](CLUSTERING.md) — it inherits its table's `Placement` and
so fires on whichever node owns its shard. No global timer wheel, no leader election for scheduling.

## 4. Reducers

A reducer is a method on a DI-resolved class. It is invoked with a scoped `IServiceProvider`, so it
can take dependencies like anything else in the host.

```csharp
public sealed class CombatReducers(
    IOptionsMonitor<CombatSettings> settings,       // hot-reloads from App Configuration
    ILogger<CombatReducers> logger)
{
    [Reducer]
    public void Attack(ReducerContext ctx, Identity target, int weaponId)
    {
        if (!settings.CurrentValue.PvpEnabled) throw new RejectedException("PvP is off");

        var attacker = ctx.Db.Players.FindByPrimaryKey(ctx.Caller)
            ?? throw new RejectedException("not joined");

        // ... mutate through ctx.Db; nothing touches disk here
        logger.LogDebug("{Attacker} hit {Target}", ctx.Caller, target);
    }
}
```

`ReducerContext` supplies everything ambient so that reducers stay deterministic and replayable:
`ctx.Caller` (identity), `ctx.ConnectionId`, `ctx.Timestamp`, `ctx.Random` (seeded per commit), and
`ctx.Db` (the transactional view). Using `DateTime.Now` or `new Random()` directly is a bug, and the
analyzer should say so.

**No I/O in a reducer body.** Reads are served from an overlay — write set on top of the store — and
writes accumulate in the write set. A reducer either returns (commit) or throws (abort, nothing
appended).

## 5. The commit log is the database

This is the load-bearing decision.

```
   reducer call
        │
        ▼
  ┌───────────┐   build in-memory write set (no I/O)
  │ dispatcher│
  └─────┬─────┘
        │  one atomic append == the commit point
        ▼
  ┌──────────────────────────────────────────┐
  │  COMMIT LOG   (ordered, LSN-addressed)   │
  └──┬───────────────┬──────────────────┬────┘
     │               │                  │
     ▼               ▼                  ▼
 hot store      Postgres          subscription
 (world state)  (servicey)         fan-out
 @lsn 1042      @lsn 1039          @lsn 1042
```

Each log record holds one committed transaction: the LSN, a timestamp, the caller identity, the
reducer name and arguments **as metadata**, and the authoritative payload — the **write set**, as
row-level `Insert`/`Update`/`Delete` operations keyed by table and primary key.

Logging the *write set* rather than the *reducer invocation* is intentional. It means replicas and
projections can be rebuilt without re-executing user code, so recovery does not depend on reducer
determinism or on running the same version of your assembly. The reducer name and args ride along for
audit and debugging only.

Each applier checkpoints its own applied-LSN, which is why Postgres is allowed to lag the hot store
without breaking anything: on restart, each applier resumes from its own checkpoint.

**What this buys:**

- Atomicity across two unrelated storage engines with no 2PC and no XA — one append succeeds or it doesn't.
- The RAM ceiling goes away: the log is on disk, and each projection manages its own residency.
- Clustering is "one log per shard" — see [CLUSTERING.md](CLUSTERING.md). The log is the primitive
  that made it possible to settle the storage design before the clustering model.
- Reducers become replayable, which makes deterministic tests cheap.

**What it costs:**

- Read-your-writes within a reducer requires the write-set overlay described above.
- Postgres reads are eventually consistent with the log. A reducer that must read a relational table
  it just wrote needs the overlay, and external SQL readers may see a slightly stale tier.
- Commit latency is bounded by the log's fsync policy, which is therefore configurable.

### Domain events: the bus falls out of the log

A reducer can emit domain events alongside its row writes:

```csharp
[Reducer]
public void Attack(ReducerContext ctx, Identity target, int weaponId)
{
    // ... row mutations ...
    if (health <= 0)
        ctx.Publish(new PlayerDied(target, ctx.Caller, ctx.Timestamp));
}
```

`Publish` performs **no I/O**. The event lands in the write set and is published only *after* the log
append commits — a transactional outbox, with the log as the outbox. Two properties follow, and both
matter:

- **An event is never observed for a transaction that rolled back.** The failure mode where a
  notification escapes but the state change didn't is structurally impossible here.
- **Delivery is at-least-once and replayable**, because a subscriber is just another log consumer with
  its own checkpoint — the same mechanism as the storage appliers. A subscriber that was down catches
  up rather than losing events.

Handlers are resolved from DI and run *outside* the emitting transaction:

```csharp
public sealed class DeathHandler(ILogger<DeathHandler> log) : IEventHandler<PlayerDied>
{
    public Task HandleAsync(PlayerDied e, CancellationToken ct) { /* ... */ }
}
```

The bus is deliberately **not** a second source of truth. It is a projection of the log, exactly like
the hot store and Postgres. `IEventTransport` is in-process by default; a distributed transport is what
carries cross-shard sagas and world events once clustering exists (see
[CLUSTERING.md](CLUSTERING.md)). Because the log already provides ordering and replay, the bus does not
need to.

## 6. Subscriptions

Clients send a query; the server returns an initial result set and then streams incremental deltas
derived from the log as transactions commit.

**v1 is single-table filtered subscriptions** with **column projection**. Three query shapes cover
the real workload (see [VIBE-SHAFT-COVERAGE.md](VIBE-SHAFT-COVERAGE.md)):

```sql
SELECT * FROM recipe                                        -- whole table
SELECT * FROM inventory_item WHERE owner_id = :id           -- equality on an index
SELECT * FROM terrain_chunk_data WHERE chunk_id BETWEEN :lo AND :hi   -- range; spatial streaming
SELECT skill_id, total_xp, level FROM player_skill WHERE player_identity = :id  -- projection
```

Delta computation is then a cheap predicate test against each row op in the write set. Projection
means the wire format must carry **partial rows**, not just whole ones.

Incrementally maintaining **joins** is a genuinely hard problem (IVM / differential dataflow
territory) and is explicitly out of scope for v1. This is not a painful compromise: an audit of a
real 82-table SpacetimeDB game found **zero** subscriptions using a join.

Subscriptions can only name `Public = true` tables (§2).

**Ad-hoc SQL** is a separate, non-subscription facility: one-shot queries including aggregates
(`COUNT(*)`, time bucketing) that admin tooling needs and live subscriptions can't express.

## 7. Identity and auth

Because MelangeDB lives in your ASP.NET Core host, it uses that host's authentication. A JWT bearer
token on the websocket handshake resolves to a stable `Identity` — a hash of the token's issuer *and*
subject, so two token sources can never collide into one identity. Every connection presents a valid
token: **the IdP is the gate.** Guest play is a token the IdP issues with a guest role, not a parallel
identity system — MelangeDB mints nothing.

Row-level access rules are **policy objects resolved from DI**, not a bespoke rules language. Two
properties are load-bearing:

- **Multiple policies on one table compose as a UNION, not an intersection.** A player must be able to
  see their own inventory *plus* the contents of any open chest or cart — that is three rules unioned,
  and intersection semantics would make it unexpressible.
- **A policy may freely read private tables — private is not the constraint; placement is.** This is a real
  advantage over SQL-string filters: in SpacetimeDB, an RLS rule that joins a private table fails to evaluate
  for ordinary clients and kills their *entire* subscription. An in-process policy object has no restricted
  namespace, so "admins bypass this filter" is a trivial lookup rather than an impossibility. The one
  qualifier, settled in phase 09: policies evaluate on the node that fans the subscription out, and may read
  only tables **present on that node** — for a `Partitioned` table's subscription that means `Replicated`,
  `Partitioned`, and `Local` tables, and a policy reading a hub-only `Global` table there fails loudly with
  the fix in the message (make the table `Replicated`, which is what `AdminIdentity`-shaped reference data
  wants anyway). See [CLUSTERING.md](CLUSTERING.md).

## 8. Storage engines

`IHotStore` is the seam. The engine behind it is swappable, which matters because of an external
constraint worth stating up front:

> **Tsavorite is not published as a NuGet package.** It lives inside the `microsoft/garnet` repo at
> `libs/storage/Tsavorite/cs` and is documented there as an internal, significantly-diverged fork of
> FASTER tuned for Garnet's needs. `Microsoft.Garnet` is the *Redis-protocol server* as a library —
> the wrong abstraction. Using Tsavorite means vendoring unversioned source.

So the hot tier is staged:

1. `InMemoryHotStore` — dictionary + log recovery. Trivially correct, fast to write, ideal for tests.
   Since the log is the source of truth, this is a *legitimate* projection, not a stub.
2. `FasterHotStore` — `Microsoft.FASTER.Core` 2.6.5, published and versioned. Brings the hybrid log
   and spill-to-disk, which is the property that actually answers the RAM complaint.
3. Vendored Tsavorite, *if and only if* benchmarks justify the source dependency.

Doing (1) first de-risks the whole project: the transaction, log, and subscription layers can be
built and tested end-to-end before any storage-engine work begins.

### Residency: the paging store vs. full table scans

Escaping RAM has a cost that has to be faced head-on. Code written against an all-in-RAM database
scans tables freely, because `foreach (var c in ctx.Db.Creature.Iter())` is nearly free when the table
is already resident. The audited game does this in **52 places**. Put a paging store underneath and
every one of those becomes potential I/O — and "just cache it" reintroduces the RAM ceiling by the
back door.

The answer is to make the memory budget **declarative instead of accidental**:

```csharp
[Table(Public = true, Residency = Residency.Resident)]   // small, hot, scan-heavy
public partial struct ItemDefinition { /* ... */ }
```

Small bounded reference tables (item definitions, species, recipes — config-shaped data) are pinned
resident, so scans over them stay fast and honest. Large tables (terrain blobs, inventories) page.
Memory is then bounded by the tables you *declared* resident rather than by the whole dataset, which is
the actual fix for the RAM complaint. Unindexed scans over a non-resident table are a smell the
analyzer should flag.

### Large values

Blob columns (`byte[]`) are the dominant memory consumer in a real workload — the audited game stores
one RLE-compressed terrain blob per chunk across ~24.6k chunks. Large values are stored **out of line**,
so scanning a table by key doesn't fault in every blob. A separate S3-style object API remains out of
scope; large in-row values do not.

### Bulk ingestion

World generation writes tens of thousands of rows in one pass. There must be a bulk path that appends
one large write set, rather than one transaction per row.

## 9. Project layout

```
src/MelangeDB.Abstractions      attributes, ReducerContext, Identity, core interfaces (no deps)
src/MelangeDB.Core              schema model, write set, transactions, commit log, dispatcher, appliers
src/MelangeDB.Storage.Faster    FASTER-backed IHotStore
src/MelangeDB.Storage.Postgres  relational applier + reader
src/MelangeDB.Server            websocket transport, subscription engine, auth
src/MelangeDB.Client            C# client
src/MelangeDB.CodeGen           Roslyn source generator: registrations, serializers, typed clients
src/MelangeDB.OpenTelemetry     optional: registers MelangeDB's ActivitySource/Meter names with OTel
tests/                          unit + integration
samples/                        worker-service sample
```

## 10. Open questions

- ~~**Residency defaults**~~ — **Settled in phase 07: opt-in `Resident`, default `Paged`.** A size
  threshold makes memory a function of data size — the SpacetimeDB failure mode with a delay — while
  opt-in makes the resident footprint a declared, computable artifact. Ships with the compile-time scan
  analyzer (MELANGE0017), the startup residency report, `.Any()`/`.Count`/`.First()`, `Residency.Auto`
  for anyone explicitly wanting threshold behaviour, and the per-table configuration override. See
  [plan-phase-07.md](plan-phase-07.md).
- **Wire serialization** — MessagePack gets us moving and has implementations in every client
  language; a source-generated binary format is faster. Put it behind `IMelangeSerializer` and defer.
- **Schema migration** — how tier changes and column adds replay against an existing log. Worth
  designing for early: in SpacetimeDB every schema change means republish plus regenerating bindings
  for every client tree, and stale-schema clients simply break. **The relational tier's half settled
  in phase 08:** additive changes (create table, add column) are automatic under
  `Postgres:AutoMigrate` — an added NOT NULL column backfills existing rows with its kind's zero
  value, so an additive migration never drops or nulls data — while anything destructive (changed
  type, dropped column) is refused loudly with the pending DDL in the log, and stays a manual,
  deliberate migration; with AutoMigrate off (the default) the applier validates, stalls, and prints
  the exact DDL an operator would run. Columns present in Postgres but absent from the schema are
  left untouched. The hot-tier half — how column adds replay against an existing *log* — remains
  open. See [plan-phase-08.md](plan-phase-08.md).
- ~~**Log compaction / snapshots**~~ — **Settled in phase 07: full snapshot + truncate.** Snapshot at an
  LSN beside the log, truncate behind it, never past the slowest applier, the slowest live event
  subscriber, or the Resume retention window; restart is snapshot plus tail replay. See
  [plan-phase-07.md](plan-phase-07.md).
- **Codegen targets** — a real project has several client trees (game client, admin web, CLI tools)
  generating from one schema. `MelangeDB.CodeGen` should emit to multiple output trees from the start.
