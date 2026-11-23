# MelangeDB

A C# alternative to [SpacetimeDB](https://spacetimedb.com). Your application logic runs *inside*
the transaction boundary, and connected clients subscribe to live query results instead of polling
an API.

> **Status: 0.4.1, on nuget.org and npm, pre-1.0.** Everything listed under
> [What's in it](#whats-in-it) is implemented and tested.
>
> **The public API may break in any release.** What 1.0 requires is
> [deliberately still open](docs/ROADMAP.md#the-10-question-deliberately-open).
>
> A live game, ported off SpacetimeDB, runs on MelangeDB and is developed on it daily. There is no
> published head-to-head comparison against SpacetimeDB yet, so treat the numbers in these docs as
> this project's own load-test measurements, not as benchmarks against SpacetimeDB.

MelangeDB exists to fix three specific things:

- **Clustering isn't an afterthought.** Tables declare where they live, with one writer and one
  commit log per shard. **You define what a shard means:** a contiguous world partitions by space,
  an MMO city shards into instances, and both are first-class.
- **Your dataset isn't pinned in RAM.** Stores are *projections* of the log, with their own paging
  and spill-to-disk. Memory is bounded by your working set, not by your whole dataset.
- **Dependency injection all the way down.** MelangeDB is a NuGet package inside *your* host
  process, not a server you deploy. Reducers are DI-resolved classes, so `IConfiguration`,
  `ILogger<T>` and `IOptionsMonitor<T>` are constructor-injected like anything else. Azure App
  Configuration and feature flags work without extra wiring.

## Install

```
dotnet add package MelangeDB.Core
dotnet add package MelangeDB.Server           # WebSocket transport and subscriptions
dotnet add package MelangeDB.Storage.Faster   # the paged hot store
dotnet add package MelangeDB.CodeGen          # the source generator: referencing it is all it takes
```

All packages ship together at one version. Add the rest as you need them:

| Package | For |
| --- | --- |
| `MelangeDB.Client` | A C# client (a game client, a bot, a tool) |
| `@melangedb/client` (npm) | A browser or Node client, with bindings from `melange generate --lang ts` ([docs](docs/CLIENT-BINDINGS.md#typescript)) |
| `MelangeDB.Cluster` | Running more than one node |
| `MelangeDB.Storage.Postgres` | The optional relational backplane and the archive tier |
| `MelangeDB.OpenTelemetry` | Exporting MelangeDB's spans and metrics through OpenTelemetry |
| `MelangeDB.Mcp` | An opt-in endpoint for coding agents ([docs](docs/MCP.md)) |
| `MelangeDB.Testing` | Testing reducers and ticks against a real engine ([docs](docs/TESTING.md)) |
| `MelangeDB.Cli` | The `melange` tool: `dotnet tool install -g MelangeDB.Cli` |

[docs/RELEASING.md](docs/RELEASING.md#consuming-the-packages) covers per-commit prereleases.

**Host the engine on Server GC.** A Web SDK project (`Microsoft.NET.Sdk.Web`) uses it by default. A
console project needs `<ServerGarbageCollection>true</ServerGarbageCollection>`. At 400 players,
p99 call-to-delta was 0.27 s on Server GC and 15 s on workstation GC
([numbers](docs/LOAD-TESTING.md#after-the-gateway-fix-2026-11-22)). The engine logs EventId 1108 at
startup when it finds itself on workstation GC with four or more cores.

## A first look

Define a table and a reducer. The reducer is an ordinary class, and its dependencies are injected.

```csharp
[Table(Public = true)]
public partial struct Player
{
    [PrimaryKey] public Identity Id;
    [Index]      public int RoomId;
    public float X, Y;
}

public sealed class MovementReducers(IOptionsMonitor<WorldSettings> settings)
{
    [Reducer]
    public void Move(ReducerContext ctx, float x, float y)
    {
        var player = ctx.Db.Player.Id.Find(ctx.Caller)
            ?? throw new RejectedException("not joined");

        if (settings.CurrentValue.FrozenWorld) throw new RejectedException("world is frozen");

        ctx.Db.Player.Update(player with { X = x, Y = y });
    }
}
```

Register it in any .NET host. The generator discovers both, so nothing else is wired up by hand:

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMelangeDb(melange => melange
    .UseHotStore(o => o.Path = "./data/hot")            // world state
    .AddPostgres(builder.Configuration                  // optional relational backplane
        .GetConnectionString("Melange"))
    .AddTablesFrom(typeof(Player).Assembly)
    .AddReducersFrom(typeof(Program).Assembly));

var app = builder.Build();
app.UseWebSockets();
app.MapMelangeSocket();                                 // defaults to /melange
app.Run();
```

A client generates typed bindings from the server's exported schema. It then calls the reducer and
watches the results arrive live:

```csharp
await using var client = new MelangeClient(new MelangeClientOptions
{
    Uri = new Uri("wss://localhost:5001/melange"),
    Token = tokenFromYourIdP,
});
await client.ConnectAsync();

var conn = new MelangeConnection(client);
conn.Db.Player.OnInsert += p => Console.WriteLine($"{p.Id} entered");
conn.Db.Player.OnUpdate += (_, p) => Console.WriteLine($"{p.Id} moved to {p.X},{p.Y}");

await conn.Db.Player.RoomId.SubscribeAsync(7);          // SELECT * FROM Player WHERE RoomId = :p
var lsn = await conn.Reducers.MoveAsync(12.0f, 4.5f);
```

The compiler checks column and reducer names, so a renamed column is a build error rather than a
runtime failure. The TypeScript client has the same shape (`conn.db.Player.onInsert(...)`,
`conn.reducers.move(...)`). [docs/CLIENT-BINDINGS.md](docs/CLIENT-BINDINGS.md) shows how the schema
manifest reaches both. Runnable hosts and a client are in [`samples/`](samples).

## What's in it

**Engine**
- Transactions with write sets, a commit log, crash recovery, and group commit at unchanged
  durability.
- Reducers resolved from DI, scheduled reducers (timers are rows), and lifecycle reducers.
- A transactional event bus that uses the log as an outbox.
- Schema migration: additive changes are applied automatically and loudly; destructive ones are
  refused. A constraint added over existing rows, such as `[Unique]`, is checked against them.

**Storage**
- A paged hot store on FASTER, with residency tiers, snapshots and compaction.
- An optional Postgres backplane, with ad-hoc SQL and aggregates over it.
- An archive tier: Postgres-only tables for unbounded history.
- Named truncation floors, and a health check that reports which one holds the log back.

**Clients**
- WebSocket subscriptions with live deltas, and resume instead of refetch after a reconnect.
- Typed bindings for C# and TypeScript, from a schema manifest and `melange generate`.
- A written client contract with a conformance suite any client can run.
- Clients that outlive a deploy: old bindings keep working across additive schema changes, and
  reconnect is automatic.
- Producers: typed, read-only calls a client makes to ask for data.
- JWT identity, connect tickets, row and column policies, and rate limits.

**Clustering**
- Table placement, hub and shard roles, instancing, spatial sharding, and seamless handoff.
- Elastic assignment: shards follow load, with provisioned capacity and scale-in.
- Shard HA: an ownership fence, failure detection, and warm replicas promoted on failure.
- Shared subscription fan-out. On the reference game's shape (16 shards), 1,200 players run cleanly
  ([numbers](docs/LOAD-TESTING.md)).

**Operations and tooling**
- Backup and restore: `.mbak` archives, point-in-time restore, clones, and verification by booting
  the restore.
- OpenTelemetry spans and metrics, with no OpenTelemetry dependency in core.
- `MelangeDB.Testing` for reducer tests, and an opt-in MCP endpoint for coding agents.

Deliberately out of scope: joins in subscriptions, an unreliable/UDP transport, a sandbox for
reducer code, and a stock admin console. [docs/DESIGN.md](docs/DESIGN.md) argues each one, or
[docs/ROADMAP.md](docs/ROADMAP.md) records it as a refusal. Questions that are still open are in
[docs/idea-bin/](docs/idea-bin/), each with the trigger that would settle it.

## Docs

Start here:

- **[GLOSSARY.md](docs/GLOSSARY.md)**: every noun and what it means here, starting with the terms
  that sound alike but aren't.
- **[DESIGN.md](docs/DESIGN.md)**: the architecture, the trade-offs it accepts, and the open
  questions.
- **[CONFIGURATION.md](docs/CONFIGURATION.md)**: every setting. A new setting is added here in the
  same change that introduces it.

Building on it:

- **[CLIENT-BINDINGS.md](docs/CLIENT-BINDINGS.md)**: the typed bindings for C# and TypeScript, and
  how generation works.
- **[CLIENT-CONFORMANCE.md](docs/CLIENT-CONFORMANCE.md)**: the contract a client implements, and the
  suite that checks it.
- **[CLUSTERING.md](docs/CLUSTERING.md)**: the four table placements, hub and shard roles, and why
  *you* define the sharding function.
- **[MIGRATION.md](docs/MIGRATION.md)**: schema changes in both tiers, and the add-a-column deploy
  end to end.
- **[TESTING.md](docs/TESTING.md)**: testing reducers and ticks with `MelangeDB.Testing`.
- **[MCP.md](docs/MCP.md)**: the agent endpoint.

Running it:

- **[OBSERVABILITY.md](docs/OBSERVABILITY.md)**: every span, metric and log EventId.
- **[BACKUP.md](docs/BACKUP.md)**: the `.mbak` archive and the `melange backup`, `restore` and
  `backup verify` commands.
- **[THREAT-MODEL.md](docs/THREAT-MODEL.md)**: what a server can enforce against an untrusted
  client, and what it deliberately doesn't. To *report* a vulnerability, see
  [SECURITY.md](SECURITY.md).
- **[LOAD-TESTING.md](docs/LOAD-TESTING.md)**: the load rig, what it measures, and the recorded
  numbers.

The project:

- **[ROADMAP.md](docs/ROADMAP.md)**: what shipped in each phase, the decisions each one settled, and
  what's left.
- **[design/](docs/design/)**: design records for decisions too big for a phase plan.
- **[REFERENCE-WORKLOAD.md](docs/REFERENCE-WORKLOAD.md)**: the design checked against a live
  82-table SpacetimeDB game, taken before the port.
- **[RELEASING.md](docs/RELEASING.md)**: how the packages are versioned and published.
- **[CHANGELOG.md](CHANGELOG.md)**: what changed in each release.

## Layout

| Path | Purpose |
| --- | --- |
| `src/MelangeDB.Abstractions` | Attributes, identities, core interfaces. Dependency-free. |
| `src/MelangeDB.Core` | Schema model, write sets, transactions, commit log, dispatcher, appliers. |
| `src/MelangeDB.Protocol` | The wire format: frames, MessagePack codecs, reducer argument encoding. |
| `src/MelangeDB.Storage.Faster` | `IHotStore` over a hybrid log with spill-to-disk. |
| `src/MelangeDB.Storage.Postgres` | The relational backplane and the archive tier. |
| `src/MelangeDB.Server` | WebSocket transport, subscription engine, auth, the HTTP endpoints. |
| `src/MelangeDB.Client` | The C# client. |
| `src/MelangeDB.Cluster` | Hub and shard roles, placement routing, handoff, HA. |
| `src/MelangeDB.CodeGen` | The Roslyn generator: registrations, serializers, typed clients. |
| `src/MelangeDB.OpenTelemetry` | Registers MelangeDB's signal names with OpenTelemetry. |
| `src/MelangeDB.Cli` | The `melange` tool: schema export, binding generation, backup and restore. |
| `src/MelangeDB.Mcp` | The MCP endpoint for agents, in the host's own pipeline. |
| `src/MelangeDB.Testing` | The reducer test kit: a real engine on a hand-cranked clock. |
| `clients/typescript` | `@melangedb/client`, the TypeScript client. |
| `conformance/` | The recorded decode corpus every client is checked against. |
| `tools/MelangeDB.Conformance` | Records the corpus and runs the live conformance suite. |
| `tools/MelangeDB.Conformance.Adapter.CSharp` | The C# client's conformance adapter, the reference for other languages. |
| `tools/MelangeDB.LoadTest` | The load rig behind the recorded performance numbers. |
| `bench/MelangeDB.Benchmarks` | BenchmarkDotNet suites for the commit, apply and wire paths. |
| `samples/` | A worker-service host with a console client, and a wallet that uses the archive tier and producers. |
| `tests/` | The test suites. |

## Building

Requires the [.NET 10 SDK](https://dotnet.microsoft.com/download).

```
dotnet build
dotnet test
```

The Postgres and cluster suites use [Testcontainers](https://testcontainers.com) and need a working
Docker daemon. The TypeScript client builds and tests with `npm ci && npm test` in
`clients/typescript`. [CONTRIBUTING.md](CONTRIBUTING.md) has the full test story, including which
suites run only locally and why.

## Contributing

Issues and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) covers the build, the test
bar, and the documentation conventions this repo follows. Participation is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

[MIT](LICENSE) © Tyler Loy
