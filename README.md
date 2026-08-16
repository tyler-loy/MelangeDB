# MelangeDB

A C# alternative to [SpacetimeDB](https://spacetimedb.com): a store where your application logic runs
*inside* the transaction boundary, and connected clients subscribe to live query results instead of
polling an API.

> **Status: alpha, unreleased.** The engine, transport, auth, scheduling, event bus, paged storage,
> Postgres tier, clustering, and typed client bindings are implemented and tested. Nothing is at 1.0
> and the public API will break between versions. It has not yet been proven against a production
> workload — that's [phase 11](docs/ROADMAP.md), the port of a live 82-table game.

MelangeDB exists to fix three specific things:

- **Clustering isn't an afterthought.** Tables declare where they live; one writer per shard, one commit
  log per shard. And **you define what a shard means** — a contiguous world partitions by space, an MMO
  city shards into instances, and both are first-class.
- **Your dataset isn't pinned in RAM.** Stores are *projections* of the log with their own paging and
  spill-to-disk. Your working set bounds memory, not your total set.
- **Dependency injection all the way down.** MelangeDB is a NuGet package inside *your* host process,
  not a server you deploy. Reducers are DI-resolved classes, so `IConfiguration`, `ILogger<T>`, and
  `IOptionsMonitor<T>` are constructor-injected like anything else — Azure App Configuration and
  feature flags just work.

## A first look

Define a table and a reducer. The reducer is an ordinary class; its dependencies are injected.

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

Register it in any .NET host. Nothing else is wired up by hand — the generator discovers both:

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMelangeDb(melange => melange
    .UseHotStore(o => o.Path = "./data/hot")            // world state machine
    .AddPostgres(builder.Configuration                  // optional relational tier
        .GetConnectionString("Melange"))
    .AddTablesFrom(typeof(Player).Assembly)
    .AddReducersFrom(typeof(Program).Assembly));

var app = builder.Build();
app.UseWebSockets();
app.MapMelangeSocket();                                 // defaults to /melange
app.Run();
```

A client generates typed bindings from the server's exported schema, then calls the reducer and
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

await conn.Db.Player.RoomId.SubscribeAsync(7);          // SELECT * FROM player WHERE room_id = 7
var lsn = await conn.Reducers.MoveAsync(12.0f, 4.5f);
```

Column and reducer names are checked by the compiler, not at runtime — a renamed column is a build
error. See [docs/CLIENT-BINDINGS.md](docs/CLIENT-BINDINGS.md) for how the schema manifest gets there.
A runnable version of all of this lives in [`samples/`](samples).

## What works today

| Area | State |
| --- | --- |
| Transactions, write sets, commit log, crash recovery | Shipped (01) |
| Source generator, DI host integration, compile-time diagnostics | Shipped (02) |
| WebSocket transport, subscriptions with live deltas, resume-not-refetch | Shipped (03) |
| JWT identity, connect tickets, row and column policies, rate limits | Shipped (04) |
| Scheduled reducers (timers as rows), lifecycle reducers | Shipped (05) |
| Transactional event bus over the log as an outbox | Shipped (06) |
| Paged hot store on FASTER, residency tiers, snapshots, compaction | Shipped (07) |
| Postgres relational tier, ad-hoc SQL with aggregates | Shipped (08) |
| Clustering: placement, hub/shard roles, instancing | Shipped (09) |
| Clustering: spatial sharding, seamless handoff | Shipped (10) |
| Typed client bindings, schema manifest, `melange` CLI | Shipped (12) |
| Production validation against a live game | Outstanding (11) |

Deliberately out of scope: joins in subscriptions, an unreliable/UDP transport, and a sandbox for
reducer code. Each is argued in [docs/DESIGN.md](docs/DESIGN.md) rather than left as an omission.

## Docs

- **[docs/GLOSSARY.md](docs/GLOSSARY.md)** — every noun and what it means here. Start here if the vocabulary
  isn't landing; it leads with the terms that sound alike and aren't.
- **[docs/DESIGN.md](docs/DESIGN.md)** — the architecture, the trade-offs it accepts, and the open questions.
- **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** — every setting MelangeDB exposes. New config items are
  added here in the same change that introduces them.
- **[docs/CLUSTERING.md](docs/CLUSTERING.md)** — the four table placements, hub/shard node roles, and
  why *you* define the sharding function rather than MelangeDB.
- **[docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)** — the span and metric register. OpenTelemetry from the
  first commit, with no OpenTelemetry dependency in core.
- **[docs/THREAT-MODEL.md](docs/THREAT-MODEL.md)** — what a server can enforce against an untrusted client,
  and what it deliberately doesn't. (To *report* a vulnerability, see [SECURITY.md](SECURITY.md).)
- **[docs/CLIENT-BINDINGS.md](docs/CLIENT-BINDINGS.md)** — the typed-binding surface and how generation works.
- **[docs/MIGRATION.md](docs/MIGRATION.md)** — schema migration, both tiers: additive is automatic
  and loud, destructive is refused and manual; the `melange.shape` sidecar and the add-a-column
  deploy end to end.
- **[docs/BACKUP.md](docs/BACKUP.md)** — the `.mbak` archive and the `melange backup` / `restore` /
  `backup verify` verbs. An unverified backup is a hope, not a backup.
- **[docs/LOAD-TESTING.md](docs/LOAD-TESTING.md)** — the load rig, what it measures, and the recorded numbers.
- **[docs/ROADMAP.md](docs/ROADMAP.md)** — what shipped in each phase, the decisions each one settled, and
  what's left.
- **[docs/REFERENCE-WORKLOAD.md](docs/REFERENCE-WORKLOAD.md)** — the design audited against a live
  82-table SpacetimeDB game, as a reality check on scope.
- **[docs/RELEASING.md](docs/RELEASING.md)** — how the packages are versioned and published.

## Layout

| Project | Purpose |
| --- | --- |
| `src/MelangeDB.Abstractions` | Attributes, identities, core interfaces. Dependency-free. |
| `src/MelangeDB.Core` | Schema model, write sets, transactions, commit log, dispatcher, appliers. |
| `src/MelangeDB.Protocol` | The wire format: frames, MessagePack codecs, reducer argument encoding. |
| `src/MelangeDB.Storage.Faster` | `IHotStore` over a hybrid log with spill-to-disk. |
| `src/MelangeDB.Storage.Postgres` | Optional relational projection. |
| `src/MelangeDB.Server` | WebSocket transport, subscription engine, auth. |
| `src/MelangeDB.Client` | C# client. |
| `src/MelangeDB.Cluster` | Hub and shard node roles, placement routing, handoff. |
| `src/MelangeDB.CodeGen` | Roslyn generator: registrations, serializers, typed clients. |
| `src/MelangeDB.OpenTelemetry` | Optional: registers MelangeDB's signal names with OpenTelemetry. |
| `src/MelangeDB.Cli` | The `melange` dotnet tool — schema export and tooling. |
| `tools/MelangeDB.LoadTest` | The load rig behind the recorded performance numbers. |
| `samples/` | A worker-service host and a console client that talk to each other. |

## Building

Requires the [.NET 10 SDK](https://dotnet.microsoft.com/download).

```
dotnet build
dotnet test
```

The Postgres and cluster suites use [Testcontainers](https://testcontainers.com) and need a working
Docker daemon. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full test story, including which suites
run only locally and why.

## Contributing

Issues and pull requests are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for the build, the test
bar, and the documentation conventions this repo holds itself to. Participation is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

[MIT](LICENSE) © Tyler Loy
