# MelangeDB

A C# alternative to [SpacetimeDB](https://spacetimedb.com): a store where your application logic runs
*inside* the transaction boundary, and connected clients subscribe to live query results instead of
polling an API.

> **Status: design phase.** The architecture is settled and the solution is scaffolded. No
> functionality is implemented yet.

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

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMelangeDb(melange =>
{
    melange.UseHotStore(o => o.Path = "./data/hot");   // world state machine
    melange.AddPostgres(builder.Configuration          // optional relational tier
        .GetConnectionString("Melange"));
    melange.AddTablesFrom(typeof(Player).Assembly);
    melange.AddReducersFrom(typeof(Program).Assembly);
});

var app = builder.Build();
app.MapMelangeSocket("/melange");
app.Run();
```

## Docs

- **[docs/DESIGN.md](docs/DESIGN.md)** — the architecture, the trade-offs it accepts, and the open questions.
- **[docs/CLUSTERING.md](docs/CLUSTERING.md)** — the four table placements, hub/shard node roles, and
  why *you* define the sharding function rather than MelangeDB.
- **[docs/VIBE-SHAFT-COVERAGE.md](docs/VIBE-SHAFT-COVERAGE.md)** — the design audited against a live
  82-table SpacetimeDB game, as a reality check on scope.

## Layout

| Project | Purpose |
| --- | --- |
| `src/MelangeDB.Abstractions` | Attributes, identities, core interfaces. Dependency-free. |
| `src/MelangeDB.Core` | Schema model, write sets, transactions, commit log, dispatcher, appliers. |
| `src/MelangeDB.Storage.Faster` | `IHotStore` over a hybrid log with spill-to-disk. |
| `src/MelangeDB.Storage.Postgres` | Optional relational projection. |
| `src/MelangeDB.Server` | WebSocket transport, subscription engine, auth. |
| `src/MelangeDB.Client` | C# client. |
| `src/MelangeDB.CodeGen` | Roslyn generator: registrations, serializers, typed clients. |

## Building

Requires the .NET 10 SDK.

```
dotnet build
dotnet test
```

## License

MIT
